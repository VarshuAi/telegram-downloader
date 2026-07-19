"""
channel_mirror.py
-----------------
Downloads files from a source Telegram channel and re-uploads them
to your private backup channel — entirely within GitHub Actions.

Flow: Source Channel → GitHub Actions Server → Private Channel

No GDrive, no disk quota issues. Files stay on Telegram forever.
"""

import os
import asyncio
import time
import json

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeFilename

# ─── Config ───────────────────────────────────────────────────────────────────
API_ID          = int(os.environ.get('TELEGRAM_API_ID', 0))
API_HASH        = os.environ.get('TELEGRAM_API_HASH', '')
SESSION_STRING  = os.environ.get('TELEGRAM_SESSION_STRING', '')

def parse_channel(val):
    if not val:
        return val
    val_str = str(val).strip()
    if val_str.startswith('-') and val_str[1:].isdigit():
        return int(val_str)
    if val_str.isdigit():
        return int(val_str)
    return val_str

SOURCE_CHANNEL  = parse_channel(os.environ.get('TELEGRAM_CHANNEL_SOURCE', ''))   # source (e.g. @StriverDSA)
DEST_CHANNEL    = parse_channel(os.environ.get('TELEGRAM_BACKUP_CHANNEL', ''))   # your private channel
PROGRESS_FILE   = 'mirrored_ids.json'   # tracks which message IDs are already mirrored
TEMP_DIR        = 'temp_mirror'
# ──────────────────────────────────────────────────────────────────────────────


def load_mirrored() -> set:
    """Load already-mirrored message IDs from the progress file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return set(json.load(f))
    return set()


def save_mirrored(ids: set):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(sorted(ids), f)


def make_progress(label, total):
    state = {'last_pct': -1, 'start': time.time(), 'last_t': time.time(), 'last_b': 0}

    def cb(received, tot):
        tot = tot or total or 1
        pct = int(received / tot * 100)
        if pct >= state['last_pct'] + 10 or received >= tot:
            state['last_pct'] = pct
            now = time.time()
            dt = now - state['last_t']
            el = now - state['start']
            inst = (received - state['last_b']) / dt / 1048576 if dt > 0 else 0
            avg  = received / el / 1048576 if el > 0 else 0
            rem  = (tot - received) / (avg * 1048576) if avg > 0 else 0
            eta  = f"{int(rem//60)}m {int(rem%60)}s" if rem > 60 else f"{int(rem)}s"
            print(f"  {label} {pct:3d}% | {received/1048576:.1f}/{tot/1048576:.1f} MB"
                  f" | {inst:.1f} MB/s | avg {avg:.1f} MB/s | ETA {eta}")
            state['last_t'] = now
            state['last_b'] = received
    return cb


async def mirror():
    if not all([API_ID, API_HASH, SESSION_STRING, SOURCE_CHANNEL, DEST_CHANNEL]):
        print("ERROR: Missing required environment variables.")
        print(f"  API_ID={API_ID}, SOURCE={SOURCE_CHANNEL}, DEST={DEST_CHANNEL}")
        return

    os.makedirs(TEMP_DIR, exist_ok=True)
    mirrored = load_mirrored()

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH,
                            connection_retries=20, retry_delay=5)
    await client.start()
    print("[TG] Connected!")

    src  = await client.get_entity(SOURCE_CHANNEL)
    dest = await client.get_entity(DEST_CHANNEL)
    print(f"[TG] Source : {src.title}")
    print(f"[TG] Dest   : {dest.title}")

    # ── Scan source channel ──────────────────────────────────────────────────
    print("[TG] Scanning source channel for media messages...")
    media_msgs = []
    scanned = 0
    async for msg in client.iter_messages(src, reverse=True):
        scanned += 1
        if scanned % 100 == 0:
            print(f"  Scanned {scanned} messages... (found {len(media_msgs)} new media so far)")
        if msg.media and msg.id not in mirrored:
            media_msgs.append(msg)

    total = len(media_msgs)
    if total == 0:
        print("[TG] Nothing new to mirror. All done!")
        await client.disconnect()
        return

    print(f"[TG] Found {total} new files to mirror.\n")

    # ── Mirror one by one ────────────────────────────────────────────────────
    done = 0
    failed = []

    for msg in media_msgs:
        # --- Get filename ---
        filename = None
        if msg.file:
            filename = msg.file.name
        if not filename and msg.document:
            for attr in msg.document.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    filename = attr.file_name
                    break
        if not filename:
            ext = msg.file.ext if msg.file else '.bin'
            filename = f"msg_{msg.id}{ext}"

        file_size = msg.file.size if msg.file else 0
        size_mb = file_size / 1048576

        local_path = os.path.join(TEMP_DIR, filename)
        print(f"\n[{done+1}/{total}] {filename} ({size_mb:.1f} MB)")

        # --- Download from source ---
        try:
            dl_cb = make_progress("↓", file_size)
            await msg.download_media(file=local_path, progress_callback=dl_cb)
            print(f"  ✓ Downloaded")
        except Exception as e:
            print(f"  ✗ Download failed: {e}")
            failed.append(msg.id)
            if os.path.exists(local_path):
                os.remove(local_path)
            continue

        # --- Upload to private channel ---
        try:
            ul_cb = make_progress("↑", file_size)
            caption = f"📎 {filename}"
            if msg.message:
                caption += f"\n{msg.message}"

            await client.send_file(
                dest,
                local_path,
                caption=caption,
                force_document=True,        # preserve original quality, no recompression
                supports_streaming=True,    # still streamable in Telegram apps
                progress_callback=ul_cb,
            )
            print(f"  ✓ Uploaded to private channel")

            mirrored.add(msg.id)
            save_mirrored(mirrored)
            done += 1

        except Exception as e:
            print(f"  ✗ Upload failed: {e}")
            failed.append(msg.id)
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"Mirrored: {done}/{total}")
    if failed:
        print(f"Failed:   {len(failed)} files — will retry next run")
    print(f"{'='*50}")

    await client.disconnect()
    # Clean up pending tasks
    await asyncio.sleep(0.5)
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in pending:
        t.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore', category=ResourceWarning)
    asyncio.run(mirror())
