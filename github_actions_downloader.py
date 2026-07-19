import os
import time
import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ----------------- CONFIGURATION (from GitHub Secrets) -----------------
API_ID               = int(os.environ.get('TELEGRAM_API_ID', 0))
API_HASH             = os.environ.get('TELEGRAM_API_HASH', '')
SESSION_STRING       = os.environ.get('TELEGRAM_SESSION_STRING', '')
CHANNEL_SOURCE       = os.environ.get('TELEGRAM_CHANNEL_SOURCE', '')
GDRIVE_FOLDER_ID     = os.environ.get('GDRIVE_FOLDER_ID', '')
GDRIVE_CLIENT_ID     = os.environ.get('GDRIVE_CLIENT_ID', '')
GDRIVE_CLIENT_SECRET = os.environ.get('GDRIVE_CLIENT_SECRET', '')
GDRIVE_REFRESH_TOKEN = os.environ.get('GDRIVE_REFRESH_TOKEN', '')

# Using standard single-connection download — no ExportAuthorizationRequest spam
# GitHub's network is fast enough (30-70 MB/s) without parallel connections
# Sequential download — one at a time, no race conditions, no skipped files
MAX_CONCURRENT_FILES = 1
TEMP_DOWNLOAD_DIR    = 'temp_downloads'
# -----------------------------------------------------------------------


# ====================== PROGRESS TRACKER ======================

def make_progress(filename, file_size, arrow='↓'):
    """Prints download/upload speed + ETA every 10%."""
    short = (filename[:30] + '...') if len(filename) > 33 else filename
    state = {
        'last_pct': -1,
        'start': time.time(),
        'last_t': time.time(),
        'last_b': 0,
    }

    def cb(received, total):
        total = total or file_size or 1
        pct = int(received / total * 100)
        if pct >= state['last_pct'] + 10 or received >= total:
            state['last_pct'] = pct
            now = time.time()
            dt = now - state['last_t']
            elapsed = now - state['start']
            inst_speed = (received - state['last_b']) / dt if dt > 0 else 0
            avg_speed  = received / elapsed if elapsed > 0 else 0
            remaining  = (total - received) / avg_speed if avg_speed > 0 else 0
            eta = f"{int(remaining//60)}m {int(remaining%60)}s" if remaining > 60 else f"{int(remaining)}s"
            print(
                f"  {arrow} [{short}] {pct:3d}%"
                f" | {received/1048576:.1f}/{total/1048576:.1f} MB"
                f" | {inst_speed/1048576:.2f} MB/s (inst)"
                f" | {avg_speed/1048576:.2f} MB/s (avg)"
                f" | ETA: {eta}"
            )
            state['last_t'] = now
            state['last_b'] = received

    return cb


# ====================== GOOGLE DRIVE ======================

gdrive_service = None
existing_gdrive_files = {}

def init_gdrive():
    global gdrive_service, existing_gdrive_files

    missing = [k for k, v in {
        'GDRIVE_CLIENT_ID':     GDRIVE_CLIENT_ID,
        'GDRIVE_CLIENT_SECRET': GDRIVE_CLIENT_SECRET,
        'GDRIVE_REFRESH_TOKEN': GDRIVE_REFRESH_TOKEN,
        'GDRIVE_FOLDER_ID':     GDRIVE_FOLDER_ID,
    }.items() if not v]

    if missing:
        print(f"[GDrive] Missing secrets: {', '.join(missing)}")
        return False

    try:
        creds = google.oauth2.credentials.Credentials(
            token=None,
            refresh_token=GDRIVE_REFRESH_TOKEN,
            client_id=GDRIVE_CLIENT_ID,
            client_secret=GDRIVE_CLIENT_SECRET,
            token_uri='https://oauth2.googleapis.com/token'
        )
        gdrive_service = build('drive', 'v3', credentials=creds)

        print("[GDrive] Scanning existing files in target folder...")
        page_token = None
        while True:
            resp = gdrive_service.files().list(
                q=f"'{GDRIVE_FOLDER_ID}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, size)",
                pageSize=1000,
                pageToken=page_token
            ).execute()
            for f in resp.get('files', []):
                existing_gdrive_files[f['name']] = int(f.get('size', 0))
            page_token = resp.get('nextPageToken')
            if not page_token:
                break

        print(f"[GDrive] Found {len(existing_gdrive_files)} existing files — will skip those.")
        return True
    except Exception as e:
        print(f"[GDrive] Failed to initialize: {e}")
        return False


def upload_to_gdrive(file_path, filename):
    if not gdrive_service:
        return False
    try:
        file_size_bytes = os.path.getsize(file_path)
        size_mb = file_size_bytes / 1048576

        file_metadata = {'name': filename, 'parents': [GDRIVE_FOLDER_ID]}
        media = MediaFileUpload(file_path, chunksize=10 * 1024 * 1024, resumable=True)

        print(f"[GDrive] ↑ Uploading: {filename} ({size_mb:.1f} MB)")
        request = gdrive_service.files().create(body=file_metadata, media_body=media, fields='id')

        t_start = time.time()
        last_pct = 0
        short = (filename[:30] + '...') if len(filename) > 33 else filename
        response = None

        while response is None:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                if pct >= last_pct + 20:
                    last_pct = pct
                    elapsed = time.time() - t_start
                    uploaded = status.resumable_progress
                    speed = uploaded / elapsed if elapsed > 0 else 0
                    remaining = (file_size_bytes - uploaded) / speed if speed > 0 else 0
                    eta = f"{int(remaining//60)}m {int(remaining%60)}s" if remaining > 60 else f"{int(remaining)}s"
                    print(
                        f"  ↑ [{short}] {pct:3d}%"
                        f" | {uploaded/1048576:.1f}/{size_mb:.1f} MB"
                        f" | {speed/1048576:.2f} MB/s"
                        f" | ETA: {eta}"
                    )

        total_time = time.time() - t_start
        avg_up = file_size_bytes / total_time / 1048576 if total_time > 0 else 0
        print(f"[GDrive] ✓ Uploaded: {filename} in {total_time:.1f}s @ avg {avg_up:.2f} MB/s")
        return True
    except Exception as e:
        print(f"[GDrive] Upload failed for {filename}: {e}")
        return False


# ====================== MAIN PIPELINE ======================

async def run_pipeline():
    if not SESSION_STRING:
        print("Error: TELEGRAM_SESSION_STRING is missing.")
        return

    if not init_gdrive():
        print("Error: Google Drive setup failed. Exiting.")
        return

    # Use standard single-connection client — no parallel DC connections = no FloodWait
    client = TelegramClient(
        StringSession(SESSION_STRING), API_ID, API_HASH,
        connection_retries=20,
        retry_delay=5,
    )
    await client.start()
    print("[Telegram] Connected successfully!")

    try:
        channel = await client.get_entity(CHANNEL_SOURCE)
        print(f"[Telegram] Channel: {channel.title}")
        os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

        # --- Scan channel for all media ---
        print("[Telegram] Scanning channel messages...")
        media_messages = []
        async for message in client.iter_messages(channel, reverse=True):
            if not message.media:
                continue
            filename = None
            if message.file:
                filename = message.file.name
            if not filename and hasattr(message.media, 'document') and message.media.document:
                for attr in message.media.document.attributes:
                    if hasattr(attr, 'file_name'):
                        filename = attr.file_name
                        break
            if not filename:
                ext = (message.file.ext if message.file and message.file.ext else '.bin')
                filename = f"media_msg_{message.id}{ext}"
            media_messages.append((message, filename))

        total_media = len(media_messages)
        pad = len(str(total_media)) if total_media > 0 else 3

        # --- Filter out already-uploaded files ---
        to_download = []
        for idx, (message, filename) in enumerate(media_messages, start=1):
            file_size = None
            if message.file:
                file_size = message.file.size
            if file_size is None and hasattr(message.media, 'document') and message.media.document:
                file_size = message.media.document.size

            prefixed = f"{idx:0{pad}d}_{filename}"

            if prefixed in existing_gdrive_files:
                if file_size is None or existing_gdrive_files[prefixed] == file_size:
                    continue  # already uploaded, skip

            to_download.append((message, prefixed, file_size))

        total = len(to_download)
        if total == 0:
            print("All files already exist in Google Drive. Nothing to do!")
            return

        print(f"Found {total} new files to transfer to Google Drive...")

        sem = asyncio.Semaphore(MAX_CONCURRENT_FILES)
        done_count = 0
        failed_files = []

        async def process(message, filename, file_size):
            nonlocal done_count
            async with sem:
                local_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)
                size_mb = (file_size or 0) / 1048576

                # --- Download from Telegram (standard, no parallel DC tricks) ---
                downloaded = False
                for attempt in range(1, 6):
                    try:
                        print(f"[Telegram] Downloading: {filename} ({size_mb:.1f} MB)  [attempt {attempt}]")
                        cb = make_progress(filename, file_size, '↓')
                        await message.download_media(file=local_path, progress_callback=cb)
                        downloaded = True
                        print(f"[Telegram] ✓ Downloaded: {filename}")
                        break
                    except FloodWaitError as fw:
                        if os.path.exists(local_path):
                            os.remove(local_path)
                        wait = fw.seconds
                        print(f"[Telegram] FloodWait {wait}s for {filename}. Waiting...")
                        await asyncio.sleep(wait + 5)
                    except Exception as e:
                        if os.path.exists(local_path):
                            os.remove(local_path)
                        print(f"[Telegram] Attempt {attempt} failed for {filename}: {e}")
                        await asyncio.sleep(15)

                if not downloaded:
                    print(f"[Error] Could not download {filename} after 5 attempts — will retry next run.")
                    failed_files.append(filename)
                    return

                # --- Upload to Google Drive ---
                success = upload_to_gdrive(local_path, filename)

                # --- Clean up temp file immediately ---
                if os.path.exists(local_path):
                    os.remove(local_path)

                if success:
                    done_count += 1
                    print(f"[Done] {filename} ({done_count}/{total})")
                else:
                    failed_files.append(filename)

        tasks = [process(msg, fname, fsize) for msg, fname, fsize in to_download]
        await asyncio.gather(*tasks)

        print(f"\nPipeline complete! Transferred {done_count}/{total} files to Google Drive.")
        
        # ── Report any failures ──────────────────────────────────────────────
        if failed_files:
            print(f"\n  ⚠️  {len(failed_files)} files FAILED (will retry next run):")
            for f in failed_files:
                print(f"    - {f}")

        # ── Detect gaps in GDrive numbering ─────────────────────────────────
        print("\n[GDrive] Checking for numbered gaps...")
        page_token = None
        all_names = []
        while True:
            resp = gdrive_service.files().list(
                q=f"'{GDRIVE_FOLDER_ID}' in parents and trashed = false",
                fields="nextPageToken, files(name)",
                pageSize=1000,
                pageToken=page_token
            ).execute()
            all_names += [f['name'] for f in resp.get('files', [])]
            page_token = resp.get('nextPageToken')
            if not page_token:
                break

        # Extract the numeric prefix from each filename (e.g. "045_video.mp4" → 45)
        present_nums = set()
        for name in all_names:
            parts = name.split('_', 1)
            if parts[0].isdigit():
                present_nums.add(int(parts[0]))

        if present_nums:
            expected = set(range(1, max(present_nums) + 1))
            missing = sorted(expected - present_nums)
            if missing:
                print(f"  ⚠️  Missing file numbers in GDrive: {missing}")
                print("  These will be re-downloaded on the next run.")
            else:
                print("  ✅ No gaps — all files present!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()
        await asyncio.sleep(0.5)
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore', category=ResourceWarning)
    asyncio.run(run_pipeline())
