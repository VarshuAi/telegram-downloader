import os
import re
import json
import asyncio
import tempfile
import logging

import requests
import yt_dlp
from flask import Flask, request
import telebot
from telethon import TelegramClient
from telethon.sessions import StringSession
import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logging.basicConfig(level=logging.INFO)

# ─── Config from Render Environment Variables ───────────────────────────────
BOT_TOKEN            = os.environ['BOT_TOKEN']
ALLOWED_USER_ID      = int(os.environ.get('ALLOWED_USER_ID', 0))
GDRIVE_FOLDER_ID     = os.environ['GDRIVE_FOLDER_ID']   # default folder
GDRIVE_CLIENT_ID     = os.environ['GDRIVE_CLIENT_ID']
GDRIVE_CLIENT_SECRET = os.environ['GDRIVE_CLIENT_SECRET']
GDRIVE_REFRESH_TOKEN = os.environ['GDRIVE_REFRESH_TOKEN']
WEBHOOK_URL          = os.environ.get('WEBHOOK_URL', '')

# Telegram USER API (for downloading from t.me links)
TG_API_ID       = int(os.environ.get('TELEGRAM_API_ID', 0))
TG_API_HASH     = os.environ.get('TELEGRAM_API_HASH', '')
TG_SESSION      = os.environ.get('TELEGRAM_SESSION_STRING', '')

# GDRIVE_FOLDERS env var: JSON like {"lectures":"id1","projects":"id2"}
# The key "default" always maps to GDRIVE_FOLDER_ID
_extra = os.environ.get('GDRIVE_FOLDERS', '{}')
try:
    FOLDER_MAP = json.loads(_extra)
except Exception:
    FOLDER_MAP = {}
FOLDER_MAP['default'] = GDRIVE_FOLDER_ID   # always available

# Per-user active folder: { user_id: folder_id }
active_folder: dict[int, str] = {}

def get_active_folder(user_id: int) -> str:
    return active_folder.get(user_id, GDRIVE_FOLDER_ID)

# ─────────────────────────────────────────────────────────────────────────────

bot = telebot.TeleBot(BOT_TOKEN, threaded=False)
app = Flask(__name__)


# ─── Google Drive helpers ────────────────────────────────────────────────────

def get_gdrive():
    creds = google.oauth2.credentials.Credentials(
        token=None,
        refresh_token=GDRIVE_REFRESH_TOKEN,
        client_id=GDRIVE_CLIENT_ID,
        client_secret=GDRIVE_CLIENT_SECRET,
        token_uri='https://oauth2.googleapis.com/token',
    )
    return build('drive', 'v3', credentials=creds)


def upload_to_gdrive(file_path, filename, folder_id=None, status_cb=None):
    """Upload file to Google Drive and return the web view link."""
    service = get_gdrive()
    folder_id = folder_id or GDRIVE_FOLDER_ID
    size_mb = os.path.getsize(file_path) / 1048576
    metadata = {'name': filename, 'parents': [folder_id]}
    # 50MB chunks = faster upload on good connections
    media = MediaFileUpload(file_path, chunksize=50 * 1024 * 1024, resumable=True)
    req = service.files().create(body=metadata, media_body=media, fields='id,webViewLink')

    last_pct = 0
    t_start = time.time()
    response = None
    while response is None:
        status, response = req.next_chunk()
        if status and status_cb:
            pct = int(status.progress() * 100)
            if pct >= last_pct + 20:
                last_pct = pct
                done_mb = size_mb * status.progress()
                elapsed = time.time() - t_start
                speed = (done_mb * 1048576) / elapsed / 1048576 if elapsed > 0 else 0
                status_cb(f"☁️ Uploading... {pct}% | {done_mb:.1f}/{size_mb:.1f} MB | {speed:.1f} MB/s")

    link = response.get('webViewLink', '')
    file_id = response.get('id', '')
    # Make it directly openable (not just viewable)
    if file_id:
        service.permissions().create(
            fileId=file_id,
            body={'type': 'anyone', 'role': 'reader'},
        ).execute()
        link = f"https://drive.google.com/file/d/{file_id}/view"
    return link


# ─── Utility ─────────────────────────────────────────────────────────────────

def is_allowed(message):
    if ALLOWED_USER_ID and message.from_user.id != ALLOWED_USER_ID:
        bot.reply_to(message, "❌ Unauthorized. This is a private bot.")
        return False
    return True

def is_url(text):
    return text.strip().startswith('http://') or text.strip().startswith('https://')

def is_youtube(url):
    return 'youtube.com' in url or 'youtu.be' in url

def is_telegram_link(url):
    return 't.me/' in url or 'telegram.me/' in url

def parse_tg_link(url):
    """
    Parse a t.me link and return (channel, message_id).
    Supports:
      https://t.me/username/123
      https://t.me/c/1234567890/123  (private channel)
    """
    # Private channel: t.me/c/CHANNEL_ID/MSG_ID
    m = re.search(r't\.me/c/(-?\d+)/(\d+)', url)
    if m:
        return int('-100' + m.group(1)), int(m.group(2))
    # Public channel: t.me/username/MSG_ID
    m = re.search(r't\.me/([^/]+)/(\d+)', url)
    if m:
        return m.group(1), int(m.group(2))
    return None, None

async def _download_tg_link(url, tmp_dir, progress_cb=None):
    """Use Telethon (user account) to download media from a t.me link."""
    if not TG_API_ID or not TG_API_HASH or not TG_SESSION:
        raise ValueError("TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_SESSION_STRING not set in env vars.")

    channel, msg_id = parse_tg_link(url)
    if not channel or not msg_id:
        raise ValueError("Could not parse Telegram link. Use format: https://t.me/channelname/123")

    client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)
    await client.connect()
    try:
        message = await client.get_messages(channel, ids=msg_id)
        if not message or not message.media:
            raise ValueError("No media found in that Telegram message.")

        filename = None
        if message.file:
            filename = message.file.name
        if not filename:
            ext = message.file.ext if message.file else '.bin'
            filename = f"tg_{msg_id}{ext}"

        local_path = os.path.join(tmp_dir, filename)

        def _cb(received, total):
            if progress_cb and total:
                pct = int(received / total * 100)
                progress_cb(received, total, pct)

        await message.download_media(file=local_path, progress_callback=_cb)
        return local_path, filename
    finally:
        await client.disconnect()



# ─── Folder Commands ──────────────────────────────────────────────────────────

@bot.message_handler(commands=['folders'])
def handle_folders(message):
    if not is_allowed(message): return
    uid = message.from_user.id
    current_id = get_active_folder(uid)
    current_name = next((k for k, v in FOLDER_MAP.items() if v == current_id), current_id)
    lines = [f"📂 *Available folders:*\n"]
    for name, fid in FOLDER_MAP.items():
        tick = "✅" if fid == current_id else "  "
        lines.append(f"{tick} `{name}`")
    lines.append(f"\n🟢 *Active:* `{current_name}`")
    lines.append("\n💡 Switch with: `/folder name`")
    bot.reply_to(message, '\n'.join(lines), parse_mode='Markdown')


@bot.message_handler(commands=['folder'])
def handle_set_folder(message):
    if not is_allowed(message): return
    uid = message.from_user.id
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        current_id = get_active_folder(uid)
        current_name = next((k for k, v in FOLDER_MAP.items() if v == current_id), current_id)
        bot.reply_to(message,
            f"🟢 Current folder: *{current_name}*\n"
            f"Usage: `/folder name`\n"
            f"List folders: `/folders`",
            parse_mode='Markdown'
        )
        return

    name = parts[1].strip().lower()
    if name in FOLDER_MAP:
        active_folder[uid] = FOLDER_MAP[name]
        bot.reply_to(message, f"✅ Switched to folder: *{name}*\nAll uploads will now go there.", parse_mode='Markdown')
    else:
        names = ', '.join(f'`{k}`' for k in FOLDER_MAP)
        bot.reply_to(message,
            f"❌ Folder `{name}` not found.\n\nAvailable: {names}\n\nAdd one with:\n`/addfolder name folder_id`",
            parse_mode='Markdown'
        )


@bot.message_handler(commands=['addfolder'])
def handle_add_folder(message):
    if not is_allowed(message): return
    parts = message.text.strip().split()
    if len(parts) != 3:
        bot.reply_to(message,
            "Usage: `/addfolder name folder_id`\n\n"
            "Example:\n`/addfolder projects 1A2B3C4D5E6F7G8H`\n\n"
            "Get folder ID from GDrive URL:\n"
            "`drive.google.com/drive/folders/`*THIS_PART*",
            parse_mode='Markdown'
        )
        return
    _, name, folder_id = parts
    name = name.lower()
    FOLDER_MAP[name] = folder_id
    uid = message.from_user.id
    active_folder[uid] = folder_id
    bot.reply_to(message,
        f"✅ Added folder *{name}* and switched to it!\n"
        f"Note: This resets on bot restart. To make it permanent, add `GDRIVE_FOLDERS` env var in Render.",
        parse_mode='Markdown'
    )


# ─── Bot Handlers ─────────────────────────────────────────────────────────────

@bot.message_handler(commands=['start', 'help'])
def handle_start(message):
    bot.reply_to(message,
        "👋 *Personal GDrive Bot*\n\n"
        "Send me any of these → saved to Google Drive:\n\n"
        "📎 *Any file* (≤20MB) — send directly\n"
        "🎬 *YouTube link* — downloads best quality\n"
        "🔗 *Any direct download URL*\n\n"
        "📂 *Folder commands:*\n"
        "`/folder name` — switch active folder\n"
        "`/folders` — list all folders\n"
        "`/addfolder name id` — add a new folder\n",
        parse_mode='Markdown'
    )


@bot.message_handler(content_types=['document', 'video', 'audio', 'voice', 'animation'])
def handle_file(message):
    if not is_allowed(message):
        return

    # Get file metadata
    f = (message.document or message.video or message.audio
         or message.voice or message.animation)
    file_id   = f.file_id
    file_size = getattr(f, 'file_size', 0) or 0
    size_mb   = file_size / 1048576

    if hasattr(f, 'file_name') and f.file_name:
        filename = f.file_name
    elif message.video:
        filename = f"video_{f.file_id}.mp4"
    elif message.audio:
        filename = f"audio_{f.file_id}.mp3"
    else:
        filename = f"file_{f.file_id}"

    # Telegram Bot API hard limit: 20MB download
    if size_mb > 20:
        bot.reply_to(message,
            f"❌ File is {size_mb:.1f} MB — Telegram Bot API only allows downloading up to 20MB directly.\n\n"
            "💡 *Tip:* Upload the file to any host and send me the direct download link instead!",
            parse_mode='Markdown'
        )
        return

    status_msg = bot.reply_to(message, f"⬇️ Downloading *{filename}* ({size_mb:.1f} MB)...", parse_mode='Markdown')

    def update(text):
        try:
            bot.edit_message_text(text, message.chat.id, status_msg.message_id, parse_mode='Markdown')
        except Exception:
            pass

    try:
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, filename)
            file_info = bot.get_file(file_id)
            data = bot.download_file(file_info.file_path)
            with open(local, 'wb') as out:
                out.write(data)

            update(f"☁️ Uploading *{filename}* to GDrive...")
            folder_id = get_active_folder(message.from_user.id)
            folder_name = next((k for k, v in FOLDER_MAP.items() if v == folder_id), folder_id)
            link = upload_to_gdrive(local, filename, folder_id=folder_id, status_cb=lambda t: update(t))
            update(
                f"✅ *Done!*\n"
                f"📁 `{filename}`\n"
                f"💾 {size_mb:.1f} MB\n"
                f"📂 Folder: *{folder_name}*\n"
                f"🔗 [Open in Drive]({link})"
            )

    except Exception as e:
        update(f"❌ Failed: {str(e)[:300]}")
        logging.exception("File upload error")


@bot.message_handler(func=lambda m: m.text and is_url(m.text) and is_telegram_link(m.text))
def handle_telegram_link(message):
    if not is_allowed(message): return
    url = message.text.strip()
    status_msg = bot.reply_to(message, "🔍 Fetching from Telegram...")

    def update(text):
        try:
            bot.edit_message_text(text, message.chat.id, status_msg.message_id, parse_mode='Markdown')
        except Exception:
            pass

    try:
        with tempfile.TemporaryDirectory() as tmp:
            last_pct = [-1]

            def progress(received, total, pct):
                if pct >= last_pct[0] + 10:
                    last_pct[0] = pct
                    update(
                        f"⬇️ Downloading from Telegram...\n"
                        f"`{pct}%` — {received/1048576:.1f}/{total/1048576:.1f} MB"
                    )

            local, filename = asyncio.run(_download_tg_link(url, tmp, progress_cb=progress))
            size_mb = os.path.getsize(local) / 1048576

            folder_id = get_active_folder(message.from_user.id)
            folder_name = next((k for k, v in FOLDER_MAP.items() if v == folder_id), folder_id)
            update(f"☁️ Uploading *{filename}* ({size_mb:.1f} MB) to GDrive...")
            link = upload_to_gdrive(local, filename, folder_id=folder_id, status_cb=lambda t: update(t))
            update(
                f"✅ *Done!*\n"
                f"📁 `{filename}`\n"
                f"💾 {size_mb:.1f} MB\n"
                f"📂 Folder: *{folder_name}*\n"
                f"🔗 [Open in Drive]({link})"
            )

    except Exception as e:
        update(f"❌ Failed: {str(e)[:300]}")
        logging.exception("Telegram link error")


@bot.message_handler(func=lambda m: m.text and is_url(m.text))
def handle_url(message):
    if not is_allowed(message):
        return

    url = message.text.strip()
    status_msg = bot.reply_to(message, "🔍 Processing link...")

    def update(text):
        try:
            bot.edit_message_text(text, message.chat.id, status_msg.message_id, parse_mode='Markdown')
        except Exception:
            pass

    try:
        with tempfile.TemporaryDirectory() as tmp:

            if is_youtube(url):
                # ── YouTube download ──────────────────────────────────────
                update("⬇️ Fetching YouTube video info...")
                ydl_opts = {
                    'outtmpl': os.path.join(tmp, '%(title)s.%(ext)s'),
                    'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                    'merge_output_format': 'mp4',
                    'quiet': True,
                    'progress_hooks': [],
                }
                downloaded_files = []
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    title = info.get('title', 'video')
                    duration = info.get('duration', 0)
                    update(f"⬇️ Downloading *{title}* ({duration//60}m {duration%60}s)...")
                    ydl.download([url])

                # Find the downloaded file
                files = [os.path.join(tmp, f) for f in os.listdir(tmp)]
                if not files:
                    update("❌ Download failed — no file found.")
                    return
                local = max(files, key=os.path.getsize)
                filename = os.path.basename(local)

            else:
                # ── Direct URL download ───────────────────────────────────
                update("⬇️ Downloading file from URL...")
                filename = url.split('/')[-1].split('?')[0] or 'downloaded_file'
                local = os.path.join(tmp, filename)
                r = requests.get(url, stream=True, timeout=600, headers={'User-Agent': 'Mozilla/5.0'})
                r.raise_for_status()
                total = int(r.headers.get('content-length', 0))
                done = 0
                with open(local, 'wb') as out:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        out.write(chunk)
                        done += len(chunk)
                        if total:
                            pct = int(done / total * 100)
                            if pct % 25 == 0:
                                update(f"⬇️ Downloading... {pct}% ({done/1048576:.1f}/{total/1048576:.1f} MB)")

            size_mb = os.path.getsize(local) / 1048576
            folder_id = get_active_folder(message.from_user.id)
            folder_name = next((k for k, v in FOLDER_MAP.items() if v == folder_id), folder_id)
            update(f"☁️ Uploading *{filename}* ({size_mb:.1f} MB) to GDrive...")
            link = upload_to_gdrive(local, filename, folder_id=folder_id, status_cb=lambda t: update(t))
            update(
                f"✅ *Done!*\n"
                f"📁 `{filename}`\n"
                f"💾 {size_mb:.1f} MB\n"
                f"📂 Folder: *{folder_name}*\n"
                f"🔗 [Open in Drive]({link})"
            )

    except Exception as e:
        update(f"❌ Failed: {str(e)[:300]}")
        logging.exception("URL download error")


@bot.message_handler(func=lambda m: True)
def handle_unknown(message):
    if not is_allowed(message):
        return
    bot.reply_to(message,
        "💡 Send me a file (≤20MB), a YouTube link, or any direct download URL.",
        parse_mode='Markdown'
    )


# ─── Flask Webhook ─────────────────────────────────────────────────────────────

@app.route(f'/{BOT_TOKEN}', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('UTF-8'))
    bot.process_new_updates([update])
    return 'ok', 200


@app.route('/')
def health():
    return '✅ GDrive Bot is running!', 200


# ─── Start ─────────────────────────────────────────────────────────────────────

# Set webhook at module level so it works with gunicorn too
if WEBHOOK_URL:
    try:
        bot.remove_webhook()
        webhook_full = f"{WEBHOOK_URL.rstrip('/')}/{BOT_TOKEN}"
        bot.set_webhook(url=webhook_full)
        logging.info(f"Webhook set to: {webhook_full}")
    except Exception as e:
        logging.error(f"Failed to set webhook: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    if WEBHOOK_URL:
        app.run(host='0.0.0.0', port=port)
    else:
        # Local testing — polling mode
        logging.info("Starting in polling mode (local)...")
        bot.remove_webhook()
        bot.infinity_polling()
