import os
import tempfile
import logging

import requests
import yt_dlp
from flask import Flask, request
import telebot
import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logging.basicConfig(level=logging.INFO)

# ─── Config from Render Environment Variables ───────────────────────────────
BOT_TOKEN            = os.environ['BOT_TOKEN']
ALLOWED_USER_ID      = int(os.environ.get('ALLOWED_USER_ID', 0))  # your Telegram user ID
GDRIVE_FOLDER_ID     = os.environ['GDRIVE_FOLDER_ID']
GDRIVE_CLIENT_ID     = os.environ['GDRIVE_CLIENT_ID']
GDRIVE_CLIENT_SECRET = os.environ['GDRIVE_CLIENT_SECRET']
GDRIVE_REFRESH_TOKEN = os.environ['GDRIVE_REFRESH_TOKEN']
WEBHOOK_URL          = os.environ.get('WEBHOOK_URL', '')  # your Render app URL
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


def upload_to_gdrive(file_path, filename, status_cb=None):
    """Upload file to Google Drive and return the web view link."""
    service = get_gdrive()
    size_mb = os.path.getsize(file_path) / 1048576
    metadata = {'name': filename, 'parents': [GDRIVE_FOLDER_ID]}
    media = MediaFileUpload(file_path, chunksize=10 * 1024 * 1024, resumable=True)
    req = service.files().create(body=metadata, media_body=media, fields='id,webViewLink')

    last_pct = 0
    response = None
    while response is None:
        status, response = req.next_chunk()
        if status and status_cb:
            pct = int(status.progress() * 100)
            if pct >= last_pct + 25:
                last_pct = pct
                done_mb = size_mb * status.progress()
                status_cb(f"☁️ Uploading to GDrive... {pct}% ({done_mb:.1f}/{size_mb:.1f} MB)")

    return response.get('webViewLink', '')


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


# ─── Bot Handlers ─────────────────────────────────────────────────────────────

@bot.message_handler(commands=['start', 'help'])
def handle_start(message):
    bot.reply_to(message,
        "👋 *Personal GDrive Bot*\n\n"
        "Send me any of these and I'll save it to your Google Drive:\n\n"
        "📎 *Any file* (≤20MB) — send directly\n"
        "🎬 *YouTube link* — downloads best quality\n"
        "🔗 *Any direct download URL* — downloads the file\n\n"
        "I'll tell you the speed and progress as it goes!",
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
            upload_to_gdrive(local, filename, status_cb=lambda t: update(t))

            update(f"✅ *Done!*\n📁 `{filename}`\n💾 {size_mb:.1f} MB → saved to Google Drive")

    except Exception as e:
        update(f"❌ Failed: {str(e)[:300]}")
        logging.exception("File upload error")


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
            update(f"☁️ Uploading *{filename}* ({size_mb:.1f} MB) to GDrive...")
            upload_to_gdrive(local, filename, status_cb=lambda t: update(t))
            update(f"✅ *Done!*\n📁 `{filename}`\n💾 {size_mb:.1f} MB → saved to Google Drive")

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

if __name__ == '__main__':
    if WEBHOOK_URL:
        bot.remove_webhook()
        bot.set_webhook(url=f"{WEBHOOK_URL.rstrip('/')}/{BOT_TOKEN}")
        port = int(os.environ.get('PORT', 10000))
        app.run(host='0.0.0.0', port=port)
    else:
        # Local testing — polling mode
        logging.info("Starting in polling mode (local)...")
        bot.remove_webhook()
        bot.infinity_polling()
