from telethon.sync import TelegramClient
from telethon.sessions import StringSession

print("=== Telegram Session String Generator ===\n")
api_id   = int(input("Enter your API_ID: "))
api_hash = input("Enter your API_HASH: ").strip()

with TelegramClient(StringSession(), api_id, api_hash) as client:
    session = client.session.save()
    print("\n========== YOUR SESSION STRING ==========\n")
    print(session)
    print("\n=========================================")
    print("Copy the string above and paste it into Render as TELEGRAM_SESSION_STRING")
