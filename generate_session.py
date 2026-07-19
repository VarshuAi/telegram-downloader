"""
Run this script ONCE on your local machine to generate a Telegram Session String.
Copy the printed string and paste it into your GitHub Repository Secret as TELEGRAM_SESSION_STRING.

Usage:
    python generate_session.py
"""
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(input("Enter your Telegram API_ID: ").strip())
API_HASH = input("Enter your Telegram API_HASH: ").strip()

async def main():
    async with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        session_string = client.session.save()
        print("\n" + "="*70)
        print("YOUR SESSION STRING (copy everything between the lines):")
        print("="*70)
        print(session_string)
        print("="*70)
        print("\nPaste this into GitHub -> Settings -> Secrets -> TELEGRAM_SESSION_STRING")

asyncio.run(main())
