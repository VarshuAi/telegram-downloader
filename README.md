# Telegram Parallel Media Downloader

A lightweight, high-performance, and secure script to download media (videos, PDFs, zip files, photos, etc.) from restricted Telegram channels where saving or forwarding is disabled.

## 🚀 Features

- **Chronological Download**: Downloads files from oldest to newest (1st lecture to last lecture).
- **Parallel Downloads**: Uses `asyncio.Semaphore` to download up to **4 files concurrently**, saturating your network bandwidth for maximum speed.
- **Smart Resuming & Deduplication**: Checks local files and automatically skips already downloaded or matching files to save time and bandwidth.
- **High-Performance Cryptography**: Leverages the `cryptg` C-extension for lightning-fast decryption speeds.
- **Robust Retry Handler**: Automatically reconnects and retries downloads if the connection drops.
- **Privacy & Safety First**: Runs fully locally. Prompts for credentials directly via the command line at runtime so your private `api_id` and `api_hash` are never saved in the code or pushed to GitHub.

---

## 🛠️ Setup Instructions

### 1. Prerequisites
Ensure you have **Python 3.8+** installed on your system.

### 2. Install Dependencies
Open your Command Prompt or Terminal and install the required Python libraries:
```bash
pip install telethon cryptg
```
> **Note:** `cryptg` is a C-extension that makes decryption up to 10x faster. If it fails to install due to a lack of C++ compilers, the script will fall back to Telethon's built-in Python-only crypt module (which still works, but is slower).

### 3. Generate Telegram API Credentials (Official & Free)
To allow the script to connect to Telegram's official API:
1. Go to [my.telegram.org](https://my.telegram.org) and log in using your phone number.
2. Enter the confirmation code sent to your **Telegram app** (not SMS).
3. Click on **API development tools**.
4. Fill in the short form (e.g., App title: `LectureDownloader`, Short name: `lecdown`).
5. Click **Create application**.
6. Copy the **App api_id** (integer) and **App api_hash** (alphanumeric string). Keep these private.

---

## 💻 How to Run

1. Open your Command Prompt/Terminal.
2. Navigate to the folder containing the script and run it:
   ```bash
   python download_telegram.py
   ```
3. Enter the requested details when prompted:
   - **Telegram App API_ID**
   - **Telegram App API_HASH**
   - **Channel Link or Username** (e.g., `StriverDSA` or `https://t.me/StriverDSA`)
4. On your first launch, the script will request:
   - Your **Phone Number** (with country code, e.g. `+91...`).
   - The login verification code sent to your Telegram app.
   - Your 2FA password (if enabled on your account).

All files will be saved in a folder named `telegram_lectures` inside the same directory.

---

## 🔒 Safety & Rate Limits
- **Account Safety**: This tool runs fully on your computer and authenticates directly with Telegram's servers. No credentials, tokens, or sessions are shared with third parties.
- **Rate Limits**: The parallel workers are capped at a safe connection count of 4. This avoids hitting aggressive rate limits. However, if you are downloading massive amounts of files (50+ GB), Telegram might temporarily throttle you (`FloodWait`). The script handles this automatically by waiting and resuming when the cooldown ends.
