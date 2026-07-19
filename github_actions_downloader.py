import os
import sys
import time
import math
import inspect
import asyncio
from typing import Optional, List, AsyncGenerator, Union, Awaitable, BinaryIO

from telethon import TelegramClient, utils
from telethon.sessions import StringSession
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import (InputDocumentFileLocation, InputPhotoFileLocation,
                                InputPeerPhotoFileLocation, InputFileLocation)

import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ----------------- CONFIGURATION (from GitHub Secrets) -----------------
API_ID                  = int(os.environ.get('TELEGRAM_API_ID', 0))
API_HASH                = os.environ.get('TELEGRAM_API_HASH', '')
SESSION_STRING          = os.environ.get('TELEGRAM_SESSION_STRING', '')
CHANNEL_SOURCE          = os.environ.get('TELEGRAM_CHANNEL_SOURCE', '')
GDRIVE_FOLDER_ID        = os.environ.get('GDRIVE_FOLDER_ID', '')
GDRIVE_CLIENT_ID        = os.environ.get('GDRIVE_CLIENT_ID', '')
GDRIVE_CLIENT_SECRET    = os.environ.get('GDRIVE_CLIENT_SECRET', '')
GDRIVE_REFRESH_TOKEN    = os.environ.get('GDRIVE_REFRESH_TOKEN', '')

MAX_CONCURRENT_FILES    = 4   # GitHub Actions has fast network, 4 concurrent is safe
CONNECTIONS_PER_FILE    = 4
CHUNK_SIZE_KB           = 512
TEMP_DOWNLOAD_DIR       = 'temp_downloads'
# -----------------------------------------------------------------------

TypeLocation = Union[InputDocumentFileLocation, InputPhotoFileLocation,
                     InputPeerPhotoFileLocation, InputFileLocation]

# ====================== PARALLEL TELEGRAM DOWNLOADER ======================

class DownloadSender:
    def __init__(self, client, sender, file, offset, limit, stride, count):
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self):
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    def disconnect(self):
        return self.sender.disconnect()

class ParallelTransferrer:
    def __init__(self, client, dc_id=None):
        self.client = client
        self.loop = client.loop
        self.dc_id = dc_id or client.session.dc_id
        self.auth_key = (None if dc_id and client.session.dc_id != dc_id
                         else client.session.auth_key)
        self.senders = None

    async def _cleanup(self):
        await asyncio.gather(*[s.disconnect() for s in self.senders])
        self.senders = None

    async def _create_sender(self):
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(self.client._connection(dc.ip_address, dc.port, dc.id,
                                                      loggers=self.client._log,
                                                      proxy=self.client._proxy))
        if not self.auth_key:
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def _create_download_sender(self, file, index, part_size, stride, part_count):
        return DownloadSender(self.client, await self._create_sender(),
                              file, index * part_size, part_size, stride, part_count)

    async def _init_download(self, connections, file, part_count, part_size):
        minimum, remainder = divmod(part_count, connections)
        def get_count():
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        self.senders = [
            await self._create_download_sender(file, 0, part_size, connections * part_size, get_count()),
            *await asyncio.gather(
                *[self._create_download_sender(file, i, part_size, connections * part_size, get_count())
                  for i in range(1, connections)])
        ]

    async def download(self, file, file_size, part_size_kb=None, connection_count=None):
        part_size = (part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024
        part_count = math.ceil(file_size / part_size)
        connection_count = min(connection_count or CONNECTIONS_PER_FILE, part_count)
        await self._init_download(connection_count, file, part_count, part_size)

        part = 0
        while part < part_count:
            tasks = [self.loop.create_task(s.next()) for s in self.senders]
            for task in tasks:
                data = await task
                if not data:
                    break
                yield data
                part += 1

        await self._cleanup()

async def parallel_download_file(client, location, out, progress_callback=None):
    size = location.size
    dc_id, loc = utils.get_input_location(location)
    downloader = ParallelTransferrer(client, dc_id)
    async for chunk in downloader.download(loc, size, part_size_kb=CHUNK_SIZE_KB,
                                           connection_count=CONNECTIONS_PER_FILE):
        out.write(chunk)
        if progress_callback:
            r = progress_callback(out.tell(), size)
            if inspect.isawaitable(r):
                await r
    return out

# ====================== GOOGLE DRIVE UPLOADER ======================

gdrive_service = None
existing_gdrive_files = {}

def init_gdrive():
    global gdrive_service, existing_gdrive_files
    
    missing = [k for k, v in {
        'GDRIVE_CLIENT_ID': GDRIVE_CLIENT_ID,
        'GDRIVE_CLIENT_SECRET': GDRIVE_CLIENT_SECRET,
        'GDRIVE_REFRESH_TOKEN': GDRIVE_REFRESH_TOKEN,
        'GDRIVE_FOLDER_ID': GDRIVE_FOLDER_ID,
    }.items() if not v]
    
    if missing:
        print(f"[GDrive] Missing secrets: {', '.join(missing)}")
        return False
        
    try:
        # Use OAuth credentials (your personal Google account)
        # Files go to YOUR Drive quota, not the service account's
        creds = google.oauth2.credentials.Credentials(
            token=None,
            refresh_token=GDRIVE_REFRESH_TOKEN,
            client_id=GDRIVE_CLIENT_ID,
            client_secret=GDRIVE_CLIENT_SECRET,
            token_uri='https://oauth2.googleapis.com/token'
        )
        gdrive_service = build('drive', 'v3', credentials=creds)
        
        print("[GDrive] Scanning existing files in target folder...")
        results = gdrive_service.files().list(
            q=f"'{GDRIVE_FOLDER_ID}' in parents and trashed = false",
            fields="files(id, name, size)",
            pageSize=1000
        ).execute()
        
        for f in results.get('files', []):
            existing_gdrive_files[f['name']] = int(f.get('size', 0))
            
        print(f"[GDrive] Found {len(existing_gdrive_files)} existing files. Will skip those.")
        return True
    except Exception as e:
        print(f"[GDrive] Failed to initialize: {e}")
        return False

def upload_to_gdrive(file_path, filename):
    if not gdrive_service:
        return False
    try:
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        file_metadata = {'name': filename, 'parents': [GDRIVE_FOLDER_ID]}
        media = MediaFileUpload(file_path, chunksize=10*1024*1024, resumable=True)
        
        print(f"[GDrive] Uploading {filename} ({file_size_mb:.1f} MB)...")
        request = gdrive_service.files().create(body=file_metadata, media_body=media, fields='id')
        
        response = None
        last_pct = 0
        while response is None:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                if pct >= last_pct + 20:
                    last_pct = pct
                    print(f"[GDrive] {filename}: {pct}% uploaded")
                    
        print(f"[GDrive] ✓ Uploaded successfully: {filename}")
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

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH,
                            connection_retries=15, retry_delay=5)
    await client.start()
    print("[Telegram] Connected successfully!")

    try:
        channel = await client.get_entity(CHANNEL_SOURCE)
        print(f"[Telegram] Channel: {channel.title}")

        os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

        print("[Telegram] Scanning channel messages...")
        media_messages = []
        async for message in client.iter_messages(channel, reverse=True):
            if message.media:
                filename = None
                if message.file:
                    filename = message.file.name
                if not filename and hasattr(message.media, 'document') and message.media.document:
                    for attr in message.media.document.attributes:
                        if hasattr(attr, 'file_name'):
                            filename = attr.file_name
                            break
                if not filename:
                    ext = message.file.ext if (message.file and message.file.ext) else '.jpg'
                    filename = f"media_msg_{message.id}{ext}"
                media_messages.append((message, filename))

        total_media = len(media_messages)
        pad_width = len(str(total_media)) if total_media > 0 else 3

        to_download = []
        for idx, (message, filename) in enumerate(media_messages, start=1):
            file_size = message.file.size if message.file else None
            if not file_size and hasattr(message.media, 'document') and message.media.document:
                file_size = message.media.document.size

            prefixed = f"{idx:0{pad_width}d}_{filename}"

            # Skip if already in Google Drive with matching size
            if prefixed in existing_gdrive_files:
                if file_size is None or existing_gdrive_files[prefixed] == file_size:
                    continue

            to_download.append((message, prefixed, file_size))

        total = len(to_download)
        if total == 0:
            print("All files already exist in Google Drive. Nothing to do!")
            return

        print(f"Found {total} new files to transfer to Google Drive...")

        sem = asyncio.Semaphore(MAX_CONCURRENT_FILES)
        done_count = 0

        async def process(message, filename, file_size):
            nonlocal done_count
            async with sem:
                local_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)

                # Step 1: Download from Telegram
                downloaded = False
                for attempt in range(5):
                    try:
                        print(f"[Telegram] Downloading: {filename}")
                        if hasattr(message.media, 'document') and message.media.document:
                            with open(local_path, 'wb') as f:
                                await parallel_download_file(client, message.media.document, f)
                        else:
                            await message.download_media(file=TEMP_DOWNLOAD_DIR)
                        downloaded = True
                        print(f"[Telegram] ✓ Downloaded: {filename}")
                        break
                    except Exception as e:
                        if os.path.exists(local_path):
                            os.remove(local_path)
                        print(f"[Telegram] Attempt {attempt+1} failed for {filename}: {e}")
                        await asyncio.sleep(5)

                if not downloaded:
                    print(f"[Error] Could not download {filename} after 5 attempts")
                    return

                # Step 2: Upload to Google Drive
                success = upload_to_gdrive(local_path, filename)

                # Step 3: Delete local temp file immediately to save disk space
                if os.path.exists(local_path):
                    os.remove(local_path)

                if success:
                    done_count += 1
                    print(f"[Done] {filename} ({done_count}/{total})")

        tasks = [process(msg, fname, fsize) for msg, fname, fsize in to_download]
        await asyncio.gather(*tasks)

        print(f"\nPipeline complete! Transferred {done_count}/{total} files to Google Drive.")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()
        # Cleanly cancel all pending Telethon background tasks to avoid
        # "Task was destroyed but it is pending!" warnings
        await asyncio.sleep(0.5)
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

if __name__ == '__main__':
    import warnings
    # Suppress ResourceWarning noise from asyncio on exit
    warnings.filterwarnings('ignore', category=ResourceWarning)
    asyncio.run(run_pipeline())
