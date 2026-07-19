import os
import sys
import time
import math
import inspect
import asyncio
from collections import defaultdict
from typing import Optional, List, AsyncGenerator, Union, Awaitable, DefaultDict, Tuple, BinaryIO

from telethon import TelegramClient, utils, helpers
from telethon.sessions import StringSession
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation, InputPhotoFileLocation, InputPeerPhotoFileLocation, InputFileLocation

# Google Drive API Imports
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except ImportError:
    pass

# ----------------- CONFIGURATION -----------------
# Read variables from environment (GitHub Actions Secrets)
API_ID = int(os.environ.get('TELEGRAM_API_ID', 0))
API_HASH = os.environ.get('TELEGRAM_API_HASH', '')
SESSION_STRING = os.environ.get('TELEGRAM_SESSION_STRING', '')
CHANNEL_SOURCE = os.environ.get('TELEGRAM_CHANNEL_SOURCE', '')
GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '')
GDRIVE_CREDENTIALS_JSON = os.environ.get('GDRIVE_SERVICE_ACCOUNT_JSON', '')

MAX_CONCURRENT_FILES = 2       # Moderate concurrency
CONNECTIONS_PER_FILE = 4       # Max download speed
CHUNK_SIZE_KB = 512            # Max chunk size
TEMP_DOWNLOAD_DIR = 'temp_downloads'
# -------------------------------------------------

TypeLocation = Union[InputDocumentFileLocation, InputPhotoFileLocation, InputPeerPhotoFileLocation, InputFileLocation]

class DownloadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: GetFileRequest
    remaining: int
    stride: int

    def __init__(self, client: TelegramClient, sender: MTProtoSender, file: TypeLocation, offset: int, limit: int,
                 stride: int, count: int) -> None:
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    def disconnect(self) -> Awaitable[None]:
        return self.sender.disconnect()

class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[DownloadSender]]
    auth_key: AuthKey

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (None if dc_id and self.client.session.dc_id != dc_id
                         else self.client.session.auth_key)
        self.senders = None

    async def _cleanup(self) -> None:
        await asyncio.gather(*[sender.disconnect() for sender in self.senders])
        self.senders = None

    @staticmethod
    def _get_connection_count(file_size: int, max_count: int = CONNECTIONS_PER_FILE,
                               full_size: int = 50 * 1024 * 1024) -> int:
        if file_size > full_size:
            return max_count
        return max(1, math.ceil((file_size / full_size) * max_count))

    async def _init_download(self, connections: int, file: TypeLocation, part_count: int,
                             part_size: int) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        self.senders = [
            await self._create_download_sender(file, 0, part_size, connections * part_size,
                                               get_part_count()),
            *await asyncio.gather(
                *[self._create_download_sender(file, i, part_size, connections * part_size,
                                               get_part_count())
                  for i in range(1, connections)])
        ]

    async def _create_download_sender(self, file: TypeLocation, index: int, part_size: int,
                                      stride: int, part_count: int) -> DownloadSender:
        return DownloadSender(self.client, await self._create_sender(), file, index * part_size, part_size,
                              stride, part_count)

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(self.client._connection(dc.ip_address, dc.port, dc.id,
                                                      loggers=self.client._log,
                                                      proxy=self.client._proxy))
        if not self.auth_key:
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(id=auth.id,
                                                                         bytes=auth.bytes)
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def download(self, file: TypeLocation, file_size: int,
                       part_size_kb: Optional[float] = None,
                       connection_count: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        part_size = (part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024
        part_count = math.ceil(file_size / part_size)
        connection_count = min(connection_count or self._get_connection_count(file_size), part_count)
        
        await self._init_download(connection_count, file, part_count, part_size)

        part = 0
        while part < part_count:
            tasks = []
            for sender in self.senders:
                tasks.append(self.loop.create_task(sender.next()))
            for task in tasks:
                data = await task
                if not data:
                    break
                yield data
                part += 1

        await self._cleanup()

async def parallel_download_file(client: TelegramClient, location, out: BinaryIO, progress_callback=None) -> BinaryIO:
    size = location.size
    dc_id, location = utils.get_input_location(location)
    downloader = ParallelTransferrer(client, dc_id)
    downloaded = downloader.download(location, size, part_size_kb=CHUNK_SIZE_KB, connection_count=CONNECTIONS_PER_FILE)
    async for chunk in downloaded:
        out.write(chunk)
        if progress_callback:
            r = progress_callback(out.tell(), size)
            if inspect.isawaitable(r):
                await r
    return out

# ----------------- GOOGLE DRIVE INTEGRATION -----------------

gdrive_service = None
existing_gdrive_files = {}

def init_gdrive():
    global gdrive_service, existing_gdrive_files
    if not GDRIVE_CREDENTIALS_JSON or not GDRIVE_FOLDER_ID:
        print("[GDrive] Missing credentials or target folder ID. Skipping Google Drive upload.")
        return False
        
    try:
        import json
        creds_dict = json.loads(GDRIVE_CREDENTIALS_JSON)
        scopes = ['https://www.googleapis.com/auth/drive']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gdrive_service = build('drive', 'v3', credentials=creds)
        
        # Load existing files in Google Drive folder to prevent redownloads
        print("[GDrive] Scanning target folder files...")
        query = f"'{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        results = gdrive_service.files().list(q=query, fields="files(id, name, size)").execute()
        files = results.get('files', [])
        for f in files:
            # Map filename -> size
            existing_gdrive_files[f['name']] = int(f.get('size', 0))
            
        print(f"[GDrive] Loaded {len(existing_gdrive_files)} existing files from Google Drive.")
        return True
    except Exception as e:
        print(f"[GDrive] Failed to initialize Google Drive: {e}")
        return False

def upload_to_gdrive(file_path, filename):
    if not gdrive_service:
        return False
        
    try:
        file_metadata = {
            'name': filename,
            'parents': [GDRIVE_FOLDER_ID]
        }
        # Use 10MB chunk size for resumable upload session
        media = MediaFileUpload(file_path, chunksize=10*1024*1024, resumable=True)
        print(f"[GDrive] Uploading {filename} to Google Drive...")
        
        request = gdrive_service.files().create(body=file_metadata, media_body=media, fields='id')
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"[GDrive] Upload progress: {int(status.progress() * 100)}%")
                
        print(f"[GDrive] Successfully uploaded! File ID: {response.get('id')}")
        return True
    except Exception as e:
        print(f"[GDrive] Upload failed for {filename}: {e}")
        return False

# ----------------- MAIN PIPELINE -----------------

async def download_and_upload_lectures():
    if not SESSION_STRING:
        print("[Telegram] Error: TELEGRAM_SESSION_STRING is missing.")
        return
        
    # Initialize GDrive
    has_gdrive = init_gdrive()
    if not has_gdrive:
        print("Error: Google Drive authentication failed. Exiting.")
        return
        
    client = TelegramClient(
        StringSession(SESSION_STRING), 
        API_ID, 
        API_HASH,
        connection_retries=15,
        retry_delay=5
    )
    
    await client.start()
    print("[Telegram] Connected successfully!")
    
    try:
        channel = await client.get_entity(CHANNEL_SOURCE)
        print(f"[Telegram] Channel accessed: {channel.title}")
        
        os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
        
        # 1. Collect and filter messages
        print("[Telegram] Scanning channel messages...")
        media_messages = []
        async for message in client.iter_messages(channel, reverse=True):
            if message.media:
                filename = None
                if message.file:
                    filename = message.file.name
                if not filename and hasattr(message.media, 'document') and message.media.document:
                    for attribute in message.media.document.attributes:
                        if hasattr(attribute, 'file_name'):
                            filename = attribute.file_name
                            break
                if not filename:
                    ext = message.file.ext if (message.file and message.file.ext) else ".jpg"
                    filename = f"media_msg_{message.id}{ext}"
                media_messages.append((message, filename))

        total_media = len(media_messages)
        pad_width = len(str(total_media)) if total_media > 0 else 3
        
        messages_to_download = []
        for idx, (message, filename) in enumerate(media_messages, start=1):
            file_size = message.file.size if message.file else None
            if not file_size and hasattr(message.media, 'document') and message.media.document:
                file_size = message.media.document.size
                
            prefixed_filename = f"{idx:0{pad_width}d}_{filename}"
            
            # CHECK AGAINST GDRIVE: Skip if file already exists in Google Drive
            if prefixed_filename in existing_gdrive_files:
                if file_size is None or existing_gdrive_files[prefixed_filename] == file_size:
                    # File exists and size matches, skip it!
                    continue
                    
            messages_to_download.append((message, prefixed_filename, file_size))
            
        total_files = len(messages_to_download)
        if total_files == 0:
            print("No new files to download. Everything on Google Drive is up to date!")
            return
            
        print(f"Found {total_files} new files. Initiating pipeline...")
        
        sem = asyncio.Semaphore(MAX_CONCURRENT_FILES)
        download_count = 0
        
        async def process_task(message, filename, file_size):
            nonlocal download_count
            async with sem:
                local_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)
                retry_attempts = 5
                
                # Step 1: Download locally
                downloaded = False
                for attempt in range(retry_attempts):
                    try:
                        print(f"\n[Telegram] Downloading: {filename}")
                        if hasattr(message.media, 'document') and message.media.document:
                            with open(local_path, 'wb') as out_file:
                                await parallel_download_file(client, message.media.document, out_file)
                        else:
                            await message.download_media(file=TEMP_DOWNLOAD_DIR)
                        downloaded = True
                        break
                    except Exception as e:
                        if os.path.exists(local_path):
                            os.remove(local_path)
                        print(f"[Telegram] Attempt {attempt+1} failed for {filename}: {e}")
                        await asyncio.sleep(5)
                        
                if not downloaded:
                    print(f"[Error] Failed to download {filename} after all attempts.")
                    return
                    
                # Step 2: Upload to Google Drive
                success = upload_to_gdrive(local_path, filename)
                
                # Step 3: Cleanup local space
                if os.path.exists(local_path):
                    os.remove(local_path)
                    
                if success:
                    download_count += 1
                    print(f"[Success] Completed: {filename} ({download_count}/{total_files})")
                else:
                    print(f"[Error] Upload failed for {filename}")
                    
        tasks = [process_task(msg, fname, fsize) for msg, fname, fsize in messages_to_download]
        await asyncio.gather(*tasks)
        print(f"\nPipeline complete! Successfully transferred {download_count} new files.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await client.disconnect()

if __name__ == '__main__':
    asyncio.run(download_and_upload_lectures())
