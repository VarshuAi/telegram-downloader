import os
import sys
import time
import math
import shutil
import hashlib
import inspect
import asyncio
from collections import defaultdict
from typing import Optional, List, AsyncGenerator, Union, Awaitable, DefaultDict, Tuple, BinaryIO

from telethon import TelegramClient, utils, helpers
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation, InputPhotoFileLocation, InputPeerPhotoFileLocation, InputFileLocation

# ----------------- COLAB PERFORMANCE CONFIGURATION -----------------
MAX_CONCURRENT_FILES = 4       # We can run more files concurrently on Colab
CONNECTIONS_PER_FILE = 4       # 4 connections per file is ideal
CHUNK_SIZE_KB = 512            # Max chunk size
# -------------------------------------------------------------------

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

# ----------------- SIMPLE LOGGER -----------------

active_downloads = {}

def make_progress_callback(fname):
    active_downloads[fname] = {'last_pct': 0, 'last_time': time.time(), 'last_bytes': 0}
    def progress_callback(received, total):
        info = active_downloads[fname]
        pct = int((received / total) * 100) if total else 0
        now = time.time()
        
        # Log every 10% change to keep logs clean in Colab
        if pct >= info['last_pct'] + 10 or received == total:
            info['last_pct'] = pct
            rec_mb = received / (1024 * 1024)
            tot_mb = total / (1024 * 1024) if total else 0
            elapsed = now - info['last_time']
            speed = (received - info['last_bytes']) / elapsed if elapsed > 0 else 0
            speed_mb = speed / (1024 * 1024)
            print(f"[{fname[:25]}...] {pct}% ({rec_mb:.1f}/{tot_mb:.1f} MB) | Speed: {speed_mb:.2f} MB/s")
            
            info['last_time'] = now
            info['last_bytes'] = received
    return progress_callback

# ----------------- MAIN DOWNLOADER -----------------

async def download_lectures(api_id, api_hash, channel_source, download_dir):
    # SQLite has lock issues on Google Drive FUSE mount.
    # To fix this, we run SQLite locally in /content, but backup/restore it from Google Drive.
    local_session_path = '/content/colab_downloader_session'
    gdrive_session_path = '/content/drive/MyDrive/colab_downloader_session.session'
    
    if os.path.exists(gdrive_session_path):
        try:
            shutil.copy2(gdrive_session_path, local_session_path + '.session')
            print("Loaded Telegram login session from Google Drive.")
        except Exception as e:
            print(f"Note: Could not import session file: {e}")
            
    client = TelegramClient(
        local_session_path, 
        api_id, 
        api_hash,
        connection_retries=15,
        retry_delay=5
    )
    
    await client.start()
    print("\nSuccessfully logged in to Telegram!")
    
    # Save session back immediately on successful start
    if os.path.exists('/content/drive/MyDrive'):
        try:
            shutil.copy2(local_session_path + '.session', gdrive_session_path)
        except Exception:
            pass
            
    try:
        channel = await client.get_entity(channel_source)
        print(f"Accessing channel: {channel.title}")
        
        os.makedirs(download_dir, exist_ok=True)
        print(f"Saving lectures to: {os.path.abspath(download_dir)}")
        
        # 1. Collect all media messages first to establish index order
        print("Scanning channel messages...")
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
            target_path = os.path.join(download_dir, prefixed_filename)
            
            if os.path.exists(target_path):
                if file_size is None or os.path.getsize(target_path) == file_size:
                    continue
                    
            messages_to_download.append((message, prefixed_filename, file_size))
            
        total_files = len(messages_to_download)
        if total_files == 0:
            print("No new files to download. All existing files are fully downloaded!")
            return
            
        print(f"Found {total_files} new files to download. Starting concurrent downloads...")
        
        sem = asyncio.Semaphore(MAX_CONCURRENT_FILES)
        download_count = 0
        
        async def download_task(message, filename, file_size):
            nonlocal download_count
            async with sem:
                retry_attempts = 5
                target_path = os.path.join(download_dir, filename)
                
                for attempt in range(retry_attempts):
                    try:
                        print(f"\n[Start] Downloading: {filename}")
                        if hasattr(message.media, 'document') and message.media.document:
                            with open(target_path, 'wb') as out_file:
                                await parallel_download_file(
                                    client, 
                                    message.media.document, 
                                    out_file, 
                                    progress_callback=make_progress_callback(filename)
                                )
                        else:
                            await message.download_media(
                                file=download_dir,
                                progress_callback=make_progress_callback(filename)
                            )
                        
                        download_count += 1
                        print(f"[Finished] Saved: {filename} ({download_count}/{total_files})")
                        break
                    except Exception as download_error:
                        if os.path.exists(target_path):
                            try:
                                os.remove(target_path)
                            except Exception:
                                pass
                                
                        if attempt < retry_attempts - 1:
                            print(f"[Retry] Error on {filename}, retrying in 5s... (Attempt {attempt + 1}/{retry_attempts})")
                            await asyncio.sleep(5)
                        else:
                            print(f"[Error] Failed to download {filename} after {retry_attempts} attempts. Error: {download_error}")
                            
        tasks = [download_task(msg, fname, fsize) for msg, fname, fsize in messages_to_download]
        await asyncio.gather(*tasks)
        print(f"\nDownload session complete! Successfully downloaded {download_count} new files.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await client.disconnect()
        
        # Backup the local session to Google Drive for next time
        if os.path.exists('/content/drive/MyDrive') and os.path.exists(local_session_path + '.session'):
            try:
                shutil.copy2(local_session_path + '.session', gdrive_session_path)
                print("Saved Telegram session back to Google Drive for future auto-logins.")
            except Exception as e:
                print(f"Note: Could not backup session to Google Drive: {e}")

if __name__ == '__main__':
    import json
    config_paths = ['config.json', '/content/drive/MyDrive/telegram_downloader_config.json']
    config = None
    for cp in config_paths:
        if os.path.exists(cp):
            try:
                with open(cp, 'r') as f:
                    config = json.load(f)
                    print(f"Loaded config from: {cp}")
                    break
            except Exception:
                pass
                
    if not config:
        print("No saved credentials found. Please input them:")
        api_id = int(input("Enter your Telegram App API_ID: ").strip())
        api_hash = input("Enter your Telegram App API_HASH: ").strip()
        channel_input = input("Enter the Channel Link or Username: ").strip()
        if 't.me/' in channel_input:
            channel_source = channel_input.split('t.me/')[-1].strip('/')
        else:
            channel_source = channel_input
            
        gdrive_config = '/content/drive/MyDrive/telegram_downloader_config.json'
        try:
            with open('config.json', 'w') as f:
                json.dump({'api_id': api_id, 'api_hash': api_hash, 'channel_source': channel_source}, f, indent=4)
            if os.path.exists('/content/drive/MyDrive'):
                with open(gdrive_config, 'w') as f:
                    json.dump({'api_id': api_id, 'api_hash': api_hash, 'channel_source': channel_source}, f, indent=4)
                print(f"Saved credentials to Google Drive: {gdrive_config}")
        except Exception as e:
            print(f"Could not save config: {e}")
    else:
        api_id = int(config['api_id'])
        api_hash = config['api_hash']
        channel_source = config['channel_source']
        
    gdrive_dir = '/content/drive/MyDrive/telegram_lectures'
    if os.path.exists('/content/drive/MyDrive'):
        download_dir = gdrive_dir
        print(f"Google Drive detected. Downloading directly to Google Drive folder: {download_dir}")
    else:
        download_dir = 'telegram_lectures'
        print(f"Google Drive NOT detected. Downloading locally to: {download_dir}")
        
    asyncio.run(download_lectures(api_id, api_hash, channel_source, download_dir))
