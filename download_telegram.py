import os
import sys
import time
import math
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

# Enable ANSI escape codes in Windows Command Prompt
if os.name == 'nt':
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass

# ----------------- PARALLEL DOWNLOAD UTILITIES -----------------

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
    def _get_connection_count(file_size: int, max_count: int = 16,
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

        # The first cross-DC sender exports+imports authorization first
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
        
        # Avoid creating more connections than total parts
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
    # Set part_size_kb=512 and connection_count=16 for maximum speed on a single file
    downloaded = downloader.download(location, size, part_size_kb=512, connection_count=16)
    async for chunk in downloaded:
        out.write(chunk)
        if progress_callback:
            r = progress_callback(out.tell(), size)
            if inspect.isawaitable(r):
                await r
    return out

# ----------------- DASHBOARD & STATE -----------------

active_downloads = {}
dashboard_lock = asyncio.Lock()
num_lines_drawn = 0
last_draw_time = 0

async def clear_dashboard():
    global num_lines_drawn
    if num_lines_drawn > 0:
        sys.stdout.write(f"\033[{num_lines_drawn}A")
        for _ in range(num_lines_drawn):
            sys.stdout.write("\033[K\n")
        sys.stdout.write(f"\033[{num_lines_drawn}A")
        sys.stdout.flush()
        num_lines_drawn = 0

async def print_log(message):
    async with dashboard_lock:
        await clear_dashboard()
        print(message)
        await draw_dashboard_instantly()

async def draw_dashboard_instantly():
    global num_lines_drawn
    if not active_downloads:
        return
    
    lines = []
    lines.append("--------------------------------------------------------------------------------")
    lines.append(f"Active Downloads ({len(active_downloads)} files concurrently):")
    for fname, info in list(active_downloads.items()):
        rec = info['received']
        tot = info['total']
        speed = info['speed']
        pct = (rec / tot * 100) if tot else 0
        speed_mb = speed / (1024 * 1024)
        
        rec_mb = rec / (1024 * 1024)
        tot_str = f"{tot / (1024 * 1024):.1f} MB" if tot else "unknown"
        
        if speed > 0 and tot:
            eta = (tot - rec) / speed
            eta_str = f"{int(eta)}s" if eta < 60 else f"{int(eta//60)}m {int(eta%60)}s"
        else:
            eta_str = "--"
            
        display_name = fname if len(fname) <= 35 else f"{fname[:16]}...{fname[-16:]}"
        lines.append(f" 📂 {display_name:<35} | {pct:5.1f}% | {rec_mb:6.1f}/{tot_str:<8} | {speed_mb:5.2f} MB/s | ETA: {eta_str}")
    lines.append("--------------------------------------------------------------------------------")
    
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()
    num_lines_drawn = len(lines)

async def draw_dashboard():
    global last_draw_time
    now = time.time()
    if now - last_draw_time < 2.0:
        return
    last_draw_time = now
    async with dashboard_lock:
        await clear_dashboard()
        await draw_dashboard_instantly()

# ----------------- MAIN DOWNLOADER -----------------

async def download_lectures(api_id, api_hash, channel_source, download_dir):
    client = TelegramClient(
        'lecture_downloader_session', 
        api_id, 
        api_hash,
        connection_retries=15,
        retry_delay=5
    )
    
    await client.start()
    print("Successfully logged in to Telegram!")
    
    try:
        channel = await client.get_entity(channel_source)
        print(f"Accessing channel: {channel.title}")
        
        os.makedirs(download_dir, exist_ok=True)
        print(f"Saving lectures to directory: {os.path.abspath(download_dir)}")
        
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
                
            # Prefix filename with zero-padded index (e.g., 001_filename.mp4)
            prefixed_filename = f"{idx:0{pad_width}d}_{filename}"
            target_path = os.path.join(download_dir, prefixed_filename)
            
            # Skip if already downloaded fully
            if os.path.exists(target_path):
                if file_size is None or os.path.getsize(target_path) == file_size:
                    continue
                    
            messages_to_download.append((message, prefixed_filename, file_size))
            
        total_files = len(messages_to_download)
        if total_files == 0:
            print("No new files to download. All existing files are fully downloaded!")
            return
            
        print(f"Found {total_files} new files to download. Starting concurrent downloads...")
        
        # 2. Download concurrently using a Semaphore (1 file at a time for maximum chunk throughput)
        sem = asyncio.Semaphore(1)
        download_count = 0
        
        async def download_task(message, filename, file_size):
            nonlocal download_count
            async with sem:
                active_downloads[filename] = {
                    'received': 0,
                    'total': file_size,
                    'start_time': time.time(),
                    'last_update': time.time(),
                    'last_received': 0,
                    'speed': 0
                }
                
                def make_progress_callback(fname):
                    def progress_callback(received, total):
                        info = active_downloads.get(fname)
                        if not info:
                            return
                        now = time.time()
                        elapsed = now - info['last_update']
                        if elapsed >= 0.5 or received == total:
                            bytes_diff = received - info['last_received']
                            info['speed'] = bytes_diff / elapsed if elapsed > 0 else 0
                            info['last_update'] = now
                            info['last_received'] = received
                        info['received'] = received
                        info['total'] = total
                        
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                loop.create_task(draw_dashboard())
                        except Exception:
                            pass
                    return progress_callback
                
                retry_attempts = 5
                target_path = os.path.join(download_dir, filename)
                
                for attempt in range(retry_attempts):
                    try:
                        # Use parallel chunk downloader for documents (videos/zips), standard downloader for photos
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
                        
                        if filename in active_downloads:
                            del active_downloads[filename]
                        
                        download_count += 1
                        await print_log(f"[Finished] Saved: {filename} ({download_count}/{total_files})")
                        break
                    except Exception as download_error:
                        if os.path.exists(target_path):
                            try:
                                os.remove(target_path)
                            except Exception:
                                pass
                                
                        if attempt < retry_attempts - 1:
                            await print_log(f"[Retry] Connection issue for {filename}, retrying in 5s... (Attempt {attempt + 1}/{retry_attempts})")
                            await asyncio.sleep(5)
                        else:
                            if filename in active_downloads:
                                del active_downloads[filename]
                            await print_log(f"[Error] Failed to download {filename} after {retry_attempts} attempts. Error: {download_error}")
                            
        tasks = [download_task(msg, fname, fsize) for msg, fname, fsize in messages_to_download]
        await asyncio.gather(*tasks)
        
        await clear_dashboard()
        print(f"\nDownload session complete! Successfully downloaded {download_count} new files.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await client.disconnect()

# Configuration persistence
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, 'config.json')

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            import json
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return None

def save_config(api_id, api_hash, channel_source):
    try:
        import json
        config = {
            'api_id': api_id,
            'api_hash': api_hash,
            'channel_source': channel_source
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        print("Credentials saved locally to config.json.")
    except Exception as e:
        print(f"Warning: Could not save credentials: {e}")

if __name__ == '__main__':
    print("=========================================")
    print("   Telegram Parallel Media Downloader   ")
    print("=========================================\n")
    try:
        print("Opening folder selection popup...")
        import tkinter as tk
        from tkinter import filedialog
        
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        selected_dir = filedialog.askdirectory(title="Select Download Folder for Telegram Lectures")
        root.destroy()
        
        if not selected_dir:
            print("No folder selected. Defaulting to 'telegram_lectures' in current directory.\n")
            download_dir = 'telegram_lectures'
        else:
            download_dir = selected_dir
            print(f"Downloads folder set to: {os.path.abspath(download_dir)}\n")
            
        config = load_config()
        use_saved = False
        
        if config:
            print("--- Found saved credentials ---")
            print(f"API_ID: {config.get('api_id')}")
            api_hash_val = config.get('api_hash', '')
            masked_hash = (api_hash_val[:4] + '*' * 10 + api_hash_val[-4:]) if len(api_hash_val) > 8 else '**********'
            print(f"API_HASH: {masked_hash}")
            print(f"Channel: {config.get('channel_source')}")
            print("--------------------------------")
            choice = input("Use these saved credentials? (y/n) [Default: y]: ").strip().lower()
            if choice in ('y', 'yes', ''):
                use_saved = True
                print("Using saved credentials.\n")
        
        if use_saved:
            api_id = int(config['api_id'])
            api_hash = config['api_hash']
            channel_source = config['channel_source']
        else:
            api_id_input = input("Enter your Telegram App API_ID: ").strip()
            api_id = int(api_id_input)
            api_hash = input("Enter your Telegram App API_HASH: ").strip()
            channel_input = input("Enter the Channel Link or Username (e.g., StriverDSA or https://t.me/StriverDSA): ").strip()
            
            if 't.me/' in channel_input:
                channel_source = channel_input.split('t.me/')[-1].strip('/')
            else:
                channel_source = channel_input
            
            save_config(api_id, api_hash, channel_source)
            
        asyncio.run(download_lectures(api_id, api_hash, channel_source, download_dir))
    except ValueError:
        print("\n[Error] API_ID must be an integer number.")
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
