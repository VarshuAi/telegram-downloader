import os
import sys
import time
import asyncio
from telethon import TelegramClient

# Enable ANSI escape codes in Windows Command Prompt
if os.name == 'nt':
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        # Enable ENABLE_VIRTUAL_TERMINAL_PROCESSING (0x0004) for stdout (-11)
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass

# Destination folder for downloaded lectures
DOWNLOAD_DIR = 'telegram_lectures'

# Dashboard state management
active_downloads = {}
dashboard_lock = asyncio.Lock()
num_lines_drawn = 0
last_draw_time = 0

async def clear_dashboard():
    global num_lines_drawn
    if num_lines_drawn > 0:
        # Move cursor up, clear line, and do it for all lines drawn
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
            
        # Format filename to fit the display line nicely
        display_name = fname if len(fname) <= 35 else f"{fname[:16]}...{fname[-16:]}"
        lines.append(f" 📂 {display_name:<35} | {pct:5.1f}% | {rec_mb:6.1f}/{tot_str:<8} | {speed_mb:5.2f} MB/s | ETA: {eta_str}")
    lines.append("--------------------------------------------------------------------------------")
    
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()
    num_lines_drawn = len(lines)

async def draw_dashboard():
    global last_draw_time
    now = time.time()
    if now - last_draw_time < 0.5:
        return
    last_draw_time = now
    async with dashboard_lock:
        await clear_dashboard()
        await draw_dashboard_instantly()

async def download_lectures(api_id, api_hash, channel_source):
    # Initialize the client session with retry parameters
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
        # Resolve the channel entity
        channel = await client.get_entity(channel_source)
        print(f"Accessing channel: {channel.title}")
        
        # Create output directory
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        print(f"Saving lectures to directory: {os.path.abspath(DOWNLOAD_DIR)}")
        
        # 1. Collect all messages containing media first
        print("Scanning channel messages...")
        messages_to_download = []
        async for message in client.iter_messages(channel, reverse=True):
            if message.media:
                # Determine filename and size
                filename = None
                file_size = None
                
                if message.file:
                    filename = message.file.name
                    file_size = message.file.size
                
                if not filename:
                    if hasattr(message.media, 'document') and message.media.document:
                        file_size = message.media.document.size
                        for attribute in message.media.document.attributes:
                            if hasattr(attribute, 'file_name'):
                                filename = attribute.file_name
                                break
                
                if not filename:
                    ext = message.file.ext if (message.file and message.file.ext) else ".jpg"
                    filename = f"media_msg_{message.id}{ext}"
                    file_size = message.file.size if message.file else None

                target_path = os.path.join(DOWNLOAD_DIR, filename)
                
                # If file already exists and size matches, skip download
                if os.path.exists(target_path):
                    if file_size is None or os.path.getsize(target_path) == file_size:
                        continue
                
                messages_to_download.append((message, filename, file_size))
                
        total_files = len(messages_to_download)
        if total_files == 0:
            print("No new files to download. All existing files are fully downloaded!")
            return
            
        print(f"Found {total_files} new files to download. Starting concurrent downloads...")
        
        # 2. Download concurrently using a Semaphore (4 files at a time)
        sem = asyncio.Semaphore(4)
        download_count = 0
        
        async def download_task(message, filename, file_size):
            nonlocal download_count
            async with sem:
                # Initialize active download state tracking
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
                for attempt in range(retry_attempts):
                    try:
                        # Start downloading
                        await message.download_media(
                            file=DOWNLOAD_DIR, 
                            progress_callback=make_progress_callback(filename)
                        )
                        
                        # Remove from active status and log completion
                        if filename in active_downloads:
                            del active_downloads[filename]
                        
                        download_count += 1
                        await print_log(f"[Finished] Saved: {filename} ({download_count}/{total_files})")
                        break
                    except Exception as download_error:
                        if attempt < retry_attempts - 1:
                            await print_log(f"[Retry] Connection issue for {filename}, retrying in 5s... (Attempt {attempt + 1}/{retry_attempts})")
                            await asyncio.sleep(5)
                        else:
                            if filename in active_downloads:
                                del active_downloads[filename]
                            await print_log(f"[Error] Failed to download {filename} after {retry_attempts} attempts. Error: {download_error}")
                            
        # Run all download tasks concurrently
        tasks = [download_task(msg, fname, fsize) for msg, fname, fsize in messages_to_download]
        await asyncio.gather(*tasks)
        
        # Clear final screen status
        await clear_dashboard()
        print(f"\nDownload session complete! Successfully downloaded {download_count} new files.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await client.disconnect()

if __name__ == '__main__':
    print("=========================================")
    print("   Telegram Parallel Media Downloader   ")
    print("=========================================\n")
    try:
        api_id_input = input("Enter your Telegram App API_ID: ").strip()
        api_id = int(api_id_input)
        api_hash = input("Enter your Telegram App API_HASH: ").strip()
        channel_input = input("Enter the Channel Link or Username (e.g., StriverDSA or https://t.me/StriverDSA): ").strip()
        
        # Parse channel name if it's a full link
        if 't.me/' in channel_input:
            channel_source = channel_input.split('t.me/')[-1].strip('/')
        else:
            channel_source = channel_input
            
        asyncio.run(download_lectures(api_id, api_hash, channel_source))
    except ValueError:
        print("\n[Error] API_ID must be an integer number.")
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
