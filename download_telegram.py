import os
import asyncio
from telethon import TelegramClient

# Destination folder for downloaded lectures
DOWNLOAD_DIR = 'telegram_lectures'

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
    print("\nSuccessfully logged in to Telegram!")
    
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
            # Check if message contains downloadable media (document, video, audio, photo)
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
                        # Already downloaded, skip scanning / downloading
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
                retry_attempts = 5
                size_str = f"{file_size / (1024*1024):.1f} MB" if file_size else "unknown size"
                for attempt in range(retry_attempts):
                    try:
                        print(f"[Start] Downloading: {filename} ({size_str})")
                        
                        # Download file in parallel
                        path = await message.download_media(file=DOWNLOAD_DIR)
                        
                        download_count += 1
                        print(f"[Finished] Saved: {filename} ({download_count}/{total_files})")
                        break
                    except Exception as download_error:
                        if attempt < retry_attempts - 1:
                            print(f"[Retry] Connection issue for {filename}, retrying in 5s... (Attempt {attempt + 1}/{retry_attempts})")
                            await asyncio.sleep(5)
                        else:
                            print(f"[Error] Failed to download {filename} after {retry_attempts} attempts. Error: {download_error}")
                            
        # Run all download tasks concurrently
        tasks = [download_task(msg, fname, fsize) for msg, fname, fsize in messages_to_download]
        await asyncio.gather(*tasks)
        
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
