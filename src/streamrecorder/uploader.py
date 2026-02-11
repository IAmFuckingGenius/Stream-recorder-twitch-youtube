"""
Telegram uploader module for Twitch Stream Recorder.
Uploads video files to Telegram channel using Telethon.
"""

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional

from telethon import TelegramClient
from telethon.tl.types import User

# FastTelethonhelper for high-speed uploads (parallel chunk uploads)
try:
    from FastTelethonhelper import fast_upload
    FAST_UPLOAD_AVAILABLE = True
except ImportError:
    FAST_UPLOAD_AVAILABLE = False

from .logger import get_logger
from .stream_monitor import StreamInfo


@dataclass
class UploadResult:
    """Result of an upload operation."""
    file_path: str
    message_id: Optional[int]
    success: bool
    error: Optional[str] = None


@dataclass
class BatchUploadResult:
    """Result of uploading multiple files."""
    channel: str
    stream_info: StreamInfo
    total_parts: int
    successful_uploads: int
    failed_uploads: int
    results: List[UploadResult]
    
    @property
    def success(self) -> bool:
        return self.failed_uploads == 0


class TelegramUploader:
    """
    Uploads video files to Telegram using Telethon userbot.
    
    Features:
    - Premium account detection for 4GB uploads
    - Progress tracking
    - Retry on failure
    - Status messages (waiting, recording status)
    - Backup channel forwarding
    """
    
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        channel_id: int,
        backup_channel_id: Optional[int] = None,
        discussion_group_id: Optional[int] = None,
        session_name: str = "stream_recorder",
        use_fast_upload_helper: bool = True,
        upload_parallelism: int = 1,
        upload_speed_limit_mbps: float = 0.0
    ):
        """
        Initialize Telegram uploader.
        
        Args:
            api_id: Telegram API ID.
            api_hash: Telegram API hash.
            channel_id: Target channel ID for uploads.
            backup_channel_id: Optional backup channel for forwards.
            discussion_group_id: Discussion group ID for comments.
            session_name: Session file name.
            use_fast_upload_helper: Enable FastTelethonhelper when installed.
            upload_parallelism: Parallel uploads in one album (1..10 useful range).
            upload_speed_limit_mbps: Max upload speed in megabits/s. 0 = unlimited.
        """
        self._logger = get_logger('uploader')
        self.api_id = api_id
        self.api_hash = api_hash
        self.channel_id = channel_id
        self.backup_channel_id = backup_channel_id
        self.discussion_group_id = discussion_group_id
        self.session_name = session_name
        self.use_fast_upload_helper = use_fast_upload_helper
        self.upload_parallelism = max(1, int(upload_parallelism or 1))
        self.upload_speed_limit_mbps = max(0.0, float(upload_speed_limit_mbps or 0.0))
        
        self._client: Optional[TelegramClient] = None
        self._is_premium: bool = False
        # Mbps -> bytes/s (decimal megabit)
        self._upload_speed_limit_bps: float = self.upload_speed_limit_mbps * 1_000_000 / 8
        self._throttle_enabled: bool = self._upload_speed_limit_bps > 0
        self._throttle_started_at: float = time.monotonic()
        self._throttle_sent_bytes: int = 0
        self._throttle_lock = asyncio.Lock()
        # FastTelethonhelper bypasses Telethon progress callback; disable it when throttling is requested.
        self._fast_upload_enabled: bool = (
            FAST_UPLOAD_AVAILABLE and self.use_fast_upload_helper and not self._throttle_enabled
        )
    
    async def connect(self) -> bool:
        """
        Connect to Telegram and check premium status.
        
        Returns:
            True if connected successfully.
        """
        try:
            self._client = TelegramClient(
                self.session_name,
                self.api_id,
                self.api_hash
            )
            
            await self._client.start()
            
            # Check premium status
            me = await self._client.get_me()
            self._is_premium = getattr(me, 'premium', False)
            
            self._logger.info(
                f"Connected to Telegram as {me.first_name} "
                f"({'Premium' if self._is_premium else 'Regular'} account)"
            )
            self._logger.info(f"Album upload parallelism: {self.upload_parallelism}")
            if self._throttle_enabled:
                self._logger.info(
                    f"Upload speed limit enabled: {self.upload_speed_limit_mbps:.2f} Mbps "
                    f"(~{self._upload_speed_limit_bps / (1024 * 1024):.2f} MiB/s)"
                )
                if self.upload_parallelism > 1:
                    self._logger.info(
                        "Speed limit mode: global cap shared across parallel uploads"
                    )
            if self._fast_upload_enabled:
                self._logger.info("FastTelethonhelper: enabled")
            elif self.use_fast_upload_helper and not FAST_UPLOAD_AVAILABLE:
                self._logger.info("FastTelethonhelper: requested, but not installed (fallback to standard upload)")
            elif self.use_fast_upload_helper and self._throttle_enabled:
                self._logger.info("FastTelethonhelper: disabled because upload speed limit is active")
            else:
                self._logger.info("FastTelethonhelper: disabled by config (using standard upload)")
            
            return True
            
        except Exception as e:
            self._logger.error(f"Failed to connect: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from Telegram."""
        if self._client:
            await self._client.disconnect()
            self._client = None
    
    @property
    def is_premium(self) -> bool:
        """Check if connected account has Premium status."""
        return self._is_premium
    
    @property
    def max_file_size_gb(self) -> float:
        """Get maximum file size in GB based on account type."""
        return 4.0 if self._is_premium else 2.0
    
    @property
    def max_file_size_mb(self) -> int:
        """Get maximum file size in MB based on account type."""
        return 4000 if self._is_premium else 2000

    def _make_rate_limited_progress(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ):
        """
        Build async progress callback with optional bandwidth throttling.
        
        Args:
            progress_callback: Optional callback(current_bytes, total_bytes).
            
        Returns:
            Async callback for Telethon upload APIs, or None when not needed.
        """
        if not self._throttle_enabled and progress_callback is None:
            return None

        last_current = 0

        async def _apply_global_rate_limit(byte_delta: int) -> None:
            """Apply shared upload cap across all parallel uploads."""
            if byte_delta <= 0 or not self._throttle_enabled:
                return

            async with self._throttle_lock:
                self._throttle_sent_bytes += byte_delta
                elapsed = time.monotonic() - self._throttle_started_at
                required_elapsed = self._throttle_sent_bytes / self._upload_speed_limit_bps
                if required_elapsed > elapsed:
                    await asyncio.sleep(required_elapsed - elapsed)

        async def _progress(current: int, total: int) -> None:
            nonlocal last_current
            if self._throttle_enabled and current > 0:
                # Telethon callback gives absolute uploaded bytes for this file;
                # throttle by delta and share budget globally across active uploads.
                if current < last_current:
                    last_current = 0
                delta = current - last_current
                last_current = current
                await _apply_global_rate_limit(delta)
            if progress_callback:
                progress_callback(current, total)

        return _progress
    
    def _format_caption(
        self,
        stream_info: StreamInfo,
        part_num: Optional[int] = None,
        total_parts: Optional[int] = None,
        file_size_mb: float = 0,
        duration_str: str = ""
    ) -> str:
        """
        Format caption for uploaded video.
        
        Args:
            stream_info: Stream information.
            part_num: Part number (if split).
            total_parts: Total number of parts.
            file_size_mb: File size in MB.
            duration_str: Formatted duration string.
            
        Returns:
            Formatted caption string.
        """
        date_str = stream_info.started_at.strftime('%d.%m.%Y') if stream_info.started_at else datetime.now().strftime('%d.%m.%Y')
        
        lines = [
            f"Канал: **{stream_info.channel}**",
            f"Дата: **{date_str}**",
            f"Стрим: **{stream_info.title}**",
        ]
        
        if duration_str:
            lines.append(f"Длительность: **{duration_str}**")
        
        return "\n".join(lines)
    
    async def _get_video_metadata(self, file_path: str) -> dict:
        """Get video duration, width, height using ffprobe."""
        try:
            process = await asyncio.create_subprocess_exec(
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height,duration',
                '-show_entries', 'format=duration',
                '-of', 'json',
                file_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await process.communicate()
            
            import json
            data = json.loads(stdout.decode())
            
            # Get duration from format or stream
            duration = 0
            if 'format' in data and 'duration' in data['format']:
                duration = float(data['format']['duration'])
            elif 'streams' in data and data['streams']:
                stream = data['streams'][0]
                if 'duration' in stream:
                    duration = float(stream['duration'])
            
            # Get dimensions
            width, height = 1920, 1080  # defaults
            if 'streams' in data and data['streams']:
                stream = data['streams'][0]
                width = stream.get('width', 1920)
                height = stream.get('height', 1080)
            
            return {
                'duration': int(duration),
                'width': width,
                'height': height
            }
        except Exception as e:
            self._logger.warning(f"Failed to get video metadata: {e}")
            return {'duration': 0, 'width': 1920, 'height': 1080}
    
    async def _generate_thumbnail(self, file_path: str, duration: int) -> Optional[str]:
        """
        Generate thumbnail from video at 10% of duration.
        
        Args:
            file_path: Path to video file.
            duration: Video duration in seconds.
            
        Returns:
            Path to thumbnail file or None on error.
        """
        thumb_path = f"{file_path}.thumb.jpg"
        
        # Take frame at 10% of video (or 5 seconds if duration unknown)
        seek_time = max(5, int(duration * 0.1)) if duration > 0 else 5
        
        try:
            process = await asyncio.create_subprocess_exec(
                'ffmpeg', '-y',
                '-ss', str(seek_time),
                '-i', file_path,
                '-vframes', '1',
                '-vf', 'scale=320:-1',  # 320px width, keep aspect ratio
                '-q:v', '5',  # Quality (2-31, lower is better)
                thumb_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            await asyncio.wait_for(process.communicate(), timeout=30)
            
            if process.returncode == 0 and Path(thumb_path).exists():
                self._logger.debug(f"Generated thumbnail: {thumb_path}")
                return thumb_path
            else:
                return None
                
        except Exception as e:
            self._logger.warning(f"Failed to generate thumbnail: {e}")
            return None
    
    async def upload_file(
        self,
        file_path: str,
        caption: str,
        progress_callback: Optional[Callable[[float], None]] = None,
        max_retries: int = 3
    ) -> UploadResult:
        """
        Upload a single file to the channel.
        
        Args:
            file_path: Path to file to upload.
            caption: Caption for the message.
            progress_callback: Optional callback for progress updates.
            max_retries: Maximum number of retry attempts.
            
        Returns:
            UploadResult with message ID or error.
        """
        if not self._client:
            return UploadResult(
                file_path=file_path,
                message_id=None,
                success=False,
                error="Not connected to Telegram"
            )
        
        path = Path(file_path)
        if not path.exists():
            return UploadResult(
                file_path=file_path,
                message_id=None,
                success=False,
                error=f"File not found: {file_path}"
            )
        
        file_size_mb = path.stat().st_size / (1024 * 1024)
        self._logger.info(f"Uploading {path.name} ({file_size_mb:.1f} MB)...")
        
        # Get video metadata for proper display
        metadata = await self._get_video_metadata(file_path)
        self._logger.debug(f"Video metadata: {metadata}")
        
        # Progress wrapper
        last_progress = [0]
        
        def progress(current, total):
            percent = (current / total) * 100
            # Log every 10%
            if int(percent) // 10 > last_progress[0] // 10:
                last_progress[0] = int(percent)
                self._logger.debug(f"Upload progress: {percent:.1f}%")
            if progress_callback:
                progress_callback(percent)
        
        # Generate thumbnail
        thumb_path = await self._generate_thumbnail(file_path, metadata['duration'])
        
        try:
            for attempt in range(max_retries):
                try:
                    # Import video attributes
                    from telethon.tl.types import DocumentAttributeVideo
                    
                    video_attrs = DocumentAttributeVideo(
                        duration=metadata['duration'],
                        w=metadata['width'],
                        h=metadata['height'],
                        supports_streaming=True
                    )
                    
                    # Use FastTelethonhelper if available (7x faster)
                    if self._fast_upload_enabled:
                        uploaded_file = await fast_upload(
                            self._client,
                            file_path,
                            reply=None,
                            name=path.name,
                            progress_bar_function=None
                        )
                        message = await self._client.send_file(
                            self.channel_id,
                            uploaded_file,
                            caption=caption,
                            supports_streaming=True,
                            attributes=[video_attrs],
                            thumb=thumb_path
                        )
                    else:
                        message = await self._client.send_file(
                            self.channel_id,
                            file_path,
                            caption=caption,
                            progress_callback=self._make_rate_limited_progress(progress),
                            supports_streaming=True,
                            attributes=[video_attrs],
                            thumb=thumb_path
                        )
                    
                    self._logger.info(f"Uploaded: {path.name}")
                    
                    return UploadResult(
                        file_path=file_path,
                        message_id=message.id,
                        success=True
                    )
                    
                except Exception as e:
                    self._logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
                    
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 30
                        self._logger.info(f"Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
            
            return UploadResult(
                file_path=file_path,
                message_id=None,
                success=False,
                error=f"Failed after {max_retries} attempts"
            )
        finally:
            # Cleanup thumbnail
            if thumb_path and Path(thumb_path).exists():
                Path(thumb_path).unlink()
    
    async def upload_stream_parts(
        self,
        stream_info: StreamInfo,
        file_paths: List[str],
        duration_str: str = ""
    ) -> BatchUploadResult:
        """
        Upload multiple parts of a stream recording as albums.
        
        Args:
            stream_info: Information about the stream.
            file_paths: List of file paths to upload.
            duration_str: Total stream duration string.
            
        Returns:
            BatchUploadResult with individual results.
        """
        total_parts = len(file_paths)
        
        self._logger.info(
            f"Uploading {total_parts} parts for {stream_info.channel}"
        )
        
        # If single file, use regular upload
        if total_parts == 1:
            path = Path(file_paths[0])
            file_size_mb = path.stat().st_size / (1024 * 1024) if path.exists() else 0
            caption = self._format_caption(
                stream_info=stream_info,
                file_size_mb=file_size_mb,
                duration_str=duration_str
            )
            result = await self.upload_file(file_paths[0], caption)
            return BatchUploadResult(
                channel=stream_info.channel,
                stream_info=stream_info,
                total_parts=1,
                successful_uploads=1 if result.success else 0,
                failed_uploads=0 if result.success else 1,
                results=[result]
            )
        
        # Split into chunks of 10 (Telegram album limit)
        chunks = [file_paths[i:i+10] for i in range(0, len(file_paths), 10)]
        album_total = len(chunks)
        
        self._logger.info(f"Sending as {len(chunks)} album(s)")
        
        # Upload albums sequentially to preserve order
        all_results: List[UploadResult] = []
        for chunk_idx, chunk in enumerate(chunks):
            start_part = chunk_idx * 10 + 1
            try:
                result = await self._upload_album(
                    stream_info=stream_info,
                    file_paths=chunk,
                    start_part_num=start_part,
                    total_parts=total_parts,
                    duration_str=duration_str if chunk_idx == 0 else "",
                    album_index=chunk_idx + 1,
                    album_total=album_total
                )
            except Exception as e:
                self._logger.error(f"Album upload error: {e}")
                continue
            
            if isinstance(result, list):
                all_results.extend(result)
        
        successful = sum(1 for r in all_results if r.success)
        failed = total_parts - successful
        
        self._logger.info(
            f"Upload complete: {successful}/{total_parts} successful"
        )
        
        return BatchUploadResult(
            channel=stream_info.channel,
            stream_info=stream_info,
            total_parts=total_parts,
            successful_uploads=successful,
            failed_uploads=failed,
            results=all_results
        )
    
    async def _upload_album(
        self,
        stream_info: StreamInfo,
        file_paths: List[str],
        start_part_num: int,
        total_parts: int,
        duration_str: str = "",
        album_index: int = 1,
        album_total: int = 1,
        compressed_note: str = "",
        max_retries: int = 5
    ) -> List[UploadResult]:
        """
        Upload files as a single album (max 10 files) with retry logic.
        
        Args:
            stream_info: Stream information.
            file_paths: Files for this album (max 10).
            start_part_num: Starting part number.
            total_parts: Total number of parts.
            duration_str: Duration string for first caption.
            max_retries: Maximum upload retry attempts.
            
        Returns:
            List of UploadResult for each file.
        """
        if not self._client:
            return [UploadResult(f, None, False, "Not connected") for f in file_paths]
        
        from telethon.tl.types import (
            DocumentAttributeVideo, 
            DocumentAttributeFilename,
            InputMediaUploadedDocument
        )
        
        for attempt in range(max_retries):
            # Prepare files with captions, attributes, and thumbnails
            valid_files = []
            captions = []
            thumbnails = []  # Track thumbnails for cleanup
            media_list = []  # List of InputMediaUploadedDocument for album
            
            try:
                # Prepare upload tasks for PARALLEL execution
                async def prepare_file(i: int, file_path: str):
                    path = Path(file_path)
                    if not path.exists():
                        return None
                    
                    part_num = start_part_num + i
                    file_size_mb = path.stat().st_size / (1024 * 1024)
                    
                    # Build caption for the album (only for first item)
                    caption = ""
                    if i == 0:
                        caption = self.build_album_caption(
                            stream_info=stream_info,
                            file_path=file_path,
                            duration_str=duration_str,
                            album_index=album_index,
                            album_total=album_total,
                            compressed_note=compressed_note,
                            part_num=part_num,
                            total_parts=total_parts,
                            file_size_mb=file_size_mb
                        )
                    
                    # Get video metadata
                    metadata = await self._get_video_metadata(file_path)
                    
                    video_attrs = DocumentAttributeVideo(
                        duration=metadata['duration'],
                        w=metadata['width'],
                        h=metadata['height'],
                        supports_streaming=True
                    )
                    
                    filename_attr = DocumentAttributeFilename(file_name=path.name)
                    
                    # Generate thumbnail
                    thumb = await self._generate_thumbnail(file_path, metadata['duration'])
                    
                    # Upload the file with retry for network issues
                    self._logger.debug(f"Uploading file for album: {path.name} (attempt {attempt + 1})")
                    
                    upload_success = False
                    uploaded_file = None
                    for upload_attempt in range(3):
                        try:
                            # Use FastTelethonhelper if available (7x faster)
                            if self._fast_upload_enabled:
                                uploaded_file = await fast_upload(
                                    self._client,
                                    file_path,
                                    reply=None,
                                    name=path.name,
                                    progress_bar_function=None
                                )
                            else:
                                uploaded_file = await self._client.upload_file(
                                    file_path,
                                    progress_callback=self._make_rate_limited_progress()
                                )
                            upload_success = True
                            break
                        except Exception as upload_err:
                            if upload_attempt < 2:
                                wait_time = (upload_attempt + 1) * 10
                                self._logger.warning(f"Upload error for {path.name}: {upload_err}, retrying in {wait_time}s...")
                                await asyncio.sleep(wait_time)
                            else:
                                raise upload_err
                    
                    if not upload_success or not uploaded_file:
                        raise Exception(f"Failed to upload {path.name}")
                    
                    # Upload thumbnail if exists
                    uploaded_thumb = None
                    if thumb and Path(thumb).exists():
                        try:
                            uploaded_thumb = await self._client.upload_file(thumb)
                        except Exception:
                            pass  # Thumbnail is optional
                    
                    # Create InputMediaUploadedDocument
                    media = InputMediaUploadedDocument(
                        file=uploaded_file,
                        mime_type='video/mp4',
                        attributes=[video_attrs, filename_attr],
                        thumb=uploaded_thumb,
                        force_file=False
                    )
                    
                    return {
                        'file_path': file_path,
                        'caption': caption,
                        'thumb': thumb,
                        'media': media,
                        'index': i
                    }
                
                # Run file uploads with configured limited concurrency
                upload_semaphore = asyncio.Semaphore(self.upload_parallelism)
                
                async def limited_prepare(i, fp):
                    async with upload_semaphore:
                        return await prepare_file(i, fp)
                
                self._logger.info(
                    f"Uploading {len(file_paths)} files (max {self.upload_parallelism} parallel)..."
                )
                tasks = [limited_prepare(i, fp) for i, fp in enumerate(file_paths)]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results (maintain order)
                for result in sorted([r for r in results if r and not isinstance(r, Exception)], key=lambda x: x['index']):
                    valid_files.append(result['file_path'])
                    # Caption только для первого видео в альбоме
                    captions.append(result['caption'] if len(valid_files) == 1 else "")
                    thumbnails.append(result['thumb'])
                    media_list.append(result['media'])
                
                # Check for errors
                for result in results:
                    if isinstance(result, Exception):
                        raise result
                
                if not valid_files:
                    return []
                
                # Send all media as an album
                self._logger.info(f"Sending album with {len(media_list)} files...")
                
                messages = await self._client.send_file(
                    self.channel_id,
                    media_list,
                    caption=captions
                )
                
                # Ensure messages is a list
                if not isinstance(messages, list):
                    messages = [messages]
                
                self._logger.info(f"Album uploaded: {len(messages)} files with metadata and thumbnails")
                
                results = [
                    UploadResult(
                        file_path=valid_files[i],
                        message_id=msg.id if i < len(messages) else None,
                        success=True
                    )
                    for i, msg in enumerate(messages)
                ]
                
                return results
                
            except Exception as e:
                error_str = str(e)
                self._logger.warning(f"Album upload attempt {attempt + 1}/{max_retries} failed: {error_str}")
                
                # Cleanup thumbnails on failure
                for thumb in thumbnails:
                    if thumb and Path(thumb).exists():
                        try:
                            Path(thumb).unlink()
                        except:
                            pass
                
                # Check if retryable error
                retryable_errors = [
                    "Failed to upload file part",
                    "Timeout",
                    "Connection",
                    "FLOOD_WAIT",
                    "SERVER_ERROR",
                    "RPC_CALL_FAIL",
                    "missing from storage",  # Part X of the file is missing from storage
                    "FILE_PART",  # Various file part errors
                    "NETWORK",
                    "502",
                    "503",
                    "500"
                ]
                
                is_retryable = any(err in error_str for err in retryable_errors)
                
                if is_retryable and attempt < max_retries - 1:
                    # Handle FLOOD_WAIT specially - extract wait time
                    if "FLOOD_WAIT" in error_str or "FloodWait" in error_str:
                        import re
                        match = re.search(r'(\d+)\s*(second|sec|s)', error_str, re.IGNORECASE)
                        if match:
                            wait_time = int(match.group(1)) + 5  # Add 5s buffer
                        else:
                            # Try to get from exception if it's FloodWaitError
                            wait_time = getattr(e, 'seconds', 60) + 5
                        self._logger.warning(f"Rate limited! Waiting {wait_time}s...")
                    else:
                        wait_time = (attempt + 1) * 60  # 60s, 120s, 180s, 240s
                    
                    self._logger.info(f"Retrying album upload in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    self._logger.error(f"Album upload failed: {e}")
                    return [
                        UploadResult(f, None, False, str(e))
                        for f in file_paths
                    ]
            finally:
                # Cleanup thumbnails
                for thumb in thumbnails:
                    if thumb and Path(thumb).exists():
                        try:
                            Path(thumb).unlink()
                        except:
                            pass
        
        # Should not reach here, but just in case
        return [UploadResult(f, None, False, "Max retries exceeded") for f in file_paths]
    
    async def send_text_message(self, text: str) -> Optional[int]:
        """
        Send a text message to the channel.
        
        Args:
            text: Message text.
            
        Returns:
            Message ID or None on error.
        """
        if not self._client:
            return None
        
        try:
            message = await self._client.send_message(
                self.channel_id,
                text
            )
            return message.id
        except Exception as e:
            self._logger.error(f"Failed to send message: {e}")
            return None
    
    async def send_waiting_message(
        self,
        channel: str,
        title: str,
        category: str
    ) -> Optional[int]:
        """
        Send "waiting for stream" message.
        
        Args:
            channel: Channel name.
            title: Stream title.
            category: Stream category.
            
        Returns:
            Message ID.
        """
        text = (
            f"⏳ **Ожидание стрима**\n\n"
            f"Канал: **{channel}**\n"
            f"Название: **{title}**\n"
            f"Категория: **{category}**"
        )
        return await self.send_text_message(text)
    
    async def update_waiting_message(
        self,
        message_id: int,
        channel: str,
        title: str,
        category: str
    ) -> bool:
        """Update waiting message with new info."""
        if not self._client:
            return False
        
        text = (
            f"⏳ **Ожидание стрима**\n\n"
            f"Канал: **{channel}**\n"
            f"Название: **{title}**\n"
            f"Категория: **{category}**"
        )
        
        try:
            await self._client.edit_message(
                self.channel_id,
                message_id,
                text
            )
            return True
        except Exception as e:
            self._logger.warning(f"Failed to update waiting message: {e}")
            return False
    
    async def send_status_message(
        self,
        channel: str,
        title: str,
        category: str,
        status: str = "🔴 Запись"
    ) -> Optional[int]:
        """
        Send status message during recording.
        
        Args:
            channel: Channel name.
            title: Stream title.
            category: Category name.
            status: Current status text.
            
        Returns:
            Message ID.
        """
        text = (
            f"Канал: **{channel}**\n"
            f"Стрим: **{title}**\n"
            f"Категория: **{category}**\n\n"
            f"Статус: **{status}**"
        )
        return await self.send_text_message(text)
    
    async def update_status(
        self,
        message_id: int,
        channel: str,
        title: str,
        category: str,
        status: str
    ) -> bool:
        """Update status message."""
        if not self._client:
            return False
        
        text = (
            f"Канал: **{channel}**\n"
            f"Стрим: **{title}**\n"
            f"Категория: **{category}**\n\n"
            f"Статус: **{status}**"
        )
        
        try:
            await self._client.edit_message(
                self.channel_id,
                message_id,
                text
            )
            return True
        except Exception as e:
            self._logger.warning(f"Failed to update status: {e}")
            return False
    
    async def delete_message(self, message_id: int) -> bool:
        """Delete a message by ID."""
        if not self._client:
            return False
        
        try:
            await self._client.delete_messages(
                self.channel_id,
                message_id
            )
            return True
        except Exception as e:
            self._logger.warning(f"Failed to delete message: {e}")
            return False

    def build_album_caption(
        self,
        stream_info: StreamInfo,
        file_path: str,
        duration_str: str,
        album_index: int,
        album_total: int,
        compressed_note: str = "",
        part_num: Optional[int] = None,
        total_parts: Optional[int] = None,
        file_size_mb: Optional[float] = None
    ) -> str:
        """Build caption for the first item of an album."""
        if file_size_mb is None:
            path = Path(file_path)
            file_size_mb = path.stat().st_size / (1024 * 1024) if path.exists() else 0
        
        caption = self._format_caption(
            stream_info=stream_info,
            part_num=part_num,
            total_parts=total_parts,
            file_size_mb=file_size_mb,
            duration_str=duration_str
        )
        
        if album_total > 1:
            caption += f"\n\nЧасть {album_index}/{album_total}"
        
        if compressed_note:
            caption += f"\n{compressed_note}"
        
        return caption

    async def update_message_caption(self, message_id: int, text: str) -> bool:
        """Update caption/text for a message."""
        if not self._client:
            return False
        
        try:
            await self._client.edit_message(
                self.channel_id,
                message_id,
                text
            )
            return True
        except Exception as e:
            self._logger.warning(f"Failed to update message caption: {e}")
            return False
    
    async def forward_to_backup(self, message_ids: List[int]) -> List[int]:
        """
        Forward messages to backup channel.
        
        Args:
            message_ids: IDs of messages to forward.
            
        Returns:
            List of forwarded message IDs.
        """
        if not self._client or not self.backup_channel_id:
            return []
        
        forwarded = []
        
        try:
            for msg_id in message_ids:
                result = await self._client.forward_messages(
                    self.backup_channel_id,
                    msg_id,
                    self.channel_id
                )
                if result:
                    forwarded.append(result.id)
                await asyncio.sleep(0.5)  # Rate limit
            
            self._logger.info(f"Forwarded {len(forwarded)} messages to backup")
            
        except Exception as e:
            self._logger.error(f"Forward failed: {e}")
        
        return forwarded
    
    async def send_to_discussion(
        self,
        reply_to_msg_id: int,
        file_path: str,
        caption: str = ""
    ) -> Optional[int]:
        """
        Send file to discussion/comments of a post.
        
        Args:
            reply_to_msg_id: Message ID to reply to.
            file_path: Path to file.
            caption: Optional caption.
            
        Returns:
            Message ID of sent file.
        """
        if not self._client:
            return None
        
        try:
            # Get video metadata for this file
            metadata = await self._get_video_metadata(file_path)
            
            from telethon.tl.types import DocumentAttributeVideo
            
            video_attrs = DocumentAttributeVideo(
                duration=metadata['duration'],
                w=metadata['width'],
                h=metadata['height'],
                supports_streaming=True
            )
            
            # Generate thumbnail
            thumb = await self._generate_thumbnail(file_path, metadata['duration'])
            
            async def send_to_target(target_id: int, use_comment_to: bool, send_caption: str):
                send_kwargs = {
                    'caption': send_caption,
                    'supports_streaming': True,
                    'attributes': [video_attrs],
                    'thumb': thumb,
                }
                if use_comment_to:
                    send_kwargs['comment_to'] = reply_to_msg_id
                
                if self._fast_upload_enabled:
                    uploaded_file = await fast_upload(
                        self._client,
                        file_path,
                        reply=None,
                        name=Path(file_path).name,
                        progress_bar_function=None
                    )
                    return await self._client.send_file(
                        target_id,
                        uploaded_file,
                        **send_kwargs
                    )
                return await self._client.send_file(
                    target_id,
                    file_path,
                    progress_callback=self._make_rate_limited_progress(),
                    **send_kwargs
                )
            
            try:
                try:
                    base_caption = caption or "📦 Сжатая версия"
                    
                    # Preferred mode: comment to channel post.
                    message = await send_to_target(
                        target_id=self.channel_id,
                        use_comment_to=True,
                        send_caption=base_caption
                    )
                except Exception as primary_error:
                    # Optional fallback: send to explicit discussion group if configured.
                    if not self.discussion_group_id:
                        raise primary_error
                    self._logger.warning(
                        f"Comment send failed, falling back to discussion group {self.discussion_group_id}: {primary_error}"
                    )
                    fallback_caption = (
                        f"{caption or '📦 Сжатая версия'}\n\n"
                        f"(исходный пост: {reply_to_msg_id})"
                    )
                    message = await send_to_target(
                        target_id=self.discussion_group_id,
                        use_comment_to=False,
                        send_caption=fallback_caption
                    )
                
                self._logger.info("Sent compressed file as comment to post")
                return message.id
            finally:
                if thumb and Path(thumb).exists():
                    Path(thumb).unlink()
            
        except Exception as e:
            self._logger.error(f"Failed to send to discussion: {e}")
            return None
    
    def format_recording_caption(
        self,
        channel: str,
        title: str,
        date: str
    ) -> str:
        """
        Format caption for final recording upload.
        
        Args:
            channel: Channel name.
            title: Stream title.
            date: Formatted date string.
            
        Returns:
            Formatted caption.
        """
        return (
            f"Канал: {channel}\n"
            f"Дата: {date}\n"
            f"Стрим: {title}"
        )


async def main():
    """Test the Telegram uploader."""
    import sys
    from .logger import setup_logging
    from .config import load_config
    
    setup_logging(level="INFO")
    
    # Load config
    try:
        config = load_config("config.yaml")
    except FileNotFoundError:
        print("Please create config.yaml first")
        sys.exit(1)
    
    uploader = TelegramUploader(
        api_id=config.telegram.api_id,
        api_hash=config.telegram.api_hash,
        channel_id=config.telegram.channel_id,
        session_name=config.telegram.session_name
    )
    
    # Connect
    if not await uploader.connect():
        print("Failed to connect to Telegram")
        sys.exit(1)
    
    print(f"Connected! Premium: {uploader.is_premium}")
    print(f"Max file size: {uploader.max_file_size_gb} GB")
    
    # Test sending a message
    msg_id = await uploader.send_text_message(
        "🔧 Stream Recorder test message"
    )
    print(f"Sent test message: {msg_id}")
    
    await uploader.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
