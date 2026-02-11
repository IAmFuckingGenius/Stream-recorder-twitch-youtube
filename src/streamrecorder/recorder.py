"""
Stream recorder module for Twitch Stream Recorder.
Records live streams using yt-dlp with robust error handling.
"""

import asyncio
import logging
import os
import re
import signal
import subprocess
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

from .logger import get_channel_logger, get_logger
from .stream_monitor import StreamInfo


class RecordingStatus(Enum):
    """Recording status enumeration."""
    PENDING = "pending"
    RECORDING = "recording"
    PAUSED = "paused"        # Stream dropped, waiting to resume
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class RecordingResult:
    """Result of a recording session."""
    channel: str
    stream_info: StreamInfo
    output_path: str
    status: RecordingStatus
    duration_seconds: float
    file_size_bytes: int
    started_at: datetime
    ended_at: datetime
    error: Optional[str] = None
    
    @property
    def duration_formatted(self) -> str:
        """Get human-readable duration."""
        hours = int(self.duration_seconds // 3600)
        minutes = int((self.duration_seconds % 3600) // 60)
        seconds = int(self.duration_seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    @property
    def file_size_formatted(self) -> str:
        """Get human-readable file size."""
        size = self.file_size_bytes
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} TB"


class StreamRecorder:
    """
    Records Twitch streams using yt-dlp.
    
    Features:
    - Automatic retry on stream drops
    - Live recording with HLS/MPEGTS
    - Robust error handling
    - Progress tracking
    """
    
    def __init__(
        self,
        output_dir: str = "./recordings",
        temp_dir: str = "./temp",
        format_spec: str = "best",
        retries: int = 10,
        fragment_retries: int = 10,
        live_from_start: bool = False,
        cookies_file: str = "",
        move_atom_to_front: bool = True,
        use_mpegts: bool = False,
        ytdlp_log_noise: bool = False
    ):
        """
        Initialize stream recorder.
        
        Args:
            output_dir: Directory for final recordings.
            temp_dir: Directory for temporary files.
            format_spec: yt-dlp format specification.
            retries: Number of download retries.
            fragment_retries: Number of fragment retries.
            live_from_start: Try to record from stream start.
            cookies_file: Path to Netscape cookies file.
            move_atom_to_front: Move moov atom for fast streaming start.
            ytdlp_log_noise: Include noisy yt-dlp lines (connection/open/progress).
        """
        self.output_dir = Path(output_dir)
        self.temp_dir = Path(temp_dir)
        self.format_spec = format_spec
        self.retries = retries
        self.fragment_retries = fragment_retries
        self.live_from_start = live_from_start
        self.cookies_file = cookies_file
        self.move_atom_to_front = move_atom_to_front
        self.use_mpegts = use_mpegts
        self.ytdlp_log_noise = ytdlp_log_noise
        
        self._logger = get_logger('recorder')
        self._active_recordings: dict[str, asyncio.subprocess.Process] = {}
        
        # Callback for cross mode - fired when yt-dlp starts merging
        self._on_merge_started_callback = None
        
        # Ensure directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def set_on_merge_started(self, callback) -> None:
        """Set callback for when yt-dlp starts merging (cross mode)."""
        self._on_merge_started_callback = callback

    async def _fix_moov_atom(self, file_path: str, logger) -> bool:
        """
        Fix moov atom position for fast video start.
        
        Runs ffmpeg to move moov atom to the beginning of the file.
        This allows streaming to start immediately without downloading entire file.
        
        Args:
            file_path: Path to MP4 file.
            logger: Logger instance.
            
        Returns:
            True if successful.
        """
        logger.info("Fixing moov atom for fast start...")
        
        temp_fixed = f"{file_path}.faststart.mp4"
        
        try:
            process = await asyncio.create_subprocess_exec(
                'ffmpeg', '-y',
                '-thread_queue_size', '4096',
                '-i', file_path,
                '-c', 'copy',  # No re-encoding, just remux
                '-movflags', '+faststart',
                '-threads', '0',
                temp_fixed,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=3600)  # Increase to 1 hour
            
            if process.returncode == 0 and Path(temp_fixed).exists():
                # Replace original with fixed file
                Path(file_path).unlink()
                Path(temp_fixed).rename(file_path)
                logger.info("Moov atom fixed successfully")
                return True
            else:
                logger.warning(f"Failed to fix moov atom: {stderr.decode()[:200]}")
                # Cleanup temp file if exists
                if Path(temp_fixed).exists():
                    Path(temp_fixed).unlink()
                return False
                
        except asyncio.TimeoutError:
            logger.warning("Timeout fixing moov atom")
            if Path(temp_fixed).exists():
                Path(temp_fixed).unlink()
            return False
        except Exception as e:
            logger.warning(f"Error fixing moov atom: {e}")
            if Path(temp_fixed).exists():
                Path(temp_fixed).unlink()
            return False
    
    async def _merge_fragments(
        self, 
        video_path: Path, 
        audio_path: Path, 
        output_path: Path, 
        logger
    ) -> bool:
        """
        Merge separate video and audio files into a single MP4.
        
        Used when YouTube --live-from-start is interrupted.
        
        Args:
            video_path: Path to video file.
            audio_path: Path to audio file.
            output_path: Path for merged output.
            logger: Logger instance.
            
        Returns:
            True if merge was successful.
        """
        try:
            process = await asyncio.create_subprocess_exec(
                'ffmpeg', '-y',
                '-thread_queue_size', '4096',
                '-i', str(video_path),
                '-thread_queue_size', '4096',
                '-i', str(audio_path),
                '-c', 'copy',  # No re-encoding
                '-movflags', '+faststart',
                '-threads', '0',
                str(output_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=14400)  # Increase to 2 hours
            
            if process.returncode == 0 and output_path.exists():
                return True
            else:
                error_lines = stderr.decode('utf-8', errors='ignore').splitlines()
                # Get last few lines for more context
                error_msg = '\n'.join(error_lines[-10:])
                logger.error(f"FFmpeg merge failed:\n{error_msg}")
                return False
                
        except asyncio.TimeoutError:
            logger.error("Timeout during fragment merge")
            return False
        except Exception as e:
            logger.error(f"Error merging fragments: {e}")
            return False
    
    async def _concatenate_fragments(
        self,
        frag_files: list,
        output_path: Path,
        logger
    ) -> bool:
        """
        Concatenate -Frag files into a single file.
        
        Used when yt-dlp was interrupted before it could combine fragments.
        
        Args:
            frag_files: List of fragment file paths, sorted.
            output_path: Path for the concatenated output.
            logger: Logger instance.
            
        Returns:
            True if concatenation was successful.
        """
        if not frag_files:
            return False
        
        try:
            # Binary concatenation for media fragments
            with open(output_path, 'wb') as outfile:
                for frag in frag_files:
                    if frag.exists() and frag.stat().st_size > 0:
                        with open(frag, 'rb') as infile:
                            outfile.write(infile.read())
            
            if output_path.exists() and output_path.stat().st_size > 0:
                logger.info(f"Concatenated {len(frag_files)} fragments ({output_path.stat().st_size / 1024 / 1024:.1f} MB)")
                return True
            else:
                logger.error("Concatenation resulted in empty file")
                return False
                
        except Exception as e:
            logger.error(f"Error concatenating fragments: {e}")
            return False
    
    async def _cleanup_temp_fragments(self, base_name: str, logger) -> None:
        """
        Cleanup temporary fragment files after successful merge.
        
        Args:
            base_name: Base name of the temp file (without extension).
            logger: Logger instance.
        """
        basename_only = Path(base_name).name
        cleanup_count = 0
        
        # Cleanup video/audio fragment files
        for pattern in [f"{basename_only}*.mp4", f"{basename_only}*.webm", f"{basename_only}*.m4a"]:
            for f in self.temp_dir.glob(pattern):
                try:
                    f.unlink()
                    cleanup_count += 1
                except:
                    pass
        
        # Cleanup .ytdl files
        for ytdl in self.temp_dir.glob(f"{basename_only}*.ytdl"):
            try:
                ytdl.unlink()
                cleanup_count += 1
            except:
                pass
        
        # Cleanup -Frag files
        for frag in self.temp_dir.glob(f"{basename_only}*-Frag*"):
            try:
                frag.unlink()
                cleanup_count += 1
            except:
                pass
        
        if cleanup_count > 0:
            logger.debug(f"Cleaned up {cleanup_count} temp files")
    
    def strip_title_clock(self, title: str) -> str:
        """Strip trailing timestamps like 2026-01-14 05:28 from title."""
        # Pattern for YYYY-MM-DD HH:MM or similar at the end
        pattern = r'\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:?\d{0,2}$'
        return re.sub(pattern, '', title).strip()

    def generate_filename(self, stream_info: StreamInfo) -> str:
        """Generate output filename for recording."""
        # Sanitize title
        safe_title = re.sub(r'[<>:"/\\|?*\n\r]', '', stream_info.title)
        safe_title = safe_title[:80].strip()
        
        # Format: channel_date_title.ext
        # Use CURRENT time to ensure uniqueness for segments (prevent overwrites)
        date_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        ext = getattr(stream_info, 'output_ext', 'mp4') if not self.use_mpegts else 'ts'
        filename = f"{stream_info.channel}_{date_str}_{safe_title}.{ext}"
        
        return filename
    
    async def record_stream(
        self,
        stream_info: StreamInfo,
        on_progress: Optional[Callable[[str], None]] = None,
        live_from_start: Optional[bool] = None
    ) -> RecordingResult:
        """
        Record a live stream.
        
        Args:
            stream_info: Information about the stream to record.
            on_progress: Optional callback for progress updates.
            live_from_start: Override default live_from_start setting.
            
        Returns:
            RecordingResult with recording details.
        """
        channel = stream_info.channel
        logger = get_channel_logger(channel)
        debug_mode = logger.isEnabledFor(logging.DEBUG)

        def should_log_ytdlp_line(line_text: str) -> bool:
            """Filter noisy yt-dlp/ffmpeg lines, keep only useful ones."""
            lower = line_text.lower()
            
            if self.ytdlp_log_noise and debug_mode:
                return True
            
            keep_phrases = [
                '[download]',
                'error',
                'warning',
                'retry',
                'fragment',
                'merging formats',
                '[merger]',
                'stream is offline',
                'http error',
                '403',
                '404',
                '429',
                'rate limit',
                'flood_wait',
                'input file #0',
                'output file #0',
            ]
            
            drop_phrases = [
                'hls request for url',
                "opening 'http",
                "opening \"http",
                "opening 'https",
                "opening \"https",
                'skip (\'#ext-x-',
                'skip ("#ext-x-',
                'aviocontext',
                'statistics:',
                'starting connection attempt',
                'successfully connected',
                'found duplicated moov atom',
                'program-date-time',
                'daterange',
            ]
            if any(p in lower for p in drop_phrases):
                return False
            
            if any(p in lower for p in keep_phrases):
                return True
            
            # Default: drop anything not explicitly whitelisted above.
            return False
        
        # Determine live_from_start setting
        should_live_from_start = live_from_start if live_from_start is not None else self.live_from_start
        
        # Use yt-dlp for everything
        filename = self.generate_filename(stream_info)
        output_path = self.output_dir / filename
        temp_path = self.temp_dir / f"temp_{filename}"
        
        logger.info(f"Starting recording: {filename}")
        
        started_at = datetime.now()
        status = RecordingStatus.PENDING
        error_msg = None
        
        # Determine if this is a YouTube URL
        is_youtube = 'youtube.com' in stream_info.stream_url or 'youtu.be' in stream_info.stream_url
        
        retry_without_live_start = False
        
        # Retry loop for handling specific errors (like 403 with live-from-start)
        while True:
            # Build yt-dlp command
            cmd = ['yt-dlp']
            
            if debug_mode:
                # More verbose output and newline-separated progress for easier debugging
                cmd.extend(['--verbose', '--newline', '--progress'])
            
            # Output template (use temp directory first)
            cmd.extend(['--output', str(temp_path)])
            
            # Effective live_from_start setting for this attempt
            use_live_from_start = should_live_from_start and not retry_without_live_start
            
            if is_youtube and use_live_from_start:
                # ULTRA-CLEAN command for YouTube live-from-start
                # User reported issues with too many flags stalling the download
                
                # Add cookies if available
                if self.cookies_file and Path(self.cookies_file).exists():
                    cmd.extend(['--cookies', self.cookies_file])
                
                cmd.append('--live-from-start')
                
                if self.use_mpegts:
                    cmd.append('--hls-use-mpegts')
                    
                # Enable multi-threading for the downloader (ffmpeg)
                cmd.extend(['--downloader-args', 'ffmpeg:-threads 0'])
                
                # Pass just the URL
                cmd.append(stream_info.stream_url)
                
                logger.info(f"Running CLEAN command: {' '.join(cmd)}")
                
            else:
                # Ultra-robust command for Twitch/Other
                # Designed to survive brief stream drops and network issues
                
                # AGGRESSIVE retry settings for stream interruptions/lags
                cmd.extend([
                    '--retries', 'infinite',  # Keep retrying forever
                    '--fragment-retries', 'infinite',  # Never give up on fragments
                    '--extractor-retries', '30',  # More extractor retries
                    '--retry-sleep', 'exp=1:300',  # Exponential backoff up to 5 minutes
                    '--retry-sleep', 'fragment:exp=1:120',  # Fragment retry up to 2 minutes
                    '--retry-sleep', 'extractor:exp=5:300',  # Extractor retry up to 5 minutes
                    '--socket-timeout', '120',  # 2 minute socket timeout
                    '--no-mtime',
                    '--ignore-errors',
                    '--no-abort-on-error',
                    '--wait-for-video', '300-900',  # Wait 5-15 minutes for video
                    '--file-access-retries', '30',  # More file retry attempts
                ])
                
                # HLS-specific settings for better stream handling
                cmd.extend([
                    '--hls-prefer-native',  # Use native HLS implementation
                    '--downloader', 'ffmpeg',  # Use ffmpeg for more stable downloads
                    # Reconnect up to 5 minutes on connection issues
                    '--downloader-args', 'ffmpeg:-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 300 -reconnect_on_network_error 1 -reconnect_on_http_error 4xx,5xx -threads 0',
                ])
                
                # Format settings
                cmd.extend(['--format', self.format_spec])
                
                # Use MPEG-TS for resilient recording (handles interruptions better)
                if self.use_mpegts:
                    cmd.append('--hls-use-mpegts')
                    cmd.extend(['--no-part'])  # Don't use .part files with mpegts
                else:
                    cmd.extend(['--no-part'])  # Don't use .part files
                    # Remux to proper MP4 container
                    cmd.extend(['--remux-video', 'mp4'])
                    
                    # Move moov atom for fast start
                    if self.move_atom_to_front:
                        cmd.extend(['--ppa', 'ffmpeg_o:-movflags +faststart -threads 0'])
                
                cmd.extend(['--concurrent-fragments', '4'])
                
                # Optional: record from stream start (works well on Twitch)
                if use_live_from_start:
                    cmd.append('--live-from-start')
                
                # Add cookies if available
                if self.cookies_file and Path(self.cookies_file).exists():
                    cmd.extend(['--cookies', self.cookies_file])
                    
                cmd.append(stream_info.stream_url)
            
            logger.debug(f"Running: {' '.join(cmd)}")
            
            try:
                # Start recording process
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT  # Merge stderr into stdout to avoid pipe deadlock
                )
                
                self._active_recordings[channel] = process
                status = RecordingStatus.RECORDING
                
                logger.info(f"Recording in progress... (live_from_start={use_live_from_start})")
                
                # Read stdout (now contains both stdout and stderr) in real-time
                output_lines = deque(maxlen=2000)  # keep only last lines for errors
                merge_callback_fired = False
                forbidden_error_detected = False
                idle_log_interval = 30  # seconds without output before debug heartbeat
                
                while True:
                    try:
                        line = await asyncio.wait_for(
                            process.stdout.readline(),
                            timeout=idle_log_interval
                        )
                    except asyncio.TimeoutError:
                        # No output for a while; log a heartbeat with file size
                        if process.returncode is not None:
                            break
                        size_mb = None
                        try:
                            if temp_path.exists():
                                size_mb = temp_path.stat().st_size / (1024 * 1024)
                        except Exception:
                            size_mb = None
                        if size_mb is not None:
                            logger.debug(f"yt-dlp: no output for {idle_log_interval}s, temp size={size_mb:.1f} MB")
                        else:
                            logger.debug(f"yt-dlp: no output for {idle_log_interval}s (process running)")
                        continue
                    if not line:
                        break
                    line_text = line.decode('utf-8', errors='ignore').strip()
                    if line_text:
                        output_lines.append(line_text)
                        if should_log_ytdlp_line(line_text):
                            logger.debug(f"yt-dlp: {line_text}")
                        
                        # Detect 403 Forbidden specifically
                        if 'HTTP Error 403: Forbidden' in line_text or 'Server returned 403 Forbidden' in line_text:
                            forbidden_error_detected = True
                        
                        # CROSS MODE: Detect when yt-dlp starts merging (stream has ended)
                        if not merge_callback_fired and ('Merging formats' in line_text or '[Merger]' in line_text):
                            merge_callback_fired = True
                            logger.info("yt-dlp: Merge started (stream ended)")
                            # Fire callback if registered
                            if self._on_merge_started_callback:
                                asyncio.create_task(self._on_merge_started_callback(stream_info))
                
                # Wait for process to finish
                await process.wait()
                stderr = '\n'.join(output_lines)
                
                # Process completed - remove from active recordings
                if channel in self._active_recordings:
                    del self._active_recordings[channel]
                
                # CHECK FOR FALLBACK CONDITION
                # Only retry if command FAILED (non-zero return) WITH 403 AND we were using live_from_start
                # If return code is 0, we assume success even if there was a metadata warning
                if forbidden_error_detected and use_live_from_start and process.returncode != 0:
                    logger.warning("⚠️ 403 Forbidden detected with live-from-start! Retrying standard recording...")
                    retry_without_live_start = True
                    continue  # Loop again with retry_without_live_start=True
                
                if process.returncode == 0:
                    status = RecordingStatus.COMPLETED
                    logger.info("Recording completed successfully")
                else:
                    stderr_text = stderr  # Already a string
                    
                    # Check if stream ended naturally or was stopped by user
                    # SIGINT = -2, SIGTERM = -15, or 255 for quick exit
                    user_stopped = process.returncode in (-2, -15, 255, 130)
                    interrupted = 'Interrupted by user' in stderr_text
                    stream_offline = 'stream is offline' in stderr_text.lower()
                    io_error = 'Input/output error' in stderr_text or 'Error opening input' in stderr_text
                    
                    if stream_offline:
                        status = RecordingStatus.COMPLETED
                        logger.info("Stream ended")
                    elif user_stopped or interrupted:
                        # User stopped - this is a graceful stop, save the file
                        status = RecordingStatus.COMPLETED
                        logger.info("Recording stopped by user - saving file")
                    elif io_error:
                        status = RecordingStatus.COMPLETED
                        logger.info("Recording stopped (quick interrupt)")
                    else:
                        status = RecordingStatus.FAILED
                        error_msg = stderr_text[:500]
                        logger.error(f"Recording failed: {error_msg}")
                
            except asyncio.CancelledError:
                logger.warning("Recording task cancelled - saving partial file")
                # Mark as completed so the file gets processed
                status = RecordingStatus.COMPLETED
                
                # Terminate process if still running
                if channel in self._active_recordings:
                    proc = self._active_recordings[channel]
                    try:
                        proc.terminate()
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            proc.kill()
                            try:
                                await proc.wait()
                            except:
                                pass
                    except ProcessLookupError:
                        pass
                    del self._active_recordings[channel]
                
            except Exception as e:
                status = RecordingStatus.FAILED
                error_msg = str(e)
                logger.error(f"Recording error: {e}")
            
            # Break the loop if we didn't continue earlier
            break
        
        ended_at = datetime.now()
        duration = (ended_at - started_at).total_seconds()
        
        # Move temp file to final location if successful or stopped by user
        file_size = 0
        final_path = str(output_path)
        
        # Try to find the actual file - yt-dlp might sanitize filename differently
        actual_temp_file = None
        
        if temp_path.exists():
            actual_temp_file = temp_path
        else:
            # Search for matching file in temp directory
            channel_safe = stream_info.channel.lower()
            search_patterns = [
                f"temp_{channel_safe}_*.ts",
                f"temp_{channel_safe}_*.mp4",
                f"temp_{channel_safe}_*.mkv",
                f"temp_{channel_safe}_*.ts.mp4",
                f"*{channel_safe}*.ts",
                f"*{channel_safe}*.mp4",
                f"*{channel_safe}*.mkv",
                f"*{channel_safe}*.ts.mp4",
            ]
            
            for pattern in search_patterns:
                matching_files = [f for f in self.temp_dir.glob(pattern) if f.is_file()]
                if matching_files:
                    # Get the most recently modified file
                    matching_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                    actual_temp_file = matching_files[0]
                    logger.info(f"Found temp file by pattern {pattern}: {actual_temp_file.name}")
                    break
        
        if actual_temp_file and actual_temp_file.exists() and actual_temp_file.stat().st_size > 0:
            file_size = actual_temp_file.stat().st_size
            
            # Generate output path based on actual temp file name (strip temp_ prefix)
            actual_output_name = actual_temp_file.name
            if actual_output_name.startswith('temp_'):
                actual_output_name = actual_output_name[5:]  # Remove 'temp_' prefix
            actual_output_path = self.output_dir / actual_output_name
            
            try:
                actual_temp_file.rename(actual_output_path)
                status = RecordingStatus.COMPLETED
                final_path = str(actual_output_path)
                logger.info(f"Saved: {actual_output_path}")
                
                # Fix moov atom
                if self.move_atom_to_front and actual_output_path.suffix.lower() == '.mp4':
                    await self._fix_moov_atom(str(actual_output_path), logger)
            except Exception as move_error:
                logger.error(f"Failed to move file: {move_error}")
                # Keep file in temp location
                final_path = str(actual_temp_file)
                status = RecordingStatus.COMPLETED
                
        elif Path(str(temp_path) + '.part').exists() and Path(str(temp_path) + '.part').stat().st_size > 0:
            part_file = Path(str(temp_path) + '.part')
            file_size = part_file.stat().st_size
            logger.info(f"Found .part file: {part_file.name}")
            part_file.rename(output_path)
            status = RecordingStatus.COMPLETED
            logger.info(f"Saved: {output_path}")
            # Fix moov atom
            if self.move_atom_to_front and output_path.suffix.lower() == '.mp4':
                await self._fix_moov_atom(str(output_path), logger)
        else:
            # Main file doesn't exist or is empty - try recovery logic
            logger.info("Main file not found, trying fragment recovery...")
            recovery_result = await self.recover_interrupted_recording(stream_info, temp_path)
            status = recovery_result.status
            file_size = recovery_result.file_size_bytes
            final_path = recovery_result.output_path
            error_msg = recovery_result.error
        
        return RecordingResult(
            channel=channel,
            stream_info=stream_info,
            output_path=final_path,
            status=status,
            duration_seconds=duration,
            file_size_bytes=file_size,
            started_at=started_at,
            ended_at=ended_at,
            error=error_msg
        )
    
    async def recover_interrupted_recording(
        self,
        stream_info: StreamInfo,
        temp_path: Path
    ) -> RecordingResult:
        """
        Attempt to recover and merge fragments from an interrupted recording.
        
        Args:
            stream_info: Stream info.
            temp_path: Path where the main recording was supposed to be.
            
        Returns:
            RecordingResult.
        """
        channel = stream_info.channel
        logger = get_channel_logger(channel)
        output_path = self.output_dir / self.generate_filename(stream_info)
        
        status = RecordingStatus.FAILED
        file_size = 0
        final_path = str(temp_path)
        error_msg = None
        
        # First, check if a main recording file already exists (various extensions)
        # yt-dlp sometimes creates .ts.mp4 or other double extensions
        temp_path_str = str(temp_path)
        
        # Try to find existing main file with various patterns
        main_file = None
        possible_extensions = ['.ts.mp4', '.mp4', '.ts', '.mkv']
        base_without_ext = temp_path_str.rsplit('.', 1)[0]  # Remove last extension
        
        for ext in possible_extensions:
            candidate = Path(base_without_ext + ext)
            if candidate.exists() and candidate.stat().st_size > 0:
                main_file = candidate
                logger.info(f"Found main recording file: {candidate.name} ({candidate.stat().st_size / 1024 / 1024:.1f} MB)")
                break
        
        # Also try globbing for files with similar name pattern
        if not main_file:
            date_prefix = stream_info.started_at.strftime('%Y-%m-%d')
            for pattern in [f"temp_{channel}_{date_prefix}*.mp4", f"temp_{channel}_{date_prefix}*.ts", f"temp_{channel}_{date_prefix}*.ts.mp4"]:
                candidates = [f for f in self.temp_dir.glob(pattern) if f.stat().st_size > 100_000_000]  # > 100 MB
                # Exclude fragment files
                candidates = [f for f in candidates if '.f299.' not in f.name and '.f140.' not in f.name and '-Frag' not in f.name]
                if candidates:
                    main_file = max(candidates, key=lambda x: x.stat().st_size)  # Take largest
                    logger.info(f"Found main file via glob: {main_file.name} ({main_file.stat().st_size / 1024 / 1024:.1f} MB)")
                    break
        
        if main_file:
            # Main file exists! Just move it to output
            file_size = main_file.stat().st_size
            
            # SAFETY: Don't move .temp files - they're incomplete merges
            if '.temp.' in main_file.name:
                logger.warning(f"Found INCOMPLETE merge file: {main_file.name} - keeping fragments as backup")
                # Don't delete fragments, mark as failed
                return RecordingResult(
                    channel=channel,
                    stream_info=stream_info,
                    output_path=str(main_file),
                    status=RecordingStatus.FAILED,
                    duration_seconds=0,
                    file_size_bytes=file_size,
                    started_at=datetime.now(),
                    ended_at=datetime.now(),
                    error="Merge was interrupted - file incomplete"
                )
            
            main_file.rename(output_path)
            final_path = str(output_path)
            status = RecordingStatus.COMPLETED
            logger.info(f"Moved recording to: {output_path}")
            
            # Fix moov atom if it's an MP4
            if self.move_atom_to_front and output_path.suffix.lower() == '.mp4':
                await self._fix_moov_atom(str(output_path), logger)
            
            # DON'T cleanup fragments - keep as backup until successful upload
            # await self._cleanup_temp_fragments(base_without_ext, logger)
            
            return RecordingResult(
                channel=channel,
                stream_info=stream_info,
                output_path=final_path,
                status=status,
                duration_seconds=0,
                file_size_bytes=file_size,
                started_at=datetime.now(),
                ended_at=datetime.now(),
                error=error_msg
            )
        
        # No main file found - try YouTube fragment recovery
        base_name = temp_path_str.rsplit('.', 1)[0]  # Remove extension for fragment matching
        
        # Find video and audio fragments
        video_fragment = None
        audio_fragment = None
        
        # Try to find or build video fragment
        # 1. Try exact matches first
        for suffix in ['.f299.mp4', '.f298.mp4', '.f303.mp4', '.f302.mp4', '.f313.mp4', '.f308.mp4', '.f137.mp4', '.f136.mp4']:
            for ext in ['', '.part']:
                candidate = Path(base_name + suffix + ext)
                if candidate.exists() and candidate.stat().st_size > 0:
                    video_fragment = candidate
                    break
            if video_fragment:
                break
                
        # 2. Try globbing by channel and date if exact match fails
        if not video_fragment:
            date_prefix = stream_info.started_at.strftime('%Y-%m-%d')
            # Look for "temp_{channel}_{date}*.mp4"
            pattern = f"temp_{channel}_{date_prefix}*.mp4"
            candidates = sorted(list(self.temp_dir.glob(pattern)), key=lambda x: x.stat().st_size, reverse=True)
            
            for candidate in candidates:
                # Basic check to ensure it's a YouTube fragment (has .fNNN.)
                if any(f".{code}." in candidate.name for code in ['f299', 'f298', 'f303', 'f302', 'f313', 'f308', 'f137', 'f136']):
                    video_fragment = candidate
                    logger.info(f"Found video fragment via glob: {candidate.name}")
                    break

        # Similarly for audio
        for suffix in ['.f140.mp4', '.f251.webm', '.f141.mp4', '.f139.mp4', '.f250.webm', '.f249.webm']:
            for ext in ['', '.part']:
                candidate = Path(base_name + suffix + ext)
                if candidate.exists() and candidate.stat().st_size > 0:
                    audio_fragment = candidate
                    break
            if audio_fragment:
                break
                
        if not audio_fragment:
            date_prefix = stream_info.started_at.strftime('%Y-%m-%d')
            pattern = f"temp_{channel}_{date_prefix}*"
            candidates = sorted(list(self.temp_dir.glob(pattern)), key=lambda x: x.stat().st_size, reverse=True)
            
            for candidate in candidates:
                if any(f".{code}." in candidate.name for code in ['f140', 'f251', 'f141', 'f139', 'f250', 'f249']):
                    audio_fragment = candidate
                    logger.info(f"Found audio fragment via glob: {candidate.name}")
                    break
        
        if video_fragment and audio_fragment:
            logger.info(f"Found fragments: {video_fragment.name}, {audio_fragment.name}")
            logger.info("Merging video and audio fragments...")
            
            # Merge with ffmpeg
            merged = await self._merge_fragments(
                video_fragment, audio_fragment, output_path, logger
            )
            
            if merged:
                file_size = output_path.stat().st_size
                final_path = str(output_path)
                status = RecordingStatus.COMPLETED
                logger.info(f"Merged and saved: {output_path} ({file_size / 1024 / 1024:.1f} MB)")
                
                # Cleanup fragments and temp files
                await self._cleanup_temp_fragments(base_name, logger)
            else:
                logger.error("Failed to merge fragments")
                status = RecordingStatus.FAILED
                error_msg = "Failed to merge fragments"
        elif video_fragment:
            # Only video, no audio - still save it
            logger.warning("Only video fragment found, saving without audio")
            video_fragment.rename(output_path)
            file_size = output_path.stat().st_size
            final_path = str(output_path)
            status = RecordingStatus.COMPLETED
            await self._cleanup_temp_fragments(base_name, logger)
        else:
            logger.info("Recovery check: No recording fragments found (clean)")
            # IMPORTANT: Set final_path to empty string so main.py knows nothing was recovered
            final_path = ""
            status = RecordingStatus.COMPLETED
            error_msg = "No fragments found (clean)"
            
        return RecordingResult(
            channel=channel,
            stream_info=stream_info,
            output_path=final_path,
            status=status,
            duration_seconds=0,
            file_size_bytes=file_size,
            started_at=datetime.now(),
            ended_at=datetime.now(),
            error=error_msg
        )

    async def _is_stream_live(self, stream_url: str) -> bool:
        """Best-effort check if a stream URL is live."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._extract_is_live, stream_url)

    def _extract_is_live(self, url: str) -> bool:
        """Blocking extraction to determine live status via yt-dlp."""
        try:
            import yt_dlp
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'skip_download': True,
                'ignoreerrors': True,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
            if not info:
                return False
            if info.get('is_live'):
                return True
            return info.get('live_status') == 'is_live'
        except Exception:
            return False

    async def record_with_recovery(
        self,
        stream_info: StreamInfo,
        max_retries: int = 3,
        retry_delay: int = 30
    ) -> RecordingResult:
        """
        Record stream with automatic recovery on failure.
        
        If recording fails but stream is still live, retry.
        Concatenates multiple recording segments if needed.
        
        Args:
            stream_info: Stream information.
            max_retries: Maximum recovery attempts.
            retry_delay: Seconds to wait before retry.
            
        Returns:
            RecordingResult for the recording session.
        """
        logger = get_channel_logger(stream_info.channel)
        results: list[RecordingResult] = []
        
        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.info(f"Retry attempt {attempt}/{max_retries}")
                await asyncio.sleep(retry_delay)
            
            result = await self.record_stream(stream_info)
            results.append(result)
            
            if result.status == RecordingStatus.COMPLETED:
                break
            
            if result.status == RecordingStatus.CANCELLED:
                break
            
            # Check if stream is still live before retry
            is_live = await self._is_stream_live(stream_info.stream_url)
            
            if not is_live:
                logger.info("Stream is offline, no retry needed")
                # Mark as completed if we have some content
                if result.file_size_bytes > 0:
                    result.status = RecordingStatus.COMPLETED
                break
        
        # Return the last result (most complete)
        return results[-1] if results else RecordingResult(
            channel=stream_info.channel,
            stream_info=stream_info,
            output_path="",
            status=RecordingStatus.FAILED,
            duration_seconds=0,
            file_size_bytes=0,
            started_at=datetime.now(),
            ended_at=datetime.now(),
            error="No recording attempts made"
        )
    
    async def stop_recording(self, channel: str) -> bool:
        """
        Stop an active recording.
        
        Args:
            channel: Channel name to stop recording.
            
        Returns:
            True if recording was stopped, False if not found.
        """
        if channel not in self._active_recordings:
            return False
        
        logger = get_channel_logger(channel)
        logger.info("Stopping recording...")
        
        process = self._active_recordings[channel]
        
        try:
            # Send SIGINT for graceful shutdown
            process.send_signal(signal.SIGINT)
            
            try:
                await asyncio.wait_for(process.wait(), timeout=10)
                logger.info("Recording stopped gracefully")
            except asyncio.TimeoutError:
                logger.warning("Timeout, sending SIGTERM...")
                process.terminate()
                
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    logger.warning("Force killing process...")
                    process.kill()
                    await process.wait()
        except ProcessLookupError:
            # Process already dead
            pass
        finally:
            # Remove from active recordings
            if channel in self._active_recordings:
                del self._active_recordings[channel]
        
        return True
    
    def get_active_recordings(self) -> list[str]:
        """Get list of channels currently being recorded."""
        return list(self._active_recordings.keys())


async def main():
    """Test the stream recorder."""
    from .logger import setup_logging
    from .stream_monitor import StreamInfo
    from .twitch_api import TwitchAPI
    import os
    
    setup_logging(level="INFO")
    
    # Test recording with a known channel
    test_channel = "shroud"  # Change to an active channel
    
    client_id = os.getenv("TWITCH_CLIENT_ID", "")
    client_secret = os.getenv("TWITCH_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        print("Set TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET to run this test")
        return
    
    api = TwitchAPI(client_id=client_id, client_secret=client_secret)
    
    if not await api.connect():
        print("Failed to connect to Twitch API")
        return
    
    print(f"Checking if {test_channel} is live...")
    stream_data = await api.get_stream(test_channel)
    stream_info = StreamInfo.from_api(stream_data) if stream_data else None
    
    if stream_info:
        print(f"Stream is live! Starting test recording (10 seconds)...")
        
        recorder = StreamRecorder(
            output_dir="./test_recordings",
            temp_dir="./test_temp"
        )
        
        # Record for 10 seconds then cancel
        async def record_with_timeout():
            task = asyncio.create_task(recorder.record_stream(stream_info))
            await asyncio.sleep(10)
            await recorder.stop_recording(test_channel)
            return await task
        
        result = await record_with_timeout()
        
        print(f"\nRecording result:")
        print(f"  Status: {result.status.value}")
        print(f"  Duration: {result.duration_formatted}")
        print(f"  Size: {result.file_size_formatted}")
        print(f"  Path: {result.output_path}")
    else:
        print(f"{test_channel} is offline")
    
    await api.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
