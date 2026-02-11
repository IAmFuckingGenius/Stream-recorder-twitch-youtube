"""
Stream Recorder v2 - Main Orchestrator.

Coordinates all modules for complete stream recording workflow:
1. Monitor Twitch/YouTube channels
2. Send waiting/status messages to Telegram
3. Record streams with yt-dlp
4. Process, split, upload, compress
5. Forward to backup, send compressed to comments
6. Persist state for crash recovery
"""

import asyncio
import hashlib
import signal
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from .config import Config, load_config
from .logger import setup_logging, get_logger, get_channel_logger
from .twitch_api import TwitchAPI
from .stream_monitor import StreamEvent, StreamInfo, MonitorEvent
from .platform_monitor import PlatformMonitor
from .state_manager import StateManager, StreamState, StreamStatus
from .recorder import StreamRecorder, RecordingResult, RecordingStatus
from .splitter import FileSplitter
from .uploader import TelegramUploader, UploadResult, BatchUploadResult
from .compressor import VideoCompressor


# Status icons for Telegram messages
STATUS_ICONS = {
    StreamStatus.WAITING: "⏳ Ожидание",
    StreamStatus.RECORDING: "🔴 Запись",
    StreamStatus.PROCESSING: "⚙️ Обработка",
    StreamStatus.UPLOADING: "📤 Отправка",
    StreamStatus.COMPRESSING: "📦 Сжатие",
    StreamStatus.SENDING_COMPRESSED: "💬 Комментарии",
    StreamStatus.FORWARDING: "🔄 Бэкап",
    StreamStatus.DONE: "✅ Готово",
    StreamStatus.ERROR: "❌ Ошибка",
}


class StreamRecorderApp:
    """
    Main application orchestrating stream recording workflow.
    
    Handles:
    - Stream monitoring via Twitch API
    - Recording lifecycle management
    - Telegram status messages
    - State persistence and recovery
    """
    
    def __init__(self, config: Config):
        """Initialize application with configuration."""
        self.config = config
        self._logger = get_logger('app')
        
        # Initialize Twitch API if needed
        self.twitch_api = None
        if config.platform in ("twitch", "both", "cross") and config.twitch.client_id:
            self.twitch_api = TwitchAPI(
                client_id=config.twitch.client_id,
                client_secret=config.twitch.client_secret,
                oauth_token=config.twitch.oauth_token or None
            )
        
        # Initialize platform monitor (supports Twitch and YouTube)
        self.monitor = PlatformMonitor(config, self.twitch_api)
        
        self.recorder = StreamRecorder(
            output_dir=config.recording.output_dir,
            temp_dir=config.recording.temp_dir,
            format_spec=config.recording.format,
            retries=config.recording.retries,
            fragment_retries=config.recording.fragment_retries,
            live_from_start=config.recording.live_from_start,
            cookies_file=config.recording.cookies_file,
            move_atom_to_front=config.recording.move_atom_to_front,
            use_mpegts=config.recording.use_mpegts,
            ytdlp_log_noise=config.logging.ytdlp_log_noise
        )
        
        self.splitter = FileSplitter(
            output_dir=config.recording.temp_dir
        )
        
        self.uploader = TelegramUploader(
            api_id=config.telegram.api_id,
            api_hash=config.telegram.api_hash,
            channel_id=config.telegram.channel_id,
            backup_channel_id=config.telegram.backup_channel_id,
            discussion_group_id=config.telegram.discussion_group_id,
            session_name=config.telegram.session_name,
            use_fast_upload_helper=config.telegram.use_fast_upload_helper,
            upload_parallelism=config.telegram.upload_parallelism,
            upload_speed_limit_mbps=config.telegram.upload_speed_limit_mbps
        )
        
        self.compressor = VideoCompressor(
            audio_bitrate_kbps=config.compression.audio_bitrate_kbps,
            two_pass=config.compression.two_pass
        )
        
        self.state = StateManager(
            state_file=config.state.state_file
        )
        
        # Runtime state
        self._shutdown_event = asyncio.Event()
        self._shutting_down = False  # Flag to indicate graceful shutdown
        self._active_recordings: Dict[str, asyncio.Task] = {}
        # Channels where monitor already confirmed end (skip extra grace wait)
        self._skip_grace_for: set[str] = set()
        # Track recovery tasks to avoid duplicate recovery per channel
        self._recovery_tasks: Dict[str, asyncio.Task] = {}
        # Per-channel processing guard to avoid state corruption on new stream start.
        self._processing_channels: set[str] = set()
        # If stream started while channel processing was busy, re-check after processing completes.
        self._pending_live_recheck: set[str] = set()
        # Cross mode dedup: per YT session key -> Twitch task
        self._cross_twitch_tasks: Dict[str, asyncio.Task] = {}
    
    async def start(self) -> None:
        """Start the application."""
        self._logger.info("Starting Stream Recorder v2...")
        
        # Load persistent state
        await self.state.load()
        await self._cleanup_stale_waiting()
        
        # Connect to Twitch API if configured
        if self.twitch_api:
            if not await self.twitch_api.connect():
                raise RuntimeError("Failed to connect to Twitch API")
        
        # Connect to Telegram
        if not await self.uploader.connect():
            raise RuntimeError("Failed to connect to Telegram")
        
        self._logger.info(
            f"Telegram: {'Premium' if self.uploader.is_premium else 'Regular'} account, "
            f"max file size: {self.uploader.max_file_size_gb} GB"
        )
        
        # Log monitored channels
        all_channels = self.monitor.get_all_channels()
        self._logger.info(
            f"Platform: {self.config.platform}, "
            f"Monitoring {len(all_channels)} channels: {', '.join(all_channels)}"
        )
        
        # CROSS MODE: Register callback to start Twitch when YouTube starts merging
        if self.config.platform == "cross":
            self.recorder.set_on_merge_started(self._on_youtube_merge_started)
            self._logger.info("Cross mode: Will check Twitch when YouTube starts merging")
        
        # Start monitoring first (so we don't miss streams during recovery)
        self.monitor.start()
        
        # Create monitor task - this runs in parallel with everything
        monitor_task = asyncio.create_task(self._run_monitor())
        
        # Recover incomplete operations in background (doesn't block monitoring)
        recovery_task = asyncio.create_task(self._recover_incomplete())
        waiting_cleanup_task = asyncio.create_task(self._cleanup_waiting_loop())
        
        # Wait for shutdown signal
        loop = asyncio.get_event_loop()
        stop_event = asyncio.Event()
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        
        try:
            await stop_event.wait()
        finally:
            self._logger.info("Shutdown signal received...")
            
            # Cancel background tasks
            monitor_task.cancel()
            recovery_task.cancel()
            waiting_cleanup_task.cancel()
            
            for task in [monitor_task, recovery_task, waiting_cleanup_task]:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            await self._cleanup()
    
    async def _run_monitor(self) -> None:
        """Run monitor loop."""
        try:
            async for event in self.monitor.monitor():
                await self._handle_event(event)
        except asyncio.CancelledError:
            pass
    
    async def _handle_shutdown(self) -> None:
        """Handle shutdown signal."""
        self._logger.info("Shutdown signal received...")
        self._shutdown_event.set()
    
    async def _cleanup(self) -> None:
        """Cleanup resources."""
        self._logger.info("Cleaning up...")
        
        # Set shutdown flag - recording tasks will just save files, not upload
        self._shutting_down = True
        
        # Stop monitor first
        self.monitor.stop()
        
        # Stop active recordings gracefully and wait for them to save files
        recording_tasks = list(self._active_recordings.items())
        
        for channel, task in recording_tasks:
            self._logger.info(f"Stopping recording: {channel}")
            try:
                await self.recorder.stop_recording(channel)
            except Exception as e:
                self._logger.warning(f"Error stopping recording {channel}: {e}")
        
        # Wait for recording tasks to save files (not upload - that happens on restart)
        if self._active_recordings:
            self._logger.info("Waiting for recordings to save...")
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(
                        *self._active_recordings.values(),
                        return_exceptions=True
                    ),
                    timeout=3600.0  # Allow up to 1 hour for merging large files during shutdown
                )
                
                # Log results
                for result in results:
                    if isinstance(result, Exception):
                        self._logger.warning(f"Recording task error: {result}")
                    elif hasattr(result, 'output_path') and result.output_path:
                        self._logger.info(f"Recording saved: {result.output_path}")
                        
            except asyncio.TimeoutError:
                self._logger.warning("Timeout waiting for recordings, cancelling tasks...")
                for channel, task in self._active_recordings.items():
                    task.cancel()
        
        # Clear active recordings
        self._active_recordings.clear()
        
        # Disconnect services with timeouts
        try:
            if self.twitch_api:
                await asyncio.wait_for(self.twitch_api.disconnect(), timeout=5.0)
        except Exception as e:
            self._logger.warning(f"Error disconnecting Twitch: {e}")
        
        try:
            await asyncio.wait_for(self.uploader.disconnect(), timeout=5.0)
        except Exception as e:
            self._logger.warning(f"Error disconnecting Telegram: {e}")
        
        self._logger.info("Cleanup complete")

    async def _cleanup_waiting_loop(self) -> None:
        """Periodically drop stale waiting states/messages."""
        try:
            while True:
                await asyncio.sleep(300)  # 5 minutes
                await self._cleanup_stale_waiting()
        except asyncio.CancelledError:
            pass

    async def _cleanup_stale_waiting(self) -> None:
        """Delete waiting entries older than configured timeout."""
        timeout_hours = float(getattr(self.config.state, 'waiting_timeout_hours', 0) or 0)
        if timeout_hours <= 0:
            return
        
        waiting_states = await self.state.get_waiting_streams()
        if not waiting_states:
            return
        
        now = datetime.now()
        timeout_sec = timeout_hours * 3600
        
        for stream_state in waiting_states:
            ts_raw = stream_state.detected_at
            if not ts_raw:
                continue
            try:
                detected_at = datetime.fromisoformat(ts_raw)
            except Exception:
                continue
            
            age_sec = (now - detected_at).total_seconds()
            if age_sec < timeout_sec:
                continue
            
            # Re-check current status before deletion.
            current = await self.state.get_state(stream_state.channel)
            if not current or current.status != StreamStatus.WAITING:
                continue
            
            logger = get_channel_logger(stream_state.channel)
            try:
                if current.message_ids.waiting_msg:
                    await self.uploader.delete_message(current.message_ids.waiting_msg)
            except Exception as e:
                logger.warning(f"Failed to delete stale waiting message: {e}")
            
            await self.state.delete_stream(stream_state.channel)
            logger.info(
                f"Removed stale waiting state after {age_sec / 3600:.1f}h "
                f"(timeout={timeout_hours:.1f}h)"
            )

    async def _process_recording_guarded(self, state: StreamState) -> None:
        """Ensure only one processing pipeline per channel at a time."""
        channel = state.channel
        logger = get_channel_logger(channel)
        
        if channel in self._processing_channels:
            logger.warning("Processing already running for this channel, skipping duplicate trigger")
            return
        
        self._processing_channels.add(channel)
        try:
            await self._process_recording(state)
        finally:
            self._processing_channels.discard(channel)
            if channel in self._pending_live_recheck and not self._shutting_down:
                self._pending_live_recheck.discard(channel)
                asyncio.create_task(self._recheck_live_after_processing(channel))

    async def _recheck_live_after_processing(self, channel: str) -> None:
        """If live event was deferred due to processing, re-check now and resume recording."""
        if self._shutting_down:
            return
        
        logger = get_channel_logger(channel)
        await asyncio.sleep(1)
        
        if channel in self._active_recordings or channel in self._processing_channels:
            return
        
        is_youtube = channel.startswith('@') or channel.startswith('UC')
        if is_youtube:
            live_info = await self.monitor.check_youtube_live(channel)
        else:
            live_info = await self.monitor.check_twitch_live(channel)
        
        if not live_info:
            logger.info("Deferred live recheck: stream is offline")
            return
        
        logger.info("Deferred live recheck: stream is still live, starting recording")
        await self._handle_went_live(MonitorEvent(StreamEvent.WENT_LIVE, channel, live_info))

    def _cross_session_key(self, yt_channel: str, session_id: Optional[str]) -> str:
        """Build unique key for one YouTube session in cross mode."""
        return f"{yt_channel}:{session_id or 'nosession'}"
    
    async def _recover_incomplete(self, only_channel: Optional[str] = None) -> None:
        """Recover incomplete operations from previous run."""
        incomplete = await self.state.get_incomplete_streams()
        
        if only_channel:
            incomplete = [s for s in incomplete if s.channel == only_channel]
        
        if not incomplete:
            return
        
        self._logger.info(f"Recovering {len(incomplete)} incomplete streams...")
        
        for stream_state in incomplete:
            logger = get_channel_logger(stream_state.channel)
            
            try:
                if stream_state.status in (StreamStatus.RECORDING, StreamStatus.ERROR):
                    # Recording was interrupted or failed during merge/process
                    if stream_state.recording_file and Path(stream_state.recording_file).exists():
                        logger.info("Resuming processing of interrupted recording")
                        await self._process_recording_guarded(stream_state)
                    else:
                        # First, search for temp files that might match this channel
                        temp_dir = Path(self.config.recording.temp_dir)
                        channel_safe = stream_state.channel.lower()
                        
                        # Search patterns (support multiple containers)
                        found_temp_file = None
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
                            matching = list(temp_dir.glob(pattern))
                            if matching:
                                matching.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                                found_temp_file = matching[0]
                                break
                        
                        if found_temp_file and found_temp_file.stat().st_size > 0:
                            # Found a temp file! Move it to recordings
                            logger.info(f"Found temp file: {found_temp_file.name}")
                            output_name = found_temp_file.name
                            if output_name.startswith('temp_'):
                                output_name = output_name[5:]
                            output_path = Path(self.config.recording.output_dir) / output_name
                            
                            try:
                                found_temp_file.rename(output_path)
                                logger.info(f"Recovered: {output_path}")
                                await self.state.set_recording_file(stream_state.channel, str(output_path))
                                stream_state.recording_file = str(output_path)
                                await self._process_recording_guarded(stream_state)
                            except Exception as e:
                                logger.error(f"Failed to move temp file: {e}")
                        else:
                            # Try fragment recovery as last resort
                            logger.info(f"Recording file not found (status: {stream_state.status}), attempting fragment recovery...")
                            
                            # Reconstruct stream info and stable temp-path hint (without using "now")
                            stream_info = self._state_to_stream_info(stream_state)
                            date_hint = stream_info.started_at.strftime('%Y-%m-%d')
                            channel_hint = stream_state.channel.lower()
                            temp_path = temp_dir / f"temp_{channel_hint}_{date_hint}_recovery.ts"
                            
                            result = await self.recorder.recover_interrupted_recording(stream_info, temp_path)
                            
                            if result.status == RecordingStatus.COMPLETED:
                                if not result.output_path:
                                    # Clean recovery - no files found for the interrupted segment
                                    # BUT check if we have previous segments
                                    if stream_state.recording_files:
                                        logger.info(f"Recovery check clean, but found {len(stream_state.recording_files)} previous segments. Processing...")
                                        await self._process_recording_guarded(stream_state)
                                    else:
                                        logger.info("Recovery check complete: No files to process (clean state)")
                                        # Mark as completed and move to history so we have a record
                                        await self.state.complete_stream(stream_state.channel)
                                else:
                                    logger.info(f"Successfully recovered recording: {result.output_path}")
                                    # Update state with the recovered file and process
                                    await self.state.set_recording_file(stream_state.channel, result.output_path)
                                    stream_state.recording_file = result.output_path
                                    await self._process_recording_guarded(stream_state)
                            else:
                                logger.warning(f"Fragment recovery failed: {result.error}")
                                await self.state.update_status(
                                    stream_state.channel, 
                                    StreamStatus.ERROR,
                                    f"Recovery failed: {result.error}"
                                )
                
                elif stream_state.status == StreamStatus.SENDING_COMPRESSED:
                    # Resuming compressed file sending - need compressed file, not original
                    if stream_state.compressed_file and Path(stream_state.compressed_file).exists():
                        logger.info(f"Resuming from {stream_state.status.value}")
                        await self._resume_send_compressed(stream_state)
                    else:
                        logger.warning("Compressed file not found, skipping to forwarding")
                        await self._resume_forwarding(stream_state)
                
                elif stream_state.status == StreamStatus.FORWARDING:
                    # Resuming forwarding - just need message IDs
                    logger.info(f"Resuming from {stream_state.status.value}")
                    await self._resume_forwarding(stream_state)
                
                elif stream_state.status in (
                    StreamStatus.PROCESSING,
                    StreamStatus.UPLOADING,
                    StreamStatus.COMPRESSING
                ):
                    # Mid-processing - need original file(s)
                    # Check both legacy singular file and new list of files
                    has_files = False
                    if stream_state.recording_file and Path(stream_state.recording_file).exists():
                        has_files = True
                    elif stream_state.recording_files:
                        # Check if at least one file exists
                        if any(Path(f).exists() for f in stream_state.recording_files):
                            has_files = True
                    
                    if has_files:
                        logger.info(f"Resuming from {stream_state.status.value}")
                        await self._process_recording_guarded(stream_state)
                    else:
                        logger.error("Recording file not found for recovery")
                        await self.state.update_status(
                            stream_state.channel,
                            StreamStatus.ERROR,
                            "Recording file not found"
                        )
                    
            except Exception as e:
                logger.error(f"Recovery failed: {e}")
                await self.state.update_status(
                    stream_state.channel,
                    StreamStatus.ERROR,
                    str(e)
                )
    
    async def _handle_event(self, event: MonitorEvent) -> None:
        """Handle monitor event."""
        if event.stream_info:
            event.stream_info.title = self.recorder.strip_title_clock(event.stream_info.title)
            
        if event.event == StreamEvent.INFO_CHANGED:
            # Title/category changed while offline - send/update waiting message
            await self._handle_info_changed(event)
        
        elif event.event == StreamEvent.WENT_LIVE:
            # Stream started - begin recording
            await self._handle_went_live(event)
        
        elif event.event == StreamEvent.WENT_OFFLINE:
            # Stream ended - processing handled by recording task
            await self._handle_went_offline(event)
    
    async def _handle_info_changed(self, event: MonitorEvent) -> None:
        """Handle title/category change while offline."""
        channel = event.channel
        info = event.stream_info
        logger = get_channel_logger(channel)
        
        # Get state
        state = await self.state.get_state(channel)
        
        # ONLY handle info changes if we are NOT recording and NOT processing
        if channel in self._active_recordings:
            logger.debug(f"Ignoring info change for {channel}: recording in progress")
            return
            
        if state and state.status not in (StreamStatus.WAITING, StreamStatus.OFFLINE):
            logger.debug(f"Ignoring info change for {channel}: current status is {state.status.value}")
            return

        if state and state.message_ids.waiting_msg:
            # Update existing waiting message
            await self.uploader.update_waiting_message(
                state.message_ids.waiting_msg,
                channel,
                info.title,
                info.category
            )
            await self.state.update_title_category(channel, info.title, info.category)
            logger.info("Updated waiting message")
        else:
            # Create new waiting message
            state = await self.state.create_stream(
                channel=channel,
                title=info.title,
                category=info.category,
                status=StreamStatus.WAITING
            )
            
            msg_id = await self.uploader.send_waiting_message(
                channel, info.title, info.category
            )
            
            if msg_id:
                await self.state.set_message_id(channel, 'waiting', msg_id)
            
            logger.info("Sent waiting message")
    
    async def _handle_went_live(self, event: MonitorEvent) -> None:
        """Handle stream going live."""
        channel = event.channel
        info = event.stream_info
        logger = get_channel_logger(channel)
        
        # Clear any pending skip-grace flag for this channel
        self._skip_grace_for.discard(channel)
        
        # Skip if already recording
        if channel in self._active_recordings:
            return
        
        # Get existing state
        state = await self.state.get_state(channel)
        
        # Guard against state corruption: if previous session is still being recovered/processed,
        # do not overwrite active state with a new stream yet.
        busy_statuses = {
            StreamStatus.PROCESSING,
            StreamStatus.UPLOADING,
            StreamStatus.COMPRESSING,
            StreamStatus.SENDING_COMPRESSED,
            StreamStatus.FORWARDING,
        }
        if channel in self._processing_channels or (state and state.status in busy_statuses):
            logger.warning(
                f"Live detected while previous session is still {state.status.value if state else 'processing'}; "
                "deferring recording start to avoid state overwrite"
            )
            self._pending_live_recheck.add(channel)
            if channel not in self._recovery_tasks:
                task = asyncio.create_task(self._recover_incomplete(only_channel=channel))
                self._recovery_tasks[channel] = task
                task.add_done_callback(lambda t: self._recovery_tasks.pop(channel, None))
            return
        
        # Check if this is a session continuation (stream returned after drop)
        # Session continuation: state exists, has recording_files, and status is RECORDING (waiting for grace period)
        is_continuation = (
            state is not None and 
            state.status == StreamStatus.RECORDING and
            len(state.recording_files) > 0 and 
            state.session_id is not None
        )
        
        if is_continuation:
            # Stream returned! This is a continuation of existing session
            logger.info(f"🔄 Stream returned! Continuing session {state.session_id} (segment #{len(state.recording_files) + 1})")
            
            # Update status back to recording
            await self.state.update_status(channel, StreamStatus.RECORDING)
            await self.state.update_title_category(channel, info.title, info.category)
            
        else:
            # New stream session
            
            # Delete waiting message if exists
            if state and state.message_ids.waiting_msg:
                await self.uploader.delete_message(state.message_ids.waiting_msg)
                await self.state.clear_message_id(channel, 'waiting')
            
            # Create new state
            if not state or state.status not in (StreamStatus.WAITING,):
                state = await self.state.create_stream(
                    channel=channel,
                    title=info.title,
                    category=info.category,
                    status=StreamStatus.RECORDING
                )
            else:
                await self.state.update_title_category(channel, info.title, info.category)
                await self.state.update_status(channel, StreamStatus.RECORDING)
            
            # Capture stream-start title/category for final caption
            await self.state.start_recording(channel)
            
            # Generate new session ID
            session_id = f"{channel}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            await self.state.set_session_id(channel, session_id)
            logger.info(f"📹 New session started: {session_id}")
            
            # Clear old message IDs from previous recordings
            await self.state.clear_message_id(channel, 'upload')
            await self.state.clear_message_id(channel, 'backup')
            await self.state.clear_message_id(channel, 'compressed')
            
            # Send status message
            msg_id = await self.uploader.send_status_message(
                channel, info.title, info.category, STATUS_ICONS[StreamStatus.RECORDING]
            )
            if msg_id:
                await self.state.set_message_id(channel, 'status', msg_id)
        
        # Start recording task
        task = asyncio.create_task(self._record_stream(channel, info))
        self._active_recordings[channel] = task
        
        logger.info(f"Started recording: {info.title}")

    async def _handle_went_offline(self, event: MonitorEvent) -> None:
        """Handle stream going offline (after monitor grace period)."""
        channel = event.channel
        logger = get_channel_logger(channel)
        
        # Mark to skip any additional grace wait in the recorder task
        self._skip_grace_for.add(channel)
        
        # If a recording is still active, stop it so processing can start.
        if channel in self._active_recordings or channel in self.recorder.get_active_recordings():
            logger.info("Stream ended (monitor confirmed). Stopping recorder to start processing...")
            try:
                await self.recorder.stop_recording(channel)
            except Exception as e:
                logger.warning(f"Failed to stop recording on stream end: {e}")
        else:
            logger.debug("Stream ended but no active recording task found")
        
        # If no active task, kick off recovery/processing for this channel
        if channel not in self._active_recordings and channel not in self._recovery_tasks:
            async def _recover_after_offline():
                # Small delay to allow recorder to flush temp file
                await asyncio.sleep(2)
                await self._recover_incomplete(only_channel=channel)
            task = asyncio.create_task(_recover_after_offline())
            self._recovery_tasks[channel] = task
            task.add_done_callback(lambda t: self._recovery_tasks.pop(channel, None))
    
    async def _record_stream(self, channel: str, info: StreamInfo) -> None:
        """Record stream and process when done."""
        logger = get_channel_logger(channel)
        
        try:
            # Check for continuation to disable live_from_start
            state = await self.state.get_state(channel)
            is_continuation = state and len(state.recording_files) > 0
            
            # Record the stream
            result = await self.recorder.record_stream(
                info, 
                live_from_start=False if is_continuation else None
            )
            
            if result.status.value == 'completed' and result.output_path:
                # Skip phantom short segments on continuation (typically after stream end)
                state = await self.state.get_state(channel)
                is_continuation = state and len(state.recording_files) > 0
                is_twitch = not channel.startswith('@') and not channel.startswith('UC')
                phantom_cfg = self.config.recording.phantom_filter
                should_check = phantom_cfg.enabled and is_twitch
                if phantom_cfg.only_on_continuation:
                    should_check = should_check and is_continuation
                
                if should_check:
                    duration_sec = await self._probe_duration_seconds(result.output_path)
                    size_bytes = 0
                    size_mb = 0.0
                    try:
                        size_bytes = Path(result.output_path).stat().st_size
                        size_mb = size_bytes / (1024 * 1024)
                    except Exception:
                        size_bytes = 0
                        size_mb = 0.0
                    
                    is_short = bool(duration_sec and duration_sec < phantom_cfg.min_duration_sec)
                    is_small = bool(size_mb > 0 and size_mb < phantom_cfg.min_size_mb)
                    
                    # If ffprobe couldn't read duration, estimate using previous segment bitrate.
                    estimated_duration_sec = 0.0
                    if not duration_sec and state and state.recording_files:
                        prev_file = state.recording_files[-1]
                        if Path(prev_file).exists() and size_bytes > 0:
                            prev_duration = await self._probe_duration_seconds(prev_file)
                            try:
                                prev_size_bytes = Path(prev_file).stat().st_size
                            except Exception:
                                prev_size_bytes = 0
                            if prev_duration > 0 and prev_size_bytes > 0:
                                estimated_duration_sec = (size_bytes / prev_size_bytes) * prev_duration
                                if estimated_duration_sec < phantom_cfg.min_duration_sec:
                                    is_short = True
                    
                    is_duplicate_tail = False
                    if (
                        phantom_cfg.duplicate_hash_check
                        and is_continuation
                        and state
                        and state.recording_files
                    ):
                        prev_file = state.recording_files[-1]
                        if Path(prev_file).exists():
                            is_duplicate_tail = await self._is_tail_duplicate_segment(
                                previous_file=prev_file,
                                candidate_file=result.output_path,
                                compare_mb=max(1, int(phantom_cfg.duplicate_hash_mb))
                            )
                    
                    is_phantom = is_short or is_small or is_duplicate_tail
                    
                    if is_phantom:
                        should_skip = True
                        if phantom_cfg.check_twitch_api:
                            is_live = False
                            try:
                                live_info = await self.monitor.check_twitch_live(channel)
                                is_live = bool(live_info)
                            except Exception:
                                is_live = False
                            should_skip = not is_live
                        
                        if should_skip:
                            reasons = []
                            if is_short:
                                if duration_sec > 0:
                                    reasons.append(f"short={duration_sec:.1f}s<{phantom_cfg.min_duration_sec}s")
                                elif estimated_duration_sec > 0:
                                    reasons.append(
                                        f"short(est)={estimated_duration_sec:.1f}s<{phantom_cfg.min_duration_sec}s"
                                    )
                            if is_small:
                                reasons.append(f"small={size_mb:.1f}MB<{phantom_cfg.min_size_mb}MB")
                            if is_duplicate_tail:
                                reasons.append("tail-duplicate")
                            reason_text = ", ".join(reasons) if reasons else "phantom-heuristic"
                            
                            logger.warning(
                                "Skipping phantom segment "
                                f"({reason_text}); check_twitch_api={phantom_cfg.check_twitch_api}"
                            )
                            # Process existing session files if any
                            if state and state.recording_files and not self._shutting_down:
                                await self._process_recording_guarded(state)
                            return
                
                # Add this segment to the session
                await self.state.add_recording_file(channel, result.output_path)
                await self.state.end_recording(channel)
                
                state = await self.state.get_state(channel)
                segment_count = len(state.recording_files) if state else 1
                logger.info(f"📁 Segment saved: {result.output_path} (segment #{segment_count} in session)")
                
                # Check shutdown before grace period
                if self._shutting_down:
                    logger.info("Shutting down - skipping grace period/restart check")
                    return

                # If monitor already confirmed stream end, skip extra grace wait
                if channel in self._skip_grace_for:
                    self._skip_grace_for.discard(channel)
                    # Allow new recordings while processing
                    self._active_recordings.pop(channel, None)
                    logger.info("Skipping grace period (monitor already confirmed end)")
                else:
                    # WAIT for monitor's grace period to complete before processing
                    # This prevents processing while stream might come back
                    grace_period = self.config.twitch.offline_grace_period
                    if grace_period > 0:
                        logger.info(f"⏳ Waiting {grace_period}s grace period before processing...")
                        
                        # Remove from active recordings BEFORE waiting
                        # This allows _handle_went_live to start a new recording if stream returns
                        self._active_recordings.pop(channel, None)
                        
                        # Periodic explicit check during grace period
                        # We check every 15 seconds or at least once if grace_period < 15
                        check_interval = 15
                        start_time = datetime.now()
                        
                        while (datetime.now() - start_time).total_seconds() < grace_period:
                            # 1. Check if monitor already restarted it (Passive check)
                            if channel in self._active_recordings:
                                logger.info("🔄 Stream returned during grace period (via monitor) - deferring processing")
                                return
                            # 1b. Monitor already confirmed offline; skip remaining grace wait
                            if channel in self._skip_grace_for:
                                logger.info("Skipping remaining grace period (monitor confirmed end)")
                                self._skip_grace_for.discard(channel)
                                break
                            
                            # Calculate remaining time
                            elapsed = (datetime.now() - start_time).total_seconds()
                            remaining = grace_period - elapsed
                            
                            if remaining <= 0:
                                break
                                
                            # 2. Explicit check (Active check)
                            # Helps if monitor loop is stuck or slow
                            try:
                                stream_live_now = await self.monitor.check_twitch_live(channel)
                                if stream_live_now:
                                    logger.info("🔴 Stream is live (explicit check)! Restarting recording...")
                                    # Trigger new recording via event handler logic
                                    event = MonitorEvent(StreamEvent.WENT_LIVE, channel, stream_live_now)
                                    await self._handle_went_live(event)
                                    return
                            except Exception as e:
                                logger.debug(f"Error checking if stream is live: {e}")
                            
                            # Sleep for interval or whatever is left
                            sleep_time = min(check_interval, remaining)
                            await asyncio.sleep(sleep_time)
                        
                        # Double check at the very end just to be sure
                        if channel in self._active_recordings:
                            logger.info("🔄 Stream returned at end of grace period - deferring processing")
                            return
                
                # Stream didn't return - process ALL segments in this session
                state = await self.state.get_state(channel)
                if state:
                    all_files = state.recording_files
                    if len(all_files) > 1:
                        logger.info(f"📼 Processing {len(all_files)} segments from session {state.session_id}")
                    
                    # CROSS MODE: After YouTube ends, check if Twitch is live
                    is_youtube = channel.startswith('@') or channel.startswith('UC')
                    
                    if self.config.platform == "cross" and is_youtube:
                        await self._handle_cross_mode_transition(channel, result.output_path)
                    else:
                        # Normal processing - processes all segments
                        await self._process_recording_guarded(state)
            else:
                logger.error(f"Recording failed: {result.error}")
                await self.state.update_status(channel, StreamStatus.ERROR, result.error)
                
        except asyncio.CancelledError:
            logger.info("Recording cancelled, saving partial file...")
            # Recorder handles partial file saving
            raise
        except Exception as e:
            logger.error(f"Recording error: {e}")
            await self.state.update_status(channel, StreamStatus.ERROR, str(e))
        finally:
            # Remove from active recordings ONLY if it's this task
            # (To avoid removing a new task started during grace period)
            current_task = asyncio.current_task()
            if self._active_recordings.get(channel) == current_task:
                self._active_recordings.pop(channel, None)
    
    async def _start_twitch_for_cross(self, yt_channel: str, cross_key: str, logger) -> None:
        """Start cross-mode Twitch recording once per YT session key."""
        existing_task = self._cross_twitch_tasks.get(cross_key)
        if existing_task:
            logger.info("Cross mode: Twitch transition already started for this YT session")
            return
        
        twitch_info = await self.monitor.check_twitch_live()
        if not twitch_info:
            logger.info("Cross mode: No Twitch stream active")
            return
        
        twitch_channel = twitch_info.channel
        logger.info(f"Cross mode: Twitch stream active on {twitch_channel}, starting recording...")
        
        # Create state for Twitch recording, linked to YouTube.
        # If previous Twitch state is still processing, do not overwrite it.
        existing_state = await self.state.get_state(twitch_channel)
        busy_statuses = {
            StreamStatus.PROCESSING,
            StreamStatus.UPLOADING,
            StreamStatus.COMPRESSING,
            StreamStatus.SENDING_COMPRESSED,
            StreamStatus.FORWARDING,
        }
        if existing_state and existing_state.status in busy_statuses:
            logger.warning(
                f"Cross mode: cannot start Twitch transition, previous state still {existing_state.status.value}"
            )
            return
        
        await self.state.create_stream(
            channel=twitch_channel,
            title=twitch_info.title,
            category=twitch_info.category,
            status=StreamStatus.RECORDING
        )
        
        # Link YT and Twitch states.
        await self.state.update_linked_channel(yt_channel, twitch_channel)
        await self.state.update_linked_channel(twitch_channel, yt_channel)
        
        # Start Twitch recording in background and remember it to prevent duplicate starts.
        twitch_task = asyncio.create_task(self._record_twitch_for_cross(yt_channel, twitch_info))
        self._cross_twitch_tasks[cross_key] = twitch_task
        self._active_recordings[twitch_channel] = twitch_task

    async def _on_youtube_merge_started(self, yt_stream_info) -> None:
        """
        Called when yt-dlp starts merging (YouTube stream has ended).
        Immediately checks Twitch and starts recording if live.
        """
        yt_channel = yt_stream_info.channel
        logger = get_channel_logger(yt_channel)
        
        yt_state = await self.state.get_state(yt_channel)
        cross_key = self._cross_session_key(
            yt_channel,
            yt_state.session_id if yt_state else None
        )
        
        logger.info("Cross mode: YouTube merge started, checking Twitch NOW...")
        await asyncio.sleep(2)
        await self._start_twitch_for_cross(yt_channel, cross_key, logger)
    
    async def _record_twitch_for_cross(self, yt_channel: str, twitch_info) -> None:
        """Record Twitch stream for cross mode."""
        twitch_channel = twitch_info.channel
        logger = get_channel_logger(twitch_channel)
        
        try:
            result = await self.recorder.record_stream(twitch_info)
            
            if result.status.value == 'completed' and result.output_path:
                await self.state.set_recording_file(twitch_channel, result.output_path)
                await self.state.end_recording(twitch_channel)
                
                # Add Twitch recording to linked files on YT state
                await self.state.add_linked_recording_file(yt_channel, result.output_path)
                
                logger.info("Cross mode: Twitch recording complete!")
            else:
                logger.error(f"Cross mode: Twitch recording failed: {result.error}")
                
        except Exception as e:
            logger.error(f"Cross mode: Twitch recording error: {e}")
        finally:
            self._active_recordings.pop(twitch_channel, None)

    async def _handle_cross_mode_transition(self, yt_channel: str, yt_recording_path: str) -> None:
        """
        Handle YouTube -> Twitch transition in cross mode.
        
        After YouTube stream ends, wait a bit then check if Twitch is live.
        If Twitch is live, record it and combine both recordings.
        """
        logger = get_channel_logger(yt_channel)
        yt_state = await self.state.get_state(yt_channel)
        cross_key = self._cross_session_key(
            yt_channel,
            yt_state.session_id if yt_state else None
        )
        
        # Wait for yt-dlp to finish merging before checking Twitch
        delay = self.config.twitch.cross_check_delay
        logger.info(f"Cross mode: Waiting {delay}s before checking Twitch...")
        try:
            await asyncio.sleep(delay)
            await self._start_twitch_for_cross(yt_channel, cross_key, logger)
            
            twitch_task = self._cross_twitch_tasks.get(cross_key)
            if twitch_task:
                logger.info("Cross mode: Waiting Twitch transition recording to finish...")
                try:
                    await twitch_task
                except Exception as e:
                    logger.warning(f"Cross mode: Twitch transition task failed: {e}")
            
            # Process YouTube recording (with any linked Twitch recording)
            yt_state = await self.state.get_state(yt_channel)
            if yt_state:
                await self._process_recording_guarded(yt_state)
        finally:
            # Session is complete, release dedup key.
            self._cross_twitch_tasks.pop(cross_key, None)

    async def _process_recording(self, state: StreamState) -> None:
        """Process a completed recording (supports multiple segments)."""
        channel = state.channel
        logger = get_channel_logger(channel)
        
        # Check if we are shutting down
        if self._shutting_down:
            logger.info("Shutting down - skipping upload. Will resume on next start.")
            return
        
        # Get all recording files for this session
        recording_files = state.recording_files if state.recording_files else []
        
        # Fallback to legacy single file if recording_files is empty
        if not recording_files and state.recording_file:
            recording_files = [state.recording_file]
        
        if not recording_files:
            logger.error("No recording files found")
            await self.state.update_status(channel, StreamStatus.ERROR, "No files found")
            return
        
        # Filter to only existing files
        existing_files = [f for f in recording_files if Path(f).exists()]
        if not existing_files:
            logger.error(f"Recording files not found: {recording_files}")
            await self.state.update_status(channel, StreamStatus.ERROR, "Files not found")
            return
        
        if len(existing_files) > 1:
            logger.info(f"📼 Processing {len(existing_files)} segments in session {state.session_id}")
        
        # VALIDATE: Check each file is readable before processing
        valid_files = []
        for recording_file in existing_files:
            try:
                validate_proc = await asyncio.create_subprocess_exec(
                    'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                    '-of', 'default=noprint_wrappers=1:nokey=1', recording_file,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await validate_proc.communicate()
                duration_str = stdout.decode().strip()
                if duration_str and validate_proc.returncode == 0:
                    valid_files.append(recording_file)
                else:
                    logger.warning(f"Segment corrupted/incomplete, skipping: {Path(recording_file).name}")
            except Exception as e:
                logger.warning(f"Failed to validate segment {Path(recording_file).name}: {e}")
        
        if not valid_files:
            logger.error("All segments are corrupted or incomplete")
            await self.state.update_status(channel, StreamStatus.ERROR, "All files corrupted - keeping for manual recovery")
            return
        
        temp_concat = None
        try:
            # Update status: Processing
            await self._update_status_msg(state, StreamStatus.PROCESSING)
            await self.state.update_status(channel, StreamStatus.PROCESSING)
            
            size_limit_gb = (
                self.config.splitting.premium_size_gb 
                if self.uploader.is_premium 
                else self.config.splitting.default_size_gb
            )
            
            # CHECK: If split files already exist in state, skip re-splitting
            existing_split_files = state.split_files if state.split_files else []
            valid_split_files = [f for f in existing_split_files if Path(f).exists()]
            
            if valid_split_files:
                # Resume from existing split files
                logger.info(f"📂 Found {len(valid_split_files)} existing split files, skipping re-split")
                all_split_files = valid_split_files
            else:
                # Split ALL segments and collect parts in order
                all_split_files = []
                for i, recording_file in enumerate(valid_files):
                    segment_name = Path(recording_file).name
                    if len(valid_files) > 1:
                        logger.info(f"Splitting segment {i+1}/{len(valid_files)}: {segment_name}")
                    
                    split_result = await self.splitter.split_file(
                        recording_file, max_size_gb=size_limit_gb
                    )
                    
                    if split_result.success:
                        all_split_files.extend(split_result.output_files)
                        logger.info(f"  → {len(split_result.output_files)} parts")
                    else:
                        logger.warning(f"Split failed for {segment_name}: {split_result.error}")
                
                if not all_split_files:
                    raise Exception("No files after splitting")
                
                # CROSS MODE: Also split any linked recordings (YT + Twitch)
                if state.linked_recording_files:
                    logger.info(f"Cross mode: Processing {len(state.linked_recording_files)} linked recordings...")
                    for linked_file in state.linked_recording_files:
                        if Path(linked_file).exists():
                            linked_result = await self.splitter.split_file(
                                linked_file, max_size_gb=size_limit_gb
                            )
                            if linked_result.success:
                                all_split_files.extend(linked_result.output_files)
                                logger.info(f"Cross mode: Added {len(linked_result.output_files)} parts from linked recording")
                            else:
                                logger.warning(f"Cross mode: Failed to split linked file: {linked_result.error}")
                
                await self.state.set_split_files(channel, all_split_files)
            
            logger.info(f"📦 Total: {len(all_split_files)} parts ready for upload")
            
            # Use all_split_files instead of split_files for the rest of processing
            split_files = all_split_files
            
            # Update status: Uploading
            await self._update_status_msg(state, StreamStatus.UPLOADING)
            await self.state.update_status(channel, StreamStatus.UPLOADING)
            
            # Calculate total duration from all segments
            total_duration_str = await self._format_duration_multi(valid_files)
            
            # Resume-safe upload: skip parts that were already uploaded.
            latest_state = await self.state.get_state(channel)
            uploaded_parts = dict(latest_state.uploaded_parts) if latest_state else {}
            uploaded_parts = {
                path: msg_id
                for path, msg_id in uploaded_parts.items()
                if msg_id and Path(path).exists()
            }
            # Backward compatibility: infer file->message mapping from ordered upload IDs
            # if state was created before uploaded_parts support.
            if (
                not uploaded_parts
                and latest_state
                and latest_state.message_ids.upload_msgs
            ):
                for file_path, msg_id in zip(split_files, latest_state.message_ids.upload_msgs):
                    if msg_id:
                        uploaded_parts[file_path] = msg_id
            
            pending_split_files = [p for p in split_files if p not in uploaded_parts]
            if uploaded_parts:
                logger.info(
                    f"Resuming upload: {len(uploaded_parts)} already uploaded, "
                    f"{len(pending_split_files)} pending"
                )
            
            failed_by_path: Dict[str, UploadResult] = {}
            if pending_split_files:
                pending_result = await self.uploader.upload_stream_parts(
                    stream_info=self._state_to_stream_info(state),
                    file_paths=pending_split_files,
                    duration_str=total_duration_str
                )
                
                for result in pending_result.results:
                    if result.success and result.message_id:
                        uploaded_parts[result.file_path] = result.message_id
                        await self.state.set_uploaded_part(channel, result.file_path, result.message_id)
                    else:
                        failed_by_path[result.file_path] = result
            else:
                pending_result = BatchUploadResult(
                    channel=state.channel,
                    stream_info=self._state_to_stream_info(state),
                    total_parts=0,
                    successful_uploads=0,
                    failed_uploads=0,
                    results=[]
                )
            
            combined_results: list[UploadResult] = []
            for file_path in split_files:
                msg_id = uploaded_parts.get(file_path)
                if msg_id:
                    combined_results.append(
                        UploadResult(
                            file_path=file_path,
                            message_id=msg_id,
                            success=True
                        )
                    )
                elif file_path in failed_by_path:
                    combined_results.append(failed_by_path[file_path])
                else:
                    combined_results.append(
                        UploadResult(
                            file_path=file_path,
                            message_id=None,
                            success=False,
                            error="Missing upload result"
                        )
                    )
            
            successful_uploads = sum(1 for p in split_files if p in uploaded_parts)
            failed_uploads = len(split_files) - successful_uploads
            upload_result = BatchUploadResult(
                channel=state.channel,
                stream_info=self._state_to_stream_info(state),
                total_parts=len(split_files),
                successful_uploads=successful_uploads,
                failed_uploads=failed_uploads,
                results=combined_results
            )
            
            logger.info(f"Uploaded {upload_result.successful_uploads}/{upload_result.total_parts}")
            
            # Stop here if any uploads failed (keep files for retry)
            if upload_result.failed_uploads > 0:
                msg = f"Upload incomplete: {upload_result.failed_uploads}/{upload_result.total_parts} failed"
                logger.error(msg)
                await self.state.update_status(channel, StreamStatus.UPLOADING, msg)
                return
            
            # Compress if enabled AND file was split (meaning it exceeded upload limits)
            compressed_file = None
            compression_failed = False  # Флаг для отслеживания провала сжатия
            if self.config.compression.enabled:
                # If file fits in one part, user likely doesn't want a compressed copy
                if len(split_files) == 1:
                    logger.info("Skipping compression: file fits within upload limits (single part)")
                else:
                    await self._update_status_msg(state, StreamStatus.COMPRESSING)
                    await self.state.update_status(channel, StreamStatus.COMPRESSING)
                
                    target_size = (
                        self.config.compression.premium_target_size_mb
                        if self.uploader.is_premium
                        else self.config.compression.default_target_size_mb
                    )
                    
                    compression_input = None
                    if len(valid_files) == 1:
                        compression_input = valid_files[0]
                    else:
                        temp_concat = await self._concat_segments_for_compression(
                            valid_files, state.session_id or channel, logger
                        )
                        if temp_concat:
                            compression_input = temp_concat
                        else:
                            compression_failed = True
                            logger.error("Compression skipped: failed to concat segments")
                    
                    if compression_input:
                        result = await self.compressor.compress_to_size(
                            compression_input,
                            target_size_mb=target_size
                        )
                        
                        if result.success and result.output_file:
                            compressed_file = result.output_file
                            await self.state.set_compressed_file(channel, compressed_file)
                            logger.info(f"Compressed: {result.size_reduction_percent:.1f}% reduction")
                        else:
                            # Сжатие провалилось - НЕ удаляем оригинал!
                            compression_failed = True
                            logger.error(f"Compression failed: {result.error}")
            
            # Send compressed to comments (reply to first upload)
            if compressed_file and upload_result.results:
                await self._update_status_msg(state, StreamStatus.SENDING_COMPRESSED)
                await self.state.update_status(channel, StreamStatus.SENDING_COMPRESSED)
                
                first_msg_id = upload_result.results[0].message_id
                if first_msg_id:
                    msg_id = await self.uploader.send_to_discussion(
                        first_msg_id, compressed_file, "📦 Сжатая версия"
                    )
                    if msg_id:
                        await self.state.set_message_id(channel, 'compressed', msg_id)
                        
                        # Update album captions with compressed note
                        try:
                            chunks = [split_files[i:i+10] for i in range(0, len(split_files), 10)]
                            album_total = len(chunks)
                            upload_map = {
                                r.file_path: r.message_id
                                for r in upload_result.results
                                if r.message_id
                            }
                            
                            stream_info = self._state_to_stream_info(state)
                            
                            for idx, chunk in enumerate(chunks, start=1):
                                if not chunk:
                                    continue
                                first_path = chunk[0]
                                first_album_msg = upload_map.get(first_path)
                                if not first_album_msg:
                                    continue
                                
                                album_duration = total_duration_str if idx == 1 else ""
                                caption = self.uploader.build_album_caption(
                                    stream_info=stream_info,
                                    file_path=first_path,
                                    duration_str=album_duration,
                                    album_index=idx,
                                    album_total=album_total,
                                    compressed_note="✅ Сжатый файл отправлен"
                                )
                                
                                await self.uploader.update_message_caption(first_album_msg, caption)
                        except Exception as e:
                            logger.warning(f"Failed to update album captions: {e}")
            
            # Forward to backup
            if self.uploader.backup_channel_id:
                await self._update_status_msg(state, StreamStatus.FORWARDING)
                await self.state.update_status(channel, StreamStatus.FORWARDING)
                
                # Get all upload message IDs
                state = await self.state.get_state(channel)
                if state:
                    msg_ids = state.message_ids.upload_msgs.copy()
                    if not msg_ids and state.uploaded_parts:
                        msg_ids = [
                            state.uploaded_parts[p]
                            for p in split_files
                            if p in state.uploaded_parts
                        ]
                    if state.message_ids.compressed_msg:
                        msg_ids.append(state.message_ids.compressed_msg)
                    
                    forwarded = await self.uploader.forward_to_backup(msg_ids)
                    for fwd_id in forwarded:
                        await self.state.set_message_id(channel, 'backup', fwd_id)
            
            # Delete status message
            state = await self.state.get_state(channel)
            if state and state.message_ids.status_msg:
                await self.uploader.delete_message(state.message_ids.status_msg)
            
            # Complete!
            await self.state.complete_stream(channel)
            logger.info("Processing complete!")
            
            # Cleanup split files
            # НЕ удаляем оригинал, если сжатие было запланировано но провалилось!
            await self._cleanup_files(split_files, valid_files, compressed_file, compression_failed)
            
        except Exception as e:
            logger.error(f"Processing error: {e}")
            await self.state.update_status(channel, StreamStatus.ERROR, str(e))
        finally:
            if temp_concat and Path(temp_concat).exists():
                try:
                    Path(temp_concat).unlink()
                    logger.debug(f"Deleted temp concat file: {Path(temp_concat).name}")
                except Exception as e:
                    logger.warning(f"Failed to delete temp concat file: {e}")
    
    async def _resume_send_compressed(self, state: StreamState) -> None:
        """Resume sending compressed file to discussion."""
        channel = state.channel
        logger = get_channel_logger(channel)
        
        try:
            compressed_file = state.compressed_file
            
            # Get first upload message ID for reply
            if state.message_ids.upload_msgs:
                first_msg_id = state.message_ids.upload_msgs[0]
                
                await self._update_status_msg(state, StreamStatus.SENDING_COMPRESSED)
                
                msg_id = await self.uploader.send_to_discussion(
                    first_msg_id, compressed_file, "📦 Сжатая версия"
                )
                if msg_id:
                    await self.state.set_message_id(channel, 'compressed', msg_id)
                    logger.info("Sent compressed file to discussion")
            
            # Continue to forwarding
            await self._resume_forwarding(state)
            
        except Exception as e:
            logger.error(f"Resume send compressed error: {e}")
            await self.state.update_status(channel, StreamStatus.ERROR, str(e))
    
    async def _resume_forwarding(self, state: StreamState) -> None:
        """Resume forwarding to backup channel."""
        channel = state.channel
        logger = get_channel_logger(channel)
        
        try:
            # Forward to backup if configured
            if self.uploader.backup_channel_id:
                await self._update_status_msg(state, StreamStatus.FORWARDING)
                await self.state.update_status(channel, StreamStatus.FORWARDING)
                
                # Get current state with updated message IDs
                state = await self.state.get_state(channel)
                if state:
                    msg_ids = state.message_ids.upload_msgs.copy()
                    if state.message_ids.compressed_msg:
                        msg_ids.append(state.message_ids.compressed_msg)
                    
                    # Only forward if we have messages and haven't forwarded yet
                    if msg_ids and not state.message_ids.backup_msgs:
                        forwarded = await self.uploader.forward_to_backup(msg_ids)
                        for fwd_id in forwarded:
                            await self.state.set_message_id(channel, 'backup', fwd_id)
                        logger.info(f"Forwarded {len(forwarded)} messages to backup")
            
            # Delete status message
            state = await self.state.get_state(channel)
            if state and state.message_ids.status_msg:
                await self.uploader.delete_message(state.message_ids.status_msg)
            
            # Complete!
            await self.state.complete_stream(channel)
            logger.info("Recovery complete!")
            
            # Cleanup compressed file if exists
            if state and state.compressed_file and Path(state.compressed_file).exists():
                try:
                    Path(state.compressed_file).unlink()
                    logger.info(f"Deleted compressed file: {Path(state.compressed_file).name}")
                except Exception as e:
                    logger.warning(f"Failed to delete compressed: {e}")
            
        except Exception as e:
            logger.error(f"Resume forwarding error: {e}")
            await self.state.update_status(channel, StreamStatus.ERROR, str(e))
    
    async def _update_status_msg(self, state: StreamState, status: StreamStatus) -> None:
        """Update status message in Telegram."""
        if state.message_ids.status_msg:
            await self.uploader.update_status(
                state.message_ids.status_msg,
                state.channel,
                state.title,
                state.category,
                STATUS_ICONS[status]
            )
    
    def _state_to_stream_info(self, state: StreamState) -> StreamInfo:
        """Convert state to StreamInfo. Uses title/category from stream start for caption."""
        # Use title from when stream started (went live), not current or from waiting phase
        title = state.stream_start_title or (
            state.title_history[0]['value'] if state.title_history else state.title
        )
        category = state.stream_start_category or (
            state.category_history[0]['value'] if state.category_history else state.category
        )
        
        if state.channel.startswith('UC'):
            stream_url = f"https://www.youtube.com/channel/{state.channel}/live"
        elif state.channel.startswith('@'):
            stream_url = f"https://www.youtube.com/{state.channel}/live"
        else:
            stream_url = f"https://www.twitch.tv/{state.channel}"
            
        # Use .ts if mpegts is enabled
        ext = "ts" if self.config.recording.use_mpegts else "mp4"
            
        return StreamInfo(
            channel=state.channel,
            title=title,
            category=category,
            started_at=datetime.fromisoformat(state.started_at or state.detected_at or datetime.now().isoformat()),
            stream_url=stream_url,
            is_live=False
        )

    async def _is_tail_duplicate_segment(
        self,
        previous_file: str,
        candidate_file: str,
        compare_mb: int = 8
    ) -> bool:
        """
        Detect tail-duplicate artifacts:
        compare head hash of candidate with tail hash of previous segment.
        """
        try:
            prev_path = Path(previous_file)
            cand_path = Path(candidate_file)
            if not prev_path.exists() or not cand_path.exists():
                return False
            
            compare_bytes = max(1, compare_mb) * 1024 * 1024
            cand_size = cand_path.stat().st_size
            prev_size = prev_path.stat().st_size
            if cand_size <= 0 or prev_size <= 0:
                return False
            
            # Compare only up to candidate size and previous tail availability.
            compare_bytes = min(compare_bytes, cand_size, prev_size)
            if compare_bytes < 1024 * 1024:
                return False
            
            cand_head_hash = await asyncio.to_thread(
                self._hash_file_slice,
                candidate_file,
                0,
                compare_bytes
            )
            prev_tail_hash = await asyncio.to_thread(
                self._hash_file_slice,
                previous_file,
                prev_size - compare_bytes,
                compare_bytes
            )
            return bool(cand_head_hash and prev_tail_hash and cand_head_hash == prev_tail_hash)
        except Exception:
            return False

    def _hash_file_slice(self, file_path: str, offset: int, size: int) -> Optional[str]:
        """Return SHA256 hex digest for a file slice."""
        try:
            h = hashlib.sha256()
            with open(file_path, 'rb') as f:
                f.seek(max(0, offset))
                remaining = max(0, size)
                chunk_size = 1024 * 1024
                while remaining > 0:
                    chunk = f.read(min(chunk_size, remaining))
                    if not chunk:
                        break
                    h.update(chunk)
                    remaining -= len(chunk)
            return h.hexdigest()
        except Exception:
            return None
    
    def _format_duration(self, file_path: str) -> str:
        """Get formatted duration from file."""
        # This would use ffprobe, simplified here
        return ""

    async def _probe_duration_seconds(self, file_path: str) -> float:
        """Get duration in seconds using ffprobe (returns 0 on failure)."""
        try:
            process = await asyncio.create_subprocess_exec(
                'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1', file_path,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await process.communicate()
            if process.returncode == 0 and stdout.decode().strip():
                return float(stdout.decode().strip())
        except Exception:
            pass
        return 0.0

    async def _format_duration_multi(self, file_paths: list) -> str:
        """Get formatted total duration from multiple files without blocking event loop."""
        total_seconds = 0.0
        
        for file_path in file_paths:
            duration = await self._probe_duration_seconds(file_path)
            if duration > 0:
                total_seconds += duration
        
        if total_seconds == 0:
            return ""
        
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

    async def _concat_segments_for_compression(
        self,
        file_paths: list,
        label: str,
        logger
    ) -> Optional[str]:
        """
        Concatenate multiple segment files into one for compression.
        
        Returns path to concatenated file or None on failure.
        """
        if len(file_paths) < 2:
            return file_paths[0] if file_paths else None
        
        temp_dir = Path(self.config.recording.temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        ext = Path(file_paths[0]).suffix or ".mp4"
        list_path = temp_dir / f"concat_{label}_{timestamp}.txt"
        output_path = temp_dir / f"concat_{label}_{timestamp}{ext}"
        
        try:
            with open(list_path, 'w', encoding='utf-8') as f:
                for p in file_paths:
                    safe_path = p.replace("'", "'\\''")
                    f.write(f"file '{safe_path}'\n")
            
            logger.info(f"Concatenating {len(file_paths)} segments for compression...")
            process = await asyncio.create_subprocess_exec(
                'ffmpeg', '-y',
                '-fflags', '+genpts',
                '-f', 'concat',
                '-safe', '0',
                '-i', str(list_path),
                '-c', 'copy',
                str(output_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await process.communicate()
            
            if process.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
                logger.info(f"Concat complete: {output_path.name}")
                return str(output_path)
            
            error = stderr.decode('utf-8', errors='ignore')[-1000:]
            logger.error(f"Concat failed: {error}")
            return None
        
        except Exception as e:
            logger.error(f"Concat error: {e}")
            return None
        finally:
            if list_path.exists():
                try:
                    list_path.unlink()
                except Exception:
                    pass
    
    async def _cleanup_files(
        self, 
        split_files: list, 
        originals: list,  # Changed: now accepts list of original files
        compressed: Optional[str],
        compression_failed: bool = False
    ) -> None:
        """Cleanup files after successful upload.
        
        Args:
            split_files: List of split file paths.
            originals: List of original recording paths (all segments).
            compressed: Compressed file path (if compression succeeded).
            compression_failed: If True, compression was attempted but failed - keep originals!
        """
        logger = get_logger('app')
        
        # Ensure originals is a list
        if isinstance(originals, str):
            originals = [originals]
        
        # Delete split parts (but not originals)
        for f in split_files:
            if f not in originals and Path(f).exists():
                try:
                    Path(f).unlink()
                    logger.debug(f"Deleted split part: {f}")
                except Exception as e:
                    logger.warning(f"Failed to delete {f}: {e}")
        
        # Delete original recordings ONLY if compression didn't fail
        if compression_failed:
            logger.warning(f"Keeping original recordings (compression failed): {len(originals)} files")
        else:
            for original in originals:
                if original and Path(original).exists():
                    try:
                        Path(original).unlink()
                        logger.info(f"Deleted original recording: {Path(original).name}")
                    except Exception as e:
                        logger.warning(f"Failed to delete original: {e}")
        
        # Delete compressed file
        if compressed and Path(compressed).exists():
            try:
                Path(compressed).unlink()
                logger.info(f"Deleted compressed file: {Path(compressed).name}")
            except Exception as e:
                logger.warning(f"Failed to delete compressed: {e}")


async def main():
    """Main entry point."""
    # Load configuration
    try:
        config = load_config("config.yaml")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please create config.yaml from config.example.yaml")
        return
    except Exception as e:
        print(f"Configuration error: {e}")
        return
    
    # Setup logging
    setup_logging(
        level=config.logging.level,
        log_file=config.logging.file,
        max_size_mb=config.logging.max_size_mb,
        backup_count=config.logging.backup_count
    )
    
    # Validate credentials based on platform
    if config.platform in ("twitch", "both", "cross"):
        if not config.twitch.client_id or not config.twitch.client_secret:
            print("Error: Twitch client_id and client_secret are required for Twitch monitoring")
            return
        if not config.twitch.channels:
            print("Warning: No Twitch channels configured")
    
    if config.platform in ("youtube", "both", "cross"):
        if not config.youtube.channels:
            print("Error: No YouTube channels configured")
            return
    
    # Run application
    app = StreamRecorderApp(config)
    
    try:
        await app.start()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        get_logger('app').error(f"Fatal error: {e}")
        raise


if __name__ == '__main__':
    asyncio.run(main())
