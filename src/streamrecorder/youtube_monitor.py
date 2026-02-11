"""
YouTube Stream Monitor for Stream Recorder.
Monitors YouTube channels for live streams using yt-dlp.
No API key required!
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional

import yt_dlp

from .logger import get_logger, get_channel_logger
from .stream_monitor import StreamInfo, StreamStatus, StreamEvent, MonitorEvent


@dataclass
class YouTubeStreamData:
    """Data from YouTube stream."""
    channel_id: str
    channel_name: str
    video_id: str
    title: str
    description: str
    is_live: bool
    view_count: int
    thumbnail_url: Optional[str] = None
    categories: List[str] = None


class YouTubeMonitor:
    """
    Monitor YouTube channels for live streams.
    
    Uses yt-dlp to check /live endpoint for each channel.
    No API key required!
    
    Supports:
    - Channel handles: @ChannelName
    - Channel IDs: UCxxxxxxxxxxxxxxxxxxxxxxxx
    """
    
    class _SilentLogger:
        """Silent logger to suppress yt-dlp stderr output."""
        def debug(self, msg): pass
        def info(self, msg): pass
        def warning(self, msg): pass
        def error(self, msg): pass
    
    def __init__(
        self,
        channels: List[str],
        check_interval: int = 30
    ):
        """
        Initialize YouTube monitor.
        
        Args:
            channels: List of channel handles (@name) or IDs (UCxxx).
            check_interval: Seconds between checks.
        """
        self.channels = channels
        self.check_interval = check_interval
        self._logger = get_logger('youtube_monitor')
        
        # Track channel states
        self._channel_states: Dict[str, StreamStatus] = {}
        self._channel_info: Dict[str, StreamInfo] = {}
        self._running = False
        
        # yt-dlp options for info extraction (no download)
        self._ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'skip_download': True,
            'ignoreerrors': True,
            'logger': self._SilentLogger(),  # Suppress all yt-dlp stderr output
        }
        
        # Initialize states
        for channel in channels:
            self._channel_states[channel] = StreamStatus.UNKNOWN
    
    def _get_live_url(self, channel: str) -> str:
        """Get the live stream URL for a channel."""
        if channel.startswith('@'):
            return f"https://www.youtube.com/{channel}/live"
        elif channel.startswith('UC'):
            return f"https://www.youtube.com/channel/{channel}/live"
        else:
            # Assume it's a handle without @
            return f"https://www.youtube.com/@{channel}/live"
    
    def _get_channel_url(self, channel: str) -> str:
        """Get the channel page URL."""
        if channel.startswith('@'):
            return f"https://www.youtube.com/{channel}"
        elif channel.startswith('UC'):
            return f"https://www.youtube.com/channel/{channel}"
        else:
            return f"https://www.youtube.com/@{channel}"
    
    async def check_stream_status(self, channel: str) -> Optional[StreamInfo]:
        """
        Check if a YouTube channel is currently live.
        
        Args:
            channel: Channel handle (@name) or ID (UCxxx).
            
        Returns:
            StreamInfo if live, None if offline.
        """
        logger = get_channel_logger(channel)
        url = self._get_live_url(channel)
        
        try:
            # Run yt-dlp in executor to not block event loop
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                None,
                self._extract_info,
                url
            )
            
            if info and info.get('is_live'):
                # Stream is live!
                stream_info = StreamInfo(
                    channel=channel,
                    title=info.get('title', 'Untitled Stream'),
                    category=info.get('categories', [''])[0] if info.get('categories') else '',
                    viewers=info.get('concurrent_view_count', 0) or info.get('view_count', 0),
                    started_at=datetime.now(),  # yt-dlp doesn't give exact start time
                    thumbnail_url=info.get('thumbnail'),
                    stream_url=info.get('webpage_url', url),
                    is_live=True
                )
                return stream_info
            
        except Exception as e:
            logger.debug(f"Error checking stream: {e}")
        
        return None
    
    def _extract_info(self, url: str) -> Optional[dict]:
        """Extract info using yt-dlp (blocking)."""
        try:
            with yt_dlp.YoutubeDL(self._ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return info
        except Exception:
            return None
    
    def start(self) -> None:
        """Start monitoring."""
        self._running = True
        self._logger.info(f"Started monitoring {len(self.channels)} YouTube channels")
    
    def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        self._logger.info("YouTube monitor stopped")
    
    async def monitor(self) -> AsyncIterator[MonitorEvent]:
        """
        Monitor channels and yield events.
        
        Yields:
            MonitorEvent for each state change.
        """
        self.start()
        
        while self._running:
            try:
                events = await self._check_all_channels()
                for event in events:
                    yield event
            except Exception as e:
                self._logger.error(f"Monitor error: {e}")
            
            await asyncio.sleep(self.check_interval)
    
    async def _check_all_channels(self) -> List[MonitorEvent]:
        """Check all channels and return events."""
        events = []
        
        for channel in self.channels:
            logger = get_channel_logger(channel)
            
            try:
                stream_info = await self.check_stream_status(channel)
                old_status = self._channel_states[channel]
                
                if stream_info:
                    # Channel is live
                    if old_status != StreamStatus.LIVE:
                        # Just went live!
                        self._channel_states[channel] = StreamStatus.LIVE
                        self._channel_info[channel] = stream_info
                        
                        logger.info(f"🔴 YouTube stream started: {stream_info.title}")
                        
                        events.append(MonitorEvent(
                            event=StreamEvent.WENT_LIVE,
                            channel=channel,
                            stream_info=stream_info
                        ))
                    else:
                        # Still live - check for title changes
                        old_info = self._channel_info.get(channel)
                        if old_info and old_info.title != stream_info.title:
                            self._channel_info[channel] = stream_info
                            
                            events.append(MonitorEvent(
                                event=StreamEvent.INFO_CHANGED,
                                channel=channel,
                                stream_info=stream_info,
                                old_title=old_info.title,
                                old_category=old_info.category
                            ))
                else:
                    # Channel is offline
                    if old_status == StreamStatus.LIVE:
                        # Just went offline
                        self._channel_states[channel] = StreamStatus.OFFLINE
                        old_info = self._channel_info.get(channel)
                        
                        if old_info:
                            logger.info(f"⚫ YouTube stream ended")
                            
                            events.append(MonitorEvent(
                                event=StreamEvent.WENT_OFFLINE,
                                channel=channel,
                                stream_info=old_info
                            ))
                    elif old_status == StreamStatus.UNKNOWN:
                        self._channel_states[channel] = StreamStatus.OFFLINE
                
            except Exception as e:
                logger.error(f"Error checking channel: {e}")
        
        return events
    
    def get_channel_status(self, channel: str) -> tuple:
        """Get current status and info for a channel."""
        status = self._channel_states.get(channel, StreamStatus.UNKNOWN)
        info = self._channel_info.get(channel)
        return status, info
    
    def is_live(self, channel: str) -> bool:
        """Check if channel is currently live."""
        return self._channel_states.get(channel) == StreamStatus.LIVE


async def main():
    """Test YouTube monitor."""
    from .logger import setup_logging
    
    setup_logging(level="DEBUG")
    
    # Test with some channels
    test_channels = [
        "@MrBeast",  # Handle format
    ]
    
    monitor = YouTubeMonitor(
        channels=test_channels,
        check_interval=30
    )
    
    print(f"Checking {len(test_channels)} YouTube channels...")
    
    for channel in test_channels:
        print(f"\nChecking {channel}...")
        info = await monitor.check_stream_status(channel)
        
        if info:
            print(f"  ✅ LIVE: {info.title}")
            print(f"     Viewers: {info.viewers}")
            print(f"     URL: {info.stream_url}")
        else:
            print(f"  ⚫ Offline")
    
    print("\n\nStarting continuous monitor (Ctrl+C to stop)...")
    
    try:
        async for event in monitor.monitor():
            print(f"\n🔔 Event: {event.event.value}")
            print(f"   Channel: {event.channel}")
            print(f"   Title: {event.stream_info.title}")
    except KeyboardInterrupt:
        monitor.stop()
        print("\nMonitor stopped")


if __name__ == '__main__':
    asyncio.run(main())
