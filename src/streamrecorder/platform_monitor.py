"""
Platform Monitor - Unified monitor for multiple streaming platforms.
Supports Twitch and YouTube.
"""

import asyncio
from typing import AsyncIterator, List, Optional

from .config import Config
from .logger import get_logger
from .stream_monitor import StreamMonitor, MonitorEvent, StreamInfo
from .youtube_monitor import YouTubeMonitor
from .twitch_api import TwitchAPI


class PlatformMonitor:
    """
    Unified monitor for multiple streaming platforms.
    
    Combines Twitch and YouTube monitors based on configuration.
    """
    
    def __init__(self, config: Config, twitch_api: Optional[TwitchAPI] = None):
        """
        Initialize platform monitor.
        
        Args:
            config: Application configuration.
            twitch_api: Initialized Twitch API (required if monitoring Twitch).
        """
        self._config = config
        self._logger = get_logger('platform_monitor')
        self._running = False
        
        self._twitch_monitor: Optional[StreamMonitor] = None
        self._youtube_monitor: Optional[YouTubeMonitor] = None
        self._twitch_api: Optional[TwitchAPI] = twitch_api  # Keep reference for cross mode
        
        # Initialize monitors based on platform setting
        platform = config.platform
        
        if platform in ("twitch", "both"):
            if twitch_api and config.twitch.channels:
                self._twitch_monitor = StreamMonitor(
                    twitch_api=twitch_api,
                    channels=config.twitch.channels,
                    check_interval=config.twitch.check_interval,
                    offline_grace_period=config.twitch.offline_grace_period
                )
                self._logger.info(f"Twitch monitor: {len(config.twitch.channels)} channels")
            elif platform == "twitch":
                self._logger.warning("Twitch selected but no channels or API configured")
        
        if platform in ("youtube", "both", "cross"):
            if config.youtube.channels:
                self._youtube_monitor = YouTubeMonitor(
                    channels=config.youtube.channels,
                    check_interval=config.youtube.check_interval
                )
                self._logger.info(f"YouTube monitor: {len(config.youtube.channels)} channels")
            elif platform in ("youtube", "cross"):
                self._logger.warning("YouTube selected but no channels configured")
        
        # Cross mode: don't start Twitch monitor, but keep API for manual checks
        if platform == "cross":
            self._logger.info(f"Cross mode: YouTube monitors first, Twitch checked after YT stream ends")
    
    def start(self) -> None:
        """Start all monitors."""
        if self._running:
            return  # Already started
        
        self._running = True
        
        # Note: individual monitors log their own start in monitor()
    
    def stop(self) -> None:
        """Stop all monitors."""
        self._running = False
        
        if self._twitch_monitor:
            self._twitch_monitor.stop()
        
        if self._youtube_monitor:
            self._youtube_monitor.stop()
        
        self._logger.info("Platform monitor stopped")
    
    def get_all_channels(self) -> List[str]:
        """Get list of all monitored channels."""
        channels = []
        
        if self._twitch_monitor:
            channels.extend(self._config.twitch.channels)
        
        if self._youtube_monitor:
            channels.extend(self._config.youtube.channels)
        
        # In cross mode, also include Twitch channels for reference
        if self._config.platform == "cross":
            channels.extend(self._config.twitch.channels)
        
        return channels
    
    async def check_twitch_live(self, channel: str = None) -> Optional[StreamInfo]:
        """
        Check if Twitch channel is live (for cross mode).
        
        Args:
            channel: Specific channel to check, or None to check all configured channels.
            
        Returns:
            StreamInfo if any channel is live, None otherwise.
        """
        if not self._twitch_api:
            return None
        
        channels_to_check = [channel] if channel else self._config.twitch.channels
        
        try:
            streams = await self._twitch_api.get_streams_batch(channels_to_check)
            for ch, stream_data in streams.items():
                if stream_data:
                    from .stream_monitor import StreamInfo
                    return StreamInfo.from_api(stream_data)
        except Exception as e:
            self._logger.error(f"Error checking Twitch: {e}")
        
        return None

    async def check_youtube_live(self, channel: str) -> Optional[StreamInfo]:
        """
        Check if a YouTube channel is live right now.

        Args:
            channel: YouTube handle or UC channel ID.

        Returns:
            StreamInfo if live, None otherwise.
        """
        if not self._youtube_monitor:
            return None
        try:
            return await self._youtube_monitor.check_stream_status(channel)
        except Exception as e:
            self._logger.error(f"Error checking YouTube: {e}")
            return None

    
    async def monitor(self) -> AsyncIterator[MonitorEvent]:
        """
        Monitor all platforms and yield events.
        
        Combines events from Twitch and YouTube monitors.
        
        Yields:
            MonitorEvent for each state change.
        """
        if not self._running:
            self.start()
        
        # Create tasks for each monitor
        tasks = []
        
        if self._twitch_monitor:
            tasks.append(self._run_twitch_monitor())
        
        if self._youtube_monitor:
            tasks.append(self._run_youtube_monitor())
        
        if not tasks:
            self._logger.error("No monitors configured!")
            return
        
        # Use asyncio.Queue to merge events from multiple monitors
        event_queue: asyncio.Queue[MonitorEvent] = asyncio.Queue()
        
        async def feed_queue(monitor_coro):
            """Feed events from a monitor into the queue."""
            try:
                async for event in monitor_coro:
                    await event_queue.put(event)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._logger.error(f"Monitor error: {e}")
        
        # Start monitor tasks
        monitor_tasks = []
        
        if self._twitch_monitor:
            monitor_tasks.append(
                asyncio.create_task(feed_queue(self._twitch_monitor.monitor()))
            )
        
        if self._youtube_monitor:
            monitor_tasks.append(
                asyncio.create_task(feed_queue(self._youtube_monitor.monitor()))
            )
        
        try:
            while self._running:
                try:
                    # Wait for events with timeout to check running flag
                    event = await asyncio.wait_for(event_queue.get(), timeout=1.0)
                    yield event
                except asyncio.TimeoutError:
                    continue
        finally:
            # Cancel monitor tasks
            for task in monitor_tasks:
                task.cancel()
            
            await asyncio.gather(*monitor_tasks, return_exceptions=True)
    
    async def _run_twitch_monitor(self) -> AsyncIterator[MonitorEvent]:
        """Run Twitch monitor."""
        if self._twitch_monitor:
            async for event in self._twitch_monitor.monitor():
                yield event
    
    async def _run_youtube_monitor(self) -> AsyncIterator[MonitorEvent]:
        """Run YouTube monitor."""
        if self._youtube_monitor:
            async for event in self._youtube_monitor.monitor():
                yield event
    
    def is_live(self, channel: str) -> bool:
        """Check if a channel is currently live on any platform."""
        if self._twitch_monitor and channel in self._config.twitch.channels:
            return self._twitch_monitor.is_live(channel)
        
        if self._youtube_monitor and channel in self._config.youtube.channels:
            return self._youtube_monitor.is_live(channel)
        
        return False


async def main():
    """Test platform monitor."""
    from .logger import setup_logging
    from .config import load_config
    from .twitch_api import TwitchAPI
    
    setup_logging(level="INFO")
    
    config = load_config()
    
    # Initialize Twitch API if needed
    twitch_api = None
    if config.platform in ("twitch", "both") and config.twitch.client_id:
        twitch_api = TwitchAPI(
            client_id=config.twitch.client_id,
            client_secret=config.twitch.client_secret
        )
        await twitch_api.authenticate()
    
    monitor = PlatformMonitor(config, twitch_api)
    
    print(f"Platform: {config.platform}")
    print(f"Monitoring channels: {monitor.get_all_channels()}")
    print("\nStarting monitor (Ctrl+C to stop)...\n")
    
    try:
        async for event in monitor.monitor():
            print(f"🔔 {event.event.value}: {event.channel}")
            print(f"   Title: {event.stream_info.title}")
    except KeyboardInterrupt:
        monitor.stop()
        print("\nMonitor stopped")


if __name__ == '__main__':
    asyncio.run(main())
