"""
Stream Monitor v2 for Twitch streams.
Uses Twitch Helix API for fast and reliable stream detection.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import AsyncIterator, Dict, Optional, Tuple

from .logger import get_logger, get_channel_logger
from .twitch_api import TwitchAPI, StreamData, ChannelInfo


class StreamStatus(Enum):
    """Stream status states."""
    UNKNOWN = "unknown"
    OFFLINE = "offline"
    WAITING = "waiting"  # Title/category changed, stream expected
    LIVE = "live"


class StreamEvent(Enum):
    """Events emitted by monitor."""
    WENT_LIVE = "went_live"
    WENT_OFFLINE = "went_offline"
    INFO_CHANGED = "info_changed"  # Title or category changed while offline


@dataclass
class StreamInfo:
    """Information about a stream."""
    channel: str
    title: str
    category: str
    viewers: int = 0
    started_at: Optional[datetime] = None
    thumbnail_url: Optional[str] = None
    stream_url: str = ""
    is_live: bool = False
    output_ext: str = "mp4"  # Default extension
    
    @classmethod
    def from_api(cls, data: StreamData) -> 'StreamInfo':
        """Create from Twitch API response."""
        return cls(
            channel=data.user_login,
            title=data.title,
            category=data.game_name,
            viewers=data.viewer_count,
            started_at=data.started_at,
            thumbnail_url=data.thumbnail_url,
            stream_url=f"https://www.twitch.tv/{data.user_login}",
            is_live=True
        )
    
    @classmethod
    def from_channel_info(cls, info: ChannelInfo) -> 'StreamInfo':
        """Create from channel info (offline)."""
        return cls(
            channel=info.broadcaster_login,
            title=info.title,
            category=info.game_name,
            stream_url=f"https://www.twitch.tv/{info.broadcaster_login}",
            is_live=False
        )


@dataclass
class MonitorEvent:
    """Event from stream monitor."""
    event: StreamEvent
    channel: str
    stream_info: StreamInfo
    old_title: Optional[str] = None
    old_category: Optional[str] = None


class StreamMonitor:
    """
    Monitors Twitch channels using Helix API.
    
    Features:
    - Fast polling (every few seconds)
    - Detects stream start/end
    - Detects title/category changes when offline
    - Emits events for state changes
    """
    
    def __init__(
        self,
        twitch_api: TwitchAPI,
        channels: list[str],
        check_interval: int = 5,
        offline_grace_period: int = 180
    ):
        """
        Initialize stream monitor.
        
        Args:
            twitch_api: Initialized TwitchAPI client.
            channels: List of channel usernames to monitor.
            check_interval: Seconds between checks.
            offline_grace_period: Seconds to wait before confirming stream ended.
        """
        self.api = twitch_api
        self.channels = [ch.lower() for ch in channels]
        self.check_interval = check_interval
        self.offline_grace_period = offline_grace_period
        
        self._logger = get_logger('monitor')
        self._running = False
        
        # Track state per channel
        self._states: Dict[str, StreamStatus] = {}
        self._last_info: Dict[str, StreamInfo] = {}
        
        # Grace period tracking - when channel first appeared offline
        self._offline_since: Dict[str, datetime] = {}
    
    def start(self) -> None:
        """Start monitoring."""
        self._running = True
        self._logger.info(f"Starting to monitor {len(self.channels)} Twitch channels")
        
        # Initialize states
        for channel in self.channels:
            self._states[channel] = StreamStatus.UNKNOWN
    
    def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        self._logger.info("Twitch monitor stopped")
    
    async def monitor(self) -> AsyncIterator[MonitorEvent]:
        """
        Monitor channels and yield events.
        
        Yields:
            MonitorEvent for each state change.
        """
        # Ensure we're started
        if not self._running:
            self.start()
        
        while self._running:
            try:
                # Check all channels in batch
                events = await self._check_all_channels()
                
                for event in events:
                    yield event
                
            except Exception as e:
                self._logger.error(f"Monitor error: {e}")
            
            # Sleep in small chunks to allow quick shutdown
            for _ in range(self.check_interval):
                if not self._running:
                    return
                await asyncio.sleep(1)
    
    async def _check_all_channels(self) -> list[MonitorEvent]:
        """Check all channels and return events."""
        events = []
        
        # Get stream status for all channels (batch request)
        streams = await self.api.get_streams_batch(self.channels)
        
        # Verbose logging removed - too spammy
        # self._logger.debug(f"API returned streams for: {[k for k, v in streams.items() if v]}")
        
        for channel in self.channels:
            stream_data = streams.get(channel)
            old_status = self._states.get(channel, StreamStatus.UNKNOWN)
            old_info = self._last_info.get(channel)
            
            logger = get_channel_logger(channel)
            
            if stream_data:
                # Stream is LIVE
                new_info = StreamInfo.from_api(stream_data)
                
                # Clear any grace period tracking - stream is back/still live
                recovered_from_drop = False
                if channel in self._offline_since:
                    elapsed = (datetime.now() - self._offline_since[channel]).total_seconds()
                    logger.info(f"🔄 Stream recovered after {elapsed:.0f}s drop")
                    del self._offline_since[channel]
                    recovered_from_drop = True
                
                if old_status != StreamStatus.LIVE or recovered_from_drop:
                    # Just went live OR recovered from a drop!
                    self._states[channel] = StreamStatus.LIVE
                    self._last_info[channel] = new_info
                    
                    if recovered_from_drop:
                        logger.info(f"🔴 Stream recovered! triggering recording restart")
                    else:
                        logger.info(f"🔴 Stream started!")
                        
                    events.append(MonitorEvent(
                        event=StreamEvent.WENT_LIVE,
                        channel=channel,
                        stream_info=new_info
                    ))
                else:
                    # Still live, update info
                    self._last_info[channel] = new_info
            else:
                # Stream appears OFFLINE
                # Get channel info to check for title/category changes
                channel_info = await self.api.get_channel_info(channel)
                
                if channel_info:
                    new_info = StreamInfo.from_channel_info(channel_info)
                    
                    if old_status == StreamStatus.LIVE:
                        # Stream just dropped - start grace period
                        if channel not in self._offline_since:
                            self._offline_since[channel] = datetime.now()
                            logger.info(f"⏸️ Stream dropped, waiting {self.offline_grace_period}s before ending...")
                        
                        # Check if grace period expired
                        elapsed = (datetime.now() - self._offline_since[channel]).total_seconds()
                        if elapsed >= self.offline_grace_period:
                            # Grace period expired - stream is truly offline
                            self._states[channel] = StreamStatus.OFFLINE
                            self._last_info[channel] = new_info
                            del self._offline_since[channel]
                            
                            logger.info(f"⚫ Stream ended (after {elapsed:.0f}s grace period)")
                            events.append(MonitorEvent(
                                event=StreamEvent.WENT_OFFLINE,
                                channel=channel,
                                stream_info=new_info
                            ))
                        else:
                            # Still in grace period - don't emit offline event yet
                            logger.debug(f"Grace period: {elapsed:.0f}s / {self.offline_grace_period}s")
                    
                    elif old_info and (
                        old_info.title != new_info.title or 
                        old_info.category != new_info.category
                    ):
                        # Title or category changed while offline
                        old_title = old_info.title
                        old_category = old_info.category
                        
                        self._states[channel] = StreamStatus.WAITING
                        self._last_info[channel] = new_info
                        
                        logger.info(
                            f"📝 Info changed: '{new_info.title}' / {new_info.category}"
                        )
                        events.append(MonitorEvent(
                            event=StreamEvent.INFO_CHANGED,
                            channel=channel,
                            stream_info=new_info,
                            old_title=old_title,
                            old_category=old_category
                        ))
                    
                    elif old_status == StreamStatus.UNKNOWN:
                        # First check, just save info
                        self._states[channel] = StreamStatus.OFFLINE
                        self._last_info[channel] = new_info
                else:
                    # Channel info API failed/unavailable.
                    # Do not keep stream "LIVE" forever - fallback to grace-period offline handling.
                    if old_status == StreamStatus.LIVE:
                        if channel not in self._offline_since:
                            self._offline_since[channel] = datetime.now()
                            logger.warning(
                                "Twitch channel info unavailable while stream appears offline; "
                                f"waiting {self.offline_grace_period}s before forcing offline..."
                            )
                        
                        elapsed = (datetime.now() - self._offline_since[channel]).total_seconds()
                        if elapsed >= self.offline_grace_period:
                            if old_info:
                                offline_info = StreamInfo(
                                    channel=channel,
                                    title=old_info.title,
                                    category=old_info.category,
                                    viewers=0,
                                    started_at=old_info.started_at,
                                    thumbnail_url=old_info.thumbnail_url,
                                    stream_url=old_info.stream_url or f"https://www.twitch.tv/{channel}",
                                    is_live=False
                                )
                            else:
                                offline_info = StreamInfo(
                                    channel=channel,
                                    title="",
                                    category="",
                                    stream_url=f"https://www.twitch.tv/{channel}",
                                    is_live=False
                                )
                            
                            self._states[channel] = StreamStatus.OFFLINE
                            self._last_info[channel] = offline_info
                            del self._offline_since[channel]
                            
                            logger.warning(
                                "⚫ Stream ended (forced offline after grace period due missing channel info)"
                            )
                            events.append(MonitorEvent(
                                event=StreamEvent.WENT_OFFLINE,
                                channel=channel,
                                stream_info=offline_info
                            ))
                    elif old_status == StreamStatus.UNKNOWN:
                        # Keep monitor state consistent even if channel metadata endpoint is flaky.
                        self._states[channel] = StreamStatus.OFFLINE
        
        return events
    
    def get_channel_status(self, channel: str) -> Tuple[StreamStatus, Optional[StreamInfo]]:
        """Get current status and info for a channel."""
        return (
            self._states.get(channel, StreamStatus.UNKNOWN),
            self._last_info.get(channel)
        )
    
    def is_live(self, channel: str) -> bool:
        """Check if channel is currently live."""
        return self._states.get(channel) == StreamStatus.LIVE


async def main():
    """Test stream monitor."""
    from .logger import setup_logging
    
    setup_logging(level="DEBUG")
    
    api = TwitchAPI(
        client_id="YOUR_CLIENT_ID",
        client_secret="YOUR_CLIENT_SECRET"
    )
    
    if not await api.connect():
        print("Failed to connect to Twitch API")
        return
    
    monitor = StreamMonitor(
        twitch_api=api,
        channels=["shroud", "pokimane"],
        check_interval=5
    )
    
    monitor.start()
    
    try:
        async for event in monitor.monitor():
            print(f"Event: {event.event.value}")
            print(f"  Channel: {event.channel}")
            print(f"  Title: {event.stream_info.title}")
            print(f"  Live: {event.stream_info.is_live}")
    except KeyboardInterrupt:
        pass
    finally:
        monitor.stop()
        await api.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
