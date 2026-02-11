"""
Twitch Helix API client for Stream Recorder v2.
Handles authentication and stream/channel info fetching.
"""

import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List

from .logger import get_logger


@dataclass
class StreamData:
    """Live stream information from Twitch API."""
    stream_id: str
    user_id: str
    user_login: str
    user_name: str
    game_id: str
    game_name: str
    title: str
    viewer_count: int
    started_at: datetime
    thumbnail_url: str
    is_live: bool = True


@dataclass
class ChannelInfo:
    """Channel information from Twitch API."""
    broadcaster_id: str
    broadcaster_login: str
    broadcaster_name: str
    game_id: str
    game_name: str
    title: str


class TwitchAPI:
    """
    Twitch Helix API client.
    
    Features:
    - Client Credentials authentication
    - Automatic token refresh
    - Stream status checking
    - Channel info fetching
    """
    
    BASE_URL = "https://api.twitch.tv/helix"
    AUTH_URL = "https://id.twitch.tv/oauth2/token"
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        oauth_token: Optional[str] = None
    ):
        """
        Initialize Twitch API client.
        
        Args:
            client_id: Twitch application Client ID.
            client_secret: Twitch application Client Secret.
            oauth_token: Optional user OAuth token for authenticated requests.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.oauth_token = oauth_token
        
        self._app_token: Optional[str] = None
        self._token_expires: Optional[datetime] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._logger = get_logger('twitch_api')
    
    async def connect(self) -> bool:
        """
        Initialize session and authenticate.
        
        Returns:
            True if connected successfully.
        """
        try:
            self._session = aiohttp.ClientSession()
            
            # Get app access token
            if not await self._refresh_token():
                return False
            
            self._logger.info("Connected to Twitch API")
            return True
            
        except Exception as e:
            self._logger.error(f"Failed to connect: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Close session."""
        if self._session:
            await self._session.close()
            self._session = None
    
    async def _refresh_token(self) -> bool:
        """Get or refresh app access token."""
        try:
            async with self._session.post(
                self.AUTH_URL,
                params={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'grant_type': 'client_credentials'
                }
            ) as resp:
                if resp.status != 200:
                    self._logger.error(f"Auth failed: {resp.status}")
                    return False
                
                data = await resp.json()
                self._app_token = data['access_token']
                expires_in = data.get('expires_in', 3600)
                self._token_expires = datetime.now() + timedelta(seconds=expires_in - 60)
                
                self._logger.debug("Got new app access token")
                return True
                
        except Exception as e:
            self._logger.error(f"Token refresh failed: {e}")
            return False
    
    async def _ensure_token(self) -> bool:
        """Ensure we have a valid token."""
        if not self._app_token or datetime.now() >= self._token_expires:
            return await self._refresh_token()
        return True
    
    def _headers(self) -> dict:
        """Get request headers."""
        # Always use app token - oauth_token may be from different app
        return {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {self._app_token}'
        }
    
    async def get_stream(self, channel: str) -> Optional[StreamData]:
        """
        Get live stream info for a channel.
        
        Args:
            channel: Twitch username (login).
            
        Returns:
            StreamData if live, None if offline.
        """
        if not await self._ensure_token():
            return None
        
        try:
            async with self._session.get(
                f"{self.BASE_URL}/streams",
                headers=self._headers(),
                params={'user_login': channel}
            ) as resp:
                if resp.status != 200:
                    self._logger.warning(f"API error: {resp.status}")
                    return None
                
                data = await resp.json()
                streams = data.get('data', [])
                
                if not streams:
                    return None
                
                s = streams[0]
                return StreamData(
                    stream_id=s['id'],
                    user_id=s['user_id'],
                    user_login=s['user_login'],
                    user_name=s['user_name'],
                    game_id=s.get('game_id', ''),
                    game_name=s.get('game_name', 'Unknown'),
                    title=s.get('title', ''),
                    viewer_count=s.get('viewer_count', 0),
                    started_at=datetime.fromisoformat(
                        s['started_at'].replace('Z', '+00:00')
                    ),
                    thumbnail_url=s.get('thumbnail_url', ''),
                    is_live=True
                )
                
        except Exception as e:
            self._logger.error(f"Failed to get stream: {e}")
            return None
    
    async def get_channel_info(self, channel: str) -> Optional[ChannelInfo]:
        """
        Get channel info (title, category) even when offline.
        
        Args:
            channel: Twitch username.
            
        Returns:
            ChannelInfo with current title/category.
        """
        if not await self._ensure_token():
            return None
        
        try:
            # First get user ID
            async with self._session.get(
                f"{self.BASE_URL}/users",
                headers=self._headers(),
                params={'login': channel}
            ) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                users = data.get('data', [])
                
                if not users:
                    return None
                
                user_id = users[0]['id']
            
            # Then get channel info
            async with self._session.get(
                f"{self.BASE_URL}/channels",
                headers=self._headers(),
                params={'broadcaster_id': user_id}
            ) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                channels = data.get('data', [])
                
                if not channels:
                    return None
                
                c = channels[0]
                return ChannelInfo(
                    broadcaster_id=c['broadcaster_id'],
                    broadcaster_login=c['broadcaster_login'],
                    broadcaster_name=c['broadcaster_name'],
                    game_id=c.get('game_id', ''),
                    game_name=c.get('game_name', 'Unknown'),
                    title=c.get('title', '')
                )
                
        except Exception as e:
            self._logger.error(f"Failed to get channel info: {e}")
            return None
    
    async def get_streams_batch(self, channels: List[str]) -> dict:
        """
        Get stream info for multiple channels at once.
        
        Args:
            channels: List of channel usernames.
            
        Returns:
            Dict mapping channel -> StreamData (or None if offline).
        """
        if not channels or not await self._ensure_token():
            return {}
        
        # Normalize to lowercase
        result = {ch.lower(): None for ch in channels}
        
        try:
            # API allows up to 100 channels per request
            params = [('user_login', ch) for ch in channels[:100]]
            
            # Verbose logging removed - too spammy
            # self._logger.debug(f"Checking streams for: {channels}")
            
            async with self._session.get(
                f"{self.BASE_URL}/streams",
                headers=self._headers(),
                params=params
            ) as resp:
                # self._logger.debug(f"API status: {resp.status}")
                
                if resp.status != 200:
                    text = await resp.text()
                    self._logger.error(f"API error: {resp.status} - {text}")
                    return result
                
                data = await resp.json()
                
                for s in data.get('data', []):
                    channel = s['user_login'].lower()
                    result[channel] = StreamData(
                        stream_id=s['id'],
                        user_id=s['user_id'],
                        user_login=s['user_login'],
                        user_name=s['user_name'],
                        game_id=s.get('game_id', ''),
                        game_name=s.get('game_name', 'Unknown'),
                        title=s.get('title', ''),
                        viewer_count=s.get('viewer_count', 0),
                        started_at=datetime.fromisoformat(
                            s['started_at'].replace('Z', '+00:00')
                        ),
                        thumbnail_url=s.get('thumbnail_url', ''),
                        is_live=True
                    )
                
        except Exception as e:
            self._logger.error(f"Failed to get streams batch: {e}")
        
        return result
    
    async def get_channels_info_batch(self, channels: List[str]) -> dict:
        """
        Get channel info for multiple channels.
        
        Args:
            channels: List of channel usernames.
            
        Returns:
            Dict mapping channel -> ChannelInfo.
        """
        if not channels or not await self._ensure_token():
            return {}
        
        result = {}
        
        try:
            # Get user IDs first
            params = [('login', ch) for ch in channels[:100]]
            
            async with self._session.get(
                f"{self.BASE_URL}/users",
                headers=self._headers(),
                params=params
            ) as resp:
                if resp.status != 200:
                    return result
                
                data = await resp.json()
                user_ids = [u['id'] for u in data.get('data', [])]
            
            if not user_ids:
                return result
            
            # Get channel info
            params = [('broadcaster_id', uid) for uid in user_ids]
            
            async with self._session.get(
                f"{self.BASE_URL}/channels",
                headers=self._headers(),
                params=params
            ) as resp:
                if resp.status != 200:
                    return result
                
                data = await resp.json()
                
                for c in data.get('data', []):
                    channel = c['broadcaster_login'].lower()
                    result[channel] = ChannelInfo(
                        broadcaster_id=c['broadcaster_id'],
                        broadcaster_login=c['broadcaster_login'],
                        broadcaster_name=c['broadcaster_name'],
                        game_id=c.get('game_id', ''),
                        game_name=c.get('game_name', 'Unknown'),
                        title=c.get('title', '')
                    )
                
        except Exception as e:
            self._logger.error(f"Failed to get channels info: {e}")
        
        return result


async def main():
    """Test the Twitch API client."""
    from .logger import setup_logging
    
    setup_logging(level="DEBUG")
    
    # Test with your credentials
    api = TwitchAPI(
        client_id="YOUR_CLIENT_ID",
        client_secret="YOUR_CLIENT_SECRET"
    )
    
    if await api.connect():
        # Test stream check
        stream = await api.get_stream("shroud")
        if stream:
            print(f"LIVE: {stream.user_name} - {stream.title}")
            print(f"  Game: {stream.game_name}")
            print(f"  Viewers: {stream.viewer_count}")
        else:
            print("Stream is offline")
        
        # Test channel info
        info = await api.get_channel_info("shroud")
        if info:
            print(f"Channel: {info.broadcaster_name}")
            print(f"  Title: {info.title}")
            print(f"  Game: {info.game_name}")
        
        await api.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
