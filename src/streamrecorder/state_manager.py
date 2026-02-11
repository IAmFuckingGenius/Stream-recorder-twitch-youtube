"""
State Manager for Stream Recorder v2.
JSON-based database for tracking stream states and recovery.
"""

import asyncio
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any
import aiofiles

from .logger import get_logger


class StreamStatus(Enum):
    """Stream processing status."""
    OFFLINE = "offline"           # Stream not live
    WAITING = "waiting"           # Title/category changed, waiting for stream
    RECORDING = "recording"       # Currently recording
    PROCESSING = "processing"     # Splitting/preparing files
    UPLOADING = "uploading"       # Uploading to Telegram
    COMPRESSING = "compressing"   # Compressing video
    SENDING_COMPRESSED = "sending_compressed"  # Sending to comments
    FORWARDING = "forwarding"     # Forwarding to backup
    DONE = "done"                 # All operations complete
    ERROR = "error"               # Error occurred


@dataclass
class MessageIds:
    """Telegram message IDs for a stream."""
    waiting_msg: Optional[int] = None      # "Waiting for stream" message
    status_msg: Optional[int] = None       # Status message during recording
    upload_msgs: List[int] = field(default_factory=list)  # Uploaded video messages
    compressed_msg: Optional[int] = None   # Compressed file in comments
    backup_msgs: List[int] = field(default_factory=list)  # Forwarded backup messages
    
    def to_dict(self) -> dict:
        return {
            'waiting_msg': self.waiting_msg,
            'status_msg': self.status_msg,
            'upload_msgs': self.upload_msgs,
            'compressed_msg': self.compressed_msg,
            'backup_msgs': self.backup_msgs
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'MessageIds':
        return cls(
            waiting_msg=data.get('waiting_msg'),
            status_msg=data.get('status_msg'),
            upload_msgs=data.get('upload_msgs', []),
            compressed_msg=data.get('compressed_msg'),
            backup_msgs=data.get('backup_msgs', [])
        )


@dataclass
class StreamState:
    """State of a single stream/recording session."""
    channel: str
    stream_id: str                          # Unique ID for this stream session
    title: str = ""
    category: str = ""
    status: StreamStatus = StreamStatus.OFFLINE
    
    # Title/category at stream start (for post caption)
    stream_start_title: Optional[str] = None
    stream_start_category: Optional[str] = None
    
    # History of title/category changes with timestamps
    # Format: [{"value": "title", "timestamp": "2026-01-10T12:00:00"}, ...]
    title_history: List[dict] = field(default_factory=list)
    category_history: List[dict] = field(default_factory=list)
    
    # Timing
    detected_at: Optional[str] = None       # When we first detected waiting/live
    started_at: Optional[str] = None        # When stream actually started
    ended_at: Optional[str] = None          # When stream ended
    
    # Files
    recording_file: Optional[str] = None    # Main recording file path (deprecated, use recording_files)
    recording_files: List[str] = field(default_factory=list)  # All recording segments (.ts files)
    split_files: List[str] = field(default_factory=list)  # Split parts
    compressed_file: Optional[str] = None   # Compressed file path
    
    # Session tracking - groups multiple segments from same stream
    session_id: Optional[str] = None
    
    # Cross-platform mode: link YT and Twitch recordings
    linked_channel: Optional[str] = None    # Channel this recording is linked to
    linked_recording_files: List[str] = field(default_factory=list)  # Recording files from linked channel
    
    # Telegram
    message_ids: MessageIds = field(default_factory=MessageIds)
    uploaded_parts: Dict[str, int] = field(default_factory=dict)  # file_path -> message_id
    
    # Metadata
    viewer_count: int = 0
    thumbnail_url: Optional[str] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'channel': self.channel,
            'stream_id': self.stream_id,
            'title': self.title,
            'category': self.category,
            'stream_start_title': self.stream_start_title,
            'stream_start_category': self.stream_start_category,
            'title_history': self.title_history,
            'category_history': self.category_history,
            'status': self.status.value,
            'detected_at': self.detected_at,
            'started_at': self.started_at,
            'ended_at': self.ended_at,
            'recording_file': self.recording_file,
            'recording_files': self.recording_files,
            'split_files': self.split_files,
            'compressed_file': self.compressed_file,
            'session_id': self.session_id,
            'message_ids': self.message_ids.to_dict(),
            'uploaded_parts': self.uploaded_parts,
            'viewer_count': self.viewer_count,
            'thumbnail_url': self.thumbnail_url,
            'error_message': self.error_message,
            'linked_channel': self.linked_channel,
            'linked_recording_files': self.linked_recording_files
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'StreamState':
        return cls(
            channel=data['channel'],
            stream_id=data['stream_id'],
            title=data.get('title', ''),
            category=data.get('category', ''),
            stream_start_title=data.get('stream_start_title'),
            stream_start_category=data.get('stream_start_category'),
            title_history=data.get('title_history', []),
            category_history=data.get('category_history', []),
            status=StreamStatus(data.get('status', 'offline')),
            detected_at=data.get('detected_at'),
            started_at=data.get('started_at'),
            ended_at=data.get('ended_at'),
            recording_file=data.get('recording_file'),
            recording_files=data.get('recording_files', []),
            split_files=data.get('split_files', []),
            compressed_file=data.get('compressed_file'),
            session_id=data.get('session_id'),
            message_ids=MessageIds.from_dict(data.get('message_ids', {})),
            uploaded_parts=data.get('uploaded_parts', {}),
            viewer_count=data.get('viewer_count', 0),
            thumbnail_url=data.get('thumbnail_url'),
            error_message=data.get('error_message'),
            linked_channel=data.get('linked_channel'),
            linked_recording_files=data.get('linked_recording_files', [])
        )


class StateManager:
    """
    Manages persistent state for stream recordings.
    
    Features:
    - JSON-based storage
    - Automatic saving on changes
    - Recovery of incomplete operations
    - Thread-safe with asyncio locks
    """
    
    def __init__(self, state_file: str = "./data/state.json"):
        """
        Initialize state manager.
        
        Args:
            state_file: Path to JSON state file.
        """
        self.state_file = Path(state_file)
        self._logger = get_logger('state')
        self._lock = asyncio.Lock()
        
        # Current states per channel
        self._states: Dict[str, StreamState] = {}
        
        # History of completed streams
        self._history: List[dict] = []
        
        # Ensure directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
    
    async def load(self) -> None:
        """Load state from file."""
        async with self._lock:
            if not self.state_file.exists():
                self._logger.info("No state file found, starting fresh")
                return
            
            try:
                async with aiofiles.open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.loads(await f.read())
                
                # Load active states
                for channel, state_data in data.get('active', {}).items():
                    self._states[channel] = StreamState.from_dict(state_data)
                
                # Load history
                self._history = data.get('history', [])
                
                self._logger.info(
                    f"Loaded state: {len(self._states)} active, "
                    f"{len(self._history)} in history"
                )
                
            except Exception as e:
                self._logger.error(f"Failed to load state: {e}")
    
    async def _save_unlocked(self) -> None:
        """Save state to file (caller must hold lock)."""
        data = {
            'active': {
                channel: state.to_dict()
                for channel, state in self._states.items()
            },
            'history': self._history[-100:],  # Keep last 100
            'last_updated': datetime.now().isoformat()
        }
        
        tmp_file = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        
        try:
            async with aiofiles.open(tmp_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
            tmp_file.replace(self.state_file)
        except Exception as e:
            self._logger.error(f"Failed to save state: {e}")
    
    async def save(self) -> None:
        """Save state to file."""
        async with self._lock:
            await self._save_unlocked()
    
    async def get_state(self, channel: str) -> Optional[StreamState]:
        """Get current state for a channel."""
        return self._states.get(channel)
    
    async def set_state(self, state: StreamState) -> None:
        """Set state for a channel and save."""
        async with self._lock:
            self._states[state.channel] = state
            await self._save_unlocked()
    
    async def update_status(
        self,
        channel: str,
        status: StreamStatus,
        error: Optional[str] = None
    ) -> None:
        """Update status for a channel."""
        async with self._lock:
            if channel in self._states:
                self._states[channel].status = status
                if error:
                    self._states[channel].error_message = error
                await self._save_unlocked()
    
    async def update_title_category(
        self,
        channel: str,
        title: str,
        category: str
    ) -> bool:
        """
        Update title/category, return True if changed.
        Adds changes to history with timestamps.
        """
        async with self._lock:
            if channel not in self._states:
                return False
            
            state = self._states[channel]
            now = datetime.now().isoformat()
            changed = False
            
            # Track title change
            if state.title != title:
                state.title_history.append({
                    'value': title,
                    'timestamp': now
                })
                state.title = title
                changed = True
            
            # Track category change
            if state.category != category:
                state.category_history.append({
                    'value': category,
                    'timestamp': now
                })
                state.category = category
                changed = True
            
            if changed:
                await self._save_unlocked()
            
            return changed
    
    async def set_message_id(
        self,
        channel: str,
        msg_type: str,
        msg_id: int
    ) -> None:
        """Set a message ID for a channel."""
        async with self._lock:
            if channel not in self._states:
                return
            
            ids = self._states[channel].message_ids
            
            if msg_type == 'waiting':
                ids.waiting_msg = msg_id
            elif msg_type == 'status':
                ids.status_msg = msg_id
            elif msg_type == 'upload':
                if msg_id not in ids.upload_msgs:
                    ids.upload_msgs.append(msg_id)
            elif msg_type == 'compressed':
                ids.compressed_msg = msg_id
            elif msg_type == 'backup':
                if msg_id not in ids.backup_msgs:
                    ids.backup_msgs.append(msg_id)
            
            await self._save_unlocked()
    
    async def clear_message_id(self, channel: str, msg_type: str) -> None:
        """Clear a message ID or list."""
        async with self._lock:
            if channel not in self._states:
                return
            
            ids = self._states[channel].message_ids
            
            if msg_type == 'waiting':
                ids.waiting_msg = None
            elif msg_type == 'status':
                ids.status_msg = None
            elif msg_type == 'upload':
                ids.upload_msgs = []
                self._states[channel].uploaded_parts = {}
            elif msg_type == 'backup':
                ids.backup_msgs = []
            elif msg_type == 'compressed':
                ids.compressed_msg = None
            
            await self._save_unlocked()

    async def set_uploaded_part(self, channel: str, file_path: str, msg_id: int) -> None:
        """Persist mapping from split file path to uploaded Telegram message ID."""
        async with self._lock:
            if channel not in self._states:
                return
            self._states[channel].uploaded_parts[file_path] = msg_id
            if msg_id not in self._states[channel].message_ids.upload_msgs:
                self._states[channel].message_ids.upload_msgs.append(msg_id)
            await self._save_unlocked()
    
    async def set_recording_file(self, channel: str, file_path: str) -> None:
        """Set recording file path (legacy, also adds to recording_files)."""
        async with self._lock:
            if channel in self._states:
                self._states[channel].recording_file = file_path
                # Also add to recording_files list if not already there
                if file_path not in self._states[channel].recording_files:
                    self._states[channel].recording_files.append(file_path)
                await self._save_unlocked()
    
    async def add_recording_file(self, channel: str, file_path: str) -> None:
        """Add a recording file to the session."""
        async with self._lock:
            if channel in self._states:
                if file_path not in self._states[channel].recording_files:
                    self._states[channel].recording_files.append(file_path)
                await self._save_unlocked()
    
    async def get_all_recording_files(self, channel: str) -> List[str]:
        """Get all recording files for a channel session."""
        if channel in self._states:
            return self._states[channel].recording_files.copy()
        return []
    
    async def set_session_id(self, channel: str, session_id: str) -> None:
        """Set session ID for grouping multiple recordings."""
        async with self._lock:
            if channel in self._states:
                self._states[channel].session_id = session_id
                await self._save_unlocked()
    
    async def get_session_id(self, channel: str) -> Optional[str]:
        """Get session ID for a channel."""
        if channel in self._states:
            return self._states[channel].session_id
        return None
    
    async def set_split_files(self, channel: str, files: List[str]) -> None:
        """Set split file paths."""
        async with self._lock:
            if channel in self._states:
                self._states[channel].split_files = files
                await self._save_unlocked()
    
    async def set_compressed_file(self, channel: str, file_path: str) -> None:
        """Set compressed file path."""
        async with self._lock:
            if channel in self._states:
                self._states[channel].compressed_file = file_path
                await self._save_unlocked()
    
    async def update_linked_channel(self, channel: str, linked_channel: str) -> None:
        """Set linked channel for cross-platform mode."""
        async with self._lock:
            if channel in self._states:
                self._states[channel].linked_channel = linked_channel
                await self._save_unlocked()
    
    async def add_linked_recording_file(self, channel: str, file_path: str) -> None:
        """Add a linked recording file for cross-platform mode."""
        async with self._lock:
            if channel in self._states:
                self._states[channel].linked_recording_files.append(file_path)
                await self._save_unlocked()
    
    async def complete_stream(self, channel: str) -> None:
        """Mark stream as complete and move to history."""
        async with self._lock:
            if channel not in self._states:
                return
            
            state = self._states[channel]
            state.status = StreamStatus.DONE
            
            # Add to history
            self._history.append({
                **state.to_dict(),
                'completed_at': datetime.now().isoformat()
            })
            
            # Remove from active
            del self._states[channel]
            await self._save_unlocked()
        
        self._logger.info(f"[{channel}] Stream completed and archived")
    
    async def get_incomplete_streams(self) -> List[StreamState]:
        """
        Get streams that need recovery.
        
        Returns streams in states: RECORDING, PROCESSING, UPLOADING, etc.
        """
        async with self._lock:
            incomplete = []
            
            for state in self._states.values():
                if state.status not in (StreamStatus.OFFLINE, StreamStatus.WAITING, StreamStatus.DONE):
                    incomplete.append(state)
            
            return incomplete

    async def get_waiting_streams(self) -> List[StreamState]:
        """Get streams currently in WAITING state."""
        async with self._lock:
            return [
                state
                for state in self._states.values()
                if state.status == StreamStatus.WAITING
            ]
    
    async def create_stream(
        self,
        channel: str,
        title: str = "",
        category: str = "",
        status: StreamStatus = StreamStatus.OFFLINE
    ) -> StreamState:
        """Create a new stream state with initial title/category in history."""
        async with self._lock:
            stream_id = f"{channel}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            now = datetime.now().isoformat()
            
            # Initialize history with first values
            title_history = [{'value': title, 'timestamp': now}] if title else []
            category_history = [{'value': category, 'timestamp': now}] if category else []
            
            state = StreamState(
                channel=channel,
                stream_id=stream_id,
                title=title,
                category=category,
                title_history=title_history,
                category_history=category_history,
                status=status,
                detected_at=now
            )
            
            self._states[channel] = state
            await self._save_unlocked()
            
            return state
    
    async def start_recording(self, channel: str) -> None:
        """Mark stream as started recording. Saves current title/category for caption."""
        async with self._lock:
            if channel in self._states:
                state = self._states[channel]
                state.status = StreamStatus.RECORDING
                state.started_at = datetime.now().isoformat()
                # Save title/category at stream start for use in post caption
                state.stream_start_title = state.title
                state.stream_start_category = state.category
                await self._save_unlocked()
    
    async def end_recording(self, channel: str) -> None:
        """Mark recording as ended."""
        async with self._lock:
            if channel in self._states:
                self._states[channel].ended_at = datetime.now().isoformat()
                await self._save_unlocked()

    async def delete_stream(self, channel: str) -> None:
        """Delete a stream from active state (used for clean cleaning)."""
        async with self._lock:
            if channel in self._states:
                del self._states[channel]
                await self._save_unlocked()
        
        self._logger.info(f"[{channel}] Stream deleted from active state")
