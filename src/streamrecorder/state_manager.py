"""
SQLite-backed recovery state for Stream Recorder.

The public API intentionally keeps channel-based methods for existing callers,
but active rows are stored by source_id + session_id. Channel lookups resolve to
the latest active row for that source.
"""

from __future__ import annotations

import asyncio
import copy
import json
import shutil
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from .logger import get_logger


@contextmanager
def sqlite_conn(path: Path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class StreamStatus(Enum):
    """Stream processing status."""

    OFFLINE = "offline"
    WAITING = "waiting"
    RECORDING = "recording"
    PROCESSING = "processing"
    UPLOADING = "uploading"
    COMPRESSING = "compressing"
    SENDING_COMPRESSED = "sending_compressed"
    FORWARDING = "forwarding"
    DONE = "done"
    ERROR = "error"


@dataclass
class MessageIds:
    """Telegram message IDs for a stream."""

    waiting_msg: Optional[int] = None
    status_msg: Optional[int] = None
    upload_msgs: List[int] = field(default_factory=list)
    compressed_msg: Optional[int] = None
    backup_msgs: List[int] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "waiting_msg": self.waiting_msg,
            "status_msg": self.status_msg,
            "upload_msgs": self.upload_msgs,
            "compressed_msg": self.compressed_msg,
            "backup_msgs": self.backup_msgs,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MessageIds":
        return cls(
            waiting_msg=data.get("waiting_msg"),
            status_msg=data.get("status_msg"),
            upload_msgs=list(data.get("upload_msgs", [])),
            compressed_msg=data.get("compressed_msg"),
            backup_msgs=list(data.get("backup_msgs", [])),
        )


@dataclass
class StreamState:
    """State of a single stream/recording session."""

    channel: str
    stream_id: str
    title: str = ""
    category: str = ""
    status: StreamStatus = StreamStatus.OFFLINE
    source_id: Optional[str] = None
    target_profile_id: Optional[str] = None
    target_upload_channel_id: Optional[int] = None
    target_discussion_group_id: Optional[int] = None
    target_backup_channel_id: Optional[int] = None
    target_upload_parallelism: Optional[int] = None
    target_upload_speed_limit_mbps: Optional[float] = None
    target_compression_enabled: Optional[bool] = None
    target_splitting_default_size_gb: Optional[float] = None
    target_splitting_premium_size_gb: Optional[float] = None
    stream_start_title: Optional[str] = None
    stream_start_category: Optional[str] = None
    title_history: List[dict] = field(default_factory=list)
    category_history: List[dict] = field(default_factory=list)
    detected_at: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    recording_file: Optional[str] = None
    recording_files: List[str] = field(default_factory=list)
    split_files: List[str] = field(default_factory=list)
    compressed_file: Optional[str] = None
    session_id: Optional[str] = None
    linked_channel: Optional[str] = None
    linked_recording_files: List[str] = field(default_factory=list)
    message_ids: MessageIds = field(default_factory=MessageIds)
    uploaded_parts: Dict[str, int] = field(default_factory=dict)
    viewer_count: int = 0
    thumbnail_url: Optional[str] = None
    error_message: Optional[str] = None
    skip_upload: bool = False
    skip_compression: bool = False
    skip_forwarding: bool = False
    abort_requested: bool = False

    def to_dict(self) -> dict:
        return {
            "channel": self.channel,
            "source_id": self.source_id or source_id_for_channel(self.channel),
            "target_profile_id": self.target_profile_id,
            "target_upload_channel_id": self.target_upload_channel_id,
            "target_discussion_group_id": self.target_discussion_group_id,
            "target_backup_channel_id": self.target_backup_channel_id,
            "target_upload_parallelism": self.target_upload_parallelism,
            "target_upload_speed_limit_mbps": self.target_upload_speed_limit_mbps,
            "target_compression_enabled": self.target_compression_enabled,
            "target_splitting_default_size_gb": self.target_splitting_default_size_gb,
            "target_splitting_premium_size_gb": self.target_splitting_premium_size_gb,
            "stream_id": self.stream_id,
            "title": self.title,
            "category": self.category,
            "stream_start_title": self.stream_start_title,
            "stream_start_category": self.stream_start_category,
            "title_history": self.title_history,
            "category_history": self.category_history,
            "status": self.status.value,
            "detected_at": self.detected_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "recording_file": self.recording_file,
            "recording_files": self.recording_files,
            "split_files": self.split_files,
            "compressed_file": self.compressed_file,
            "session_id": self.session_id,
            "message_ids": self.message_ids.to_dict(),
            "uploaded_parts": self.uploaded_parts,
            "viewer_count": self.viewer_count,
            "thumbnail_url": self.thumbnail_url,
            "error_message": self.error_message,
            "linked_channel": self.linked_channel,
            "linked_recording_files": self.linked_recording_files,
            "skip_upload": self.skip_upload,
            "skip_compression": self.skip_compression,
            "skip_forwarding": self.skip_forwarding,
            "abort_requested": self.abort_requested,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "StreamState":
        channel = data["channel"]
        return cls(
            channel=channel,
            source_id=data.get("source_id") or source_id_for_channel(channel),
            target_profile_id=data.get("target_profile_id"),
            target_upload_channel_id=data.get("target_upload_channel_id"),
            target_discussion_group_id=data.get("target_discussion_group_id"),
            target_backup_channel_id=data.get("target_backup_channel_id"),
            target_upload_parallelism=data.get("target_upload_parallelism"),
            target_upload_speed_limit_mbps=data.get("target_upload_speed_limit_mbps"),
            target_compression_enabled=data.get("target_compression_enabled"),
            target_splitting_default_size_gb=data.get("target_splitting_default_size_gb"),
            target_splitting_premium_size_gb=data.get("target_splitting_premium_size_gb"),
            stream_id=data["stream_id"],
            title=data.get("title", ""),
            category=data.get("category", ""),
            stream_start_title=data.get("stream_start_title"),
            stream_start_category=data.get("stream_start_category"),
            title_history=list(data.get("title_history", [])),
            category_history=list(data.get("category_history", [])),
            status=StreamStatus(data.get("status", "offline")),
            detected_at=data.get("detected_at"),
            started_at=data.get("started_at"),
            ended_at=data.get("ended_at"),
            recording_file=data.get("recording_file"),
            recording_files=list(data.get("recording_files", [])),
            split_files=list(data.get("split_files", [])),
            compressed_file=data.get("compressed_file"),
            session_id=data.get("session_id"),
            message_ids=MessageIds.from_dict(data.get("message_ids", {})),
            uploaded_parts=dict(data.get("uploaded_parts", {})),
            viewer_count=int(data.get("viewer_count", 0) or 0),
            thumbnail_url=data.get("thumbnail_url"),
            error_message=data.get("error_message"),
            linked_channel=data.get("linked_channel"),
            linked_recording_files=list(data.get("linked_recording_files", [])),
            skip_upload=bool(data.get("skip_upload", False)),
            skip_compression=bool(data.get("skip_compression", False)),
            skip_forwarding=bool(data.get("skip_forwarding", False)),
            abort_requested=bool(data.get("abort_requested", False)),
        )


def source_id_for_channel(channel: str) -> str:
    platform = "youtube" if channel.startswith("@") or channel.startswith("UC") else "twitch"
    return f"{platform}:{channel.strip().lower()}"


class StateManager:
    """Manages persistent stream recovery state in SQLite."""

    def __init__(self, state_file: str = "./data/state.sqlite3"):
        self.state_file = Path(state_file)
        self._logger = get_logger("state")
        self._lock = asyncio.Lock()
        self._states: Dict[str, StreamState] = {}
        self._state_keys_by_channel: Dict[str, tuple[str, str]] = {}
        self._history: List[dict] = []
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._sqlite_path = self._resolve_sqlite_path(self.state_file)
        self._sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    def _resolve_sqlite_path(self, path: Path) -> Path:
        if path.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
            return path
        return path.with_suffix(".sqlite3")

    def _backup_corrupt_state(self) -> Optional[Path]:
        if not self.state_file.exists():
            return None
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.state_file.with_name(f"{self.state_file.name}.corrupt-{timestamp}.bak")
        try:
            shutil.copy2(self.state_file, backup_path)
            return backup_path
        except Exception as e:
            self._logger.error(f"Failed to backup corrupt state file: {e}")
            return None

    async def load(self) -> None:
        async with self._lock:
            self._load_unlocked_sync()

    def _load_unlocked_sync(self) -> None:
        self._init_db()
        if self.state_file.exists() and self.state_file != self._sqlite_path:
            self._migrate_json_file()
        self._states = self._load_active_from_db()
        self._state_keys_by_channel = {
            channel: self._state_keys(state)
            for channel, state in self._states.items()
        }
        self._history = self._load_history_from_db()
        self._logger.info(
            f"Loaded state: {len(self._states)} active, {len(self._history)} in history"
        )

    def _init_db(self) -> None:
        with sqlite_conn(self._sqlite_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS active_streams (
                    source_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    status TEXT NOT NULL,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_id, session_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_active_streams_channel_updated
                ON active_streams(channel, updated_at DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    data TEXT NOT NULL
                )
                """
            )

    def _migrate_json_file(self) -> None:
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            states = {
                channel: StreamState.from_dict(state_data)
                for channel, state_data in data.get("active", {}).items()
            }
            history = list(data.get("history", []))
        except Exception as e:
            backup_path = self._backup_corrupt_state()
            backup_msg = f" Backup saved to: {backup_path}" if backup_path else " Backup failed."
            if isinstance(e, json.JSONDecodeError):
                message = (
                    f"Failed to load state file {self.state_file}: {e}."
                    f" Refusing to start with empty state to avoid losing recovery data.{backup_msg}"
                )
            else:
                message = (
                    f"State file {self.state_file} has invalid contents: {e}."
                    f" Refusing to start with empty state to avoid losing recovery data.{backup_msg}"
                )
            self._logger.error(message)
            raise RuntimeError(message) from e

        with sqlite_conn(self._sqlite_path) as conn:
            existing = conn.execute("SELECT COUNT(*) FROM active_streams").fetchone()[0]
            if existing:
                return
            for state in states.values():
                self._upsert_state_conn(conn, state)
            for item in history:
                self._insert_history_conn(conn, item)
        backup = self.state_file.with_name(f"{self.state_file.name}.migrated-{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak")
        shutil.copy2(self.state_file, backup)
        self._logger.info(f"Migrated JSON state to SQLite: {self._sqlite_path}; backup: {backup}")

    def _load_active_from_db(self) -> Dict[str, StreamState]:
        states: Dict[str, StreamState] = {}
        with sqlite_conn(self._sqlite_path) as conn:
            rows = conn.execute("SELECT data FROM active_streams ORDER BY updated_at").fetchall()
        for row in rows:
            state = StreamState.from_dict(json.loads(row["data"]))
            if state.channel in states:
                previous = states[state.channel]
                self._logger.warning(
                    "Multiple active sessions found for %s; using latest session %s and preserving older row %s",
                    state.channel,
                    state.session_id or state.stream_id,
                    previous.session_id or previous.stream_id,
                )
            states[state.channel] = state
        return states

    def _load_history_from_db(self) -> List[dict]:
        with sqlite_conn(self._sqlite_path) as conn:
            rows = conn.execute("SELECT data FROM stream_history ORDER BY id DESC LIMIT 100").fetchall()
        return [json.loads(row["data"]) for row in reversed(rows)]

    def _state_keys(self, state: StreamState) -> tuple[str, str]:
        source_id = state.source_id or source_id_for_channel(state.channel)
        session_id = state.session_id or state.stream_id
        state.source_id = source_id
        state.session_id = session_id
        return source_id, session_id

    def _upsert_state_conn(self, conn: sqlite3.Connection, state: StreamState) -> None:
        source_id, session_id = self._state_keys(state)
        conn.execute(
            """
            INSERT INTO active_streams(source_id, session_id, channel, status, data, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, session_id) DO UPDATE SET
                channel=excluded.channel,
                status=excluded.status,
                data=excluded.data,
                updated_at=excluded.updated_at
            """,
            (
                source_id,
                session_id,
                state.channel,
                state.status.value,
                json.dumps(state.to_dict(), ensure_ascii=False),
                datetime.now().isoformat(),
            ),
        )

    def _delete_state_conn(self, conn: sqlite3.Connection, state: StreamState) -> None:
        source_id, session_id = self._state_keys(state)
        conn.execute(
            "DELETE FROM active_streams WHERE source_id = ? AND session_id = ?",
            (source_id, session_id),
        )

    def _insert_history_conn(self, conn: sqlite3.Connection, item: dict) -> None:
        channel = item.get("channel", "")
        source_id = item.get("source_id") or source_id_for_channel(channel)
        session_id = item.get("session_id") or item.get("stream_id") or "unknown"
        completed_at = item.get("completed_at") or datetime.now().isoformat()
        conn.execute(
            """
            INSERT INTO stream_history(source_id, session_id, channel, completed_at, data)
            VALUES(?, ?, ?, ?, ?)
            """,
            (source_id, session_id, channel, completed_at, json.dumps(item, ensure_ascii=False)),
        )

    async def _save_unlocked(self) -> None:
        snapshot = {key: copy.deepcopy(value) for key, value in self._states.items()}
        history = copy.deepcopy(self._history[-100:])
        self._save_snapshot_sync(snapshot, history)

    def _save_snapshot_sync(self, states: Dict[str, StreamState], history: List[dict]) -> None:
        self._init_db()
        with sqlite_conn(self._sqlite_path) as conn:
            desired_keys = {
                self._state_keys(state)
                for state in states.values()
            }
            if desired_keys:
                for row in conn.execute("SELECT source_id, session_id FROM active_streams").fetchall():
                    key = (row["source_id"], row["session_id"])
                    if key not in desired_keys:
                        conn.execute(
                            "DELETE FROM active_streams WHERE source_id = ? AND session_id = ?",
                            key,
                        )
            else:
                conn.execute("DELETE FROM active_streams")

            for state in states.values():
                self._upsert_state_conn(conn, state)
            existing_history_count = conn.execute("SELECT COUNT(*) FROM stream_history").fetchone()[0]
            if existing_history_count == 0:
                for item in history:
                    self._insert_history_conn(conn, item)

    async def save(self) -> None:
        async with self._lock:
            await self._save_unlocked()

    async def get_state(self, channel: str) -> Optional[StreamState]:
        async with self._lock:
            state = self._states.get(channel)
            return copy.deepcopy(state) if state else None

    async def set_state(self, state: StreamState) -> None:
        async with self._lock:
            state.source_id = state.source_id or source_id_for_channel(state.channel)
            state.session_id = state.session_id or state.stream_id
            self._states[state.channel] = state
            self._state_keys_by_channel[state.channel] = self._state_keys(state)
            await self._save_unlocked()

    async def update_status(self, channel: str, status: StreamStatus, error: Optional[str] = None) -> None:
        async with self._lock:
            if channel in self._states:
                self._states[channel].status = status
                if error:
                    self._states[channel].error_message = error
                await self._save_unlocked()

    async def update_title_category(self, channel: str, title: str, category: str) -> bool:
        async with self._lock:
            if channel not in self._states:
                return False
            state = self._states[channel]
            now = datetime.now().isoformat()
            changed = False
            if state.title != title:
                state.title_history.append({"value": title, "timestamp": now})
                state.title = title
                changed = True
            if state.category != category:
                state.category_history.append({"value": category, "timestamp": now})
                state.category = category
                changed = True
            if changed:
                await self._save_unlocked()
            return changed

    async def set_message_id(self, channel: str, msg_type: str, msg_id: int) -> None:
        async with self._lock:
            if channel not in self._states:
                return
            ids = self._states[channel].message_ids
            if msg_type == "waiting":
                ids.waiting_msg = msg_id
            elif msg_type == "status":
                ids.status_msg = msg_id
            elif msg_type == "upload" and msg_id not in ids.upload_msgs:
                ids.upload_msgs.append(msg_id)
            elif msg_type == "compressed":
                ids.compressed_msg = msg_id
            elif msg_type == "backup" and msg_id not in ids.backup_msgs:
                ids.backup_msgs.append(msg_id)
            await self._save_unlocked()

    async def clear_message_id(self, channel: str, msg_type: str) -> None:
        async with self._lock:
            if channel not in self._states:
                return
            ids = self._states[channel].message_ids
            if msg_type == "waiting":
                ids.waiting_msg = None
            elif msg_type == "status":
                ids.status_msg = None
            elif msg_type == "upload":
                ids.upload_msgs = []
                self._states[channel].uploaded_parts = {}
            elif msg_type == "backup":
                ids.backup_msgs = []
            elif msg_type == "compressed":
                ids.compressed_msg = None
            await self._save_unlocked()

    async def set_target_snapshot(self, channel: str, target: Any) -> None:
        async with self._lock:
            if channel not in self._states:
                return
            state = self._states[channel]
            state.target_profile_id = getattr(target, "id", None)
            state.target_upload_channel_id = getattr(target, "upload_channel_id", None)
            state.target_discussion_group_id = getattr(target, "discussion_group_id", None)
            state.target_backup_channel_id = getattr(target, "backup_channel_id", None)
            state.target_upload_parallelism = getattr(target, "upload_parallelism", None)
            state.target_upload_speed_limit_mbps = getattr(target, "upload_speed_limit_mbps", None)
            state.target_compression_enabled = getattr(target, "compression_enabled", None)
            state.target_splitting_default_size_gb = getattr(target, "splitting_default_size_gb", None)
            state.target_splitting_premium_size_gb = getattr(target, "splitting_premium_size_gb", None)
            await self._save_unlocked()

    async def set_control_flag(self, channel: str, flag_name: str, enabled: bool) -> None:
        async with self._lock:
            if channel not in self._states:
                return
            if flag_name not in {"skip_upload", "skip_compression", "skip_forwarding", "abort_requested"}:
                raise ValueError(f"unknown control flag: {flag_name}")
            setattr(self._states[channel], flag_name, bool(enabled))
            await self._save_unlocked()

    async def clear_control_flags(self, channel: str) -> None:
        async with self._lock:
            if channel not in self._states:
                return
            state = self._states[channel]
            state.skip_upload = False
            state.skip_compression = False
            state.skip_forwarding = False
            state.abort_requested = False
            await self._save_unlocked()

    async def set_uploaded_part(self, channel: str, file_path: str, msg_id: int) -> None:
        async with self._lock:
            if channel not in self._states:
                return
            self._states[channel].uploaded_parts[file_path] = msg_id
            if msg_id not in self._states[channel].message_ids.upload_msgs:
                self._states[channel].message_ids.upload_msgs.append(msg_id)
            await self._save_unlocked()

    async def set_recording_file(self, channel: str, file_path: str) -> None:
        async with self._lock:
            if channel in self._states:
                self._states[channel].recording_file = file_path
                if file_path not in self._states[channel].recording_files:
                    self._states[channel].recording_files.append(file_path)
                await self._save_unlocked()

    async def add_recording_file(self, channel: str, file_path: str) -> None:
        async with self._lock:
            if channel in self._states and file_path not in self._states[channel].recording_files:
                self._states[channel].recording_files.append(file_path)
                await self._save_unlocked()

    async def get_all_recording_files(self, channel: str) -> List[str]:
        async with self._lock:
            if channel in self._states:
                return self._states[channel].recording_files.copy()
            return []

    async def set_session_id(self, channel: str, session_id: str) -> None:
        async with self._lock:
            if channel in self._states:
                state = self._states[channel]
                old_source_id, old_session_id = self._state_keys(state)
                state.session_id = session_id
                self._state_keys_by_channel[channel] = self._state_keys(state)
                self._move_state_key_sync(old_source_id, old_session_id, copy.deepcopy(state))

    def _move_state_key_sync(self, old_source_id: str, old_session_id: str, state: StreamState) -> None:
        self._init_db()
        with sqlite_conn(self._sqlite_path) as conn:
            conn.execute(
                "DELETE FROM active_streams WHERE source_id = ? AND session_id = ?",
                (old_source_id, old_session_id),
            )
            self._upsert_state_conn(conn, state)

    async def get_session_id(self, channel: str) -> Optional[str]:
        async with self._lock:
            return self._states[channel].session_id if channel in self._states else None

    async def set_split_files(self, channel: str, files: List[str]) -> None:
        async with self._lock:
            if channel in self._states:
                self._states[channel].split_files = files
                await self._save_unlocked()

    async def set_compressed_file(self, channel: str, file_path: str) -> None:
        async with self._lock:
            if channel in self._states:
                self._states[channel].compressed_file = file_path
                await self._save_unlocked()

    async def update_linked_channel(self, channel: str, linked_channel: str) -> None:
        async with self._lock:
            if channel in self._states:
                self._states[channel].linked_channel = linked_channel
                await self._save_unlocked()

    async def add_linked_recording_file(self, channel: str, file_path: str) -> None:
        async with self._lock:
            if channel in self._states and file_path not in self._states[channel].linked_recording_files:
                self._states[channel].linked_recording_files.append(file_path)
                await self._save_unlocked()

    async def complete_stream(self, channel: str) -> None:
        async with self._lock:
            if channel not in self._states:
                return
            state = self._states[channel]
            state.status = StreamStatus.DONE
            history_item = {**state.to_dict(), "completed_at": datetime.now().isoformat()}
            self._history.append(history_item)
            del self._states[channel]
            self._state_keys_by_channel.pop(channel, None)
            self._complete_state_sync(copy.deepcopy(state), history_item)
        self._logger.info(f"[{channel}] Stream completed and archived")

    async def archive_stream(self, channel: str) -> None:
        """Move an active session to history without forcing DONE status.

        Used when the session is terminally cancelled/aborted by the user, or
        when recovery decides the session must not be auto-resumed again. The
        current status is preserved in the history row so operators can still
        see why the session ended.
        """
        async with self._lock:
            state = self._states.pop(channel, None)
            if not state:
                return
            history_item = {**state.to_dict(), "completed_at": datetime.now().isoformat()}
            self._history.append(history_item)
            self._state_keys_by_channel.pop(channel, None)
            self._complete_state_sync(copy.deepcopy(state), history_item)
        self._logger.info(f"[{channel}] Stream archived (status={state.status.value})")

    def _complete_state_sync(self, state: StreamState, history_item: dict) -> None:
        self._init_db()
        with sqlite_conn(self._sqlite_path) as conn:
            self._delete_state_conn(conn, state)
            self._insert_history_conn(conn, history_item)

    async def get_incomplete_streams(self) -> List[StreamState]:
        async with self._lock:
            return [
                copy.deepcopy(state)
                for state in self._states.values()
                if state.status not in (StreamStatus.OFFLINE, StreamStatus.WAITING, StreamStatus.DONE)
            ]

    async def get_waiting_streams(self) -> List[StreamState]:
        async with self._lock:
            return [copy.deepcopy(state) for state in self._states.values() if state.status == StreamStatus.WAITING]

    async def create_stream(
        self,
        channel: str,
        title: str = "",
        category: str = "",
        status: StreamStatus = StreamStatus.OFFLINE,
        source_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> StreamState:
        async with self._lock:
            previous_state = self._states.get(channel)
            now = datetime.now().isoformat()
            stream_id = session_id or f"{channel}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            state = StreamState(
                channel=channel,
                source_id=source_id or source_id_for_channel(channel),
                stream_id=stream_id,
                session_id=session_id or stream_id,
                title=title,
                category=category,
                title_history=[{"value": title, "timestamp": now}] if title else [],
                category_history=[{"value": category, "timestamp": now}] if category else [],
                status=status,
                detected_at=now,
            )
            self._states[channel] = state
            self._state_keys_by_channel[channel] = self._state_keys(state)
            if previous_state:
                self._delete_state_sync(copy.deepcopy(previous_state))
            await self._save_unlocked()
            return copy.deepcopy(state)

    async def start_recording(self, channel: str) -> None:
        async with self._lock:
            if channel in self._states:
                state = self._states[channel]
                state.status = StreamStatus.RECORDING
                state.started_at = datetime.now().isoformat()
                state.stream_start_title = state.title
                state.stream_start_category = state.category
                await self._save_unlocked()

    async def end_recording(self, channel: str) -> None:
        async with self._lock:
            if channel in self._states:
                self._states[channel].ended_at = datetime.now().isoformat()
                await self._save_unlocked()

    async def delete_stream(self, channel: str) -> None:
        async with self._lock:
            state = self._states.pop(channel, None)
            if state:
                self._state_keys_by_channel.pop(channel, None)
                self._delete_state_sync(copy.deepcopy(state))
        self._logger.info(f"[{channel}] Stream deleted from active state")

    def _delete_state_sync(self, state: StreamState) -> None:
        self._init_db()
        with sqlite_conn(self._sqlite_path) as conn:
            self._delete_state_conn(conn, state)
