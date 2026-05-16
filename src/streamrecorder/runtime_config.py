"""Runtime configuration storage for Telegram-managed operation.

The static YAML config remains the bootstrap source for secrets and legacy
single-target setups. This module stores the mutable operational model that can
be changed while the process is running or from the Telegram control group.
"""

import asyncio
import copy
import json
import re
import shutil
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import Config
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


RUNTIME_SCHEMA_VERSION = 1


def _slug(value: str) -> str:
    """Return a stable identifier fragment for user-provided names."""
    text = re.sub(r"[^a-zA-Z0-9_@.-]+", "_", value.strip().lower())
    text = text.strip("_")
    return text or "item"


def _normalize_source_handle(platform: str, handle: str) -> str:
    platform = platform.lower().strip()
    value = handle.strip()
    if platform == "youtube" and value and not value.startswith("@") and not value.startswith("UC"):
        return f"@{value}"
    return value


@dataclass
class TargetProfile:
    """Telegram destination profile for uploads, comments, and backups."""

    id: str
    name: str
    upload_channel_id: int
    discussion_group_id: Optional[int] = None
    backup_channel_id: Optional[int] = None
    upload_parallelism: int = 1
    upload_speed_limit_mbps: float = 0.0
    compression_enabled: Optional[bool] = None
    splitting_default_size_gb: Optional[float] = None
    splitting_premium_size_gb: Optional[float] = None
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, data: dict) -> "TargetProfile":
        compression_enabled = data.get("compression_enabled")
        if compression_enabled is not None:
            compression_enabled = _parse_optional_bool(compression_enabled)
        return cls(
            id=str(data["id"]),
            name=str(data.get("name") or data["id"]),
            upload_channel_id=int(data["upload_channel_id"]),
            discussion_group_id=_optional_int(data.get("discussion_group_id")),
            backup_channel_id=_optional_int(data.get("backup_channel_id")),
            upload_parallelism=max(1, int(data.get("upload_parallelism") or 1)),
            upload_speed_limit_mbps=max(0.0, float(data.get("upload_speed_limit_mbps") or 0.0)),
            compression_enabled=compression_enabled,
            splitting_default_size_gb=_optional_float(data.get("splitting_default_size_gb")),
            splitting_premium_size_gb=_optional_float(data.get("splitting_premium_size_gb")),
            created_at=str(data.get("created_at") or ""),
            updated_at=str(data.get("updated_at") or ""),
        )


@dataclass
class SourceChannel:
    """A monitored Twitch or YouTube source bound to a Telegram target."""

    id: str
    platform: str
    handle: str
    target_profile_id: str = "default"
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, data: dict) -> "SourceChannel":
        platform = str(data["platform"]).lower()
        if platform not in ("twitch", "youtube"):
            raise ValueError(f"Unsupported source platform: {platform}")
        handle = _normalize_source_handle(platform, str(data["handle"]))
        return cls(
            id=f"{platform}:{_slug(handle)}",
            platform=platform,
            handle=handle,
            target_profile_id=str(data.get("target_profile_id") or "default"),
            enabled=_parse_bool(data.get("enabled", True)),
            tags=[str(item) for item in data.get("tags", [])],
            notes=str(data.get("notes") or ""),
            created_at=str(data.get("created_at") or ""),
            updated_at=str(data.get("updated_at") or ""),
        )


@dataclass
class RuntimeConfigSnapshot:
    """Immutable-ish copy of the mutable runtime configuration."""

    schema_version: int
    targets: Dict[str, TargetProfile]
    sources: Dict[str, SourceChannel]
    last_updated: str = ""

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "targets": {key: value.to_dict() for key, value in self.targets.items()},
            "sources": {key: value.to_dict() for key, value in self.sources.items()},
            "last_updated": self.last_updated,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RuntimeConfigSnapshot":
        return cls(
            schema_version=int(data.get("schema_version") or RUNTIME_SCHEMA_VERSION),
            targets={
                item.id: item
                for item in (TargetProfile.from_dict(value) for value in (data.get("targets") or {}).values())
            },
            sources={
                item.id: item
                for item in (SourceChannel.from_dict(value) for value in (data.get("sources") or {}).values())
            },
            last_updated=str(data.get("last_updated") or ""),
        )


def _optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.strip().lower() in ("none", "null", "global", "inherit", "default", "-"):
        return None
    return int(value)


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.strip().lower() in ("none", "null", "global", "inherit", "default", "-"):
        return None
    return float(value)


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "yes", "on", "y"):
            return True
        if text in ("0", "false", "no", "off", "n"):
            return False
    raise ValueError(f"invalid bool value: {value!r}")


def _parse_optional_bool(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.strip().lower() in ("global", "inherit", "default", "null", "none"):
        return None
    return _parse_bool(value)


class RuntimeConfigManager:
    """Persist and validate mutable runtime configuration."""

    def __init__(self, runtime_file: str):
        self.runtime_file = Path(runtime_file)
        self.runtime_file.parent.mkdir(parents=True, exist_ok=True)
        self._sqlite_path = self._resolve_sqlite_path(self.runtime_file)
        self._sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self._logger = get_logger("runtime_config")
        self._lock = asyncio.Lock()
        self._snapshot = RuntimeConfigSnapshot(
            schema_version=RUNTIME_SCHEMA_VERSION,
            targets={},
            sources={},
        )

    async def load_or_initialize(self, static_config: Config) -> RuntimeConfigSnapshot:
        """Load runtime config or create one from the current static config."""
        async with self._lock:
            self._init_db()
            loaded = self._load_snapshot_from_db()
            if loaded:
                self._snapshot = loaded
                self._ensure_defaults_unlocked(static_config)
                await self._save_unlocked()
                self._logger.info(
                    "Loaded runtime config: %d targets, %d sources",
                    len(self._snapshot.targets),
                    len(self._snapshot.sources),
                )
                return copy.deepcopy(self._snapshot)

            if self.runtime_file.exists() and self.runtime_file != self._sqlite_path:
                self._snapshot = RuntimeConfigSnapshot.from_dict(
                    json.loads(self.runtime_file.read_text(encoding="utf-8"))
                )
                self._ensure_defaults_unlocked(static_config)
                await self._save_unlocked()
                backup = self.runtime_file.with_name(
                    f"{self.runtime_file.name}.migrated-{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
                )
                shutil.copy2(self.runtime_file, backup)
                self._logger.info("Migrated runtime config JSON to SQLite: %s", self._sqlite_path)
                return copy.deepcopy(self._snapshot)

            self._snapshot = self._migrate_from_static(static_config)
            await self._save_unlocked()
            self._logger.info(
                "Initialized runtime config: %d targets, %d sources",
                len(self._snapshot.targets),
                len(self._snapshot.sources),
            )
            return copy.deepcopy(self._snapshot)

    async def get_snapshot(self) -> RuntimeConfigSnapshot:
        async with self._lock:
            return copy.deepcopy(self._snapshot)

    async def add_source(
        self,
        platform: str,
        handle: str,
        target_profile_id: str = "default",
        enabled: bool = True,
    ) -> SourceChannel:
        platform = platform.lower().strip()
        if platform not in ("twitch", "youtube"):
            raise ValueError("platform must be twitch or youtube")
        handle = _normalize_source_handle(platform, handle)
        if not handle:
            raise ValueError("handle must not be empty")
        target_profile_id = _slug(target_profile_id)

        async with self._lock:
            if target_profile_id not in self._snapshot.targets:
                raise ValueError(f"unknown target profile: {target_profile_id}")
            source_id = self._source_id(platform, handle)
            now = datetime.now().isoformat()
            existing = self._snapshot.sources.get(source_id)
            if existing:
                existing.enabled = enabled
                existing.target_profile_id = target_profile_id
                existing.updated_at = now
                source = existing
            else:
                source = SourceChannel(
                    id=source_id,
                    platform=platform,
                    handle=handle,
                    target_profile_id=target_profile_id,
                    enabled=enabled,
                    created_at=now,
                    updated_at=now,
                )
                self._snapshot.sources[source_id] = source
            await self._save_unlocked()
            return copy.deepcopy(source)

    async def remove_source(self, source_id: str) -> SourceChannel:
        async with self._lock:
            if source_id not in self._snapshot.sources:
                raise ValueError(f"unknown source: {source_id}")
            source = self._snapshot.sources.pop(source_id)
            await self._save_unlocked()
            return copy.deepcopy(source)

    async def set_source_enabled(self, source_id: str, enabled: bool) -> SourceChannel:
        async with self._lock:
            if source_id not in self._snapshot.sources:
                raise ValueError(f"unknown source: {source_id}")
            source = self._snapshot.sources[source_id]
            source.enabled = enabled
            source.updated_at = datetime.now().isoformat()
            await self._save_unlocked()
            return copy.deepcopy(source)

    async def set_source_target(self, source_id: str, target_profile_id: str) -> SourceChannel:
        target_profile_id = _slug(target_profile_id)
        async with self._lock:
            if source_id not in self._snapshot.sources:
                raise ValueError(f"unknown source: {source_id}")
            if target_profile_id not in self._snapshot.targets:
                raise ValueError(f"unknown target profile: {target_profile_id}")
            source = self._snapshot.sources[source_id]
            source.target_profile_id = target_profile_id
            source.updated_at = datetime.now().isoformat()
            await self._save_unlocked()
            return copy.deepcopy(source)

    async def set_source_notes(self, source_id: str, notes: str) -> SourceChannel:
        async with self._lock:
            if source_id not in self._snapshot.sources:
                raise ValueError(f"unknown source: {source_id}")
            source = self._snapshot.sources[source_id]
            source.notes = str(notes or "")
            source.updated_at = datetime.now().isoformat()
            await self._save_unlocked()
            return copy.deepcopy(source)

    async def set_source_tags(self, source_id: str, tags: List[str]) -> SourceChannel:
        async with self._lock:
            if source_id not in self._snapshot.sources:
                raise ValueError(f"unknown source: {source_id}")
            source = self._snapshot.sources[source_id]
            source.tags = [str(item).strip() for item in tags if str(item).strip()]
            source.updated_at = datetime.now().isoformat()
            await self._save_unlocked()
            return copy.deepcopy(source)

    async def add_target(
        self,
        name: str,
        upload_channel_id: int,
        discussion_group_id: Optional[int] = None,
        backup_channel_id: Optional[int] = None,
    ) -> TargetProfile:
        target_id = _slug(name)
        if not target_id:
            raise ValueError("target name must not be empty")
        upload_channel_id = self._validate_upload_channel_id(upload_channel_id)
        async with self._lock:
            if target_id in self._snapshot.targets:
                raise ValueError(f"target already exists: {target_id}")
            now = datetime.now().isoformat()
            target = TargetProfile(
                id=target_id,
                name=name.strip(),
                upload_channel_id=upload_channel_id,
                discussion_group_id=discussion_group_id,
                backup_channel_id=backup_channel_id,
                created_at=now,
                updated_at=now,
            )
            self._snapshot.targets[target_id] = target
            await self._save_unlocked()
            return copy.deepcopy(target)

    async def remove_target(self, target_id: str, reassign_to: Optional[str] = None) -> TargetProfile:
        target_id = _slug(target_id)
        if reassign_to:
            reassign_to = _slug(reassign_to)
        async with self._lock:
            if target_id not in self._snapshot.targets:
                raise ValueError(f"unknown target: {target_id}")
            if target_id == "default":
                raise ValueError("default target cannot be removed")
            references = [source.id for source in self._snapshot.sources.values() if source.target_profile_id == target_id]
            if references and not reassign_to:
                raise ValueError(f"target is used by sources: {', '.join(references)}")
            if reassign_to:
                if reassign_to not in self._snapshot.targets:
                    raise ValueError(f"unknown reassign target: {reassign_to}")
                for source in self._snapshot.sources.values():
                    if source.target_profile_id == target_id:
                        source.target_profile_id = reassign_to
                        source.updated_at = datetime.now().isoformat()
            target = self._snapshot.targets.pop(target_id)
            await self._save_unlocked()
            return copy.deepcopy(target)

    async def rename_target(self, target_id: str, new_name: str) -> TargetProfile:
        target_id = _slug(target_id)
        new_id = _slug(new_name)
        if not new_id:
            raise ValueError("new target name must not be empty")
        async with self._lock:
            if target_id not in self._snapshot.targets:
                raise ValueError(f"unknown target: {target_id}")
            if target_id == "default":
                raise ValueError("default target cannot be renamed")
            if new_id in self._snapshot.targets:
                raise ValueError(f"target already exists: {new_id}")
            target = self._snapshot.targets.pop(target_id)
            target.id = new_id
            target.name = new_name.strip()
            target.updated_at = datetime.now().isoformat()
            self._snapshot.targets[new_id] = target
            for source in self._snapshot.sources.values():
                if source.target_profile_id == target_id:
                    source.target_profile_id = new_id
                    source.updated_at = target.updated_at
            await self._save_unlocked()
            return copy.deepcopy(target)

    async def update_target(self, target_id: str, **updates: Any) -> TargetProfile:
        target_id = _slug(target_id)
        async with self._lock:
            if target_id not in self._snapshot.targets:
                raise ValueError(f"unknown target: {target_id}")
            target = self._snapshot.targets[target_id]
            allowed = {
                "upload_channel_id",
                "discussion_group_id",
                "backup_channel_id",
                "upload_parallelism",
                "upload_speed_limit_mbps",
                "compression_enabled",
                "splitting_default_size_gb",
                "splitting_premium_size_gb",
            }
            for key, value in updates.items():
                if key not in allowed:
                    raise ValueError(f"unsupported target setting: {key}")
                if key == "upload_channel_id":
                    value = self._validate_upload_channel_id(value)
                elif key in {"discussion_group_id", "backup_channel_id"}:
                    value = _optional_int(value)
                elif key == "upload_parallelism":
                    value = min(10, max(1, int(value)))
                elif key == "upload_speed_limit_mbps":
                    value = max(0.0, float(value))
                elif key in {"splitting_default_size_gb", "splitting_premium_size_gb"}:
                    value = _optional_float(value)
                    if value is not None and value <= 0:
                        raise ValueError(f"{key} must be > 0")
                    if value is not None and value > 4.0:
                        raise ValueError(f"{key} must be <= 4.0")
                elif key == "compression_enabled" and value is not None:
                    value = _parse_optional_bool(value)
                setattr(target, key, value)
            target.updated_at = datetime.now().isoformat()
            await self._save_unlocked()
            return copy.deepcopy(target)

    def validate_import_dict(self, data: dict) -> RuntimeConfigSnapshot:
        snapshot = RuntimeConfigSnapshot.from_dict(data)
        self._validate_snapshot(snapshot)
        return copy.deepcopy(snapshot)

    async def export_dict(self) -> dict:
        """Return runtime config as a JSON-serializable dict."""
        async with self._lock:
            return copy.deepcopy(self._snapshot.to_dict())

    async def import_dict(self, data: dict) -> RuntimeConfigSnapshot:
        """Replace runtime config from a validated exported payload."""
        snapshot = self.validate_import_dict(data)
        async with self._lock:
            self._snapshot = snapshot
            await self._save_unlocked()
            return copy.deepcopy(self._snapshot)

    def _validate_snapshot(self, snapshot: RuntimeConfigSnapshot) -> None:
        if not snapshot.targets:
            raise ValueError("runtime config must contain at least one target")
        for target in snapshot.targets.values():
            target.upload_channel_id = self._validate_upload_channel_id(target.upload_channel_id)
            target.upload_parallelism = min(10, max(1, int(target.upload_parallelism or 1)))
            target.upload_speed_limit_mbps = max(0.0, float(target.upload_speed_limit_mbps or 0.0))
            for field_name in ("splitting_default_size_gb", "splitting_premium_size_gb"):
                value = getattr(target, field_name)
                if value is not None and value <= 0:
                    raise ValueError(f"{target.id}.{field_name} must be > 0")
                if value is not None and value > 4.0:
                    raise ValueError(f"{target.id}.{field_name} must be <= 4.0")
        for source in snapshot.sources.values():
            if source.target_profile_id not in snapshot.targets:
                raise ValueError(
                    f"source {source.id} references unknown target {source.target_profile_id}"
                )

    async def export_to_file(self, path: str) -> Path:
        """Write runtime config export to a file."""
        export_path = Path(path)
        export_path.parent.mkdir(parents=True, exist_ok=True)
        data = await self.export_dict()
        tmp_file = export_path.with_suffix(export_path.suffix + ".tmp")
        tmp_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_file.replace(export_path)
        return export_path

    async def import_from_file(self, path: str) -> RuntimeConfigSnapshot:
        """Load runtime config from an exported JSON file."""
        import_path = Path(path)
        if not import_path.exists():
            raise FileNotFoundError(f"runtime config import file not found: {path}")
        data = json.loads(import_path.read_text(encoding="utf-8"))
        return await self.import_dict(data)

    async def validate_import_file(self, path: str) -> RuntimeConfigSnapshot:
        import_path = Path(path)
        if not import_path.exists():
            raise FileNotFoundError(f"runtime config import file not found: {path}")
        data = json.loads(import_path.read_text(encoding="utf-8"))
        return self.validate_import_dict(data)

    def _ensure_defaults_unlocked(self, static_config: Config) -> None:
        if "default" not in self._snapshot.targets:
            default = self._migrate_from_static(static_config).targets["default"]
            self._snapshot.targets["default"] = default

    def _migrate_from_static(self, static_config: Config) -> RuntimeConfigSnapshot:
        now = datetime.now().isoformat()
        default_target = TargetProfile(
            id="default",
            name="default",
            upload_channel_id=static_config.telegram.channel_id,
            discussion_group_id=static_config.telegram.discussion_group_id,
            backup_channel_id=static_config.telegram.backup_channel_id,
            upload_parallelism=static_config.telegram.upload_parallelism,
            upload_speed_limit_mbps=static_config.telegram.upload_speed_limit_mbps,
            compression_enabled=static_config.compression.enabled,
            splitting_default_size_gb=static_config.splitting.default_size_gb,
            splitting_premium_size_gb=static_config.splitting.premium_size_gb,
            created_at=now,
            updated_at=now,
        )
        sources: Dict[str, SourceChannel] = {}
        for handle in static_config.twitch.channels:
            source = SourceChannel(
                id=self._source_id("twitch", handle),
                platform="twitch",
                handle=handle,
                target_profile_id="default",
                created_at=now,
                updated_at=now,
            )
            sources[source.id] = source
        for handle in static_config.youtube.channels:
            handle = _normalize_source_handle("youtube", handle)
            source = SourceChannel(
                id=self._source_id("youtube", handle),
                platform="youtube",
                handle=handle,
                target_profile_id="default",
                created_at=now,
                updated_at=now,
            )
            sources[source.id] = source
        return RuntimeConfigSnapshot(
            schema_version=RUNTIME_SCHEMA_VERSION,
            targets={"default": default_target},
            sources=sources,
            last_updated=now,
        )

    def _source_id(self, platform: str, handle: str) -> str:
        return f"{platform}:{_slug(_normalize_source_handle(platform, handle))}"

    def _validate_upload_channel_id(self, value: Any) -> int:
        result = _optional_int(value)
        if result is None or result == 0:
            raise ValueError("upload_channel_id must be a non-zero integer")
        return result

    async def _save_unlocked(self) -> None:
        self._snapshot.last_updated = datetime.now().isoformat()
        self._save_snapshot_to_db(copy.deepcopy(self._snapshot))

    def _resolve_sqlite_path(self, path: Path) -> Path:
        if path.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
            return path
        return path.with_suffix(".sqlite3")

    def _init_db(self) -> None:
        with sqlite_conn(self._sqlite_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_targets (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_sources (
                    id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    handle TEXT NOT NULL,
                    target_profile_id TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def _load_snapshot_from_db(self) -> Optional[RuntimeConfigSnapshot]:
        self._init_db()
        with sqlite_conn(self._sqlite_path) as conn:
            target_rows = conn.execute("SELECT data FROM runtime_targets ORDER BY id").fetchall()
            source_rows = conn.execute("SELECT data FROM runtime_sources ORDER BY id").fetchall()
            if not target_rows and not source_rows:
                return None
            meta_rows = conn.execute("SELECT key, value FROM runtime_meta").fetchall()
        meta = {row["key"]: row["value"] for row in meta_rows}
        return RuntimeConfigSnapshot(
            schema_version=int(meta.get("schema_version") or RUNTIME_SCHEMA_VERSION),
            targets={
                item.id: item
                for item in (TargetProfile.from_dict(json.loads(row["data"])) for row in target_rows)
            },
            sources={
                item.id: item
                for item in (SourceChannel.from_dict(json.loads(row["data"])) for row in source_rows)
            },
            last_updated=meta.get("last_updated", ""),
        )

    def _save_snapshot_to_db(self, snapshot: RuntimeConfigSnapshot) -> None:
        self._init_db()
        with sqlite_conn(self._sqlite_path) as conn:
            conn.execute("DELETE FROM runtime_targets")
            conn.execute("DELETE FROM runtime_sources")
            for target in snapshot.targets.values():
                conn.execute(
                    "INSERT INTO runtime_targets(id, data, updated_at) VALUES(?, ?, ?)",
                    (target.id, json.dumps(target.to_dict(), ensure_ascii=False), target.updated_at or snapshot.last_updated),
                )
            for source in snapshot.sources.values():
                conn.execute(
                    """
                    INSERT INTO runtime_sources(id, platform, handle, target_profile_id, enabled, data, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source.id,
                        source.platform,
                        source.handle,
                        source.target_profile_id,
                        1 if source.enabled else 0,
                        json.dumps(source.to_dict(), ensure_ascii=False),
                        source.updated_at or snapshot.last_updated,
                    ),
                )
            conn.execute("DELETE FROM runtime_meta")
            conn.execute("INSERT INTO runtime_meta(key, value) VALUES(?, ?)", ("schema_version", str(snapshot.schema_version)))
            conn.execute("INSERT INTO runtime_meta(key, value) VALUES(?, ?)", ("last_updated", snapshot.last_updated))
