"""Telegram control group command handler."""

from __future__ import annotations

import shlex
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Optional

from .config import ControlConfig
from .logger import get_logger
from .runtime_config import RuntimeConfigManager


StatusProvider = Callable[[], Awaitable[str]]
RuntimeChangedCallback = Callable[[], Awaitable[None]]
SessionActionCallback = Callable[[str, str], Awaitable[str]]


@dataclass
class CommandResult:
    text: str
    changed_runtime: bool = False
    audit: bool = False


class TelegramController:
    """Listen for admin commands in a dedicated Telegram group."""

    def __init__(
        self,
        client,
        control_config: ControlConfig,
        runtime_config: RuntimeConfigManager,
        status_provider: StatusProvider,
        runtime_changed_callback: Optional[RuntimeChangedCallback] = None,
        session_action_callback: Optional[SessionActionCallback] = None,
    ):
        self.client = client
        self.control_config = control_config
        self.runtime_config = runtime_config
        self.status_provider = status_provider
        self.runtime_changed_callback = runtime_changed_callback
        self.session_action_callback = session_action_callback
        self._logger = get_logger("telegram_controller")
        self._started = False
        self._handler = None
        self._callback_handler = None

    async def start(self) -> None:
        if not self.control_config.enabled:
            return
        if self._started:
            return

        from telethon import events

        self._handler = self._on_message
        self.client.add_event_handler(self._handler, events.NewMessage(chats=self.control_config.group_id))
        self._callback_handler = self._on_callback
        self.client.add_event_handler(self._callback_handler, events.CallbackQuery(chats=self.control_config.group_id))
        self._started = True
        self._logger.info("Telegram control enabled for group %s", self.control_config.group_id)

    async def stop(self) -> None:
        if self._started:
            from telethon import events

            if self._handler:
                self.client.remove_event_handler(self._handler, events.NewMessage)
            if self._callback_handler:
                self.client.remove_event_handler(self._callback_handler, events.CallbackQuery)
            self._started = False

    async def _on_message(self, event) -> None:
        text = (event.raw_text or "").strip()
        prefix = self.control_config.command_prefix or "/"
        if not text.startswith(prefix):
            return

        sender_id = int(getattr(event, "sender_id", 0) or 0)
        if not self._is_allowed(sender_id):
            await event.reply("Нет доступа.")
            return

        command_text = text[len(prefix):]
        try:
            result = await self._handle_command(
                command_text,
                readonly=self._is_readonly(sender_id),
                sender_id=sender_id,
            )
            if result.changed_runtime and self.runtime_changed_callback:
                await self.runtime_changed_callback()
            if result.changed_runtime or result.audit:
                await self._audit(sender_id, text, True, result.text)
            await self._reply(event, result, command_text)
        except Exception as exc:
            self._logger.warning("Control command failed: %s", exc)
            await self._audit(sender_id, text, False, str(exc))
            await event.reply(f"Ошибка: {exc}")

    async def _on_callback(self, event) -> None:
        sender_id = int(getattr(event, "sender_id", 0) or 0)
        if not self._is_allowed(sender_id):
            await event.answer("Нет доступа.", alert=True)
            return
        data = getattr(event, "data", b"") or b""
        try:
            command_text = data.decode("utf-8")
        except Exception:
            await event.answer("Некорректная команда", alert=True)
            return
        if not command_text.startswith("cmd:"):
            return
        command_text = command_text[4:]
        try:
            result = await self._handle_command(
                command_text,
                readonly=self._is_readonly(sender_id),
                sender_id=sender_id,
            )
            if result.changed_runtime and self.runtime_changed_callback:
                await self.runtime_changed_callback()
            if result.changed_runtime or result.audit:
                await self._audit(sender_id, f"button:{command_text}", True, result.text)
            await event.edit(result.text, buttons=await self._buttons_for_command(command_text, result))
            await event.answer("Готово")
        except Exception as exc:
            self._logger.warning("Control callback failed: %s", exc)
            await self._audit(sender_id, f"button:{command_text}", False, str(exc))
            await event.answer(f"Ошибка: {exc}", alert=True)

    async def _reply(self, event, result: CommandResult, command_text: str) -> None:
        await event.reply(result.text, buttons=await self._buttons_for_command(command_text, result))

    async def _buttons_for_command(self, command_text: str, result: CommandResult):
        try:
            from telethon import Button
        except Exception:
            return None
        args = shlex.split(command_text) if command_text else []
        first = args[0].lower() if args else ""
        if first in ("status", "health", "dashboard", "sessions", "session", "sess") or result.text.startswith("Stream Recorder status"):
            rows = [[Button.inline("🔄 Status", b"cmd:status"), Button.inline("📋 Sessions", b"cmd:sessions")]]
            active_channels = self._extract_active_channels(result.text)
            for channel in active_channels[:4]:
                rows.append([
                    Button.inline(f"ℹ️ {channel}", f"cmd:session details {channel}".encode()),
                    Button.inline(f"🛑 Stop {channel}", f"cmd:session stop {channel}".encode()),
                ])
            return rows
        if first in ("sources", "source", "src") or result.text.startswith("Источники:"):
            return [
                [Button.inline("🔄 Sources", b"cmd:sources"), Button.inline("🎯 Targets", b"cmd:targets")],
                [Button.inline("❔ Source help", b"cmd:help source")],
            ]
        if first in ("targets", "target", "tgt") or result.text.startswith("Telegram targets:"):
            return [
                [Button.inline("🔄 Targets", b"cmd:targets"), Button.inline("📺 Sources", b"cmd:sources")],
                [Button.inline("❔ Target help", b"cmd:help target")],
            ]
        if first in ("help", "start", ""):
            return [
                [Button.inline("📊 Status", b"cmd:status"), Button.inline("📺 Sources", b"cmd:sources")],
                [Button.inline("🎯 Targets", b"cmd:targets"), Button.inline("⚙️ Config", b"cmd:config")],
                [Button.inline("📋 Sessions", b"cmd:sessions")],
            ]
        return None

    def _extract_active_channels(self, text: str) -> list[str]:
        for line in text.splitlines():
            if line.startswith("active recordings:") and "(" in line and ")" in line:
                inner = line.split("(", 1)[1].split(")", 1)[0]
                return [item.strip() for item in inner.split(",") if item.strip()]
        return []

    def _is_allowed(self, sender_id: int) -> bool:
        allowed = set(self.control_config.allowed_user_ids or [])
        readonly = set(self.control_config.readonly_user_ids or [])
        if not allowed and not readonly:
            return bool(getattr(self.control_config, "allow_all_group_members", False))
        return sender_id in allowed or sender_id in readonly

    def _is_readonly(self, sender_id: int) -> bool:
        return sender_id in set(self.control_config.readonly_user_ids or []) and sender_id not in set(
            self.control_config.allowed_user_ids or []
        )

    async def _handle_command(
        self,
        command_text: str,
        readonly: bool = False,
        sender_id: Optional[int] = None,
    ) -> CommandResult:
        args = shlex.split(command_text)
        if not args:
            return CommandResult(self._help_text())

        command = args[0].lower()
        rest = args[1:]

        if command in ("help", "start", "?"):
            return CommandResult(self._help_text(rest[0].lower() if rest else None))
        if command in ("status", "health", "dashboard", "jobs", "queue"):
            return CommandResult(await self.status_provider())
        if command in ("sources", "source", "src"):
            return await self._handle_sources(rest, readonly)
        if command in ("targets", "target", "tgt"):
            return await self._handle_targets(rest, readonly)
        if command in ("config", "cfg"):
            return await self._handle_config(rest, readonly)
        if command in ("sessions", "session", "sess"):
            return await self._handle_sessions(rest, readonly)
        if command in {
            "stop",
            "cancel",
            "abort",
            "retry-upload",
            "retry_upload",
            "retry-compression",
            "retry_compression",
            "retry-forwarding",
            "retry_forwarding",
            "reprocess",
            "recover",
            "skip-upload",
            "skip_upload",
            "skip-compression",
            "skip_compression",
            "skip-forwarding",
            "skip_forwarding",
            "stop-upload",
            "stop_upload",
            "stop-compression",
            "stop_compression",
            "stop-forwarding",
            "stop_forwarding",
            "clear-flags",
            "clear_flags",
        }:
            return await self._handle_sessions([command, *rest], readonly)
        if command == "whoami":
            role = "readonly" if readonly else "admin"
            return CommandResult(f"user_id: {sender_id or '-'}\nrole: {role}")

        return CommandResult(f"Неизвестная команда. Используй {self._cmd('help')}")

    async def _handle_config(self, args: list[str], readonly: bool) -> CommandResult:
        action = args[0].lower() if args else "summary"
        if action in ("summary", "show", "status"):
            snapshot = await self.runtime_config.get_snapshot()
            return CommandResult(
                "Runtime config:\n"
                f"targets: {len(snapshot.targets)}\n"
                f"sources: {len(snapshot.sources)}\n"
                f"last_updated: {snapshot.last_updated or '-'}"
            )
        if action == "export":
            if readonly:
                return CommandResult("Только чтение: команда запрещена.", audit=True)
            export_path = self._safe_runtime_path(
                args[1] if len(args) > 1 else None,
                default_name=Path(self.control_config.runtime_config_file).name + ".export.json",
            )
            result_path = await self.runtime_config.export_to_file(str(export_path))
            return CommandResult(f"Runtime config exported: {result_path}", audit=True)
        if action in ("import-check", "validate"):
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('config import-check')} <file.json>")
            import_path = self._safe_runtime_path(args[1], must_exist=True)
            snapshot = await self.runtime_config.validate_import_file(str(import_path))
            return CommandResult(
                "Import file is valid:\n"
                f"targets: {len(snapshot.targets)}\n"
                f"sources: {len(snapshot.sources)}\n"
                f"To apply: {self._cmd('config import')} {import_path.name} confirm=yes"
            )
        if action == "import":
            if readonly:
                return CommandResult("Только чтение: команда запрещена.", audit=True)
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('config import')} <file.json> confirm=yes")
            import_path = self._safe_runtime_path(args[1], must_exist=True)
            snapshot = await self.runtime_config.validate_import_file(str(import_path))
            if not self._confirmed(args[2:]):
                return CommandResult(
                    "Import validation passed, but config was not changed.\n"
                    f"targets: {len(snapshot.targets)}\n"
                    f"sources: {len(snapshot.sources)}\n"
                    f"Run: {self._cmd('config import')} {import_path.name} confirm=yes"
                )
            backup_name = f"runtime_config.backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            backup_path = self._safe_runtime_path(backup_name)
            await self.runtime_config.export_to_file(str(backup_path))
            await self.runtime_config.import_from_file(str(import_path))
            return CommandResult(
                f"Runtime config imported from {import_path.name}\nBackup saved: {backup_path.name}",
                changed_runtime=True,
                audit=True,
            )
        return CommandResult(self._help_text("config"))

    async def _handle_sources(self, args: list[str], readonly: bool) -> CommandResult:
        action = args[0].lower() if args else "list"
        if action in ("list", "ls"):
            return await self._list_sources(args[1:] if args else [])
        if action in ("info", "details", "show"):
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('source info')} <source_id>")
            return await self._source_details(args[1])

        if readonly:
            return CommandResult("Только чтение: команда запрещена.", audit=True)

        if action == "add":
            if len(args) < 3:
                return CommandResult(f"Формат: {self._cmd('source add')} twitch|youtube handle [target=default]")
            platform = args[1]
            handle = args[2]
            target = self._get_option(args[3:], "target") or "default"
            source = await self.runtime_config.add_source(platform, handle, target_profile_id=target)
            return CommandResult(f"Добавлен источник {source.id} -> {source.target_profile_id}", True, True)

        if action in ("pause", "disable", "off"):
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('source pause')} <source_id>")
            source = await self.runtime_config.set_source_enabled(args[1], False)
            return CommandResult(f"Источник остановлен: {source.id}", True, True)

        if action in ("resume", "enable", "on"):
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('source resume')} <source_id>")
            source = await self.runtime_config.set_source_enabled(args[1], True)
            return CommandResult(f"Источник включен: {source.id}", True, True)

        if action in ("remove", "delete", "rm"):
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('source remove')} <source_id>")
            source = await self.runtime_config.remove_source(args[1])
            return CommandResult(f"Источник удалён: {source.id}", True, True)

        if action in ("target", "set-target"):
            if len(args) < 3:
                return CommandResult(f"Формат: {self._cmd('source target')} <source_id> <target_id>")
            source = await self.runtime_config.set_source_target(args[1], args[2])
            return CommandResult(f"Источник {source.id} привязан к {source.target_profile_id}", True, True)

        if action == "notes":
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('source notes')} <source_id> [text]")
            source = await self.runtime_config.set_source_notes(args[1], " ".join(args[2:]))
            return CommandResult(f"Заметка обновлена: {source.id}", True, True)

        if action == "tags":
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('source tags')} <source_id> tag1,tag2")
            tags = self._parse_tags(args[2:])
            source = await self.runtime_config.set_source_tags(args[1], tags)
            return CommandResult(f"Теги обновлены: {source.id} -> {', '.join(source.tags) or '-'}", True, True)

        return CommandResult(self._help_text("source"))

    async def _list_sources(self, args: list[str]) -> CommandResult:
        snapshot = await self.runtime_config.get_snapshot()
        sources = list(snapshot.sources.values())
        platform_filter = self._get_option(args, "platform")
        target_filter = self._get_option(args, "target")
        state_filter = self._get_option(args, "state")
        if platform_filter:
            sources = [source for source in sources if source.platform == platform_filter.lower()]
        if target_filter:
            sources = [source for source in sources if source.target_profile_id == target_filter]
        if state_filter:
            want_enabled = state_filter.lower() in ("on", "enabled", "true", "1")
            sources = [source for source in sources if source.enabled == want_enabled]
        if not sources:
            return CommandResult("Источников нет по заданному фильтру.")
        page, limit = self._page_args(args)
        start = (page - 1) * limit
        selected = sorted(sources, key=lambda item: item.id)[start:start + limit]
        lines = [f"Источники: {len(sources)} total, page {page}"]
        for source in selected:
            state = "on" if source.enabled else "off"
            tags = f" tags={','.join(source.tags)}" if source.tags else ""
            lines.append(f"{source.id} | {state} | {source.handle} -> {source.target_profile_id}{tags}")
        return CommandResult("\n".join(lines))

    async def _source_details(self, source_id: str) -> CommandResult:
        snapshot = await self.runtime_config.get_snapshot()
        source = snapshot.sources.get(source_id)
        if not source:
            return CommandResult(f"Источник не найден: {source_id}")
        return CommandResult(
            f"Source {source.id}\n"
            f"platform: {source.platform}\n"
            f"handle: {source.handle}\n"
            f"enabled: {source.enabled}\n"
            f"target: {source.target_profile_id}\n"
            f"tags: {', '.join(source.tags) or '-'}\n"
            f"notes: {source.notes or '-'}\n"
            f"updated: {source.updated_at or '-'}"
        )

    async def _handle_targets(self, args: list[str], readonly: bool) -> CommandResult:
        action = args[0].lower() if args else "list"
        if action in ("list", "ls"):
            return await self._list_targets(args[1:] if args else [])
        if action in ("info", "details", "show"):
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('target info')} <target_id>")
            return await self._target_details(args[1])
        if action == "test":
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('target test')} <target_id>")
            return await self._test_target(args[1])

        if readonly:
            return CommandResult("Только чтение: команда запрещена.", audit=True)

        if action == "add":
            if len(args) < 3:
                return CommandResult(f"Формат: {self._cmd('target add')} name channel_id [discussion_id=...] [backup_id=...]")
            target = await self.runtime_config.add_target(
                name=args[1],
                upload_channel_id=int(args[2]),
                discussion_group_id=self._optional_int(self._get_option(args[3:], "discussion_id")),
                backup_channel_id=self._optional_int(self._get_option(args[3:], "backup_id")),
            )
            return CommandResult(f"Добавлен target {target.id}", True, True)

        if action == "set":
            if len(args) < 3:
                return CommandResult(f"Формат: {self._cmd('target set')} <target_id> key=value [key=value]")
            updates = self._parse_target_updates(args[2:])
            target = await self.runtime_config.update_target(args[1], **updates)
            return CommandResult(f"Target обновлен: {target.id}", True, True)

        if action in ("remove", "delete", "rm"):
            if len(args) < 2:
                return CommandResult(f"Формат: {self._cmd('target remove')} <target_id> [reassign=default]")
            target = await self.runtime_config.remove_target(args[1], reassign_to=self._get_option(args[2:], "reassign"))
            return CommandResult(f"Target удалён: {target.id}", True, True)

        if action == "rename":
            if len(args) < 3:
                return CommandResult(f"Формат: {self._cmd('target rename')} <target_id> <new_name>")
            target = await self.runtime_config.rename_target(args[1], " ".join(args[2:]))
            return CommandResult(f"Target переименован: {target.id}", True, True)

        return CommandResult(self._help_text("target"))

    async def _list_targets(self, args: list[str]) -> CommandResult:
        snapshot = await self.runtime_config.get_snapshot()
        targets = sorted(snapshot.targets.values(), key=lambda item: item.id)
        page, limit = self._page_args(args)
        start = (page - 1) * limit
        selected = targets[start:start + limit]
        lines = [f"Telegram targets: {len(targets)} total, page {page}"]
        for target in selected:
            lines.append(
                f"{target.id} | channel={target.upload_channel_id} | "
                f"discussion={target.discussion_group_id or '-'} | backup={target.backup_channel_id or '-'} | "
                f"compression={target.compression_enabled if target.compression_enabled is not None else 'global'} | "
                f"parallelism={target.upload_parallelism} | speed={target.upload_speed_limit_mbps}Mbps | "
                f"size={target.splitting_default_size_gb or 'global'}/{target.splitting_premium_size_gb or 'global'}GB"
            )
        return CommandResult("\n".join(lines))

    async def _target_details(self, target_id: str) -> CommandResult:
        snapshot = await self.runtime_config.get_snapshot()
        target = snapshot.targets.get(target_id)
        if not target:
            return CommandResult(f"Target не найден: {target_id}")
        refs = sorted(source.id for source in snapshot.sources.values() if source.target_profile_id == target_id)
        return CommandResult(
            f"Target {target.id}\n"
            f"name: {target.name}\n"
            f"channel: {target.upload_channel_id}\n"
            f"discussion: {target.discussion_group_id or '-'}\n"
            f"backup: {target.backup_channel_id or '-'}\n"
            f"compression: {target.compression_enabled if target.compression_enabled is not None else 'global'}\n"
            f"parallelism: {target.upload_parallelism}\n"
            f"speed: {target.upload_speed_limit_mbps} Mbps\n"
            f"default_size: {target.splitting_default_size_gb or 'global'} GB\n"
            f"premium_size: {target.splitting_premium_size_gb or 'global'} GB\n"
            f"sources: {', '.join(refs) if refs else '-'}\n"
            f"updated: {target.updated_at or '-'}"
        )

    async def _test_target(self, target_id: str) -> CommandResult:
        snapshot = await self.runtime_config.get_snapshot()
        target = snapshot.targets.get(target_id)
        if not target:
            return CommandResult(f"Target не найден: {target_id}")
        lines = [f"Target test: {target.id}"]
        lines.append(f"upload_channel: {await self._test_entity(target.upload_channel_id)}")
        if target.discussion_group_id:
            lines.append(f"discussion_group: {await self._test_entity(target.discussion_group_id)}")
        if target.backup_channel_id:
            lines.append(f"backup_channel: {await self._test_entity(target.backup_channel_id)}")
        return CommandResult("\n".join(lines), audit=True)

    async def _test_entity(self, chat_id: int) -> str:
        if not self.client or not hasattr(self.client, "get_entity"):
            return f"{chat_id} (client check unavailable)"
        try:
            entity = await self.client.get_entity(chat_id)
            title = getattr(entity, "title", None) or getattr(entity, "username", None) or type(entity).__name__
            return f"{chat_id} ok ({title})"
        except Exception as exc:
            return f"{chat_id} error: {exc}"

    async def _handle_sessions(self, args: list[str], readonly: bool) -> CommandResult:
        if not self.session_action_callback:
            return CommandResult("Session control is unavailable.")
        if not args or args[0].lower() in ("list", "status", "ls"):
            return CommandResult(await self.status_provider())
        if readonly:
            return CommandResult("Только чтение: команда запрещена.", audit=True)
        if len(args) < 2:
            return CommandResult(self._help_text("session"))
        action = args[0].lower().replace("_", "-")
        channel = args[1]
        result = await self.session_action_callback(action, channel)
        audit = action not in ("details", "info", "show")
        return CommandResult(result, changed_runtime=False, audit=audit)

    def _parse_target_updates(self, args: list[str]) -> dict:
        updates = {}
        aliases = {
            "channel": "upload_channel_id",
            "channel_id": "upload_channel_id",
            "upload_channel": "upload_channel_id",
            "discussion": "discussion_group_id",
            "discussion_id": "discussion_group_id",
            "discussion_group": "discussion_group_id",
            "backup": "backup_channel_id",
            "backup_id": "backup_channel_id",
            "backup_channel": "backup_channel_id",
            "compression": "compression_enabled",
            "parallelism": "upload_parallelism",
            "speed": "upload_speed_limit_mbps",
            "speed_mbps": "upload_speed_limit_mbps",
            "default_size": "splitting_default_size_gb",
            "premium_size": "splitting_premium_size_gb",
        }
        for item in args:
            if "=" not in item:
                raise ValueError(f"Ожидал key=value, получил: {item}")
            key, value = item.split("=", 1)
            updates[aliases.get(key, key)] = value
        return updates

    def _get_option(self, args: list[str], name: str) -> Optional[str]:
        prefix = f"{name}="
        for arg in args:
            if arg.startswith(prefix):
                return arg[len(prefix):]
        return None

    def _optional_int(self, value: Optional[str]) -> Optional[int]:
        if value in (None, ""):
            return None
        if isinstance(value, str) and value.strip().lower() in ("none", "null", "global", "inherit", "default", "-"):
            return None
        return int(value)

    def _parse_tags(self, args: list[str]) -> list[str]:
        tags: list[str] = []
        for arg in args:
            for item in arg.split(","):
                item = item.strip()
                if item:
                    tags.append(item)
        return tags

    def _page_args(self, args: list[str]) -> tuple[int, int]:
        page = max(1, int(self._get_option(args, "page") or 1))
        limit = min(50, max(1, int(self._get_option(args, "limit") or 30)))
        return page, limit

    def _confirmed(self, args: list[str]) -> bool:
        return "--confirm" in args or (self._get_option(args, "confirm") or "").lower() in ("1", "yes", "true", "y")

    def _safe_runtime_path(
        self,
        value: Optional[str],
        default_name: str = "runtime_config.export.json",
        must_exist: bool = False,
    ) -> Path:
        root = Path(self.control_config.runtime_config_file).expanduser().resolve().parent
        root.mkdir(parents=True, exist_ok=True)
        raw = Path(value or default_name).expanduser()
        candidate = raw.resolve() if raw.is_absolute() else (root / raw).resolve()
        if candidate != root and root not in candidate.parents:
            raise ValueError(f"path must be inside runtime config directory: {root}")
        if must_exist and not candidate.exists():
            raise FileNotFoundError(f"file not found: {candidate}")
        return candidate

    def _cmd(self, command: str) -> str:
        return f"{self.control_config.command_prefix or '/'}{command}"

    def _help_text(self, topic: Optional[str] = None) -> str:
        p = self.control_config.command_prefix or "/"
        topics = {
            "source": (
                "📺 Sources\n"
                f"{p}sources [platform=twitch|youtube] [target=id] [state=on|off]\n"
                f"{p}source add twitch|youtube handle [target=default]\n"
                f"{p}source pause|resume <source_id>\n"
                f"{p}source target <source_id> <target_id>\n"
                f"{p}source remove <source_id>\n"
                f"{p}source notes <source_id> [text]\n"
                f"{p}source tags <source_id> tag1,tag2\n"
                f"{p}source info <source_id>"
            ),
            "target": (
                "🎯 Targets\n"
                f"{p}targets [page=1] [limit=30]\n"
                f"{p}target add name channel_id [discussion_id=...] [backup_id=...]\n"
                f"{p}target set <id> channel_id=... discussion_id=none backup_id=none\n"
                f"{p}target set <id> compression=true|false|global parallelism=1..10 speed=0\n"
                f"{p}target set <id> default_size=1.9 premium_size=3.9\n"
                f"{p}target rename <id> <new_name>\n"
                f"{p}target remove <id> [reassign=default]\n"
                f"{p}target test <id>\n"
                f"{p}target info <id>"
            ),
            "session": (
                "📋 Sessions\n"
                f"{p}sessions - статус очередей и активных сессий\n"
                f"{p}session details <channel>\n"
                f"{p}session stop <channel> - остановить запись и поставить обработку\n"
                f"{p}session cancel <channel> - отменить текущую стадию\n"
                f"{p}session skip-upload <channel>\n"
                f"{p}session skip-compression <channel>\n"
                f"{p}session skip-forwarding <channel>\n"
                f"{p}session retry-upload <channel>\n"
                f"{p}session retry-compression <channel>\n"
                f"{p}session retry-forwarding <channel>\n"
                f"{p}session reprocess <channel>\n"
                f"{p}session recover <channel>\n"
                f"{p}session clear-flags <channel>"
            ),
            "config": (
                "⚙️ Config\n"
                f"{p}config - runtime summary\n"
                f"{p}config export [file.json]\n"
                f"{p}config import-check <file.json>\n"
                f"{p}config import <file.json> confirm=yes\n"
                "Файлы import/export разрешены только внутри директории runtime config."
            ),
        }
        if topic in topics:
            return topics[topic]
        return (
            "🎛 Stream Recorder Control\n\n"
            f"{p}status - статус приложения, очередей и активных записей\n"
            f"{p}sources - источники Twitch/YouTube\n"
            f"{p}targets - Telegram-профили загрузки\n"
            f"{p}sessions - сессии, retry, stop, cancel, skip стадий\n"
            f"{p}config - runtime import/export\n\n"
            "Быстрые команды:\n"
            f"{p}stop <channel>\n"
            f"{p}cancel <channel>\n"
            f"{p}retry-upload <channel>\n\n"
            "Подробная помощь:\n"
            f"{p}help source\n"
            f"{p}help target\n"
            f"{p}help session\n"
            f"{p}help config"
        )

    async def _audit(self, sender_id: int, command: str, success: bool, result: str) -> None:
        path = Path(self.control_config.audit_log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = (
            f"{datetime.now().isoformat()}\tuser={sender_id}\t"
            f"success={str(success).lower()}\tcommand={command!r}\tresult={result[:300]!r}\n"
        )
        await self._append_audit_line(path, line)

    async def _append_audit_line(self, path: Path, line: str) -> None:
        import asyncio

        await asyncio.to_thread(self._append_sync, path, line)

    def _append_sync(self, path: Path, line: str) -> None:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line)
