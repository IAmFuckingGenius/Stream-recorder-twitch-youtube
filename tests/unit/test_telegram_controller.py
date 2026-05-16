import json
import tempfile
import unittest
from pathlib import Path

from streamrecorder.config import ControlConfig, Config, TelegramConfig
from streamrecorder.runtime_config import RuntimeConfigManager
from streamrecorder.telegram_controller import TelegramController


class TelegramControllerTests(unittest.IsolatedAsyncioTestCase):
    async def make_controller(self, tmpdir, session_action_callback=None, control_config=None):
        runtime = RuntimeConfigManager(str(Path(tmpdir) / "runtime.sqlite3"))
        await runtime.load_or_initialize(
            Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
        )

        async def status_provider():
            return "ok"

        runtime_calls: list[bool] = []

        async def runtime_changed():
            runtime_calls.append(True)

        if control_config is None:
            control_config = ControlConfig(
                runtime_config_file=str(Path(tmpdir) / "runtime.sqlite3"),
                audit_log_file=str(Path(tmpdir) / "audit.log"),
            )

        controller = TelegramController(
            client=object(),
            control_config=control_config,
            runtime_config=runtime,
            status_provider=status_provider,
            runtime_changed_callback=runtime_changed,
            session_action_callback=session_action_callback,
        )
        controller._runtime_calls = runtime_calls  # type: ignore[attr-defined]
        return controller, runtime

    async def test_session_action_uses_explicit_callback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            calls = []

            async def action_callback(action, channel):
                calls.append((action, channel))
                return "queued"

            controller, _ = await self.make_controller(tmpdir, action_callback)
            result = await controller._handle_command("session retry_upload channel", readonly=False)

            self.assertEqual(result.text, "queued")
            self.assertEqual(calls, [("retry-upload", "channel")])

    async def test_top_level_stop_routes_to_session_callback(self):
        """`/stop <channel>` should forward to the same handler as
        `/session stop <channel>` so quick aliases stay in sync."""
        with tempfile.TemporaryDirectory() as tmpdir:
            calls = []

            async def action_callback(action, channel):
                calls.append((action, channel))
                return "ack"

            controller, _ = await self.make_controller(tmpdir, action_callback)
            result = await controller._handle_command("stop somechannel", readonly=False)

            self.assertEqual(result.text, "ack")
            self.assertEqual(calls, [("stop", "somechannel")])

    async def test_session_skip_upload_underscore_normalized(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            calls = []

            async def action_callback(action, channel):
                calls.append((action, channel))
                return "ack"

            controller, _ = await self.make_controller(tmpdir, action_callback)
            await controller._handle_command("session skip_upload chan", readonly=False)
            await controller._handle_command("session skip-compression chan", readonly=False)
            await controller._handle_command("skip_forwarding chan", readonly=False)

            self.assertEqual(
                calls,
                [
                    ("skip-upload", "chan"),
                    ("skip-compression", "chan"),
                    ("skip-forwarding", "chan"),
                ],
            )

    async def test_readonly_cannot_import_runtime_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, _ = await self.make_controller(tmpdir)
            result = await controller._handle_command("config import /tmp/missing.json", readonly=True)

            self.assertEqual(result.text, "Только чтение: команда запрещена.")

    async def test_readonly_cannot_export_runtime_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, _ = await self.make_controller(tmpdir)
            result = await controller._handle_command("config export", readonly=True)

            self.assertEqual(result.text, "Только чтение: команда запрещена.")

    async def test_readonly_cannot_change_sources_or_targets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, _ = await self.make_controller(tmpdir)

            res_source = await controller._handle_command(
                "source add twitch newone target=default", readonly=True
            )
            self.assertEqual(res_source.text, "Только чтение: команда запрещена.")

            res_target = await controller._handle_command(
                "target add Archive -2002", readonly=True
            )
            self.assertEqual(res_target.text, "Только чтение: команда запрещена.")

    async def test_export_path_outside_runtime_dir_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, _ = await self.make_controller(tmpdir)

            with self.assertRaisesRegex(ValueError, "runtime config directory"):
                await controller._handle_command(
                    "config export /tmp/escape.json", readonly=False
                )

    async def test_import_check_does_not_change_runtime(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, runtime = await self.make_controller(tmpdir)

            payload = {
                "schema_version": 1,
                "targets": {
                    "default": {
                        "id": "default",
                        "name": "default",
                        "upload_channel_id": -9999,
                    },
                    "archive": {
                        "id": "archive",
                        "name": "archive",
                        "upload_channel_id": -2002,
                    },
                },
                "sources": {},
            }
            import_path = Path(tmpdir) / "import.json"
            import_path.write_text(json.dumps(payload), encoding="utf-8")

            result = await controller._handle_command(
                f"config import-check {import_path.name}", readonly=False
            )
            self.assertIn("Import file is valid", result.text)

            snapshot = await runtime.get_snapshot()
            self.assertEqual(snapshot.targets["default"].upload_channel_id, -1001)
            self.assertNotIn("archive", snapshot.targets)

    async def test_import_without_confirm_validates_but_does_not_apply(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, runtime = await self.make_controller(tmpdir)

            payload = {
                "schema_version": 1,
                "targets": {
                    "default": {
                        "id": "default",
                        "name": "default",
                        "upload_channel_id": -1001,
                    },
                    "archive": {
                        "id": "archive",
                        "name": "archive",
                        "upload_channel_id": -2002,
                    },
                },
                "sources": {},
            }
            import_path = Path(tmpdir) / "import.json"
            import_path.write_text(json.dumps(payload), encoding="utf-8")

            result = await controller._handle_command(
                f"config import {import_path.name}", readonly=False
            )

            self.assertIn("Import validation passed", result.text)
            self.assertFalse(result.changed_runtime)
            snapshot = await runtime.get_snapshot()
            self.assertNotIn("archive", snapshot.targets)

    async def test_import_with_confirm_applies_and_creates_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, runtime = await self.make_controller(tmpdir)

            payload = {
                "schema_version": 1,
                "targets": {
                    "default": {
                        "id": "default",
                        "name": "default",
                        "upload_channel_id": -1001,
                    },
                    "archive": {
                        "id": "archive",
                        "name": "archive",
                        "upload_channel_id": -2002,
                    },
                },
                "sources": {},
            }
            import_path = Path(tmpdir) / "import.json"
            import_path.write_text(json.dumps(payload), encoding="utf-8")

            result = await controller._handle_command(
                f"config import {import_path.name} confirm=yes", readonly=False
            )

            self.assertTrue(result.changed_runtime)
            self.assertIn("imported", result.text)
            snapshot = await runtime.get_snapshot()
            self.assertIn("archive", snapshot.targets)

            backups = list(Path(tmpdir).glob("runtime_config.backup-*.json"))
            self.assertTrue(backups, "expected a runtime config backup file before import")

    async def test_source_remove_via_controller(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, runtime = await self.make_controller(tmpdir)
            await runtime.add_source("twitch", "victim", target_profile_id="default")

            result = await controller._handle_command("source remove twitch:victim", readonly=False)

            self.assertIn("Источник удалён", result.text)
            snapshot = await runtime.get_snapshot()
            self.assertNotIn("twitch:victim", snapshot.sources)

    async def test_target_rename_via_controller_rebinds_sources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, runtime = await self.make_controller(tmpdir)
            await runtime.add_target("Archive", -2002)
            await runtime.add_source("twitch", "bound", target_profile_id="archive")

            result = await controller._handle_command(
                "target rename archive Cold Storage", readonly=False
            )

            self.assertIn("переименован", result.text)
            snapshot = await runtime.get_snapshot()
            self.assertIn("cold_storage", snapshot.targets)
            self.assertEqual(snapshot.sources["twitch:bound"].target_profile_id, "cold_storage")

    async def test_target_remove_with_reassign(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, runtime = await self.make_controller(tmpdir)
            await runtime.add_target("Archive", -2002)
            await runtime.add_source("twitch", "bound", target_profile_id="archive")

            result = await controller._handle_command(
                "target remove archive reassign=default", readonly=False
            )

            self.assertIn("удалён", result.text)
            snapshot = await runtime.get_snapshot()
            self.assertNotIn("archive", snapshot.targets)
            self.assertEqual(snapshot.sources["twitch:bound"].target_profile_id, "default")

    async def test_is_allowed_empty_lists_default_is_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, _ = await self.make_controller(
                tmpdir,
                control_config=ControlConfig(
                    enabled=True,
                    group_id=-100,
                    runtime_config_file=str(Path(tmpdir) / "runtime.sqlite3"),
                    audit_log_file=str(Path(tmpdir) / "audit.log"),
                ),
            )

            self.assertFalse(controller._is_allowed(12345))

    async def test_is_allowed_empty_lists_with_open_flag(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            controller, _ = await self.make_controller(
                tmpdir,
                control_config=ControlConfig(
                    enabled=True,
                    group_id=-100,
                    allow_all_group_members=True,
                    runtime_config_file=str(Path(tmpdir) / "runtime.sqlite3"),
                    audit_log_file=str(Path(tmpdir) / "audit.log"),
                ),
            )

            self.assertTrue(controller._is_allowed(12345))


if __name__ == "__main__":
    unittest.main()
