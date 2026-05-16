import tempfile
import unittest
from pathlib import Path

from streamrecorder.config import Config, TelegramConfig, TwitchConfig, YouTubeConfig
from streamrecorder.runtime_config import RuntimeConfigManager


class RuntimeConfigTests(unittest.IsolatedAsyncioTestCase):
    async def test_initializes_from_static_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_file = Path(tmpdir) / "runtime.json"
            config = Config(
                telegram=TelegramConfig(
                    api_id=1,
                    api_hash="hash",
                    channel_id=-1001,
                    discussion_group_id=-1002,
                    backup_channel_id=-1003,
                    upload_parallelism=2,
                    upload_speed_limit_mbps=3.5,
                ),
                twitch=TwitchConfig(channels=["Example"]),
                youtube=YouTubeConfig(channels=["@ExampleYT"]),
            )

            manager = RuntimeConfigManager(str(runtime_file))
            snapshot = await manager.load_or_initialize(config)

            self.assertIn("default", snapshot.targets)
            self.assertEqual(snapshot.targets["default"].upload_channel_id, -1001)
            self.assertIn("twitch:example", snapshot.sources)
            self.assertIn("youtube:@exampleyt", snapshot.sources)
            self.assertTrue(runtime_file.with_suffix(".sqlite3").exists())

    async def test_add_source_validates_target(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            with self.assertRaisesRegex(ValueError, "unknown target"):
                await manager.add_source("twitch", "channel", target_profile_id="missing")

            source = await manager.add_source("twitch", "channel", target_profile_id="default")
            self.assertEqual(source.id, "twitch:channel")

            paused = await manager.set_source_enabled(source.id, False)
            self.assertFalse(paused.enabled)

    async def test_updates_target_and_source_binding(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            target = await manager.add_target("Archive", -2001)
            source = await manager.add_source("youtube", "@chan", target_profile_id="default")
            rebound = await manager.set_source_target(source.id, target.id)
            self.assertEqual(rebound.target_profile_id, "archive")

            updated = await manager.update_target(
                "archive",
                compression_enabled="false",
                upload_parallelism="3",
                upload_speed_limit_mbps="12.5",
                discussion_group_id="-2002",
            )
            self.assertFalse(updated.compression_enabled)
            self.assertEqual(updated.upload_parallelism, 3)
            self.assertEqual(updated.upload_speed_limit_mbps, 12.5)
            self.assertEqual(updated.discussion_group_id, -2002)

    async def test_export_import_runtime_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            first = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await first.load_or_initialize(config)
            await first.add_target("Archive", -2001)
            await first.add_source("twitch", "Example", target_profile_id="archive")

            export_path = Path(tmpdir) / "export.json"
            await first.export_to_file(str(export_path))

            second = RuntimeConfigManager(str(Path(tmpdir) / "runtime2.json"))
            snapshot = await second.import_from_file(str(export_path))

            self.assertIn("archive", snapshot.targets)
            self.assertEqual(snapshot.sources["twitch:example"].target_profile_id, "archive")

    async def test_normalizes_youtube_handle_on_add(self):
        """YouTube handles without `@` should be normalized to `@handle` so the
        same channel maps to the same source id, regardless of how it was
        typed in the Telegram control command."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            source = await manager.add_source("youtube", "foo", target_profile_id="default")
            self.assertEqual(source.id, "youtube:@foo")
            self.assertEqual(source.handle, "@foo")

            # Adding the same handle a second time, even with the `@` prefix, must
            # not create a duplicate source.
            second = await manager.add_source("youtube", "@foo", target_profile_id="default")
            self.assertEqual(second.id, "youtube:@foo")
            snapshot = await manager.get_snapshot()
            self.assertEqual(
                [src for src in snapshot.sources.values() if src.platform == "youtube"][0].handle,
                "@foo",
            )

    async def test_youtube_channel_id_handle_is_not_at_prefixed(self):
        """`UC...` style channel ids are real YouTube identifiers and must
        stay verbatim rather than being prefixed with `@`."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            source = await manager.add_source(
                "youtube", "UCabcdefghijklmnopqrstuv", target_profile_id="default"
            )
            self.assertEqual(source.handle, "UCabcdefghijklmnopqrstuv")
            self.assertTrue(source.id.startswith("youtube:uc"))

    async def test_remove_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            source = await manager.add_source("twitch", "ToRemove", target_profile_id="default")
            removed = await manager.remove_source(source.id)
            self.assertEqual(removed.id, source.id)

            snapshot = await manager.get_snapshot()
            self.assertNotIn(source.id, snapshot.sources)

            with self.assertRaisesRegex(ValueError, "unknown source"):
                await manager.remove_source(source.id)

    async def test_remove_target_blocks_referenced_target_without_reassign(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            await manager.add_target("Archive", -2001)
            await manager.add_source("twitch", "Bound", target_profile_id="archive")

            with self.assertRaisesRegex(ValueError, "used by sources"):
                await manager.remove_target("archive")

    async def test_remove_target_with_reassign_rebinds_sources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            await manager.add_target("Archive", -2001)
            await manager.add_source("twitch", "Bound", target_profile_id="archive")

            removed = await manager.remove_target("archive", reassign_to="default")

            self.assertEqual(removed.id, "archive")
            snapshot = await manager.get_snapshot()
            self.assertNotIn("archive", snapshot.targets)
            self.assertEqual(snapshot.sources["twitch:bound"].target_profile_id, "default")

    async def test_default_target_cannot_be_removed_or_renamed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            with self.assertRaisesRegex(ValueError, "default target cannot be removed"):
                await manager.remove_target("default")
            with self.assertRaisesRegex(ValueError, "default target cannot be renamed"):
                await manager.rename_target("default", "primary")

    async def test_rename_target_updates_source_bindings(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            await manager.add_target("Archive", -2001)
            await manager.add_source("twitch", "Bound", target_profile_id="archive")

            renamed = await manager.rename_target("archive", "Cold Storage")

            self.assertEqual(renamed.id, "cold_storage")
            snapshot = await manager.get_snapshot()
            self.assertIn("cold_storage", snapshot.targets)
            self.assertNotIn("archive", snapshot.targets)
            self.assertEqual(snapshot.sources["twitch:bound"].target_profile_id, "cold_storage")

    async def test_compression_enabled_global_alias_resolves_to_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            await manager.add_target("Mixed", -2001)
            target = await manager.update_target("mixed", compression_enabled="global")
            self.assertIsNone(target.compression_enabled)

            target = await manager.update_target("mixed", compression_enabled="false")
            self.assertFalse(target.compression_enabled)

    async def test_update_target_rejects_zero_upload_channel_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            with self.assertRaisesRegex(ValueError, "upload_channel_id"):
                await manager.update_target("default", upload_channel_id=0)

    async def test_import_rejects_source_with_unknown_target(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            payload = {
                "schema_version": 1,
                "targets": {
                    "default": {
                        "id": "default",
                        "name": "default",
                        "upload_channel_id": -1001,
                    }
                },
                "sources": {
                    "twitch:orphan": {
                        "id": "twitch:orphan",
                        "platform": "twitch",
                        "handle": "orphan",
                        "target_profile_id": "missing",
                        "enabled": True,
                    }
                },
            }

            with self.assertRaisesRegex(ValueError, "unknown target"):
                await manager.import_dict(payload)

    async def test_import_rejects_zero_upload_channel_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001))
            manager = RuntimeConfigManager(str(Path(tmpdir) / "runtime.json"))
            await manager.load_or_initialize(config)

            payload = {
                "schema_version": 1,
                "targets": {
                    "default": {
                        "id": "default",
                        "name": "default",
                        "upload_channel_id": 0,
                    }
                },
                "sources": {},
            }

            with self.assertRaisesRegex(ValueError, "upload_channel_id"):
                await manager.import_dict(payload)


if __name__ == "__main__":
    unittest.main()
