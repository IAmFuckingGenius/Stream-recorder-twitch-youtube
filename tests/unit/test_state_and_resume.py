import sys
import tempfile
import types
import unittest
import sqlite3
import json
from pathlib import Path
from unittest.mock import AsyncMock

from streamrecorder.state_manager import MessageIds, StateManager, StreamState, StreamStatus


def install_telethon_stub():
    telethon = types.ModuleType("telethon")
    telethon.TelegramClient = object
    tl = types.ModuleType("telethon.tl")
    tl_types = types.ModuleType("telethon.tl.types")
    tl_types.User = object
    tl_types.DocumentAttributeVideo = object
    tl_types.DocumentAttributeFilename = object
    tl_types.InputMediaUploadedDocument = object
    sys.modules.setdefault("telethon", telethon)
    sys.modules.setdefault("telethon.tl", tl)
    sys.modules.setdefault("telethon.tl.types", tl_types)


class StateManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_state_returns_copy(self):
        manager = StateManager("/tmp/streamrecorder-test-state.json")
        manager._states["channel"] = StreamState(
            channel="channel",
            stream_id="stream",
            title="original",
            status=StreamStatus.WAITING,
        )

        state = await manager.get_state("channel")
        state.title = "mutated"

        fresh = await manager.get_state("channel")
        self.assertEqual(fresh.title, "original")

    async def test_state_list_methods_return_copies(self):
        manager = StateManager("/tmp/streamrecorder-test-state-list.json")
        manager._states["waiting"] = StreamState(
            channel="waiting",
            stream_id="stream-waiting",
            title="original waiting",
            status=StreamStatus.WAITING,
        )
        manager._states["uploading"] = StreamState(
            channel="uploading",
            stream_id="stream-uploading",
            title="original uploading",
            status=StreamStatus.UPLOADING,
        )

        waiting = await manager.get_waiting_streams()
        incomplete = await manager.get_incomplete_streams()
        waiting[0].title = "mutated"
        incomplete[0].title = "mutated"

        self.assertEqual((await manager.get_state("waiting")).title, "original waiting")
        self.assertEqual((await manager.get_state("uploading")).title, "original uploading")

    async def test_persists_state_by_source_and_session_in_sqlite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.sqlite3"
            manager = StateManager(str(state_path))
            state = await manager.create_stream(
                "@channel",
                title="title",
                status=StreamStatus.RECORDING,
                source_id="youtube:@channel",
                session_id="session-1",
            )

            self.assertEqual(state.source_id, "youtube:@channel")
            self.assertEqual(state.session_id, "session-1")
            self.assertTrue(state_path.exists())

            reloaded = StateManager(str(state_path))
            await reloaded.load()
            loaded = await reloaded.get_state("@channel")
            self.assertEqual(loaded.source_id, "youtube:@channel")
            self.assertEqual(loaded.session_id, "session-1")

    async def test_save_removes_superseded_active_rows_for_same_channel(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.sqlite3"
            manager = StateManager(str(state_path))
            await manager.create_stream(
                "channel",
                title="old",
                status=StreamStatus.RECORDING,
                source_id="twitch:channel",
                session_id="session-old",
            )
            await manager.create_stream(
                "channel",
                title="new",
                status=StreamStatus.PROCESSING,
                source_id="twitch:channel",
                session_id="session-new",
            )

            reloaded = StateManager(str(state_path))
            await reloaded.load()
            latest = await reloaded.get_state("channel")
            self.assertEqual(latest.session_id, "session-new")
            await reloaded.update_status("channel", StreamStatus.UPLOADING)

            with sqlite3.connect(state_path) as conn:
                rows = conn.execute(
                    "SELECT session_id, data FROM active_streams ORDER BY session_id"
                ).fetchall()

            self.assertEqual([row[0] for row in rows], ["session-new"])
            by_session = {row[0]: json.loads(row[1]) for row in rows}
            self.assertEqual(by_session["session-new"]["status"], "uploading")

    async def test_load_corrupt_json_backs_up_and_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{not-json", encoding="utf-8")
            manager = StateManager(str(state_path))

            with self.assertRaisesRegex(RuntimeError, "Failed to load state file"):
                await manager.load()

            backups = list(Path(tmpdir).glob("state.json.corrupt-*.bak"))
            self.assertEqual(len(backups), 1)
            self.assertEqual(backups[0].read_text(encoding="utf-8"), "{not-json")

    async def test_load_invalid_state_contents_backs_up_and_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                '{"active":{"bad":{"channel":"bad"}},"history":[]}',
                encoding="utf-8",
            )
            manager = StateManager(str(state_path))

            with self.assertRaisesRegex(RuntimeError, "invalid contents"):
                await manager.load()

            backups = list(Path(tmpdir).glob("state.json.corrupt-*.bak"))
            self.assertEqual(len(backups), 1)

    async def test_archive_stream_preserves_status_and_moves_to_history(self):
        """archive_stream should remove the channel from active state but keep
        the current status (e.g. ERROR) in the history row, so /cancel does not
        leave a zombie active session that recovery would re-pick on next start.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.sqlite3"
            manager = StateManager(str(state_path))
            await manager.create_stream(
                "channel",
                title="t",
                status=StreamStatus.PROCESSING,
                source_id="twitch:channel",
                session_id="session-x",
            )
            await manager.update_status("channel", StreamStatus.ERROR, "Cancelled by control command")

            await manager.archive_stream("channel")

            self.assertIsNone(await manager.get_state("channel"))
            self.assertEqual(await manager.get_incomplete_streams(), [])
            with sqlite3.connect(state_path) as conn:
                active_rows = conn.execute("SELECT COUNT(*) FROM active_streams").fetchone()[0]
                history_rows = conn.execute("SELECT data FROM stream_history ORDER BY id DESC").fetchall()
            self.assertEqual(active_rows, 0)
            self.assertEqual(len(history_rows), 1)
            archived = json.loads(history_rows[0][0])
            # Status must NOT be auto-promoted to DONE — preserve ERROR/etc.
            self.assertEqual(archived["status"], "error")
            self.assertEqual(archived["error_message"], "Cancelled by control command")

    async def test_archive_stream_on_missing_channel_is_noop(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(str(Path(tmpdir) / "state.sqlite3"))
            # Should not raise.
            await manager.archive_stream("nonexistent")
            self.assertEqual(manager._states, {})


class ResumeCompressedTests(unittest.IsolatedAsyncioTestCase):
    async def test_resume_send_compressed_skips_existing_comment(self):
        install_telethon_stub()
        from streamrecorder.main import StreamRecorderApp

        class StateStub:
            def __init__(self):
                self.completed = False

            async def get_state(self, channel):
                return StreamState(
                    channel="channel",
                    stream_id="stream",
                    title="title",
                    status=StreamStatus.SENDING_COMPRESSED,
                    message_ids=MessageIds(upload_msgs=[111], compressed_msg=222),
                    compressed_file="/tmp/compressed.mp4",
                )

            async def complete_stream(self, channel):
                self.completed = True

        state_store = StateStub()
        state = await state_store.get_state("channel")
        app = object.__new__(StreamRecorderApp)
        app.state = state_store
        app.uploader = type(
            "UploaderStub",
            (),
            {
                "backup_channel_id": None,
                "send_to_discussion": AsyncMock(),
                "delete_message": AsyncMock(),
            },
        )()
        app._update_status_msg = AsyncMock()

        await StreamRecorderApp._resume_send_compressed(app, state)

        app.uploader.send_to_discussion.assert_not_awaited()
        self.assertTrue(state_store.completed)


if __name__ == "__main__":
    unittest.main()
