"""Tests for /cancel archival and recovery skipping aborted sessions."""

from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock


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


install_telethon_stub()

from streamrecorder.config import Config, TelegramConfig
from streamrecorder.main import StreamRecorderApp
from streamrecorder.state_manager import StateManager, StreamState, StreamStatus


class CancelArchivesSessionTests(unittest.IsolatedAsyncioTestCase):
    """`/cancel` and `/abort` must archive the session so it does not show up
    on the next startup as an "incomplete" stream that gets re-resumed."""

    async def _make_app(self, tmpdir: str) -> tuple[StreamRecorderApp, StateManager]:
        state_path = Path(tmpdir) / "state.sqlite3"
        state = StateManager(str(state_path))
        await state.load()

        app = object.__new__(StreamRecorderApp)
        app.state = state
        app.recorder = type("RecorderStub", (), {"stop_recording": AsyncMock(return_value=False)})()
        app._active_recordings = {}
        app._processing_tasks = {}
        app._processing_channels = set()
        app._skip_grace_for = set()
        app._recovery_tasks = {}
        return app, state

    async def test_cancel_archives_active_session(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app, state = await self._make_app(tmpdir)
            await state.create_stream(
                "channel",
                title="t",
                status=StreamStatus.PROCESSING,
                source_id="twitch:channel",
                session_id="session-1",
            )

            result = await StreamRecorderApp._control_session_action_unqueued(
                app, "cancel", "channel"
            )

            self.assertIn("Cancelled channel", result)
            self.assertIn("session archived", result)
            # Active state is gone, history holds the archived session.
            self.assertIsNone(await state.get_state("channel"))
            self.assertEqual(await state.get_incomplete_streams(), [])

    async def test_abort_archives_active_session(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app, state = await self._make_app(tmpdir)
            await state.create_stream(
                "channel",
                title="t",
                status=StreamStatus.UPLOADING,
                source_id="twitch:channel",
                session_id="session-1",
            )

            await StreamRecorderApp._control_session_action_unqueued(
                app, "abort", "channel"
            )

            self.assertIsNone(await state.get_state("channel"))


class RecoverySkipsAbortedSessionsTests(unittest.IsolatedAsyncioTestCase):
    """If a session was cancelled and somehow survived as active (e.g. legacy
    crash before the archival fix shipped), recovery on startup must NOT
    re-run _process_recording_guarded for it. It should archive instead."""

    async def test_recover_archives_aborted_session_without_processing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.sqlite3"
            state = StateManager(str(state_path))
            await state.load()
            await state.create_stream(
                "channel",
                title="t",
                status=StreamStatus.ERROR,
                source_id="twitch:channel",
                session_id="session-1",
            )
            await state.set_control_flag("channel", "abort_requested", True)

            app = object.__new__(StreamRecorderApp)
            app.state = state
            app.config = Config(telegram=TelegramConfig(api_id=1, api_hash="h", channel_id=-1))
            app._processing_tasks = {}
            app._processing_channels = set()
            app._active_recordings = {}
            app._skip_grace_for = set()
            app._recovery_tasks = {}
            app._logger = type("L", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None, "error": lambda *a, **k: None})()
            app._process_recording_guarded = AsyncMock()
            app._recover_cross_linked_files = AsyncMock()

            await StreamRecorderApp._recover_incomplete(app)

            # Aborted session must NOT be re-run through the processing pipeline.
            app._process_recording_guarded.assert_not_awaited()
            # And it must be archived, leaving no active row.
            self.assertIsNone(await state.get_state("channel"))
            self.assertEqual(await state.get_incomplete_streams(), [])


if __name__ == "__main__":
    unittest.main()
