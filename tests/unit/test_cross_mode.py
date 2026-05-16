import asyncio
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import AsyncMock

from streamrecorder.recorder import RecordingResult, RecordingStatus
from streamrecorder.state_manager import StreamStatus
from streamrecorder.stream_monitor import StreamInfo


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


class CrossModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_cross_twitch_recording_does_not_create_active_twitch_state(self):
        asyncio.get_running_loop().slow_callback_duration = 1.0
        install_telethon_stub()
        from streamrecorder.main import StreamRecorderApp

        class StateStub:
            def __init__(self):
                self.linked_channel = None
                self.created = []

            async def get_state(self, channel):
                return None

            async def create_stream(self, *args, **kwargs):
                self.created.append((args, kwargs))

            async def update_linked_channel(self, channel, linked_channel):
                self.linked_channel = (channel, linked_channel)

            async def add_linked_recording_file(self, channel, file_path):
                pass

        state = StateStub()
        twitch_info = StreamInfo(
            channel="twitchchan",
            title="twitch",
            category="game",
            started_at=datetime.now(),
            stream_url="https://www.twitch.tv/twitchchan",
            is_live=True,
        )

        app = object.__new__(StreamRecorderApp)
        app.state = state
        app.monitor = type("MonitorStub", (), {"check_twitch_live": AsyncMock(return_value=twitch_info)})()
        app.recorder = type(
            "RecorderStub",
            (),
            {
                "record_stream": AsyncMock(
                    return_value=RecordingResult(
                        channel="twitchchan",
                        stream_info=twitch_info,
                        output_path="/tmp/twitch.mp4",
                        status=RecordingStatus.COMPLETED,
                        duration_seconds=1,
                        file_size_bytes=1,
                        started_at=datetime.now(),
                        ended_at=datetime.now(),
                    )
                )
            },
        )()
        app._cross_twitch_tasks = {}
        app._active_recordings = {}

        logger = type("LoggerStub", (), {"info": lambda *args, **kwargs: None, "warning": lambda *args, **kwargs: None})()

        await StreamRecorderApp._start_twitch_for_cross(app, "@yt", "cross-key", logger)
        task = app._cross_twitch_tasks["cross-key"]
        await task

        self.assertEqual(state.created, [])
        self.assertEqual(state.linked_channel, ("@yt", "twitchchan"))


if __name__ == "__main__":
    unittest.main()
