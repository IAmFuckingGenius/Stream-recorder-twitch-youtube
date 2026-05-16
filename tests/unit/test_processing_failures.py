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
from streamrecorder.runtime_config import TargetProfile
from streamrecorder.state_manager import MessageIds, StreamState, StreamStatus
from streamrecorder.uploader import UploadResult


class StateStub:
    def __init__(self, state):
        self.state = state
        self.status_updates = []
        self.completed = False

    async def get_state(self, channel):
        return self.state

    async def update_status(self, channel, status, error=None):
        self.state.status = status
        if error:
            self.state.error_message = error
        self.status_updates.append((status, error))

    async def set_uploaded_part(self, channel, file_path, msg_id):
        self.state.uploaded_parts[file_path] = msg_id
        if msg_id not in self.state.message_ids.upload_msgs:
            self.state.message_ids.upload_msgs.append(msg_id)

    async def set_split_files(self, channel, files):
        self.state.split_files = list(files)

    async def set_compressed_file(self, channel, file_path):
        self.state.compressed_file = file_path

    async def clear_message_id(self, channel, msg_type):
        if msg_type == "upload":
            self.state.message_ids.upload_msgs = []
            self.state.uploaded_parts = {}
        elif msg_type == "backup":
            self.state.message_ids.backup_msgs = []
        elif msg_type == "compressed":
            self.state.message_ids.compressed_msg = None

    async def set_message_id(self, channel, msg_type, msg_id):
        if msg_type == "compressed":
            self.state.message_ids.compressed_msg = msg_id
        elif msg_type == "backup":
            self.state.message_ids.backup_msgs.append(msg_id)

    async def complete_stream(self, channel):
        self.completed = True

    async def set_target_snapshot(self, channel, target):
        self.state.target_profile_id = getattr(target, "id", None)
        self.state.target_upload_channel_id = getattr(target, "upload_channel_id", None)
        self.state.target_discussion_group_id = getattr(target, "discussion_group_id", None)
        self.state.target_backup_channel_id = getattr(target, "backup_channel_id", None)
        self.state.target_upload_parallelism = getattr(target, "upload_parallelism", None)
        self.state.target_upload_speed_limit_mbps = getattr(target, "upload_speed_limit_mbps", None)
        self.state.target_compression_enabled = getattr(target, "compression_enabled", None)
        self.state.target_splitting_default_size_gb = getattr(target, "splitting_default_size_gb", None)
        self.state.target_splitting_premium_size_gb = getattr(target, "splitting_premium_size_gb", None)

    async def set_control_flag(self, channel, flag_name, enabled):
        if flag_name in {"skip_upload", "skip_compression", "skip_forwarding", "abort_requested"}:
            setattr(self.state, flag_name, bool(enabled))

    async def clear_control_flags(self, channel):
        self.state.skip_upload = False
        self.state.skip_compression = False
        self.state.skip_forwarding = False
        self.state.abort_requested = False


class ImmediateJobs:
    async def run(self, queue_name, job_name, job):
        return await job()


class ProcessingFailureTests(unittest.IsolatedAsyncioTestCase):
    def make_app(self, state_store, tmpdir):
        install_telethon_stub()
        app = object.__new__(StreamRecorderApp)
        app.config = Config(
            telegram=TelegramConfig(api_id=1, api_hash="hash", channel_id=-1001),
        )
        app.state = state_store
        app.jobs = ImmediateJobs()
        app.upload_scheduler = type("UploadSchedulerStub", (), {"run": ImmediateJobs().run})()
        app.uploader = type(
            "UploaderStub",
            (),
            {
                "is_premium": False,
                "upload_runtime": lambda self, parallelism, speed: _NullAsyncContext(),
                "upload_stream_parts": AsyncMock(),
                "send_to_discussion": AsyncMock(return_value=None),
                "forward_to_backup": AsyncMock(return_value=[]),
                "delete_message": AsyncMock(),
            },
        )()
        app.uploader.upload_stream_parts.side_effect = None
        app.uploader.send_to_discussion.side_effect = None
        app.uploader.forward_to_backup.side_effect = None
        app.splitter = type("SplitterStub", (), {"split_file": AsyncMock()})()
        app.compressor = type("CompressorStub", (), {"compress_to_size": AsyncMock()})()
        app.splitter.split_file.side_effect = None
        app.compressor.compress_to_size.side_effect = None
        app._shutting_down = False
        app._get_target_for_channel = AsyncMock(
            return_value=TargetProfile(
                id="default",
                name="default",
                upload_channel_id=-1001,
                backup_channel_id=-1002,
                compression_enabled=True,
            )
        )
        app._update_status_msg = AsyncMock()
        app._probe_duration_seconds = AsyncMock(return_value=10.0)
        app._process_recording_validate_file = None
        app._prepare_upload_sources = AsyncMock(side_effect=lambda files, label, logger: (files, []))
        app._filter_tail_phantoms_before_processing = AsyncMock(side_effect=lambda state, files, logger: files)
        app._format_duration_multi = AsyncMock(return_value="10s")
        app._cleanup_files = AsyncMock()
        app._cleanup_generated_upload_files = AsyncMock()
        return app

    async def test_failed_compressed_comment_keeps_state_and_files_for_retry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original = Path(tmpdir) / "recording.mp4"
            original.write_bytes(b"video")
            compressed = Path(tmpdir) / "recording_compressed.mp4"
            compressed.write_bytes(b"compressed")
            state = StreamState(
                channel="channel",
                stream_id="stream",
                title="title",
                status=StreamStatus.PROCESSING,
                recording_files=[str(original)],
            )
            state_store = StateStub(state)
            app = self.make_app(state_store, tmpdir)
            app._get_target_for_channel.return_value = TargetProfile(
                id="default",
                name="default",
                upload_channel_id=-1001,
                backup_channel_id=None,
                compression_enabled=True,
                splitting_default_size_gb=0.000001,
            )
            app.splitter.split_file.return_value = type(
                "SplitResultStub",
                (),
                {"success": True, "output_files": [str(original), str(original) + ".part2"]},
            )()
            app.uploader.upload_stream_parts.return_value = type(
                "BatchResultStub",
                (),
                {
                    "failed_uploads": 0,
                    "successful_uploads": 1,
                    "total_parts": 1,
                    "results": [UploadResult(str(original), 111, True)],
                },
            )()
            app.uploader.upload_stream_parts.side_effect = None
            app.uploader.upload_stream_parts = AsyncMock(return_value=type(
                "BatchResultStub",
                (),
                {
                    "failed_uploads": 0,
                    "successful_uploads": 1,
                    "total_parts": 2,
                    "results": [
                        UploadResult(str(original), 111, True),
                        UploadResult(str(original) + ".part2", 112, True),
                    ],
                },
            )())
            app.compressor.compress_to_size.return_value = type(
                "CompressionResultStub",
                (),
                {
                    "success": True,
                    "output_file": str(compressed),
                    "size_reduction_percent": 50.0,
                },
            )()
            app.compressor.compress_to_size.side_effect = None

            async def fake_create_subprocess_exec(*args, **kwargs):
                class Proc:
                    returncode = 0

                    async def communicate(self):
                        return b"10.0\n", b""

                return Proc()

            import streamrecorder.main as main_module
            original_create = main_module.asyncio.create_subprocess_exec
            main_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec

            try:
                await StreamRecorderApp._process_recording(app, state)
            finally:
                main_module.asyncio.create_subprocess_exec = original_create

            self.assertFalse(state_store.completed)
            self.assertEqual(state_store.status_updates[-1][0], StreamStatus.SENDING_COMPRESSED)
            app._cleanup_files.assert_not_awaited()

    async def test_partial_backup_forward_keeps_state_and_files_for_retry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original = Path(tmpdir) / "recording.mp4"
            original.write_bytes(b"video")
            state = StreamState(
                channel="channel",
                stream_id="stream",
                title="title",
                status=StreamStatus.FORWARDING,
                recording_files=[str(original)],
                split_files=[str(original)],
                message_ids=MessageIds(upload_msgs=[111, 112]),
            )
            state_store = StateStub(state)
            app = self.make_app(state_store, tmpdir)
            app.uploader.forward_to_backup.return_value = [211]

            await StreamRecorderApp._resume_forwarding(app, state)

            self.assertFalse(state_store.completed)
            self.assertEqual(state_store.status_updates[-1][0], StreamStatus.FORWARDING)

    async def test_skip_upload_with_completed_uploads_archives_without_error(self):
        """User scenario: session is mid-pipeline, uploads already done.
        /skip-upload sets skip_upload=True, then on next start recovery picks
        the session up. The pipeline must NOT bail out into ERROR — instead it
        should pass through compression/forwarding (also skipped) and finalize
        via complete_stream so the session is archived and not auto-resumed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            recording = Path(tmpdir) / "recording.mp4"
            recording.write_bytes(b"video")
            split_a = Path(tmpdir) / "recording_part001.mp4"
            split_a.write_bytes(b"a")
            split_b = Path(tmpdir) / "recording_part002.mp4"
            split_b.write_bytes(b"b")
            state = StreamState(
                channel="channel",
                stream_id="stream",
                title="title",
                status=StreamStatus.PROCESSING,
                recording_files=[str(recording)],
                split_files=[str(split_a), str(split_b)],
                uploaded_parts={str(split_a): 111, str(split_b): 112},
                message_ids=MessageIds(upload_msgs=[111, 112]),
                skip_upload=True,
                skip_compression=True,
            )
            state_store = StateStub(state)
            app = self.make_app(state_store, tmpdir)
            app._get_target_for_channel.return_value = TargetProfile(
                id="default",
                name="default",
                upload_channel_id=-1001,
                backup_channel_id=None,
                compression_enabled=True,
            )

            async def fake_create_subprocess_exec(*args, **kwargs):
                class Proc:
                    returncode = 0

                    async def communicate(self):
                        return b"10.0\n", b""

                return Proc()

            import streamrecorder.main as main_module
            original_create = main_module.asyncio.create_subprocess_exec
            main_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
            try:
                await StreamRecorderApp._process_recording(app, state)
            finally:
                main_module.asyncio.create_subprocess_exec = original_create

            # Pipeline must reach complete_stream (archive), not stop in ERROR.
            self.assertTrue(state_store.completed, "expected complete_stream to be called")
            error_statuses = [s for s, _ in state_store.status_updates if s == StreamStatus.ERROR]
            self.assertEqual(error_statuses, [], "skip-upload must not put session in ERROR")
            # No real upload should have been attempted.
            app.uploader.upload_stream_parts.assert_not_awaited()
            # Files should be preserved on disk.
            app._cleanup_files.assert_not_awaited()


class _NullAsyncContext:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


if __name__ == "__main__":
    unittest.main()
