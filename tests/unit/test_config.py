import tempfile
import unittest
from pathlib import Path

import yaml

from streamrecorder.config import load_config


BASE_CONFIG = {
    "telegram": {
        "api_id": 12345,
        "api_hash": "hash",
        "channel_id": -100123,
    }
}


class ConfigTests(unittest.TestCase):
    def write_config(self, data):
        tmp = tempfile.NamedTemporaryFile("w", suffix=".yaml", dir="/tmp", delete=False)
        with tmp:
            yaml.safe_dump(data, tmp)
        self.addCleanup(lambda: Path(tmp.name).unlink(missing_ok=True))
        return tmp.name

    def test_rejects_invalid_platform(self):
        data = dict(BASE_CONFIG)
        data["platform"] = "twich"

        with self.assertRaisesRegex(ValueError, "platform"):
            load_config(self.write_config(data))

    def test_parses_typed_values(self):
        data = dict(BASE_CONFIG)
        data.update(
            {
                "platform": "both",
                "telegram": {
                    **BASE_CONFIG["telegram"],
                    "use_fast_upload_helper": "false",
                    "upload_parallelism": "3",
                    "upload_speed_limit_mbps": "12,5",
                },
                "twitch": {
                    "channels": ["Example"],
                    "check_interval": "2",
                    "offline_grace_period": "0",
                },
                "recording": {
                    "live_from_start": "false",
                    "move_atom_to_front": "true",
                    "use_mpegts": "1",
                    "retries": "7",
                    "fragment_retries": "8",
                },
                "splitting": {
                    "default_size_gb": "1.8",
                    "premium_size_gb": "3.8",
                },
            }
        )

        config = load_config(self.write_config(data))

        self.assertEqual(config.platform, "both")
        self.assertFalse(config.telegram.use_fast_upload_helper)
        self.assertEqual(config.telegram.upload_parallelism, 3)
        self.assertEqual(config.telegram.upload_speed_limit_mbps, 12.5)
        self.assertFalse(config.recording.live_from_start)
        self.assertTrue(config.recording.use_mpegts)
        self.assertEqual(config.recording.retries, 7)
        self.assertEqual(config.recording.fragment_retries, 8)
        self.assertEqual(config.splitting.default_size_gb, 1.8)

    def test_rejects_invalid_positive_sizes(self):
        data = dict(BASE_CONFIG)
        data["splitting"] = {"default_size_gb": 0}

        with self.assertRaisesRegex(ValueError, "splitting.default_size_gb"):
            load_config(self.write_config(data))

    def test_parses_control_config(self):
        data = dict(BASE_CONFIG)
        data["control"] = {
            "enabled": "true",
            "group_id": "-100777",
            "allowed_user_ids": [1, "2"],
            "readonly_user_ids": ["3"],
            "runtime_config_file": "./data/runtime.json",
        }

        config = load_config(self.write_config(data))

        self.assertTrue(config.control.enabled)
        self.assertEqual(config.control.group_id, -100777)
        self.assertEqual(config.control.allowed_user_ids, [1, 2])
        self.assertEqual(config.control.readonly_user_ids, [3])
        self.assertEqual(config.control.runtime_config_file, "./data/runtime.json")
        self.assertFalse(config.control.allow_all_group_members)

    def test_control_enabled_requires_group_id(self):
        data = dict(BASE_CONFIG)
        data["control"] = {"enabled": True}

        with self.assertRaisesRegex(ValueError, "control.group_id"):
            load_config(self.write_config(data))

    def test_control_enabled_requires_allowed_users_or_open_flag(self):
        """If control is enabled but no users are listed and the open flag is
        false, config loading must fail rather than silently allowing every
        member of the control group to mutate runtime state."""
        data = dict(BASE_CONFIG)
        data["control"] = {
            "enabled": True,
            "group_id": -100777,
            # allowed_user_ids and readonly_user_ids are deliberately empty
            # and allow_all_group_members is not set.
        }

        with self.assertRaisesRegex(ValueError, "control.allowed_user_ids"):
            load_config(self.write_config(data))

    def test_control_allow_all_group_members_is_explicit_opt_in(self):
        data = dict(BASE_CONFIG)
        data["control"] = {
            "enabled": True,
            "group_id": -100777,
            "allow_all_group_members": True,
        }

        config = load_config(self.write_config(data))

        self.assertTrue(config.control.enabled)
        self.assertEqual(config.control.allowed_user_ids, [])
        self.assertEqual(config.control.readonly_user_ids, [])
        self.assertTrue(config.control.allow_all_group_members)

    def test_control_readonly_only_is_allowed(self):
        """Read-only user lists alone should be accepted as a valid
        configuration: the controller accepts the configured users for status
        commands and rejects unknown senders."""
        data = dict(BASE_CONFIG)
        data["control"] = {
            "enabled": True,
            "group_id": -100777,
            "readonly_user_ids": [42],
        }

        config = load_config(self.write_config(data))

        self.assertTrue(config.control.enabled)
        self.assertEqual(config.control.readonly_user_ids, [42])
        self.assertFalse(config.control.allow_all_group_members)

    def test_parses_jobs_and_audit_log(self):
        data = dict(BASE_CONFIG)
        data["jobs"] = {
            "recording_concurrency": "5",
            "processing_concurrency": "3",
            "upload_concurrency": "2",
            "compression_concurrency": "1",
            "control_concurrency": "4",
        }
        data["control"] = {"audit_log_file": "./logs/audit.log"}

        config = load_config(self.write_config(data))

        self.assertEqual(config.jobs.recording_concurrency, 5)
        self.assertEqual(config.jobs.processing_concurrency, 3)
        self.assertEqual(config.jobs.upload_concurrency, 2)
        self.assertEqual(config.jobs.control_concurrency, 4)
        self.assertEqual(config.control.audit_log_file, "./logs/audit.log")


if __name__ == "__main__":
    unittest.main()
