# Stream Recorder v3

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Telethon-26A5E4?style=flat-square&logo=telegram&logoColor=white)](https://github.com/LonamiWebs/Telethon)
[![yt-dlp](https://img.shields.io/badge/Recorder-yt--dlp-ff0000?style=flat-square)](https://github.com/yt-dlp/yt-dlp)
[![FFmpeg](https://img.shields.io/badge/Media-FFmpeg-007808?style=flat-square&logo=ffmpeg&logoColor=white)](https://ffmpeg.org/)
![Modes](https://img.shields.io/badge/Modes-Twitch%20%7C%20YouTube%20%7C%20Both%20%7C%20Cross-6f42c1?style=flat-square)
![Status](https://img.shields.io/badge/Status-Active-2ea44f?style=flat-square)

Production-focused live stream recorder and Telegram publisher for Twitch and YouTube.

v3 adds a Telegram control plane, runtime configuration, dynamic source management, and multi-Telegram-channel routing on top of the existing recovery-first recording pipeline.

## TL;DR

Stream Recorder v3 monitors Twitch and YouTube channels, records live streams with `yt-dlp`, processes media with `ffmpeg`, uploads recordings to Telegram, resumes unfinished work after crashes, and can now be managed from a dedicated Telegram control group.

You can add, pause, resume, and retarget monitored sources while the app is running. Different sources can upload to different Telegram channels with their own discussion group, backup channel, compression flag, upload parallelism, and speed limit.

## What v3 Can Do Now

- Monitor Twitch via Helix API.
- Monitor YouTube via `yt-dlp` `/live` checks, without a YouTube API key.
- Run in `twitch`, `youtube`, `both`, or `cross` mode.
- Record live streams through `yt-dlp` with retry and interruption recovery.
- Save partial recordings cleanly during shutdown.
- Recover interrupted recording, processing, upload, compression, and forwarding steps on startup.
- Track stream lifecycle in persistent state.
- Handle multi-segment sessions when a stream drops and returns.
- Filter likely duplicate Twitch tail segments with duration, size, and optional hash/API checks.
- Remux upload sources to Telegram-friendly MP4 when needed.
- Split large recordings to Telegram account limits.
- Upload albums in order and persist file-to-message mappings to avoid duplicate uploads after restart.
- Optionally compress large recordings and post the compressed version to discussion/comments.
- Optionally forward uploaded messages to a backup Telegram channel.
- Send WAITING and status messages to Telegram.
- Clean stale WAITING messages by timeout.
- Use FastTelethonhelper for faster uploads when available.
- Apply a global upload speed cap when configured.
- Manage runtime sources and targets from a Telegram control group.

## v3 Telegram Control Plane

The control plane is optional and configured in `config.yaml` under `control`.

When enabled, the same Telethon userbot listens to a dedicated Telegram group and accepts admin commands. It does not require a separate Bot API bot.

### Control Setup

```yaml
control:
  enabled: true
  group_id: -1001234567890
  allowed_user_ids:
    - 123456789
  readonly_user_ids: []
  allow_all_group_members: false
  command_prefix: "/"
  runtime_config_file: ./data/runtime_config.sqlite3
```

Security notes:
- use a private control group;
- set `allowed_user_ids` for real deployments;
- if both `allowed_user_ids` and `readonly_user_ids` are empty, access is denied unless `allow_all_group_members: true`. With the previous "open by default" behavior removed, leaving both lists empty without the explicit flag now causes config loading to fail;
- only enable `allow_all_group_members: true` for private/trusted groups;
- readonly users can view status/config but cannot mutate sources, targets, sessions, or import/export config.

### Available Commands

```text
/help
/help source|target|session|config

/status
/sources [platform=twitch|youtube] [target=id] [state=on|off] [page=1] [limit=30]
/source info <source_id>
/source add twitch|youtube <handle> [target=default]
/source pause <source_id>
/source resume <source_id>
/source remove <source_id>
/source target <source_id> <target_id>
/source notes <source_id> [text]
/source tags <source_id> tag1,tag2

/targets [page=1] [limit=30]
/target info <target_id>
/target test <target_id>
/target add <name> <channel_id> [discussion_id=...] [backup_id=...]
/target set <target_id> key=value [key=value]
/target rename <target_id> <new_name>
/target remove <target_id> [reassign=default]

/session details <channel>
/session stop <channel>
/session cancel <channel>
/session skip-upload <channel>
/session skip-compression <channel>
/session skip-forwarding <channel>
/session retry-upload <channel>
/session retry-compression <channel>
/session retry-forwarding <channel>
/session reprocess <channel>
/session recover <channel>
/session clear-flags <channel>

/config
/config export [file.json]
/config import-check <file.json>
/config import <file.json> confirm=yes
```

Top-level quick aliases work for the most common session actions:

```text
/stop <channel>
/cancel <channel>
/retry-upload <channel>
/skip-upload <channel>
/skip-compression <channel>
/skip-forwarding <channel>
/clear-flags <channel>
```

Examples:

```text
/source add twitch examplechannel target=main
/source add youtube @examplechannel target=youtube_archive
/source pause twitch:examplechannel
/source resume youtube:@examplechannel
/source target twitch:examplechannel backup_archive
/source remove twitch:examplechannel

/target add main -1001111111111 discussion_id=-1002222222222 backup_id=-1003333333333
/target set main compression=false parallelism=2 speed=15
/target set main discussion_id=-1002222222222 backup_id=-1003333333333
/target rename main primary
/target remove main reassign=default
```

Supported `/target set` aliases:

| Alias | Runtime field |
|---|---|
| `channel_id` | `upload_channel_id` |
| `discussion_id` | `discussion_group_id` |
| `backup_id` | `backup_channel_id` |
| `compression` | `compression_enabled` |
| `parallelism` | `upload_parallelism` |
| `speed` | `upload_speed_limit_mbps` |
| `default_size` | `splitting_default_size_gb` |
| `premium_size` | `splitting_premium_size_gb` |

`compression`, `default_size`, `premium_size` and the optional channel ids accept `global`, `inherit`, `default`, `none`, or `null` to fall back to the static YAML defaults.

Runtime source changes are applied to active monitors without restarting the app.
Mutating control commands are written to `control.audit_log_file`.
Status, source, target, and config responses include inline refresh/navigation buttons when Telegram supports them.

#### Safe config import/export

`/config export` and `/config import*` only accept paths inside the runtime config directory (the parent directory of `control.runtime_config_file`). Absolute paths and paths with `..` segments outside that directory are rejected. This avoids accidentally writing exports into arbitrary system locations or importing files from unexpected paths.

`/config import` is a two-step operation:

1. Validate the snapshot first: `/config import-check my-export.json`. The runtime config is **not** changed by this step.
2. Apply it explicitly: `/config import my-export.json confirm=yes` (or `--confirm`). Without the confirmation token, `/config import` only validates the file and leaves runtime state untouched.

Before the imported snapshot is applied, the current runtime config is exported to `runtime_config.backup-YYYYmmdd_HHMMSS.json` next to the runtime config file, so the previous state can be re-imported manually if needed.

#### Active session controls

Active sessions persist their resolved Telegram target profile in state (`target_upload_channel_id`, `target_discussion_group_id`, `target_backup_channel_id`, `target_compression_enabled`, etc.). Runtime `/source target` or `/target set` changes therefore only affect future sessions, not in-flight ones, so a recording does not silently switch upload destinations mid-session.

`/session skip-*` and `/session cancel` commands set persistent control flags on the session (`skip_upload`, `skip_compression`, `skip_forwarding`, `abort_requested`). New sessions for the same channel start with cleared flags. `/session clear-flags` resets these flags so the next retry can run normally.

## Runtime Config Model

v3 keeps `config.yaml` as the bootstrap config and stores mutable operational settings in `data/runtime_config.sqlite3` by default. Existing JSON runtime config files are migrated to SQLite automatically and backed up as `*.migrated-*.bak`.

`config.yaml` is still used for:
- Telegram API credentials;
- Telethon session name;
- Twitch API credentials;
- startup platform mode;
- default recording/media/logging settings;
- control group bootstrap settings;
- state/runtime file paths.

Runtime config storage is used for:
- Telegram target profiles;
- Twitch/YouTube sources;
- source enabled/disabled state;
- source-to-target bindings;
- per-target Telegram destination settings;
- per-target compression flag, split limits, upload parallelism, and speed cap.

On first run, v3 creates runtime config from the static YAML:
- one `default` target from `telegram.channel_id`, `discussion_group_id`, and `backup_channel_id`;
- one source for every configured Twitch channel;
- one source for every configured YouTube channel.

## Telegram Targets

A target profile describes where a source uploads.

Each target can have:
- upload channel ID;
- discussion group ID for compressed/comment fallback;
- backup channel ID;
- upload parallelism;
- upload speed cap;
- compression enabled/disabled override;
- regular and Premium split size override.

This enables setups like:
- Twitch channel A uploads to Telegram channel A;
- YouTube channel B uploads to another Telegram channel;
- a high-priority source has compression disabled;
- an archive target forwards everything to a backup channel;
- a slow target uses lower upload speed.

## Architecture

| Layer | Module(s) | Responsibility |
|---|---|---|
| Orchestration | `src/streamrecorder/main.py` | Coordinates monitor, recorder, processing, upload, recovery, runtime control |
| Jobs | `job_manager.py` | Queue and concurrency limits for recording, processing, upload, compression, and control actions |
| Runtime Config | `runtime_config.py` | Stores mutable sources and Telegram target profiles |
| Telegram Control | `telegram_controller.py` | Parses control-group commands and applies runtime changes |
| Monitoring | `platform_monitor.py`, `stream_monitor.py`, `youtube_monitor.py`, `twitch_api.py` | Detects stream events and supports runtime source updates |
| Capture | `recorder.py` | Runs `yt-dlp`, handles retries and interruption recovery |
| Media Processing | `splitter.py`, `compressor.py` | Splits oversized files and optionally compresses videos |
| Delivery | `uploader.py` | Telegram upload, albums, comments, status messages, forwarding |
| Recovery State | `state_manager.py` | Stores active sessions and recovery data in SQLite keyed by `source_id + session_id` |
| Logging | `logger.py` | Console and rotating file logs |

## Runtime Flow

1. Static config is loaded from `config.yaml`.
2. Runtime config is loaded or initialized from static config.
3. Runtime sources are applied to Twitch/YouTube monitors.
4. Telegram connects and optionally starts the control-group listener.
5. Monitor detects WAITING metadata changes, live starts, and offline transitions.
6. Recorder captures the stream with `yt-dlp`.
7. State is persisted throughout the session for recovery.
8. Recording files are validated, remuxed/concatenated when needed, and split for Telegram limits.
9. Upload target is resolved from the source's runtime target profile.
10. Files are uploaded, compressed copy is optionally sent to comments, and messages are optionally forwarded to backup.
11. Completed session is archived to history and temporary files are cleaned.

## Modes

| Mode | Behavior |
|---|---|
| `twitch` | Monitor Twitch sources only |
| `youtube` | Monitor YouTube sources only |
| `both` | Monitor Twitch and YouTube in parallel |
| `cross` | Start from YouTube and attach Twitch continuation when stream moves platform |

`cross` mode still uses YouTube as the primary monitored source and attaches Twitch continuation files to the YouTube session.

## Quick Start

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp config.yaml.example config.yaml
python3 main.py
```

On first run Telethon will authorize and create `<session_name>.session`.

## Requirements

System tools in `PATH`:
- `ffmpeg`
- `ffprobe`
- `yt-dlp`

Python dependencies:
- `telethon`
- `cryptg` recommended for Telethon crypto acceleration
- `aiohttp`
- `aiofiles`
- `pyyaml`
- `yt-dlp`

## Configuration Guide

Use `config.yaml.example` as the template.

Most important static keys:
- `platform`: `twitch`, `youtube`, `both`, or `cross`;
- `telegram.api_id`, `telegram.api_hash`, `telegram.channel_id`;
- `telegram.session_name`;
- `twitch.client_id`, `twitch.client_secret` for Twitch modes;
- `recording.cookies_file` for YouTube/cookie-protected access;
- `recording.phantom_filter.*` for Twitch duplicate-tail suppression;
- `state.state_file` and `control.runtime_config_file`;
- `control.enabled`, `control.group_id`, `control.allowed_user_ids`.

### Cookies

Set `recording.cookies_file` in `config.yaml`, for example:

```yaml
recording:
  cookies_file: ./cookies.txt
```

Notes:
- file format should be Netscape cookies format;
- use YouTube cookies for YouTube monitoring/recording;
- use Twitch cookies if your Twitch recording setup needs them;
- a combined cookies file can be used for `both` or `cross` mode;
- set it to `""` to disable cookies.

### Upload Speed and FastTelethonhelper

If an upload speed cap is active, FastTelethonhelper is disabled for those uploads because throttling requires Telethon progress callbacks.

Choose one per target:
- maximum speed with FastTelethonhelper and `speed=0`;
- strict speed cap with normal Telethon upload.

v3 serializes upload pipelines per target so one target's speed cap and album parallelism are applied consistently while that target is uploading. File uploads inside an album still use the target's `parallelism` setting.

## Recovery Model

Recovery runs automatically on startup and resumes incomplete work:
- interrupted recordings;
- recovered temp files/fragments;
- split/upload continuation;
- compression continuation;
- compressed comment sending;
- backup forwarding.

Active state is stored in `data/state.sqlite3` and keyed by `source_id + session_id`. Completed sessions are moved to history. Upload idempotency is handled by storing `uploaded_parts` as file path to Telegram message ID. Existing `data/state.json` files are migrated automatically and backed up as `*.migrated-*.bak`.

Statuses used by recovery:

```text
offline, waiting, recording, processing, uploading, compressing,
sending_compressed, forwarding, done, error
```

## Repository Structure

```text
README.md
LICENSE
main.py
config.yaml.example
requirements.txt
src/streamrecorder/
tests/unit/
```

Core package:

```text
src/streamrecorder/
  main.py
  config.py
  runtime_config.py
  telegram_controller.py
  state_manager.py
  platform_monitor.py
  stream_monitor.py
  youtube_monitor.py
  twitch_api.py
  recorder.py
  splitter.py
  compressor.py
  uploader.py
  logger.py
```

Runtime folders, created while running:
- `data/`
- `recordings/`
- `temp/`
- `logs/`

## Testing

Unit tests live in `tests/unit/` and use the standard library `unittest` runner; no external test framework is required.

```bash
# Compile-check all source and test modules.
python3 -m compileall -q main.py src tests

# Run the full unit-test suite.
PYTHONPATH=src python3 -m unittest discover -s tests/unit

# Or run a focused subset.
PYTHONPATH=src python3 -m unittest \
    tests.unit.test_config \
    tests.unit.test_runtime_config \
    tests.unit.test_telegram_controller \
    tests.unit.test_job_manager \
    tests.unit.test_state_and_resume \
    tests.unit.test_cross_mode \
    tests.unit.test_recorder_splitter
```

Some media-heavy tests may use `ffmpeg`; run focused tests if you only need config/runtime/control validation.

## Troubleshooting

Uploads do not continue after restart:
- inspect `data/state.sqlite3`;
- inspect `logs/recorder.log`;
- check that split files referenced by state still exist.

New Telegram command works but the channel is not monitored:
- verify `control.enabled` and `control.group_id`;
- verify the command was sent by an allowed user;
- check `/sources` and confirm the source is `on`;
- ensure `platform` includes that source type (`twitch`, `youtube`, `both`, or `cross`).

Messages go to the wrong Telegram channel:
- check `/targets`;
- check `/sources` for the source-to-target binding;
- update with `/source target <source_id> <target_id>`.

Still seeing Telegram flood waits:
- lower `/target set <target_id> parallelism=1`;
- set `/target set <target_id> speed=<mbps>`;
- disable FastTelethonhelper by using a non-zero speed cap.

Phantom duplicate tails still appear:
- tighten `recording.phantom_filter.min_duration_sec`;
- raise `recording.phantom_filter.duplicate_hash_mb`;
- keep `recording.phantom_filter.check_twitch_api = true`.

## Current Limitations

- There is no pinned live dashboard message yet.

## Roadmap

Open roadmap items were moved into Current Limitations and the configuration/control sections as implemented or remaining documented constraints.

## Current UX Note

Most runtime Telegram messages and captions are currently Russian.

Text templates live mainly in:
- `src/streamrecorder/main.py`
- `src/streamrecorder/uploader.py`
- `src/streamrecorder/telegram_controller.py`
