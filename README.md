# Stream Recorder v2

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Telethon-26A5E4?style=flat-square&logo=telegram&logoColor=white)](https://github.com/LonamiWebs/Telethon)
[![yt-dlp](https://img.shields.io/badge/Recorder-yt--dlp-ff0000?style=flat-square)](https://github.com/yt-dlp/yt-dlp)
[![FFmpeg](https://img.shields.io/badge/Media-FFmpeg-007808?style=flat-square&logo=ffmpeg&logoColor=white)](https://ffmpeg.org/)
![Modes](https://img.shields.io/badge/Modes-Twitch%20%7C%20YouTube%20%7C%20Cross-6f42c1?style=flat-square)
![Status](https://img.shields.io/badge/Status-Active-2ea44f?style=flat-square)

Production-focused live stream recorder and Telegram publisher for Twitch and YouTube.

It is built for unattended runs where reliability matters more than manual control.

## TL;DR

This project monitors channels, records streams with `yt-dlp`, processes media, uploads to Telegram, and safely resumes after crashes/restarts using persistent state.

## What This Project Is

Stream Recorder v2 is an async pipeline with five major responsibilities:
- detect live/offline transitions
- capture streams into local files
- process recordings for Telegram limits
- deliver media and status updates to Telegram
- recover unfinished work automatically

It supports:
- Twitch
- YouTube
- dual monitoring
- cross-platform continuation mode (`cross`)

## Design Goals

- Reliability first: no data loss on restart
- State-driven behavior: each channel has explicit lifecycle status
- Safe automation: no duplicate uploads when retrying
- Practical operations: clear logs and recovery helpers
- Config-driven runtime: behavior controlled from `config.yaml`

## Architecture and Design

The project uses a layered design where each module has one job.

| Layer | Module(s) | Responsibility |
|---|---|---|
| Orchestration | `src/streamrecorder/main.py` | Coordinates monitor, recorder, processing, upload, recovery |
| Monitoring | `platform_monitor.py`, `stream_monitor.py`, `youtube_monitor.py`, `twitch_api.py` | Detects stream events and metadata changes |
| Capture | `recorder.py` | Runs `yt-dlp`, handles retries and interruption recovery |
| Media Processing | `splitter.py`, `compressor.py` | Splits oversized files, optional target-size compression |
| Delivery | `uploader.py` | Telegram upload, album batching, comment send, forwarding |
| Persistence | `state_manager.py` | Stores active sessions and recovery data in `data/state.json` |

### Runtime Flow

1. Monitor detects metadata change while offline -> WAITING message is created/updated.
2. Monitor detects live start -> recording starts and status message is posted.
3. When stream goes offline, app waits grace period to avoid false end on short drops.
4. Recording segments are validated and merged into one session context.
5. Output is split by Telegram size limits, then uploaded in order.
6. Optional compression/comment/backup forwarding is applied.
7. Session is marked done and moved to history.

### Key Design Choices

- State-first pipeline:
  - `data/state.json` is the source of truth for recovery and idempotency.
- Resume-safe uploads:
  - each uploaded file path is mapped to message ID to avoid duplicate sends.
- Per-channel processing guard:
  - prevents state corruption when a new live event appears during processing.
- Conservative phantom filter:
  - removes likely duplicate tail segments (short/small/hash-tail match), with optional Twitch API confirmation.
- Graceful shutdown behavior:
  - recorder stops cleanly and saves files, upload continuation happens on restart.

## Features

- Twitch Helix monitoring with offline grace logic
- YouTube monitoring through `yt-dlp` (`/live`, no API key)
- Modes: `twitch`, `youtube`, `both`, `cross`
- WAITING and status messages in Telegram
- Multi-segment session handling after stream reconnects
- Parallel Telegram uploads (`upload_parallelism`)
- Global upload speed cap (`upload_speed_limit_mbps`)
- Optional FastTelethonhelper acceleration
- Optional compression with 2-pass ffmpeg
- Optional comment posting in discussion thread
- Optional backup forwarding
- Automatic stale WAITING cleanup by timeout

## Modes

| Mode | Behavior |
|---|---|
| `twitch` | Monitor Twitch channels only |
| `youtube` | Monitor YouTube channels only |
| `both` | Monitor Twitch and YouTube in parallel |
| `cross` | Start from YouTube and attach Twitch continuation when stream moves |

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
- `cryptg` (recommended for Telethon crypto acceleration)
- `aiohttp`
- `aiofiles`
- `pyyaml`
- `yt-dlp`

## Configuration Guide

Use `config.yaml.example` as the template.

### Most Important Keys

- `platform`
  - `twitch` / `youtube` / `both` / `cross`
- `telegram.use_fast_upload_helper`
  - enable FastTelethonhelper if installed
- `telegram.upload_parallelism`
  - number of concurrent file uploads inside album preparation
- `telegram.upload_speed_limit_mbps`
  - global outbound cap (`0` = unlimited)
- `recording.phantom_filter.*`
  - phantom-tail suppression settings
- `state.waiting_timeout_hours`
  - auto-clean stale WAITING sessions/messages

### Cookies (`cookies.txt`)

Set `recording.cookies_file` in `config.yaml` (example: `./cookies.txt`).

- Twitch-only setup:
  - use Twitch cookies.
- YouTube-only setup:
  - use YouTube cookies.
- `both` / `cross` setup:
  - you can use one combined `cookies.txt` containing both Twitch and YouTube cookies.

Notes:
- file format should be Netscape cookies format (standard for `yt-dlp`);
- if cookies expire, re-export and replace `cookies.txt`;
- set `recording.cookies_file` to empty (`""`) to disable cookies.

### Upload Design Note

If `upload_speed_limit_mbps > 0`, FastTelethonhelper is disabled automatically.

Reason:
- throttling requires Telethon progress callbacks
- FastTelethonhelper bypasses those callbacks

So you can choose one:
- maximum speed (FastTelethonhelper)
- strict global speed cap (Telethon throttled mode)

## Recovery Model

Recovery runs automatically on startup and resumes incomplete operations:
- interrupted recording
- interrupted split/upload/compression/forwarding
- partially uploaded sessions

Statuses are persisted (`offline`, `waiting`, `recording`, `processing`, `uploading`, `compressing`, `sending_compressed`, `forwarding`, `done`, `error`) and drive restart logic.

## Repository Structure

```text
README.md
LICENSE
main.py
config.yaml.example
requirements.txt
src/streamrecorder/
```

Runtime folders (auto-created while running):
- `data/`
- `recordings/`
- `temp/`
- `logs/`

Core package:

```text
src/streamrecorder/
  main.py
  config.py
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

## Troubleshooting

Uploads do not continue after restart:
- check `data/state.json`
- inspect `logs/recorder.log`

Still seeing Telegram flood waits:
- reduce `telegram.upload_parallelism`
- set `telegram.upload_speed_limit_mbps` to a lower value

Phantom duplicate tails still appear:
- tighten `recording.phantom_filter.min_duration_sec`
- raise `recording.phantom_filter.duplicate_hash_mb`
- keep `recording.phantom_filter.check_twitch_api = true`

## Security and Hygiene

Do not commit private runtime files:
- `config.yaml`
- `*.session`
- cookie files
- private logs/state snapshots

The repo includes `config.yaml.example` for safe sharing.

## Current UX Note

Telegram status/caption text in runtime is mostly Russian.

Change text templates in:
- `src/streamrecorder/main.py`
- `src/streamrecorder/uploader.py`
