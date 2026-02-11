"""
Configuration module for Twitch Stream Recorder.
Loads settings from YAML file and provides typed configuration.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import yaml


@dataclass
class TelegramConfig:
    """Telegram API configuration."""
    api_id: int
    api_hash: str
    channel_id: int
    backup_channel_id: Optional[int] = None  # Backup channel for forwards
    discussion_group_id: Optional[int] = None  # Discussion group for comments
    session_name: str = "stream_recorder"
    use_fast_upload_helper: bool = True  # Use FastTelethonhelper when available
    upload_parallelism: int = 1  # Parallel uploads inside one Telegram album
    upload_speed_limit_mbps: float = 0.0  # Upload limit in megabits/s, 0 = unlimited


@dataclass
class TwitchConfig:
    """Twitch API and monitoring configuration."""
    client_id: str = ""           # Twitch app Client ID
    client_secret: str = ""       # Twitch app Client Secret
    oauth_token: str = ""         # Optional OAuth token
    channels: List[str] = field(default_factory=list)
    check_interval: int = 5       # seconds between API checks
    offline_grace_period: int = 180  # seconds to wait before considering stream ended
    cross_check_delay: int = 10   # seconds to wait after YT ends before checking Twitch (cross mode)


@dataclass
class YouTubeConfig:
    """YouTube monitoring configuration."""
    channels: List[str] = field(default_factory=list)  # @handle or UCxxxxxxxx
    check_interval: int = 30      # seconds between checks (YouTube needs longer)


@dataclass
class PhantomFilterConfig:
    """Filter for phantom short segments after stream ends."""
    enabled: bool = True
    min_duration_sec: int = 60
    min_size_mb: float = 50.0
    duplicate_hash_check: bool = True  # Compare new segment head vs previous segment tail
    duplicate_hash_mb: int = 8  # MB to compare for duplicate detection
    check_twitch_api: bool = True  # If True, skip only when API says offline
    only_on_continuation: bool = True  # Apply only after first segment


@dataclass
class RecordingConfig:
    """Recording settings."""
    output_dir: str = "./recordings"
    temp_dir: str = "./temp"
    format: str = "best"
    retries: int = 10
    fragment_retries: int = 10
    live_from_start: bool = False
    cookies_file: str = ""  # Path to cookies file, empty to disable
    move_atom_to_front: bool = True  # Move moov atom for fast start
    use_mpegts: bool = False  # Use MPEG-TS container for recording
    phantom_filter: PhantomFilterConfig = field(default_factory=PhantomFilterConfig)


@dataclass
class SplittingConfig:
    """File splitting settings."""
    # Size will be determined dynamically based on Telegram Premium status
    # 4 GB for Premium, 2 GB for regular accounts
    default_size_gb: float = 2.0
    premium_size_gb: float = 4.0


@dataclass
class CompressionConfig:
    """Video compression settings."""
    enabled: bool = True
    # Target size in MB, will be adjusted based on Premium status
    # 4000 MB (4 GB) for Premium, 2000 MB (2 GB) for regular
    default_target_size_mb: int = 2000
    premium_target_size_mb: int = 4000
    audio_bitrate_kbps: int = 128
    two_pass: bool = True


@dataclass
class LoggingConfig:
    """Logging settings."""
    level: str = "INFO"
    file: str = "./logs/recorder.log"
    max_size_mb: int = 10
    backup_count: int = 5
    verbose_monitoring: bool = False  # If True, log every stream check
    ytdlp_log_noise: bool = False  # If True, include noisy yt-dlp lines (connection/open/progress)


@dataclass
class StateConfig:
    """State persistence settings."""
    state_file: str = "./data/state.json"
    waiting_timeout_hours: float = 6.0  # Drop stale waiting message/state after timeout


@dataclass
class Config:
    """Main configuration container."""
    telegram: TelegramConfig
    platform: str = "twitch"  # "twitch", "youtube", or "both"
    twitch: TwitchConfig = field(default_factory=TwitchConfig)
    youtube: YouTubeConfig = field(default_factory=YouTubeConfig)
    recording: RecordingConfig = field(default_factory=RecordingConfig)
    splitting: SplittingConfig = field(default_factory=SplittingConfig)
    compression: CompressionConfig = field(default_factory=CompressionConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    state: StateConfig = field(default_factory=StateConfig)
    
    def __post_init__(self):
        """Ensure directories exist."""
        Path(self.recording.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.recording.temp_dir).mkdir(parents=True, exist_ok=True)
        Path(self.logging.file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.state.state_file).parent.mkdir(parents=True, exist_ok=True)


def load_config(config_path: str = "config.yaml") -> Config:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to YAML configuration file.
        
    Returns:
        Config object with all settings.
        
    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If required fields are missing.
    """
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Please create a config.yaml file. See config.example.yaml for reference."
        )
    
    with open(config_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    
    if not data:
        raise ValueError("Configuration file is empty")
    
    # Validate required fields
    if 'telegram' not in data:
        raise ValueError("Missing 'telegram' section in config")
    
    telegram_data = data['telegram']
    required_telegram = ['api_id', 'api_hash', 'channel_id']
    for field_name in required_telegram:
        if field_name not in telegram_data:
            raise ValueError(f"Missing required field: telegram.{field_name}")

    def as_bool(value: Any, default: bool) -> bool:
        """Parse bool from YAML value with safe fallbacks."""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in ("1", "true", "yes", "y", "on"):
                return True
            if text in ("0", "false", "no", "n", "off"):
                return False
        return default

    def as_float(value: Any, default: float) -> float:
        """Parse float from YAML value with safe fallbacks."""
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip().replace(",", "."))
            except ValueError:
                return default
        return default

    def as_int(value: Any, default: int) -> int:
        """Parse int from YAML value with safe fallbacks."""
        if value is None:
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return default
            try:
                return int(float(text.replace(",", ".")))
            except ValueError:
                return default
        return default
    
    # Build configuration objects
    telegram_config = TelegramConfig(
        api_id=int(telegram_data['api_id']),
        api_hash=str(telegram_data['api_hash']),
        channel_id=int(telegram_data['channel_id']),
        backup_channel_id=int(telegram_data['backup_channel_id']) if telegram_data.get('backup_channel_id') else None,
        discussion_group_id=int(telegram_data['discussion_group_id']) if telegram_data.get('discussion_group_id') else None,
        session_name=telegram_data.get('session_name', 'stream_recorder'),
        use_fast_upload_helper=as_bool(telegram_data.get('use_fast_upload_helper', True), True),
        upload_parallelism=max(1, as_int(telegram_data.get('upload_parallelism', 1), 1)),
        upload_speed_limit_mbps=max(0.0, as_float(telegram_data.get('upload_speed_limit_mbps', 0.0), 0.0)),
    )
    
    twitch_data = data.get('twitch', {})
    twitch_config = TwitchConfig(
        client_id=twitch_data.get('client_id', ''),
        client_secret=twitch_data.get('client_secret', ''),
        oauth_token=twitch_data.get('oauth_token', ''),
        channels=twitch_data.get('channels', []),
        check_interval=twitch_data.get('check_interval', 5),
        offline_grace_period=twitch_data.get('offline_grace_period', 180),
        cross_check_delay=twitch_data.get('cross_check_delay', 10)
    )
    
    youtube_data = data.get('youtube', {})
    youtube_config = YouTubeConfig(
        channels=youtube_data.get('channels', []) or [],
        check_interval=youtube_data.get('check_interval', 30)
    )
    
    # Platform selection
    platform = data.get('platform', 'twitch')
    if platform not in ('twitch', 'youtube', 'both', 'cross'):
        platform = 'twitch'
    
    recording_data = data.get('recording', {})
    phantom_data = recording_data.get('phantom_filter', {})
    recording_config = RecordingConfig(
        output_dir=recording_data.get('output_dir', './recordings'),
        temp_dir=recording_data.get('temp_dir', './temp'),
        format=recording_data.get('format', 'best'),
        retries=recording_data.get('retries', 10),
        fragment_retries=recording_data.get('fragment_retries', 10),
        live_from_start=recording_data.get('live_from_start', False),
        cookies_file=recording_data.get('cookies_file', ''),
        move_atom_to_front=recording_data.get('move_atom_to_front', True),
        use_mpegts=recording_data.get('use_mpegts', False),
        phantom_filter=PhantomFilterConfig(
            enabled=phantom_data.get('enabled', True),
            min_duration_sec=phantom_data.get('min_duration_sec', 60),
            min_size_mb=phantom_data.get('min_size_mb', 50.0),
            duplicate_hash_check=phantom_data.get('duplicate_hash_check', True),
            duplicate_hash_mb=phantom_data.get('duplicate_hash_mb', 8),
            check_twitch_api=phantom_data.get('check_twitch_api', True),
            only_on_continuation=phantom_data.get('only_on_continuation', True),
        )
    )
    
    splitting_data = data.get('splitting', {})
    splitting_config = SplittingConfig(
        default_size_gb=splitting_data.get('default_size_gb', 2.0),
        premium_size_gb=splitting_data.get('premium_size_gb', 4.0)
    )
    
    compression_data = data.get('compression', {})
    compression_config = CompressionConfig(
        enabled=compression_data.get('enabled', True),
        default_target_size_mb=compression_data.get('default_target_size_mb', 2000),
        premium_target_size_mb=compression_data.get('premium_target_size_mb', 4000),
        audio_bitrate_kbps=compression_data.get('audio_bitrate_kbps', 128),
        two_pass=compression_data.get('two_pass', True)
    )
    
    logging_data = data.get('logging', {})
    logging_config = LoggingConfig(
        level=logging_data.get('level', 'INFO'),
        file=logging_data.get('file', './logs/recorder.log'),
        max_size_mb=logging_data.get('max_size_mb', 10),
        backup_count=logging_data.get('backup_count', 5),
        verbose_monitoring=logging_data.get('verbose_monitoring', False),
        ytdlp_log_noise=logging_data.get('ytdlp_log_noise', False)
    )
    
    state_data = data.get('state', {})
    state_config = StateConfig(
        state_file=state_data.get('state_file', './data/state.json'),
        waiting_timeout_hours=float(state_data.get('waiting_timeout_hours', 6.0))
    )
    
    return Config(
        telegram=telegram_config,
        platform=platform,
        twitch=twitch_config,
        youtube=youtube_config,
        recording=recording_config,
        splitting=splitting_config,
        compression=compression_config,
        logging=logging_config,
        state=state_config
    )


def create_example_config(path: str = "config.example.yaml") -> None:
    """Create an example configuration file."""
    example = """# Twitch Stream Recorder Configuration

telegram:
  api_id: YOUR_API_ID  # Get from https://my.telegram.org
  api_hash: YOUR_API_HASH
  channel_id: -1001234567890  # Channel ID to upload recordings
  discussion_group_id: -1001234567890  # Optional, for comments/discussion
  session_name: stream_recorder
  use_fast_upload_helper: true  # Use FastTelethonhelper when available
  upload_parallelism: 1  # Parallel uploads in one album (1..10)
  upload_speed_limit_mbps: 0  # 0 = unlimited; cap is shared across all parallel uploads

twitch:
  channels:
    - channel1
    - channel2
  check_interval: 60  # Seconds between stream status checks

recording:
  output_dir: ./recordings
  temp_dir: ./temp
  format: best  # yt-dlp format string
  retries: 10
  fragment_retries: 10
  live_from_start: true  # Try to record from stream start
  phantom_filter:
    enabled: true
    min_duration_sec: 60
    min_size_mb: 50
    duplicate_hash_check: true
    duplicate_hash_mb: 8
    check_twitch_api: true
    only_on_continuation: true

splitting:
  # File size limit based on Telegram account type
  default_size_gb: 2.0   # Regular account limit
  premium_size_gb: 4.0   # Premium account limit

compression:
  enabled: true
  default_target_size_mb: 2000   # 2 GB for regular accounts
  premium_target_size_mb: 4000   # 4 GB for Premium accounts
  audio_bitrate_kbps: 128
  two_pass: true  # Better quality, slower

logging:
  level: INFO  # DEBUG, INFO, WARNING, ERROR
  file: ./logs/recorder.log
  max_size_mb: 10
  backup_count: 5
  ytdlp_log_noise: false  # Show noisy yt-dlp lines (connection/open/progress)

state:
  state_file: ./data/state.json
  waiting_timeout_hours: 6
"""
    with open(path, 'w', encoding='utf-8') as f:
        f.write(example)


if __name__ == '__main__':
    # Create example config if run directly
    create_example_config()
    print("Created config.example.yaml")
