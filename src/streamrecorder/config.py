"""
Configuration module for Twitch Stream Recorder.
Loads settings from YAML file and provides typed configuration.
"""

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
    state_file: str = "./data/state.sqlite3"
    waiting_timeout_hours: float = 6.0  # Drop stale waiting message/state after timeout


@dataclass
class ControlConfig:
    """Telegram control-group settings for runtime management."""
    enabled: bool = False
    group_id: Optional[int] = None
    allowed_user_ids: List[int] = field(default_factory=list)
    readonly_user_ids: List[int] = field(default_factory=list)
    allow_all_group_members: bool = False
    command_prefix: str = "/"
    runtime_config_file: str = "./data/runtime_config.sqlite3"
    audit_log_file: str = "./logs/control_audit.log"


@dataclass
class JobsConfig:
    """In-process job queue concurrency limits."""
    recording_concurrency: int = 4
    processing_concurrency: int = 2
    upload_concurrency: int = 1
    compression_concurrency: int = 1
    control_concurrency: int = 2


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
    control: ControlConfig = field(default_factory=ControlConfig)
    jobs: JobsConfig = field(default_factory=JobsConfig)
    
    def __post_init__(self):
        """Ensure directories exist."""
        Path(self.recording.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.recording.temp_dir).mkdir(parents=True, exist_ok=True)
        Path(self.logging.file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.state.state_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.control.runtime_config_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.control.audit_log_file).parent.mkdir(parents=True, exist_ok=True)


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
    if not isinstance(telegram_data, dict):
        raise ValueError("'telegram' section must be a mapping")
    required_telegram = ['api_id', 'api_hash', 'channel_id']
    for field_name in required_telegram:
        if field_name not in telegram_data:
            raise ValueError(f"Missing required field: telegram.{field_name}")

    def require_mapping(value: Any, field_name: str) -> dict:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError(f"'{field_name}' section must be a mapping")
        return value

    def as_bool(value: Any, default: bool, field_name: str) -> bool:
        """Parse bool from YAML value."""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in ("1", "true", "yes", "y", "on"):
                return True
            if text in ("0", "false", "no", "n", "off"):
                return False
        raise ValueError(f"Invalid boolean value for {field_name}: {value!r}")

    def as_float(value: Any, default: float, field_name: str, *, min_value: Optional[float] = None) -> float:
        """Parse float from YAML value."""
        if value is None:
            result = default
        elif isinstance(value, bool):
            raise ValueError(f"Invalid numeric value for {field_name}: {value!r}")
        elif isinstance(value, (int, float)):
            result = float(value)
        elif isinstance(value, str):
            try:
                result = float(value.strip().replace(",", "."))
            except ValueError:
                raise ValueError(f"Invalid numeric value for {field_name}: {value!r}") from None
        else:
            raise ValueError(f"Invalid numeric value for {field_name}: {value!r}")
        if min_value is not None and result < min_value:
            raise ValueError(f"{field_name} must be >= {min_value}")
        return result

    def as_int(value: Any, default: int, field_name: str, *, min_value: Optional[int] = None) -> int:
        """Parse int from YAML value."""
        if value is None:
            result = default
        elif isinstance(value, bool):
            raise ValueError(f"Invalid integer value for {field_name}: {value!r}")
        elif isinstance(value, int):
            result = value
        elif isinstance(value, float):
            if not value.is_integer():
                raise ValueError(f"Invalid integer value for {field_name}: {value!r}")
            result = int(value)
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                result = default
            else:
                try:
                    parsed = float(text.replace(",", "."))
                except ValueError:
                    raise ValueError(f"Invalid integer value for {field_name}: {value!r}") from None
                if not parsed.is_integer():
                    raise ValueError(f"Invalid integer value for {field_name}: {value!r}")
                result = int(parsed)
        else:
            raise ValueError(f"Invalid integer value for {field_name}: {value!r}")
        if min_value is not None and result < min_value:
            raise ValueError(f"{field_name} must be >= {min_value}")
        return result

    def as_str(value: Any, default: str, field_name: str) -> str:
        if value is None:
            return default
        if isinstance(value, str):
            return value
        raise ValueError(f"{field_name} must be a string")

    def as_str_list(value: Any, default: Optional[List[str]], field_name: str) -> List[str]:
        if value is None:
            return list(default or [])
        if not isinstance(value, list):
            raise ValueError(f"{field_name} must be a list of strings")
        result = []
        for item in value:
            if not isinstance(item, str):
                raise ValueError(f"{field_name} must contain only strings")
            item = item.strip()
            if item:
                result.append(item)
        return result

    def optional_int(value: Any, field_name: str) -> Optional[int]:
        if value in (None, ""):
            return None
        return as_int(value, 0, field_name)

    def as_int_list(value: Any, default: Optional[List[int]], field_name: str) -> List[int]:
        if value is None:
            return list(default or [])
        if not isinstance(value, list):
            raise ValueError(f"{field_name} must be a list of integers")
        return [as_int(item, 0, field_name) for item in value]

    def positive_float(value: Any, default: float, field_name: str) -> float:
        result = as_float(value, default, field_name, min_value=0.0)
        if result <= 0:
            raise ValueError(f"{field_name} must be > 0")
        return result

    def positive_int(value: Any, default: int, field_name: str) -> int:
        result = as_int(value, default, field_name, min_value=0)
        if result <= 0:
            raise ValueError(f"{field_name} must be > 0")
        return result

    telegram_data = require_mapping(data['telegram'], 'telegram')
    for field_name in required_telegram:
        if field_name not in telegram_data:
            raise ValueError(f"Missing required field: telegram.{field_name}")
    try:
        api_id = as_int(telegram_data['api_id'], 0, 'telegram.api_id')
        channel_id = as_int(telegram_data['channel_id'], 0, 'telegram.channel_id')
    except ValueError:
        raise
    api_hash = as_str(telegram_data['api_hash'], '', 'telegram.api_hash')
    if not api_hash:
        raise ValueError("telegram.api_hash must not be empty")
    
    # Build configuration objects
    telegram_config = TelegramConfig(
        api_id=api_id,
        api_hash=api_hash,
        channel_id=channel_id,
        backup_channel_id=optional_int(telegram_data.get('backup_channel_id'), 'telegram.backup_channel_id'),
        discussion_group_id=optional_int(telegram_data.get('discussion_group_id'), 'telegram.discussion_group_id'),
        session_name=as_str(telegram_data.get('session_name'), 'stream_recorder', 'telegram.session_name'),
        use_fast_upload_helper=as_bool(telegram_data.get('use_fast_upload_helper'), True, 'telegram.use_fast_upload_helper'),
        upload_parallelism=positive_int(telegram_data.get('upload_parallelism'), 1, 'telegram.upload_parallelism'),
        upload_speed_limit_mbps=as_float(telegram_data.get('upload_speed_limit_mbps'), 0.0, 'telegram.upload_speed_limit_mbps', min_value=0.0),
    )
    
    twitch_data = require_mapping(data.get('twitch', {}), 'twitch')
    twitch_config = TwitchConfig(
        client_id=as_str(twitch_data.get('client_id'), '', 'twitch.client_id'),
        client_secret=as_str(twitch_data.get('client_secret'), '', 'twitch.client_secret'),
        oauth_token=as_str(twitch_data.get('oauth_token'), '', 'twitch.oauth_token'),
        channels=as_str_list(twitch_data.get('channels'), [], 'twitch.channels'),
        check_interval=positive_int(twitch_data.get('check_interval'), 5, 'twitch.check_interval'),
        offline_grace_period=as_int(twitch_data.get('offline_grace_period'), 180, 'twitch.offline_grace_period', min_value=0),
        cross_check_delay=as_int(twitch_data.get('cross_check_delay'), 10, 'twitch.cross_check_delay', min_value=0)
    )
    
    youtube_data = require_mapping(data.get('youtube', {}), 'youtube')
    youtube_config = YouTubeConfig(
        channels=as_str_list(youtube_data.get('channels'), [], 'youtube.channels'),
        check_interval=positive_int(youtube_data.get('check_interval'), 30, 'youtube.check_interval')
    )
    
    # Platform selection
    platform = as_str(data.get('platform'), 'twitch', 'platform')
    if platform not in ('twitch', 'youtube', 'both', 'cross'):
        raise ValueError("platform must be one of: twitch, youtube, both, cross")
    
    recording_data = require_mapping(data.get('recording', {}), 'recording')
    phantom_data = require_mapping(recording_data.get('phantom_filter', {}), 'recording.phantom_filter')
    recording_config = RecordingConfig(
        output_dir=as_str(recording_data.get('output_dir'), './recordings', 'recording.output_dir'),
        temp_dir=as_str(recording_data.get('temp_dir'), './temp', 'recording.temp_dir'),
        format=as_str(recording_data.get('format'), 'best', 'recording.format'),
        retries=as_int(recording_data.get('retries'), 10, 'recording.retries', min_value=0),
        fragment_retries=as_int(recording_data.get('fragment_retries'), 10, 'recording.fragment_retries', min_value=0),
        live_from_start=as_bool(recording_data.get('live_from_start'), False, 'recording.live_from_start'),
        cookies_file=as_str(recording_data.get('cookies_file'), '', 'recording.cookies_file'),
        move_atom_to_front=as_bool(recording_data.get('move_atom_to_front'), True, 'recording.move_atom_to_front'),
        use_mpegts=as_bool(recording_data.get('use_mpegts'), False, 'recording.use_mpegts'),
        phantom_filter=PhantomFilterConfig(
            enabled=as_bool(phantom_data.get('enabled'), True, 'recording.phantom_filter.enabled'),
            min_duration_sec=as_int(phantom_data.get('min_duration_sec'), 60, 'recording.phantom_filter.min_duration_sec', min_value=0),
            min_size_mb=as_float(phantom_data.get('min_size_mb'), 50.0, 'recording.phantom_filter.min_size_mb', min_value=0.0),
            duplicate_hash_check=as_bool(phantom_data.get('duplicate_hash_check'), True, 'recording.phantom_filter.duplicate_hash_check'),
            duplicate_hash_mb=positive_int(phantom_data.get('duplicate_hash_mb'), 8, 'recording.phantom_filter.duplicate_hash_mb'),
            check_twitch_api=as_bool(phantom_data.get('check_twitch_api'), True, 'recording.phantom_filter.check_twitch_api'),
            only_on_continuation=as_bool(phantom_data.get('only_on_continuation'), True, 'recording.phantom_filter.only_on_continuation'),
        )
    )
    
    splitting_data = require_mapping(data.get('splitting', {}), 'splitting')
    splitting_config = SplittingConfig(
        default_size_gb=positive_float(splitting_data.get('default_size_gb'), 2.0, 'splitting.default_size_gb'),
        premium_size_gb=positive_float(splitting_data.get('premium_size_gb'), 4.0, 'splitting.premium_size_gb')
    )
    
    compression_data = require_mapping(data.get('compression', {}), 'compression')
    compression_config = CompressionConfig(
        enabled=as_bool(compression_data.get('enabled'), True, 'compression.enabled'),
        default_target_size_mb=positive_int(compression_data.get('default_target_size_mb'), 2000, 'compression.default_target_size_mb'),
        premium_target_size_mb=positive_int(compression_data.get('premium_target_size_mb'), 4000, 'compression.premium_target_size_mb'),
        audio_bitrate_kbps=positive_int(compression_data.get('audio_bitrate_kbps'), 128, 'compression.audio_bitrate_kbps'),
        two_pass=as_bool(compression_data.get('two_pass'), True, 'compression.two_pass')
    )
    
    logging_data = require_mapping(data.get('logging', {}), 'logging')
    logging_config = LoggingConfig(
        level=as_str(logging_data.get('level'), 'INFO', 'logging.level'),
        file=as_str(logging_data.get('file'), './logs/recorder.log', 'logging.file'),
        max_size_mb=positive_int(logging_data.get('max_size_mb'), 10, 'logging.max_size_mb'),
        backup_count=as_int(logging_data.get('backup_count'), 5, 'logging.backup_count', min_value=0),
        verbose_monitoring=as_bool(logging_data.get('verbose_monitoring'), False, 'logging.verbose_monitoring'),
        ytdlp_log_noise=as_bool(logging_data.get('ytdlp_log_noise'), False, 'logging.ytdlp_log_noise')
    )
    
    state_data = require_mapping(data.get('state', {}), 'state')
    state_config = StateConfig(
        state_file=as_str(state_data.get('state_file'), './data/state.sqlite3', 'state.state_file'),
        waiting_timeout_hours=as_float(state_data.get('waiting_timeout_hours'), 6.0, 'state.waiting_timeout_hours', min_value=0.0)
    )

    control_data = require_mapping(data.get('control', {}), 'control')
    control_config = ControlConfig(
        enabled=as_bool(control_data.get('enabled'), False, 'control.enabled'),
        group_id=optional_int(control_data.get('group_id'), 'control.group_id'),
        allowed_user_ids=as_int_list(control_data.get('allowed_user_ids'), [], 'control.allowed_user_ids'),
        readonly_user_ids=as_int_list(control_data.get('readonly_user_ids'), [], 'control.readonly_user_ids'),
        allow_all_group_members=as_bool(
            control_data.get('allow_all_group_members'),
            False,
            'control.allow_all_group_members'
        ),
        command_prefix=as_str(control_data.get('command_prefix'), '/', 'control.command_prefix'),
        runtime_config_file=as_str(
            control_data.get('runtime_config_file'),
            './data/runtime_config.sqlite3',
            'control.runtime_config_file'
        ),
        audit_log_file=as_str(
            control_data.get('audit_log_file'),
            './logs/control_audit.log',
            'control.audit_log_file'
        )
    )
    if control_config.enabled and control_config.group_id is None:
        raise ValueError("control.group_id is required when control.enabled is true")
    if (
        control_config.enabled
        and not control_config.allowed_user_ids
        and not control_config.readonly_user_ids
        and not control_config.allow_all_group_members
    ):
        raise ValueError("control.allowed_user_ids is required unless control.allow_all_group_members is true")

    jobs_data = require_mapping(data.get('jobs', {}), 'jobs')
    jobs_config = JobsConfig(
        recording_concurrency=positive_int(jobs_data.get('recording_concurrency'), 4, 'jobs.recording_concurrency'),
        processing_concurrency=positive_int(jobs_data.get('processing_concurrency'), 2, 'jobs.processing_concurrency'),
        upload_concurrency=positive_int(jobs_data.get('upload_concurrency'), 1, 'jobs.upload_concurrency'),
        compression_concurrency=positive_int(jobs_data.get('compression_concurrency'), 1, 'jobs.compression_concurrency'),
        control_concurrency=positive_int(jobs_data.get('control_concurrency'), 2, 'jobs.control_concurrency'),
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
        state=state_config,
        control=control_config,
        jobs=jobs_config
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
  state_file: ./data/state.sqlite3
  waiting_timeout_hours: 6

jobs:
  recording_concurrency: 4
  processing_concurrency: 2
  upload_concurrency: 1
  compression_concurrency: 1
  control_concurrency: 2

control:
  enabled: false
  group_id: null
  allowed_user_ids: []
  readonly_user_ids: []
  command_prefix: "/"
  runtime_config_file: ./data/runtime_config.sqlite3
  audit_log_file: ./logs/control_audit.log
"""
    with open(path, 'w', encoding='utf-8') as f:
        f.write(example)


if __name__ == '__main__':
    # Create example config if run directly
    create_example_config()
    print("Created config.example.yaml")
