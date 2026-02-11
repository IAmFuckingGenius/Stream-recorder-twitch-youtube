"""
Logging module for Twitch Stream Recorder.
Provides structured logging with file rotation and colored console output.
"""

import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


# ANSI color codes for console output
class Colors:
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    GRAY = "\033[90m"


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output."""
    
    LEVEL_COLORS = {
        logging.DEBUG: Colors.GRAY,
        logging.INFO: Colors.GREEN,
        logging.WARNING: Colors.YELLOW,
        logging.ERROR: Colors.RED,
        logging.CRITICAL: Colors.MAGENTA,
    }
    
    def format(self, record: logging.LogRecord) -> str:
        # Add color to level name
        color = self.LEVEL_COLORS.get(record.levelno, Colors.RESET)
        
        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        
        # Format channel name if present
        channel = getattr(record, 'channel', None)
        channel_str = f"{Colors.CYAN}[{channel}]{Colors.RESET} " if channel else ""
        
        # Build message
        level_str = f"{color}{record.levelname:8}{Colors.RESET}"
        message = f"{Colors.GRAY}{timestamp}{Colors.RESET} {level_str} {channel_str}{record.getMessage()}"
        
        # Add exception info if present
        if record.exc_info:
            message += f"\n{self.formatException(record.exc_info)}"
        
        return message


class FileFormatter(logging.Formatter):
    """Plain formatter for file output."""
    
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        channel = getattr(record, 'channel', '-')
        
        message = f"{timestamp} | {record.levelname:8} | {channel:20} | {record.getMessage()}"
        
        if record.exc_info:
            message += f"\n{self.formatException(record.exc_info)}"
        
        return message


class ChannelLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds channel context to log messages."""
    
    def __init__(self, logger: logging.Logger, channel: str):
        super().__init__(logger, {'channel': channel})
    
    def process(self, msg, kwargs):
        kwargs.setdefault('extra', {})
        kwargs['extra']['channel'] = self.extra['channel']
        return msg, kwargs


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    max_size_mb: int = 10,
    backup_count: int = 5
) -> logging.Logger:
    """
    Set up the main application logger.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR).
        log_file: Path to log file. If None, logs only to console.
        max_size_mb: Maximum log file size before rotation.
        backup_count: Number of backup log files to keep.
        
    Returns:
        Configured logger instance.
    """
    # Create logger
    logger = logging.getLogger('stream_recorder')
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredFormatter())
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    
    # File handler with rotation
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(FileFormatter())
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get the application logger.
    
    Args:
        name: Optional name for child logger.
        
    Returns:
        Logger instance.
    """
    if name:
        return logging.getLogger(f'stream_recorder.{name}')
    return logging.getLogger('stream_recorder')


def get_channel_logger(channel: str) -> ChannelLoggerAdapter:
    """
    Get a logger adapter for a specific channel.
    
    Args:
        channel: Twitch channel name.
        
    Returns:
        ChannelLoggerAdapter with channel context.
    """
    logger = get_logger()
    return ChannelLoggerAdapter(logger, channel)


# Logging decorators for common operations
def log_operation(operation_name: str):
    """Decorator to log operation start and completion."""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            logger = get_logger()
            logger.info(f"Starting: {operation_name}")
            try:
                result = await func(*args, **kwargs)
                logger.info(f"Completed: {operation_name}")
                return result
            except Exception as e:
                logger.error(f"Failed: {operation_name} - {e}")
                raise
        
        def sync_wrapper(*args, **kwargs):
            logger = get_logger()
            logger.info(f"Starting: {operation_name}")
            try:
                result = func(*args, **kwargs)
                logger.info(f"Completed: {operation_name}")
                return result
            except Exception as e:
                logger.error(f"Failed: {operation_name} - {e}")
                raise
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


if __name__ == '__main__':
    # Test logging
    logger = setup_logging(level="DEBUG", log_file="./logs/test.log")
    
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    
    # Test channel logger
    ch_logger = get_channel_logger("test_streamer")
    ch_logger.info("Stream started!")
    ch_logger.warning("Stream quality dropped")
