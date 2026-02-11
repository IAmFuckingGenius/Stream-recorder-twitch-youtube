"""
File splitter module for Twitch Stream Recorder.
Splits large video files using MP4Box for Telegram upload limits.
"""

import asyncio
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from .logger import get_logger


@dataclass
class SplitResult:
    """Result of a file splitting operation."""
    input_file: str
    output_files: List[str]
    total_size_bytes: int
    part_count: int
    success: bool
    error: Optional[str] = None


class FileSplitter:
    """
    Splits video files using MP4Box.
    
    MP4Box is preferred because it splits without re-encoding,
    maintaining original quality and fast processing.
    """
    
    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize file splitter.
        
        Args:
            output_dir: Directory for split files. If None, uses same dir as input.
        """
        self.output_dir = Path(output_dir) if output_dir else None
        self._logger = get_logger('splitter')
    
    async def get_file_duration(self, file_path: str) -> float:
        """
        Get video duration in seconds using ffprobe.
        
        Args:
            file_path: Path to video file.
            
        Returns:
            Duration in seconds.
        """
        try:
            process = await asyncio.create_subprocess_exec(
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                file_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, _ = await process.communicate()
            duration = float(stdout.decode().strip())
            return duration
            
        except Exception as e:
            self._logger.error(f"Failed to get duration: {e}")
            return 0.0
    
    async def get_file_info(self, file_path: str) -> dict:
        """
        Get video file information.
        
        Args:
            file_path: Path to video file.
            
        Returns:
            Dictionary with file info (size, duration, bitrate).
        """
        path = Path(file_path)
        
        if not path.exists():
            return {'exists': False}
        
        size = path.stat().st_size
        duration = await self.get_file_duration(file_path)
        
        bitrate = 0
        if duration > 0:
            bitrate = int((size * 8) / duration)
        
        return {
            'exists': True,
            'size_bytes': size,
            'size_mb': size / (1024 * 1024),
            'size_gb': size / (1024 * 1024 * 1024),
            'duration_seconds': duration,
            'bitrate_bps': bitrate
        }
    
    def needs_splitting(self, file_path: str, max_size_gb: float) -> bool:
        """
        Check if file needs to be split.
        
        Args:
            file_path: Path to video file.
            max_size_gb: Maximum size per part in GB.
            
        Returns:
            True if file exceeds max size.
        """
        path = Path(file_path)
        if not path.exists():
            return False
        
        size_gb = path.stat().st_size / (1024 * 1024 * 1024)
        return size_gb > max_size_gb
    
    async def split_file(
        self,
        input_path: str,
        max_size_gb: float,
        output_prefix: Optional[str] = None
    ) -> SplitResult:
        """
        Split video file into parts of specified maximum size using ffmpeg.
        
        Uses time-based splitting for even part sizes.
        
        Args:
            input_path: Path to input video file.
            max_size_gb: Maximum size per part in GB.
            output_prefix: Optional prefix for output files.
            
        Returns:
            SplitResult with list of output files.
        """
        input_file = Path(input_path)
        
        if not input_file.exists():
            return SplitResult(
                input_file=input_path,
                output_files=[],
                total_size_bytes=0,
                part_count=0,
                success=False,
                error=f"File not found: {input_path}"
            )
        
        file_size = input_file.stat().st_size
        file_size_gb = file_size / (1024 * 1024 * 1024)
        
        # Check if splitting is needed
        if not self.needs_splitting(input_path, max_size_gb):
            self._logger.info(f"File doesn't need splitting (< {max_size_gb} GB)")
            return SplitResult(
                input_file=input_path,
                output_files=[input_path],
                total_size_bytes=file_size,
                part_count=1,
                success=True
            )
        
        # Get duration to calculate segment time
        duration = await self.get_file_duration(input_path)
        if duration <= 0:
            return SplitResult(
                input_file=input_path,
                output_files=[],
                total_size_bytes=file_size,
                part_count=0,
                success=False,
                error="Could not determine video duration"
            )
        
        # Calculate segment time based on target size and bitrate
        # This creates N parts of ~max_size_gb and a remainder at the end
        bitrate_bps = file_size * 8 / duration  # bits per second
        max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        max_size_bits = max_size_bytes * 8
        
        # Time in seconds to reach max_size_gb at this bitrate
        segment_time = int(max_size_bits / bitrate_bps)
        
        # Safety margin to ensure we don't exceed limit (subtract 2%)
        segment_time = int(segment_time * 0.98)
        
        num_parts = int(file_size_gb / max_size_gb) + 1
        self._logger.debug(f"Bitrate: {bitrate_bps/1000000:.2f} Mbps, segment time: {segment_time}s, expected parts: {num_parts}")
        
        # Prepare output directory
        if self.output_dir:
            output_dir = self.output_dir
            output_dir.mkdir(parents=True, exist_ok=True)
        else:
            output_dir = input_file.parent
        
        # Output prefix
        if output_prefix:
            prefix = output_prefix
        else:
            prefix = input_file.stem
        
        output_pattern = str(output_dir / f"{prefix}_part%03d.mp4")
        
        self._logger.info(f"Splitting {input_file.name} into {max_size_gb}GB parts...")
        
        try:
            # Use ffmpeg segment mode for even splitting
            process = await asyncio.create_subprocess_exec(
                'ffmpeg', '-y',
                '-i', str(input_file),
                '-c', 'copy',  # No re-encoding
                '-map', '0',   # Copy all streams
                '-segment_time', str(segment_time),
                '-f', 'segment',
                '-reset_timestamps', '1',
                '-movflags', '+faststart',
                output_pattern,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await process.communicate()
            
            if process.returncode != 0:
                error = stderr.decode('utf-8', errors='ignore')[-1000:]
                self._logger.error(f"ffmpeg split failed: {error}")
                return SplitResult(
                    input_file=input_path,
                    output_files=[],
                    total_size_bytes=file_size,
                    part_count=0,
                    success=False,
                    error=error
                )
            
            # Find output files
            output_files = sorted(output_dir.glob(f"{prefix}_part*.mp4"))
            output_paths = [str(f) for f in output_files]
            
            self._logger.info(f"Split into {len(output_paths)} parts")
            
            return SplitResult(
                input_file=input_path,
                output_files=output_paths,
                total_size_bytes=file_size,
                part_count=len(output_paths),
                success=True
            )
            
        except Exception as e:
            self._logger.error(f"Split error: {e}")
            return SplitResult(
                input_file=input_path,
                output_files=[],
                total_size_bytes=file_size,
                part_count=0,
                success=False,
                error=str(e)
            )
    
    async def split_by_duration(
        self,
        input_path: str,
        duration_seconds: int,
        output_prefix: Optional[str] = None
    ) -> SplitResult:
        """
        Split video file by duration using MP4Box.
        
        Args:
            input_path: Path to input video file.
            duration_seconds: Duration of each part in seconds.
            output_prefix: Optional prefix for output files.
            
        Returns:
            SplitResult with list of output files.
        """
        input_file = Path(input_path)
        
        if not input_file.exists():
            return SplitResult(
                input_file=input_path,
                output_files=[],
                total_size_bytes=0,
                part_count=0,
                success=False,
                error=f"File not found: {input_path}"
            )
        
        file_size = input_file.stat().st_size
        
        # Prepare output
        if self.output_dir:
            output_dir = self.output_dir
            output_dir.mkdir(parents=True, exist_ok=True)
        else:
            output_dir = input_file.parent
        
        prefix = output_prefix or input_file.stem
        
        # Convert to milliseconds for MP4Box
        duration_ms = duration_seconds * 1000
        
        self._logger.info(f"Splitting {input_file.name} into {duration_seconds}s parts...")
        
        try:
            process = await asyncio.create_subprocess_exec(
                'MP4Box',
                '-split', str(duration_ms),
                '-out', str(output_dir / f"{prefix}_part"),
                str(input_file),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error = stderr.decode('utf-8', errors='ignore')
                return SplitResult(
                    input_file=input_path,
                    output_files=[],
                    total_size_bytes=file_size,
                    part_count=0,
                    success=False,
                    error=error
                )
            
            output_files = sorted(output_dir.glob(f"{prefix}_part*.mp4"))
            output_paths = [str(f) for f in output_files]
            
            return SplitResult(
                input_file=input_path,
                output_files=output_paths,
                total_size_bytes=file_size,
                part_count=len(output_paths),
                success=True
            )
            
        except Exception as e:
            return SplitResult(
                input_file=input_path,
                output_files=[],
                total_size_bytes=file_size,
                part_count=0,
                success=False,
                error=str(e)
            )
    
    async def cleanup_parts(self, parts: List[str]) -> int:
        """
        Remove split part files after successful upload.
        
        Args:
            parts: List of file paths to remove.
            
        Returns:
            Number of files removed.
        """
        removed = 0
        for part in parts:
            try:
                path = Path(part)
                if path.exists():
                    path.unlink()
                    removed += 1
            except Exception as e:
                self._logger.warning(f"Failed to remove {part}: {e}")
        
        return removed


async def main():
    """Test the file splitter."""
    from .logger import setup_logging
    
    setup_logging(level="INFO")
    
    splitter = FileSplitter()
    
    # Create a test video file
    test_file = "test_video.mp4"
    
    print(f"Creating test video...")
    process = await asyncio.create_subprocess_exec(
        'ffmpeg', '-y',
        '-f', 'lavfi',
        '-i', 'testsrc=duration=30:size=1280x720:rate=30',
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        test_file,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )
    await process.communicate()
    
    if Path(test_file).exists():
        info = await splitter.get_file_info(test_file)
        print(f"Test video created: {info['size_mb']:.2f} MB, {info['duration_seconds']:.1f}s")
        
        # Test splitting by size (use small size to force split)
        result = await splitter.split_file(test_file, 0.001)  # 1 MB
        print(f"\nSplit result:")
        print(f"  Success: {result.success}")
        print(f"  Parts: {result.part_count}")
        for part in result.output_files:
            print(f"    - {part}")
        
        # Cleanup
        Path(test_file).unlink()
        await splitter.cleanup_parts(result.output_files)
        print("\nCleanup complete")
    else:
        print("Failed to create test video")


if __name__ == '__main__':
    asyncio.run(main())
