"""
Video compressor module for Twitch Stream Recorder.
Compresses video files to a target size using ffmpeg.
"""

import asyncio
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .logger import get_logger


@dataclass
class CompressionResult:
    """Result of a compression operation."""
    input_file: str
    output_file: str
    input_size_bytes: int
    output_size_bytes: int
    target_size_mb: int
    duration_seconds: float
    video_bitrate_kbps: int
    audio_bitrate_kbps: int
    success: bool
    error: Optional[str] = None
    
    @property
    def compression_ratio(self) -> float:
        """Get compression ratio."""
        if self.input_size_bytes == 0:
            return 0
        return self.output_size_bytes / self.input_size_bytes
    
    @property
    def size_reduction_percent(self) -> float:
        """Get size reduction percentage."""
        return (1 - self.compression_ratio) * 100


class VideoCompressor:
    """
    Compresses video files to a target file size.
    
    Uses two-pass encoding for optimal quality at the target bitrate.
    Calculates required bitrate based on duration and target size.
    """
    
    def __init__(
        self,
        audio_bitrate_kbps: int = 128,
        two_pass: bool = True,
        output_dir: Optional[str] = None
    ):
        """
        Initialize video compressor.
        
        Args:
            audio_bitrate_kbps: Audio bitrate in kbps.
            two_pass: Use two-pass encoding for better quality.
            output_dir: Directory for output files.
        """
        self.audio_bitrate_kbps = audio_bitrate_kbps
        self.two_pass = two_pass
        self.output_dir = Path(output_dir) if output_dir else None
        self._logger = get_logger('compressor')
    
    async def get_video_duration(self, file_path: str) -> float:
        """
        Get video duration in seconds.
        
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
            return float(stdout.decode().strip())
            
        except Exception as e:
            self._logger.error(f"Failed to get duration: {e}")
            return 0.0
    
    def calculate_video_bitrate(
        self,
        duration_seconds: float,
        target_size_mb: int
    ) -> int:
        """
        Calculate required video bitrate for target size.
        
        Formula:
        total_bitrate = (target_size_bytes * 8) / duration
        video_bitrate = total_bitrate - audio_bitrate
        
        Args:
            duration_seconds: Video duration in seconds.
            target_size_mb: Target file size in MB.
            
        Returns:
            Video bitrate in kbps.
        """
        if duration_seconds <= 0:
            return 0
        
        # Target size in bits
        target_size_bits = target_size_mb * 1024 * 1024 * 8
        
        # Total bitrate needed
        total_bitrate_bps = target_size_bits / duration_seconds
        total_bitrate_kbps = total_bitrate_bps / 1000
        
        # Subtract audio bitrate
        video_bitrate_kbps = max(
            100,  # Minimum video bitrate
            int(total_bitrate_kbps - self.audio_bitrate_kbps)
        )
        
        # Apply a small safety margin (5%)
        video_bitrate_kbps = int(video_bitrate_kbps * 0.95)
        
        return video_bitrate_kbps
    
    async def compress_to_size(
        self,
        input_path: str,
        target_size_mb: int,
        output_path: Optional[str] = None
    ) -> CompressionResult:
        """
        Compress video to target file size.
        
        Uses two-pass encoding for optimal quality distribution.
        
        Args:
            input_path: Path to input video file.
            target_size_mb: Target size in MB.
            output_path: Optional output path (default: input_compressed.mp4).
            
        Returns:
            CompressionResult with details.
        """
        input_file = Path(input_path)
        
        if not input_file.exists():
            return CompressionResult(
                input_file=input_path,
                output_file="",
                input_size_bytes=0,
                output_size_bytes=0,
                target_size_mb=target_size_mb,
                duration_seconds=0,
                video_bitrate_kbps=0,
                audio_bitrate_kbps=self.audio_bitrate_kbps,
                success=False,
                error=f"File not found: {input_path}"
            )
        
        input_size = input_file.stat().st_size
        input_size_mb = input_size / (1024 * 1024)
        
        # Check if compression is needed
        if input_size_mb <= target_size_mb:
            self._logger.info(
                f"File already smaller than target "
                f"({input_size_mb:.1f} MB <= {target_size_mb} MB)"
            )
            return CompressionResult(
                input_file=input_path,
                output_file=input_path,
                input_size_bytes=input_size,
                output_size_bytes=input_size,
                target_size_mb=target_size_mb,
                duration_seconds=0,
                video_bitrate_kbps=0,
                audio_bitrate_kbps=self.audio_bitrate_kbps,
                success=True
            )
        
        # Get duration
        duration = await self.get_video_duration(input_path)
        if duration <= 0:
            return CompressionResult(
                input_file=input_path,
                output_file="",
                input_size_bytes=input_size,
                output_size_bytes=0,
                target_size_mb=target_size_mb,
                duration_seconds=0,
                video_bitrate_kbps=0,
                audio_bitrate_kbps=self.audio_bitrate_kbps,
                success=False,
                error="Could not determine video duration"
            )
        
        # Calculate bitrate
        video_bitrate = self.calculate_video_bitrate(duration, target_size_mb)
        
        # Skip compression if bitrate is too low (would result in unwatchable quality)
        MIN_VIDEO_BITRATE = 200  # kbps - minimum for acceptable quality
        if video_bitrate < MIN_VIDEO_BITRATE:
            self._logger.warning(
                f"Skipping compression: calculated bitrate ({video_bitrate} kbps) "
                f"is below minimum ({MIN_VIDEO_BITRATE} kbps) for acceptable quality. "
                f"File is too large ({input_size_mb:.0f} MB) for target size ({target_size_mb} MB)."
            )
            return CompressionResult(
                input_file=input_path,
                output_file="",
                input_size_bytes=input_size,
                output_size_bytes=0,
                target_size_mb=target_size_mb,
                duration_seconds=duration,
                video_bitrate_kbps=video_bitrate,
                audio_bitrate_kbps=self.audio_bitrate_kbps,
                success=False,
                error=f"Bitrate too low ({video_bitrate} kbps) - file too large for target size"
            )
        
        self._logger.info(
            f"Compressing {input_file.name} "
            f"({input_size_mb:.1f} MB -> {target_size_mb} MB, "
            f"video: {video_bitrate} kbps, audio: {self.audio_bitrate_kbps} kbps)"
        )
        
        # Output path
        if output_path:
            out_file = Path(output_path)
        elif self.output_dir:
            out_file = self.output_dir / f"{input_file.stem}_compressed.mp4"
        else:
            out_file = input_file.parent / f"{input_file.stem}_compressed.mp4"
        
        out_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            if self.two_pass:
                success = await self._two_pass_encode(
                    input_path, str(out_file), video_bitrate
                )
            else:
                success = await self._single_pass_encode(
                    input_path, str(out_file), video_bitrate
                )
            
            if not success:
                return CompressionResult(
                    input_file=input_path,
                    output_file="",
                    input_size_bytes=input_size,
                    output_size_bytes=0,
                    target_size_mb=target_size_mb,
                    duration_seconds=duration,
                    video_bitrate_kbps=video_bitrate,
                    audio_bitrate_kbps=self.audio_bitrate_kbps,
                    success=False,
                    error="Encoding failed"
                )
            
            output_size = out_file.stat().st_size
            output_size_mb = output_size / (1024 * 1024)
            
            self._logger.info(
                f"Compression complete: {input_size_mb:.1f} MB -> {output_size_mb:.1f} MB "
                f"({(1 - output_size / input_size) * 100:.1f}% reduction)"
            )
            
            return CompressionResult(
                input_file=input_path,
                output_file=str(out_file),
                input_size_bytes=input_size,
                output_size_bytes=output_size,
                target_size_mb=target_size_mb,
                duration_seconds=duration,
                video_bitrate_kbps=video_bitrate,
                audio_bitrate_kbps=self.audio_bitrate_kbps,
                success=True
            )
            
        except Exception as e:
            self._logger.error(f"Compression error: {e}")
            return CompressionResult(
                input_file=input_path,
                output_file="",
                input_size_bytes=input_size,
                output_size_bytes=0,
                target_size_mb=target_size_mb,
                duration_seconds=duration,
                video_bitrate_kbps=video_bitrate,
                audio_bitrate_kbps=self.audio_bitrate_kbps,
                success=False,
                error=str(e)
            )
    
    async def _two_pass_encode(
        self,
        input_path: str,
        output_path: str,
        video_bitrate_kbps: int
    ) -> bool:
        """
        Perform two-pass encoding.
        
        Args:
            input_path: Input file path.
            output_path: Output file path.
            video_bitrate_kbps: Target video bitrate.
            
        Returns:
            True if successful.
        """
        # Generate unique passlog filename
        passlog = f"/tmp/ffmpeg2pass_{os.getpid()}"
        
        # Input options for handling VFR (Variable Frame Rate) streams
        # -fflags +genpts: regenerate presentation timestamps
        # -vsync cfr: force constant frame rate output
        input_opts = [
            '-fflags', '+genpts',
        ]
        
        # Common encoding options
        video_opts = [
            '-vsync', 'cfr',  # Force constant frame rate to fix "2nd pass has more frames" error
            '-c:v', 'libx264',
            '-b:v', f'{video_bitrate_kbps}k',
            '-preset', 'medium',
            '-profile:v', 'high',
            '-level', '4.1',
        ]
        
        try:
            # First pass - analyze video
            self._logger.info("Pass 1/2: Analyzing video...")
            
            cmd1 = [
                'ffmpeg', '-y',
                *input_opts,
                '-i', input_path,
                *video_opts,
                '-pass', '1',
                '-passlogfile', passlog,
                '-an',  # No audio in first pass
                '-f', 'null',
                '/dev/null'
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd1,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await process.communicate()
            
            if process.returncode != 0:
                self._logger.error(f"Pass 1 failed: {stderr.decode()[-5000:]}")
                return False
            
            # Second pass - encode with optimized bitrate distribution
            self._logger.info("Pass 2/2: Encoding video...")
            
            cmd2 = [
                'ffmpeg', '-y',
                *input_opts,
                '-i', input_path,
                *video_opts,
                '-pass', '2',
                '-passlogfile', passlog,
                '-c:a', 'aac',
                '-b:a', f'{self.audio_bitrate_kbps}k',
                '-movflags', '+faststart',
                output_path
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd2,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await process.communicate()
            
            if process.returncode != 0:
                self._logger.error(f"Pass 2 failed: {stderr.decode()[-5000:]}")
                return False
            
            return True
            
        finally:
            # Cleanup passlog files
            for ext in ['', '-0.log', '-0.log.mbtree']:
                try:
                    os.unlink(f"{passlog}{ext}")
                except OSError:
                    pass
    
    async def _single_pass_encode(
        self,
        input_path: str,
        output_path: str,
        video_bitrate_kbps: int
    ) -> bool:
        """
        Perform single-pass encoding (faster but less precise).
        
        Args:
            input_path: Input file path.
            output_path: Output file path.
            video_bitrate_kbps: Target video bitrate.
            
        Returns:
            True if successful.
        """
        self._logger.info("Encoding video (single pass)...")
        
        cmd = [
            'ffmpeg', '-y',
            '-i', input_path,
            '-c:v', 'libx264',
            '-b:v', f'{video_bitrate_kbps}k',
            '-preset', 'medium',
            '-profile:v', 'high',
            '-c:a', 'aac',
            '-b:a', f'{self.audio_bitrate_kbps}k',
            '-movflags', '+faststart',
            output_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        _, stderr = await process.communicate()
        
        if process.returncode != 0:
            self._logger.error(f"Encoding failed: {stderr.decode()[:500]}")
            return False
        
        return True


async def main():
    """Test the video compressor."""
    from .logger import setup_logging
    
    setup_logging(level="INFO")
    
    compressor = VideoCompressor()
    
    # Create a test video
    test_input = "test_video_large.mp4"
    
    print("Creating test video...")
    process = await asyncio.create_subprocess_exec(
        'ffmpeg', '-y',
        '-f', 'lavfi',
        '-i', 'testsrc=duration=60:size=1920x1080:rate=30',
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-b:v', '5000k',
        test_input,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )
    await process.communicate()
    
    if Path(test_input).exists():
        input_size = Path(test_input).stat().st_size / (1024 * 1024)
        print(f"Created test video: {input_size:.1f} MB")
        
        # Compress to smaller size
        result = await compressor.compress_to_size(
            test_input,
            target_size_mb=10
        )
        
        print(f"\nCompression result:")
        print(f"  Success: {result.success}")
        print(f"  Input size: {result.input_size_bytes / 1024 / 1024:.1f} MB")
        print(f"  Output size: {result.output_size_bytes / 1024 / 1024:.1f} MB")
        print(f"  Target size: {result.target_size_mb} MB")
        print(f"  Reduction: {result.size_reduction_percent:.1f}%")
        print(f"  Video bitrate: {result.video_bitrate_kbps} kbps")
        
        # Cleanup
        Path(test_input).unlink()
        if result.success and result.output_file != test_input:
            Path(result.output_file).unlink()
    else:
        print("Failed to create test video")


if __name__ == '__main__':
    asyncio.run(main())
