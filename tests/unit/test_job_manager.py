import asyncio
import unittest

from streamrecorder.job_manager import JobManager, TargetUploadScheduler


class JobManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_named_queue_limits_concurrency(self):
        manager = JobManager({"upload": 1})
        running = 0
        peak = 0

        async def job():
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.01)
            running -= 1

        await asyncio.gather(
            manager.run("upload", "a", job),
            manager.run("upload", "b", job),
        )

        stats = await manager.snapshot()
        self.assertEqual(peak, 1)
        self.assertEqual(stats["upload"].completed, 2)

    async def test_target_scheduler_respects_target_concurrency(self):
        scheduler = TargetUploadScheduler()
        running = 0
        peak = 0

        async def job():
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.01)
            running -= 1

        await asyncio.gather(
            scheduler.run("target", 3, job),
            scheduler.run("target", 3, job),
        )

        self.assertEqual(peak, 2)


if __name__ == "__main__":
    unittest.main()
