"""
Pipeline scheduler for Relay
Handles automatic pipeline execution based on schedules
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict

logger = logging.getLogger(__name__)

class PipelineScheduler:
    """Manages scheduled pipeline execution"""

    def __init__(self, storage, engine):
        self.storage = storage
        self.engine = engine
        self.running = False
        self.task = None

    async def start(self):
        """Start the scheduler"""
        self.running = True
        self.task = asyncio.create_task(self._schedule_loop())
        logger.info("Pipeline scheduler started")

    async def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self.task:
            self.task.cancel()
        logger.info("Pipeline scheduler stopped")

    async def _schedule_loop(self):
        """Main scheduling loop - checks every minute"""
        while self.running:
            try:
                await self._check_scheduled_pipelines()
            except Exception as e:
                logger.error(f"Scheduler error: {e}")

            # Wait 1 minute before next check
            await asyncio.sleep(60)

    async def _check_scheduled_pipelines(self):
        """Check if any pipelines need to run"""
        pipelines = self.storage.list_pipelines()
        now = datetime.now(timezone.utc)

        for pipeline in pipelines:
            schedule = pipeline.get("schedule", {})

            # Skip if scheduling not enabled
            if not schedule.get("enabled", False):
                continue

            # Check if pipeline should run
            if self._should_run(pipeline, now):
                logger.info(f"Triggering scheduled run for pipeline {pipeline['id']}")

                # Generate run ID
                import uuid
                run_id = f"run-{uuid.uuid4().hex[:8]}"

                # Execute pipeline in background
                asyncio.create_task(
                    asyncio.to_thread(
                        self.engine.execute_pipeline,
                        pipeline["id"],
                        run_id
                    )
                )

                # Update last_run timestamp
                self.storage.update_pipeline(
                    pipeline["id"],
                    {"last_scheduled_run": now.isoformat() + "Z"}
                )

    def _should_run(self, pipeline: Dict, now: datetime) -> bool:
        """Determine if pipeline should run now"""
        schedule = pipeline.get("schedule", {})
        interval = schedule.get("interval", "daily")
        last_run = pipeline.get("last_scheduled_run")

        # If never run before, run it
        if not last_run:
            return True

        last_run_time = datetime.fromisoformat(last_run.replace("Z", ""))

        # Check based on interval
        if interval == "hourly":
            # Run if more than 1 hour since last run
            return (now - last_run_time) >= timedelta(hours=1)

        elif interval == "daily":
            # Run if more than 1 day since last run
            # TODO: Check if we're at the right hour (2 AM by default)
            return (now - last_run_time) >= timedelta(days=1)

        elif interval == "weekly":
            # Run if more than 7 days since last run
            return (now - last_run_time) >= timedelta(days=7)

        elif interval == "custom":
            # TODO: Parse cron expression
            # For now, treat as daily
            return (now - last_run_time) >= timedelta(days=1)

        return False

    def get_next_run_time(self, pipeline: Dict) -> str:
        """Calculate next scheduled run time"""
        schedule = pipeline.get("schedule", {})

        if not schedule.get("enabled", False):
            return "Not scheduled"

        interval = schedule.get("interval", "daily")
        last_run = pipeline.get("last_scheduled_run")

        if not last_run:
            return "Will run on next check (within 1 minute)"

        last_run_time = datetime.fromisoformat(last_run.replace("Z", ""))

        if interval == "hourly":
            next_run = last_run_time + timedelta(hours=1)
        elif interval == "daily":
            next_run = last_run_time + timedelta(days=1)
        elif interval == "weekly":
            next_run = last_run_time + timedelta(days=7)
        else:
            return "Unknown"

        return next_run.isoformat() + "Z"
