"""
Nudge Scheduler — periodic runner for the account state machine.

Invokes core.state_machine.nudge_engine_tick() every
NUDGE_EVALUATION_INTERVAL_MINUTES (default 15), matching the spec's
"CronJob every 15 minutes" deployment. Runs as a long-lived process
so it works both under docker-compose and as a K8s CronJob wrapper
(use --once for a single evaluation cycle, suitable for real cron).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from config.settings import settings
from core.database import engine
from core.state_machine import nudge_engine_tick

logger = logging.getLogger(__name__)


class NudgeScheduler:
    """Runs nudge_engine_tick on a fixed interval until stopped."""

    def __init__(self, interval_minutes: int) -> None:
        self._interval_seconds = interval_minutes * 60
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> dict:
        """Run a single evaluation cycle."""
        try:
            stats = await nudge_engine_tick()
            logger.info("Tick complete: %s", stats)
            return stats
        except Exception:
            logger.exception("Nudge engine tick failed")
            return {}

    async def run_forever(self) -> None:
        """Run ticks on the configured interval until stop() is called."""
        logger.info(
            "Nudge scheduler started (interval: %d minutes)",
            self._interval_seconds // 60,
        )
        while not self._stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._interval_seconds
                )
            except asyncio.TimeoutError:
                continue
        logger.info("Nudge scheduler stopped")


async def main() -> None:
    parser = argparse.ArgumentParser(description="State machine / nudge engine scheduler")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single evaluation cycle and exit (for external cron/K8s CronJob)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    scheduler = NudgeScheduler(settings.nudge.evaluation_interval_minutes)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, scheduler.stop)

    try:
        if args.once:
            await scheduler.run_once()
        else:
            await scheduler.run_forever()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
