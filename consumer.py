from __future__ import annotations

from datetime import datetime

import asyncpg

from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job, Schedule


async def main() -> PgQueuer:
    connection = await asyncpg.connect(
        user="test1234",
        password="test1234",
        database="test_pgqueuer",
        host="localhost",
        port=5332,
    )
    driver = AsyncpgDriver(connection)
    pgq = PgQueuer(driver)

    # Entrypoint for jobs whose entrypoint is named 'fetch'.
    @pgq.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message:")
        print(dict(job))
        print()

    # Define and register recurring tasks using cron expressions
    # The cron expression "* * * * *" means the task will run every minute
    @pgq.schedule("scheduled_every_minute", "* * * * *")
    async def scheduled_every_minute(schedule: Schedule) -> None:
        print(f"Executed every minute {schedule!r} {datetime.now()!r}")
        print()

    return pgq