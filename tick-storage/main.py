import asyncio
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from common.config import settings
from common.kafka import KafkaConsumerWrapper
from common.logging_config import configure_logging

log = configure_logging("tick-storage")


class TickWriter:
    def __init__(self, base_dir: str = "data") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.files: dict[str, csv.DictWriter] = {}
        self.handles: dict[str, Any] = {}

    def _get_day_file(self, figi: str) -> str:
        day = datetime.now().strftime("%Y-%m-%d")
        folder = self.base_dir / figi
        folder.mkdir(parents=True, exist_ok=True)
        return str(folder / f"{day}.csv")

    def _ensure_writer(self, figi: str, row: dict[str, Any]) -> csv.DictWriter:
        filepath = self._get_day_file(figi)

        if filepath not in self.files:
            handle = open(filepath, "a", newline="")
            self.handles[filepath] = handle

            writer = csv.DictWriter(handle, fieldnames=row.keys())
            if os.stat(filepath).st_size == 0:
                writer.writeheader()

            self.files[filepath] = writer

        return self.files[filepath]

    async def write_tick(self, tick: dict[str, Any]):
        figi: str = tick["figi"]
        del tick["figi"]
        writer = self._ensure_writer(figi, tick)
        writer.writerow(tick)

    def close(self):
        for h in self.handles.values():
            h.close()


class TickStorageService:
    def __init__(self, writer: TickWriter):
        self.consumer = KafkaConsumerWrapper(
            config=settings.kafka, topics=["market.raw"], group_id="tick-storage"
        )
        self.writer = writer

    async def start(self):
        await self.consumer.start()
        log.info("tick storage service started")

        try:
            async for msg in self.consumer.consume():
                log.info("received message for saving", payload=msg)
                await self.writer.write_tick(msg) # type: ignore
        finally:
            await self.consumer.stop()
            self.writer.close()
            log.info("tick storage service stopped")


async def main():
    service = TickStorageService(TickWriter())

    await service.start()


if __name__ == "__main__":
    asyncio.run(main())
