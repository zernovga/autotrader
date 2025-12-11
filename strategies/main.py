import asyncio
from collections import deque

from common.config import settings
from common.kafka import KafkaConsumerWrapper, KafkaProducerWrapper
from common.logging_config import configure_logging

log = configure_logging("strategies")


class StrategiesService:
    def __init__(self) -> None:
        self.consumer = KafkaConsumerWrapper(
            config=settings.kafka, topics=["market.processed"], group_id="strategies"
        )
        self.producer = KafkaProducerWrapper(settings.kafka)

        self._values = deque(maxlen=10)
        self._ma_short = "ma20"
        self._ma_long = "ma50"
        self._last_cross_type = None

    async def init(self):
        await self.consumer.start()
        await self.producer.start()

    async def run(self):
        log.info("StrategiesService started")

        async for msg in self.consumer.consume():
            msg: dict = msg
            log.info("new tick", payload=msg)
            self._values.append(msg)
            if (
                len(self._values) < 2
                or self._values[-2][self._ma_short] is None
                or self._values[-2][self._ma_long] is None
            ):
                log.info("not enough data for work, waiting")
                continue

            cross_type = None
            if (
                self._values[-2][self._ma_short] < self._values[-1][self._ma_short]
                and self._values[-1][self._ma_short] >= self._values[-1][self._ma_long]
            ):
                cross_type = "BUY"
            elif (
                self._values[-2][self._ma_short] > self._values[-1][self._ma_short]
                and self._values[-1][self._ma_short] <= self._values[-1][self._ma_long]
            ):
                cross_type = "SELL"

            if cross_type and (
                self._last_cross_type or cross_type != self._last_cross_type
            ):
                log.info("got order signal", signal=cross_type)
                self._last_cross_type = cross_type

                await self.producer.send(
                    "orders.new",
                    {
                        "figi": msg["figi"],
                        "signal": cross_type,
                        "price": msg["close"],
                    },
                )

    async def close(self):
        log.info("Shutting down StrategiesService...")
        await self.consumer.stop()
        await self.producer.stop()


async def main():
    service = StrategiesService()

    await service.init()

    try:
        await service.run()
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(main())
