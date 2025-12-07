import asyncio

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

    async def init(self):
        await self.consumer.start()
        await self.producer.start()

    async def run(self):
        log.info("StrategiesService started")
        
        async for msg in self.consumer.consume():
            log.info("new tick", payload=msg)

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
