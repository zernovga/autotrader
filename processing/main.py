import asyncio

from common.config import Settings, settings
from common.kafka import KafkaConsumerWrapper, KafkaProducerWrapper
from common.logging_config import configure_logging

from .indicators import IndicatorMeta

log = configure_logging("processing")


class IndicatorService:
    def __init__(self, config: Settings):
        self.config = config

        self.consumer = KafkaConsumerWrapper(
            config=settings.kafka,
            topics=["indicators.request"],
            group_id="indicators",
        )

        self.producer = KafkaProducerWrapper(config=settings.kafka)

        self.running_indicators = {}

    async def init(self):
        await self.consumer.start()
        await self.producer.start()

        log.info("IndicatorService initialized")

    async def run(self):
        log.info("IndicatorService started")

        log.info("Available indicators", indicators=IndicatorMeta.indicators)

        try:
            async for msg in self.consumer.consume():
                log.info("Received indicator request", msg=msg)
                if msg["indicator"] not in IndicatorMeta.indicators:
                    log.info("Unknown indicator", indicator=msg["indicator"])
                    continue

                name = f"{msg['indicator']}{msg['indicator']}-{'-'.join(map(str, msg['params']))}"
                if name in self.running_indicators:
                    log.info("Indicator already running", indicator=name)
                    continue

                indicator = IndicatorMeta.indicators[msg["indicator"]](*msg["params"])
                asyncio.create_task(indicator.run())
                self.running_indicators[indicator.name] = indicator

                log.info("Indicator started", indicator=indicator.name)

        except Exception as e:
            log.exception("processing error", exception=str(e))
        finally:
            await self.close()

    async def close(self):
        log.info("Shutting down IndicatorService...")
        await self.consumer.stop()


async def main():
    service = IndicatorService(settings)
    await service.init()

    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
