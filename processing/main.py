import asyncio
from dataclasses import dataclass

from common.config import Settings, settings
from common.kafka import KafkaConsumerWrapper
from common.logging_config import configure_logging

from .indicators import MA
from .instruments import InstrumentManager

log = configure_logging("processing")


@dataclass
class Candle:
    figi: str
    time: float
    last_trade: float
    open: float
    high: float
    low: float
    close: float

    @classmethod
    def from_raw(cls, data: dict) -> "Candle":
        return cls(
            figi=data["figi"],
            time=data["time"],
            last_trade=data["last_trade"],
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
        )


class IndicatorService:
    def __init__(self, config: Settings):
        self.config = config

        self.manager = InstrumentManager()

        self.consumer = KafkaConsumerWrapper(
            config=settings.kafka,
            topics=[self.config.kafka.topic_raw],
            group_id="indicators",
        )

    async def init(self):
        """Подключает Kafka, регистрирует индикаторы."""
        await self.consumer.start()

        log.info("IndicatorService initialized")

    async def run(self):
        """Главный цикл обработки входящих свечей."""
        log.info("IndicatorService started")

        async for msg in self.consumer.consume():
            try:
                candle = Candle.from_raw(msg)  # type: ignore # стандартизируем структуру
                instrument = self.manager.ensure(candle.figi)

                if not instrument.indicators:
                    instrument.add_indicator("ma20", MA(20))
                    instrument.add_indicator("ma50", MA(50))

                result = self.manager.process_message(candle)
                if result:
                    log.info(
                        "indicator service result",
                        figi=candle.figi,
                        result=result,
                    )

            except Exception as e:
                log.error("processing error", exception=str(e))

    async def close(self):
        log.info("Shutting down IndicatorService...")
        await self.consumer.stop()


async def main():
    service = IndicatorService(settings)
    await service.init()

    try:
        await service.run()
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(main())
