from dataclasses import dataclass
from typing import Any

from common.config import settings
from common.kafka import KafkaConsumerWrapper, KafkaProducerWrapper
from common.logging_config import configure_logging

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


class IndicatorMeta(type):
    """Indicators metaclass used for registering all indicators"""

    indicators = {}

    def __new__(cls, name, bases, attrs):
        new_class = super().__new__(cls, name, bases, attrs)
        if name != "Indicator":
            IndicatorMeta.indicators[name] = new_class

        return new_class


class Indicator(metaclass=IndicatorMeta):
    """Indicators base class"""

    def __init__(self, name: str, *params):
        self.name = f"{name}-{'-'.join(map(str, params))}"
        self.params = params
        self.output_topic = f"indicators.{self.name}.output"
        self.group = f"indicators.{name}-{'-'.join(map(str, params))}"

        self.consumer = KafkaConsumerWrapper(
            config=settings.kafka,
            topics=["market.raw"],
            group_id=self.group,
        )

        self.producer = KafkaProducerWrapper(config=settings.kafka)

    async def process(self, candle: Candle) -> Any:
        raise NotImplementedError

    async def run(self):
        await self.producer.start()
        await self.consumer.start()

        log.info(f"Indicator {self.name} started")

        try:
            async for msg in self.consumer.consume():
                candle = Candle.from_raw(msg)
                result = await self.process(candle)

                if result is None:
                    continue

                await self.producer.send(self.output_topic, result)

        except Exception as e:
            log.exception(f"Indicator {self.name} error", exception=str(e))
        finally:
            await self.producer.stop()
            await self.consumer.stop()
            log.info(f"Indicator {self.name} stopped")
