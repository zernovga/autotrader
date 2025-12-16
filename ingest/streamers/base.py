from common.config import settings
from common.kafka import KafkaProducerWrapper
from common.logging_config import configure_logging
from tinkoff.invest.utils import quotation_to_decimal

log = configure_logging("ingest")


class StreamerMeta(type):
    streamers = {}

    def __new__(cls, name, bases, attrs):
        new_class = super().__new__(cls, name, bases, attrs)
        if name != "Streamer":
            cls.streamers[name] = new_class
        return new_class


class Streamer(metaclass=StreamerMeta):
    def __init__(self, **params):
        self.figi = params["figi"]
        self.params = params
        self.name = f"{self.figi}-{'-'.join(map(str, params.values()))}"

        self.topic = f"market.raw.{self.name}"
        self.producer = KafkaProducerWrapper(config=settings.kafka)

    async def stream(self):
        raise NotImplementedError

    async def run(self):
        await self.producer.start()

        try:
            async for candle in self.stream(): # type: ignore
                log.info("raw event", payload=candle)

                data = {
                    "figi": candle.figi,
                    "time": candle.time.isoformat(),
                    "last_trade": candle.last_trade_ts.isoformat(),
                    "open": float(quotation_to_decimal(candle.open)),
                    "high": float(quotation_to_decimal(candle.high)),
                    "low": float(quotation_to_decimal(candle.low)),
                    "close": float(quotation_to_decimal(candle.close)),
                }
                log.info("normalized event", payload=data)

                await self.producer.send(self.topic, data)
        except Exception as e:
            log.exception(f"Streamer {self.name} error", exception=str(e))
