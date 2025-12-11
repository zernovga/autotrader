import asyncio

from common.config import settings
from common.kafka import KafkaProducerWrapper
from common.logging_config import configure_logging
from tinkoff.invest import (
    AioRequestError,
    CandleInstrument,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
)
from tinkoff.invest.sandbox.async_client import AsyncSandboxClient as AsyncClient
from tinkoff.invest.utils import quotation_to_decimal

log = configure_logging("ingest")


class DemoStreamer:
    async def stream(self):
        while True:
            yield {"data": "streamer test message"}
            await asyncio.sleep(2)


class TinkoffSandboxStreamer:
    def __init__(self, figi):
        self.figi = figi

    async def ensure_market_open(self, client: AsyncClient):
        trading_status = await client.market_data.get_trading_status(figi=self.figi)
        while not (
            trading_status.market_order_available_flag
            and trading_status.api_trade_available_flag
        ):
            log.info("Waiting for the market to open. figi=%s", self.figi)
            await asyncio.sleep(60)
            trading_status = await client.market_data.get_trading_status(figi=self.figi)

    async def stream(self):
        yield MarketDataRequest(
            subscribe_candles_request=SubscribeCandlesRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[
                    CandleInstrument(
                        figi=self.figi,
                        interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
                    )
                ],
            )
        )
        while True:
            await asyncio.sleep(1)


async def main():
    print("starting main")

    figi = "BBG0013HRTL0"

    producer = KafkaProducerWrapper(settings.kafka)

    streamer = TinkoffSandboxStreamer(figi)

    await producer.start()
    log.info("connecting to client")
    while True:
        async with AsyncClient(settings.tinkoff_token) as client:
            try:
                await streamer.ensure_market_open(client)

                async for msg in client.market_data_stream.market_data_stream(
                    streamer.stream()
                ):
                    try:
                        log.info("raw event", payload=msg)

                        data = {
                            "figi": figi,
                            "time": msg.candle.time.isoformat(),
                            "last_trade": msg.candle.last_trade_ts.isoformat(),
                            "open": float(quotation_to_decimal(msg.candle.open)),
                            "high": float(quotation_to_decimal(msg.candle.high)),
                            "low": float(quotation_to_decimal(msg.candle.low)),
                            "close": float(quotation_to_decimal(msg.candle.close)),
                        }
                        log.info("normalized event", payload=data)

                        await producer.send(settings.kafka.topic_raw, data)

                    except AioRequestError as e:
                        log.error("request error", exception=str(e))
                    except Exception as e:
                        log.error("error", exception=str(e))

            except AioRequestError as e:
                log.error("request error", exception=str(e))
            except Exception as e:
                log.error("error", exception=str(e))


async def main_demo():
    from datetime import datetime
    from random import random

    print("starting main demo")

    figi = "DEMO"

    producer = KafkaProducerWrapper(settings.kafka)

    await producer.start()
    log.info("connecting to client")

    while True:
        try:
            data = {
                "figi": figi,
                "time": datetime.now().isoformat(),
                "last_trade": datetime.now().isoformat(),
                "open": 20 * random(),
                "high": 20 * random(),
                "low": 20 * random(),
                "close": 20 * random(),
            }
            log.info("normalized event", payload=data)

            await producer.send(settings.kafka.topic_raw, data)
            await asyncio.sleep(1)

        except Exception as e:
            log.error("error", exception=str(e))


if __name__ == "__main__":
    log.info("started")
    if settings.demo:
        asyncio.run(main_demo())
    else:
        asyncio.run(main())
