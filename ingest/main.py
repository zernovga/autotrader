import asyncio
import json

from aiokafka import AIOKafkaProducer
from tinkoff.invest import (
    AioRequestError,
    CandleInstrument,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
)
from tinkoff.invest.sandbox.async_client import AsyncSandboxClient as AsyncClient

from common.config import settings
from common.logging_config import configure_logging

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


class KafkaProducerWrapper:
    def __init__(self, servers: str):
        self.servers = servers
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.servers,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        await self.producer.start()

    async def send(self, topic: str, value: dict):
        await self.producer.send_and_wait(topic, value)


async def main():
    print("starting main")

    streamer = TinkoffSandboxStreamer("BBG0013HRTL0")
    producer = KafkaProducerWrapper(settings.kafka.bootstrap_servers)

    await producer.start()
    while True:
        log.info("connecting to client", token=settings.tinkoff_token)
        async with AsyncClient(settings.tinkoff_token) as client:
            try:
                await streamer.ensure_market_open(client)

                async for msg in client.market_data_stream.market_data_stream(
                    streamer.stream()
                ):
                    log.info("raw event", payload=msg)
                    await producer.send(settings.kafka.topic_raw, msg)
            except AioRequestError as e:
                log.error("ingest error", exception=str(e))
                break


if __name__ == "__main__":
    log.info("started")

    asyncio.run(main())
