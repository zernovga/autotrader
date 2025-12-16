import asyncio

from tinkoff.invest import (
    CandleInstrument,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
)
from tinkoff.invest.sandbox.async_client import AsyncSandboxClient as AsyncClient

from .base import Streamer


class TinkoffSandboxStreamer(Streamer):
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
