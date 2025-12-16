import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from random import random

from common.logging_config import configure_logging
from tinkoff.invest import Quotation
from tinkoff.invest.utils import decimal_to_quotation

from .base import Streamer

log = configure_logging("ingest")


class DemoStreamer(Streamer):
    def __init__(self, **params):
        params["figi"] = "DEMO"
        super().__init__(**params)

    async def stream(self):  # type: ignore
        @dataclass
        class Candle:
            figi: str
            time: datetime
            last_trade_ts: datetime
            open: Quotation
            high: Quotation
            low: Quotation
            close: Quotation

        while True:
            dt = datetime.now()
            candle_time = dt
            match self.params["interval"]:
                case 0:
                    # SUBSCRIPTION_INTERVAL_UNSPECIFIED
                    raise Exception("SUBSCRIPTION_INTERVAL_UNSPECIFIED")
                case 1:
                    # SUBSCRIPTION_INTERVAL_ONE_MINUTE
                    candle_time = dt.replace(second=0, microsecond=0)
                case 2:
                    # SUBSCRIPTION_INTERVAL_FIVE_MINUTES
                    candle_time = dt.replace(second=0, microsecond=0)
                    delta = timedelta(minutes=5)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 3:
                    # SUBSCRIPTION_INTERVAL_FIFTEEN_MINUTES
                    candle_time = dt.replace(second=0, microsecond=0)
                    delta = timedelta(minutes=15)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 4:
                    # SUBSCRIPTION_INTERVAL_ONE_HOUR
                    candle_time = dt.replace(minute=0, second=0, microsecond=0)
                case 5:
                    # SUBSCRIPTION_INTERVAL_ONE_DAY
                    candle_time = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                case 6:
                    # SUBSCRIPTION_INTERVAL_2_MIN
                    candle_time = dt.replace(second=0, microsecond=0)
                    delta = timedelta(minutes=2)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 7:
                    # SUBSCRIPTION_INTERVAL_3_MIN
                    candle_time = dt.replace(second=0, microsecond=0)
                    delta = timedelta(minutes=3)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 8:
                    # SUBSCRIPTION_INTERVAL_10_MIN
                    candle_time = dt.replace(second=0, microsecond=0)
                    delta = timedelta(minutes=10)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 9:
                    # SUBSCRIPTION_INTERVAL_30_MIN
                    candle_time = dt.replace(second=0, microsecond=0)
                    delta = timedelta(minutes=20)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 10:
                    # SUBSCRIPTION_INTERVAL_2_HOUR
                    candle_time = dt.replace(minute=0, second=0, microsecond=0)
                    delta = timedelta(hours=2)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 11:
                    # SUBSCRIPTION_INTERVAL_4_HOUR
                    candle_time = dt.replace(minute=0, second=0, microsecond=0)
                    delta = timedelta(hours=4)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 12:
                    # SUBSCRIPTION_INTERVAL_WEEK
                    candle_time = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    delta = timedelta(days=7)
                    candle_time = candle_time - (candle_time - datetime.min) % delta
                case 13:
                    # SUBSCRIPTION_INTERVAL_MONTH
                    candle_time = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    delta = timedelta(days=30)
                    candle_time = candle_time - (candle_time - datetime.min) % delta

            yield Candle(
                figi=self.figi,
                time=candle_time,
                last_trade_ts=dt,
                open=decimal_to_quotation(Decimal(20 * random())),
                high=decimal_to_quotation(Decimal(20 * random())),
                low=decimal_to_quotation(Decimal(20 * random())),
                close=decimal_to_quotation(Decimal(20 * random())),
            )
            await asyncio.sleep(1 + random() * 4)
