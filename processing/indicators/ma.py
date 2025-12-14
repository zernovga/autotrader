from collections import deque

from processing.indicators.base import Candle

from .base import Indicator


class MA(Indicator):
    def __init__(self, *params):
        super().__init__("MA", *params)
        self.period: int = params[0]
        self._values: deque[float] = deque(maxlen=self.period)
        self._sum: float = 0.0

    async def process(self, candle: Candle) -> float | None:
        close = candle.close

        if len(self._values) == self.period:
            oldest = self._values[0]
            self._sum -= oldest

        self._values.append(close)
        self._sum += close

        if len(self._values) < self.period:
            return None

        return self._sum / self.period
