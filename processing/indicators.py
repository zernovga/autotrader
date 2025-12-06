from collections import deque
from typing import Optional, Protocol


class IndicatorProtocol(Protocol):
    def update(self, candle) -> Optional[float]:
        pass


class MA:
    """
    Simple Moving Average (SMA) indicator.

    Parameters:
        period (int): number of candles to average
    """

    __slots__ = ("period", "_values", "_sum")

    def __init__(self, period: int) -> None:
        if period < 1:
            raise ValueError("MA period must be >= 1")

        self.period = period
        self._values: deque[float] = deque(maxlen=period)
        self._sum: float = 0.0

    def update(self, candle) -> Optional[float]:
        """
        Process new candle and return updated MA value or None if not enough data.
        """

        close = candle.close

        # if deque is full, subtract the outgoing element
        if len(self._values) == self.period:
            oldest = self._values[0]
            self._sum -= oldest

        # append new close
        self._values.append(close)
        self._sum += close

        # if not enough data yet — no MA value
        if len(self._values) < self.period:
            return None

        # simple moving average: sum / period
        return self._sum / self.period
