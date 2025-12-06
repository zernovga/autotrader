from typing import Any, Iterable, Optional

from .indicators import IndicatorProtocol


class Instrument:
    __slots__ = ("figi", "indicators", "last_values")

    def __init__(self, figi: str):
        self.figi = figi
        self.indicators: list[Any] = []
        self.last_values: dict[str, Optional[float]] = {}

    def add_indicator(self, name: str, indicator: IndicatorProtocol) -> None:
        """
        Add indicator to the instrument.
        `name` — unique id, e.g. "ma20", "ema50", "atr14"
        """
        self.indicators.append((name, indicator))
        self.last_values[name] = None

    def on_new_candle(self, candle) -> dict[str, Optional[float]]:
        """
        Main processing entry point.
        Takes a candle, updates all indicators, stores last values.

        Returns dict of updated indicator values:
            {"ma20": float | None, "ema50": float | None, ...}
        """

        updated = {}

        for name, indicator in self.indicators:
            value = indicator.update(candle)
            self.last_values[name] = value
            updated[name] = value

        return updated

    def get_indicator_value(self, name: str) -> Optional[float]:
        """Return last calculated value for an indicator."""
        return self.last_values.get(name)

    def get_all_indicator_values(self) -> dict[str, Optional[float]]:
        """Return dict of all latest indicator values."""
        return dict(self.last_values)


class InstrumentManager:
    """
    Manages all trading instruments.
    Provides routing of candles to corresponding Instrument objects.
    """

    __slots__ = ("_instruments", "_autocreate")

    def __init__(
        self,
        figi_list: Optional[Iterable[str]] = None,
        autocreate: bool = True,
    ):
        """
        figi_list: optional predefined list of instruments
        autocreate: if True, new instruments are created on first use
        """
        self._instruments: dict[str, Instrument] = {}

        if figi_list:
            for figi in figi_list:
                self._instruments[figi] = Instrument(figi)

        self._autocreate = autocreate

    def get(self, figi: str) -> Optional[Instrument]:
        """Return Instrument by figi or None."""
        return self._instruments.get(figi)

    def all(self) -> dict[str, Instrument]:
        """Return dict of all instruments."""
        return self._instruments

    def ensure(self, figi: str) -> Instrument:
        """
        Return instrument by figi, creating it automatically if needed.
        Raise KeyError if autocreate=False and figi missing.
        """
        instr = self._instruments.get(figi)
        if instr is not None:
            return instr

        if not self._autocreate:
            raise KeyError(f"Instrument {figi} is not registered")

        # autocreate new instrument
        instr = Instrument(figi)
        self._instruments[figi] = instr
        return instr

    def process_message(self, candle) -> dict[str, Optional[float]]:
        """
        Given a candle with .figi attribute,
        route it to corresponding Instrument and update its indicators.

        Returns dict of updated indicator values or None.
        """

        figi = candle.figi
        instr = self.ensure(figi)
        return instr.on_new_candle(candle)
