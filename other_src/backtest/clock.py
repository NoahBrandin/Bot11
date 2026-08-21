"""A settable fake clock, injected into StrategyLayer/PaperExecutionLayer in
place of time.time() so replayed historical events see correct simulated
time (window countdowns, exit hysteresis) while the replay loop itself runs
unthrottled, not paced to real time.
"""
from __future__ import annotations


class SimulatedClock:
    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def set(self, t: float) -> None:
        self._now = t

    def __call__(self) -> float:
        return self._now
