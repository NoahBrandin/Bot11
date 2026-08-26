"""Minimal Datastream-Layer runner: connects every feed and prints each
event to stdout as it arrives. No strategy/execution wiring -- just the raw
incoming data.
"""
from __future__ import annotations

import asyncio

from datastream import DatastreamLayer, PolymarketPriceChangeEvent


async def main() -> None:
    layer = DatastreamLayer()
    asyncio.create_task(layer.run())

    while True:
        event = await layer.queue.get()
        if isinstance(event, PolymarketPriceChangeEvent):
            continue
        print(event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
