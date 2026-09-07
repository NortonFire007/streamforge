import asyncio
import selectors
import sys
from collections.abc import Callable


def pytest_asyncio_loop_factories() -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
    if sys.platform == "win32":
        return {"default": lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())}
    return {"default": asyncio.new_event_loop}
