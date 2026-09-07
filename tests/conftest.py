import asyncio
import selectors
import sys
from collections.abc import Callable


# psycopg async currently requires a selector-based event loop on Windows.
# Keep this test-only override isolated from application runtime configuration.
def pytest_asyncio_loop_factories() -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
    if sys.platform == "win32":
        return {"default": lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())}
    return {"default": asyncio.new_event_loop}
