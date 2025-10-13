from __future__ import annotations

import asyncio
import contextlib
from typing import Optional

from .agent_session import AgentSession  # owns session_id & tracing


class SessionManager:
    """
    Per-connection singleton that holds exactly one active AgentSession
    and tracks background tasks. Creating a new session replaces the old one.
    """

    def __init__(self) -> None:
        self.session: Optional[AgentSession] = None
        self._tasks: set[asyncio.Task] = set()

    @property
    def current_sid(self) -> Optional[str]:
        return self.session.session_id if self.session else None

    def add_task(self, task: asyncio.Task) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def get(self, sid: str) -> Optional[AgentSession]:
        if not self.session or sid != self.session.session_id:
            return None
        return self.session

    async def _cancel_tasks(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def new_session(self) -> str:
        """Replace any existing AgentSession with a new one."""
        await self._cancel_tasks()
        if self.session is not None:
            with contextlib.suppress(Exception):
                self.session.close()
        self.session = AgentSession()  # AgentSession creates its own id/trace
        return self.session.session_id

    async def close_all(self) -> None:
        """Close everything on connection shutdown."""
        await self._cancel_tasks()
        if self.session is not None:
            with contextlib.suppress(Exception):
                self.session.close()
        self.session = None
