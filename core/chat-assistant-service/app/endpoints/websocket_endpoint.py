# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import Any, Awaitable, Callable, Dict, Optional, Annotated, Union, Type

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError

# pydantic v1/v2 union parsing compatibility
try:
    from pydantic import TypeAdapter  # v2
    _HAS_V2 = True
except Exception:  # pragma: no cover
    _HAS_V2 = False
    from pydantic.tools import parse_obj_as  # v1

from typing_extensions import Literal  # works on 3.8+ and with pydantic v1/v2

from agents import trace
from app.services.agent_as_tools.service import AgentService

router = APIRouter()
logger = logging.getLogger("websocket_endpoint")

# ---------------------------------------------------------------------------
# Protocol models (discriminated by the 'type' field)
# ---------------------------------------------------------------------------

class CreateSessionRequest(BaseModel):
    type: Literal["CreateSessionRequest"]


class HeartBeatRequest(BaseModel):
    type: Literal["HeartBeatRequest"]


class OperatorSchemaResponse(BaseModel):
    type: Literal["OperatorSchemaResponse"]
    requestId: str
    schema: Any | None = None


class AddOperatorAndLinksResponse(BaseModel):
    type: Literal["AddOperatorAndLinksResponse"]
    requestId: str
    status: str | None = None


class ChatUserMessageRequest(BaseModel):
    type: Literal["ChatUserMessageRequest"]
    sessionId: str
    message: str


Incoming = Annotated[
    Union[
        CreateSessionRequest,
        HeartBeatRequest,
        OperatorSchemaResponse,
        AddOperatorAndLinksResponse,
        ChatUserMessageRequest,
    ],
    Field(discriminator="type"),
]


def parse_incoming(payload: dict) -> BaseModel:
    """Validate and coerce an incoming payload into the right message model."""
    if _HAS_V2:
        return TypeAdapter(Incoming).validate_python(payload)  # type: ignore[arg-type]
    # pydantic v1 path
    return parse_obj_as(Incoming, payload)  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

class Session:
    """Holds per-session resources and background tasks."""
    def __init__(self, service: AgentService, exit_span: Callable):
        self.service = service
        self._exit_span = exit_span
        self._tasks: set[asyncio.Task] = set()

    def add_task(self, task: asyncio.Task) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def close(self) -> None:
        # Cancel all background tasks
        for t in list(self._tasks):
            t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close AgentService (sync close to keep compatibility)
        with contextlib.suppress(Exception):
            if hasattr(self.service, "close"):
                self.service.close()

        # Close span (no exception propagation)
        with contextlib.suppress(Exception):
            self._exit_span(None, None, None)


class SessionManager:
    """Encapsulates the per-connection maps and lifecycle helpers."""
    def __init__(self) -> None:
        self.sessions: Dict[str, Session] = {}
        self.rid_to_session: Dict[str, str] = {}

    def get(self, sid: str) -> Optional[Session]:
        return self.sessions.get(sid)

    async def new_session(self) -> str:
        sid = str(uuid.uuid4())
        # Keep one OpenTelemetry span open for the session lifetime
        cm = trace(f"Texera Workflow Builder [{sid}]")
        cm.__enter__()  # open
        service = AgentService(session_id=sid, rid_registry=self.rid_to_session, trace_cm=cm)
        self.sessions[sid] = Session(service=service, exit_span=cm.__exit__)
        return sid

    async def close_all(self) -> None:
        await asyncio.gather(*(s.close() for s in self.sessions.values()), return_exceptions=True)
        self.sessions.clear()
        self.rid_to_session.clear()


# ---------------------------------------------------------------------------
# Handlers (registry keyed by message class)
# ---------------------------------------------------------------------------

Handler = Callable[[WebSocket, SessionManager, BaseModel], Awaitable[None]]
_HANDLERS: Dict[Type[BaseModel], Handler] = {}


def handles(model: Type[BaseModel]):
    """Decorator to register a handler for a given message model class."""
    def deco(fn: Handler) -> Handler:
        _HANDLERS[model] = fn
        return fn
    return deco


@handles(CreateSessionRequest)
async def on_create_session(ws: WebSocket, sm: SessionManager, msg: CreateSessionRequest) -> None:
    sid = await sm.new_session()
    await ws.send_json({"type": "CreateSessionResponse", "sessionId": sid})


@handles(HeartBeatRequest)
async def on_heartbeat(ws: WebSocket, _sm: SessionManager, msg: HeartBeatRequest) -> None:
    await ws.send_json({"type": "HeartBeatResponse"})


@handles(OperatorSchemaResponse)
async def on_operator_schema_response(ws: WebSocket, sm: SessionManager, msg: OperatorSchemaResponse) -> None:
    rid = msg.requestId
    sid = sm.rid_to_session.pop(rid, None)
    if not sid:
        return
    session = sm.get(sid)
    if not session:
        return
    fut = session.service._schema_futures.get(rid)
    if fut and not fut.done():
        fut.set_result(msg.schema)


@handles(AddOperatorAndLinksResponse)
async def on_add_operator_response(ws: WebSocket, sm: SessionManager, msg: AddOperatorAndLinksResponse) -> None:
    rid = msg.requestId
    sid = sm.rid_to_session.pop(rid, None)
    if not sid:
        return
    session = sm.get(sid)
    if not session:
        return
    fut = session.service._add_op_futures.get(rid)
    if fut and not fut.done():
        fut.set_result(msg.status)


@handles(ChatUserMessageRequest)
async def on_chat_user_message(ws: WebSocket, sm: SessionManager, msg: ChatUserMessageRequest) -> None:
    session = sm.get(msg.sessionId)
    if not session:
        await ws.send_json({"type": "Error", "error": "Invalid sessionId"})
        return

    text = (msg.message or "").strip()
    if not text:
        await ws.send_json({"type": "Error", "error": "Empty chat message."})
        return

    async def _stream() -> None:
        try:
            async for delta in session.service.stream_chat(ws, text):
                await ws.send_json({"type": "ChatStreamResponseEvent", "delta": delta})
        except Exception as e:
            logger.exception("stream_chat failed (sid=%s): %s", msg.sessionId, e)
            await ws.send_json({"type": "Error", "error": "Streaming failed"})
        finally:
            await ws.send_json({"type": "ChatStreamResponseComplete"})

    task = asyncio.create_task(_stream(), name=f"chat-stream:{msg.sessionId}")
    session.add_task(task)


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------

@router.websocket("/chat-assistant")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    sm = SessionManager()
    logger.info("WS connected")

    try:
        while True:
            try:
                payload = await ws.receive_json()
            except Exception as e:
                logger.warning("Bad JSON frame: %s", e)
                await ws.send_json({"type": "Error", "error": "Invalid JSON payload"})
                continue

            try:
                msg = parse_incoming(payload)
            except ValidationError as ve:
                await ws.send_json({"type": "Error", "error": f"Bad payload: {ve.errors()}"})
                continue

            handler = _HANDLERS.get(type(msg))
            if not handler:
                # Shouldn't happen if all models have handlers
                await ws.send_json({"type": "Error", "error": f"Unhandled message type {getattr(msg, 'type', 'Unknown')}"})
                continue

            try:
                await handler(ws, sm, msg)
            except Exception:
                logger.exception("Handler failed for message: %s", getattr(msg, "type", "Unknown"))
                await ws.send_json({"type": "Error", "error": "Internal error"})
    except WebSocketDisconnect:
        logger.info("WS disconnected")
    finally:
        await sm.close_all()