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
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Optional, Annotated, Union, Type

from fastapi import APIRouter, WebSocket
from pydantic import BaseModel, Field, ValidationError, TypeAdapter
from typing_extensions import Literal
from starlette.websockets import WebSocketDisconnect, WebSocketState

from runtime.agent_session import AgentSession

router = APIRouter()
logger = logging.getLogger("websocket_endpoint")

# ---------------------------------------------------------------------------
# Protocol models (discriminated by the 'type' field)
# ---------------------------------------------------------------------------

# Incoming (client -> server)
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
    return TypeAdapter(Incoming).validate_python(payload)  # type: ignore[arg-type]


# Outgoing (server -> client)
class CreateSessionResponse(BaseModel):
    type: Literal["CreateSessionResponse"]
    sessionId: str


class HeartBeatResponse(BaseModel):
    type: Literal["HeartBeatResponse"]


class ErrorResponse(BaseModel):
    type: Literal["Error"]
    error: str


class ChatStreamResponseEvent(BaseModel):
    type: Literal["ChatStreamResponseEvent"]
    delta: Any


class ChatStreamResponseComplete(BaseModel):
    type: Literal["ChatStreamResponseComplete"]


Outgoing = Annotated[
    Union[
        CreateSessionResponse,
        HeartBeatResponse,
        ErrorResponse,
        ChatStreamResponseEvent,
        ChatStreamResponseComplete,
    ],
    Field(discriminator="type"),
]
OutgoingAdapter = TypeAdapter(Outgoing)


async def safe_send(ws: WebSocket, msg: BaseModel) -> None:
    """Attempt to send; drop silently if the websocket is already closed."""
    if ws.application_state is not WebSocketState.CONNECTED:
        return
    try:
        await ws.send_json(msg.model_dump())
    except (WebSocketDisconnect, RuntimeError):
        return
    except Exception:
        logger.exception("Failed to send frame (type=%s)", getattr(msg, "type", "Unknown"))

async def send_outgoing(ws: WebSocket, msg: Outgoing) -> None:
    """Validate and send a typed outbound message safely."""
    _ = OutgoingAdapter.validate_python(msg.model_dump())
    await safe_send(ws, msg)


# ---------------------------------------------------------------------------
# Session management (singleton per connection)
# ---------------------------------------------------------------------------

class SessionManager:
    """
    Per-connection singleton that holds exactly one active AgentSession
    and tracks background tasks. Creating a new session replaces the old one.
    Note: AgentSession is responsible for its own session_id and tracing.
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
        """
        Replace any existing AgentSession with a new one. AgentSession generates
        its own ID and opens its own trace.
        """
        await self._cancel_tasks()
        if self.session is not None:
            with contextlib.suppress(Exception):
                self.session.close()

        self.session = AgentSession()          # <-- no params; owns id/trace
        return self.session.session_id

    async def close_all(self) -> None:
        """Close everything on connection shutdown."""
        await self._cancel_tasks()
        if self.session is not None:
            with contextlib.suppress(Exception):
                self.session.close()
        self.session = None


# ---------------------------------------------------------------------------
# Handlers (registry keyed by message class)
# ---------------------------------------------------------------------------

Handler = Callable[[WebSocket, SessionManager, BaseModel], Awaitable[None]]
_HANDLERS: Dict[Type[BaseModel], Handler] = {}

def handles(model: Type[BaseModel]):
    def deco(fn: Handler) -> Handler:
        _HANDLERS[model] = fn
        return fn
    return deco


@handles(CreateSessionRequest)
async def on_create_session(ws: WebSocket, sm: SessionManager, msg: CreateSessionRequest) -> None:
    sid = await sm.new_session()
    await send_outgoing(ws, CreateSessionResponse(type="CreateSessionResponse", sessionId=sid))


@handles(HeartBeatRequest)
async def on_heartbeat(ws: WebSocket, _sm: SessionManager, msg: HeartBeatRequest) -> None:
    await send_outgoing(ws, HeartBeatResponse(type="HeartBeatResponse"))


@handles(OperatorSchemaResponse)
async def on_operator_schema_response(ws: WebSocket, sm: SessionManager, msg: OperatorSchemaResponse) -> None:
    sid = sm.current_sid
    if not sid:
        return
    session = sm.get(sid)
    if not session:
        return
    session.resolve_schema(msg.requestId, msg.schema)  # clean API, no privates


@handles(AddOperatorAndLinksResponse)
async def on_add_operator_response(ws: WebSocket, sm: SessionManager, msg: AddOperatorAndLinksResponse) -> None:
    sid = sm.current_sid
    if not sid:
        return
    session = sm.get(sid)
    if not session:
        return
    session.resolve_add_operator(msg.requestId, msg.status)  # clean API


@handles(ChatUserMessageRequest)
async def on_chat_user_message(ws: WebSocket, sm: SessionManager, msg: ChatUserMessageRequest) -> None:
    session = sm.get(msg.sessionId)
    if not session:
        await send_outgoing(ws, ErrorResponse(type="Error", error="Invalid sessionId"))
        return

    text = (msg.message or "").strip()
    if not text:
        await send_outgoing(ws, ErrorResponse(type="Error", error="Empty chat message."))
        return

    async def _stream() -> None:
        try:
            async for delta in session.stream_chat(ws, text):
                await send_outgoing(ws, ChatStreamResponseEvent(type="ChatStreamResponseEvent", delta=delta))
        except WebSocketDisconnect:
            return
        except Exception as e:
            logger.exception("stream_chat failed (sid=%s): %s", msg.sessionId, e)
            await send_outgoing(ws, ErrorResponse(type="Error", error="Streaming failed"))
        finally:
            await send_outgoing(ws, ChatStreamResponseComplete(type="ChatStreamResponseComplete"))

    sm.add_task(asyncio.create_task(_stream(), name=f"chat-stream:{msg.sessionId}"))


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
                text = await ws.receive_text()
            except WebSocketDisconnect as e:
                logger.info("WS disconnected (%s)", getattr(e, "code", "unknown"))
                break
            except Exception as e:
                logger.exception("Receive error: %s", e)
                break

            # Parse JSON
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as e:
                logger.warning("Bad JSON frame: %s", e)
                await send_outgoing(ws, ErrorResponse(type="Error", error="Invalid JSON payload"))
                continue

            # Validate & dispatch
            try:
                msg = parse_incoming(payload)
            except ValidationError as ve:
                await send_outgoing(ws, ErrorResponse(type="Error", error=f"Bad payload: {ve.errors()}"))
                continue

            handler = _HANDLERS.get(type(msg))
            if not handler:
                await send_outgoing(ws, ErrorResponse(type="Error", error=f"Unhandled message type {getattr(msg, 'type', 'Unknown')}"))
                continue

            try:
                await handler(ws, sm, msg)
            except WebSocketDisconnect:
                logger.info("Client disconnected during handler")
                break
            except Exception:
                logger.exception("Handler failed for message: %s", getattr(msg, "type", "Unknown"))
                await send_outgoing(ws, ErrorResponse(type="Error", error="Internal error"))
    finally:
        await sm.close_all()