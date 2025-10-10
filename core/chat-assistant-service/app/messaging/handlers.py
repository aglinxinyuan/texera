from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Dict, Type
from pydantic import BaseModel
from starlette.websockets import WebSocket, WebSocketDisconnect

from .protocol import (
    CreateSessionRequest,
    HeartBeatRequest,
    OperatorSchemaResponse,
    AddOperatorAndLinksResponse,
    ChatUserMessageRequest,
    CreateSessionResponse,
    HeartBeatResponse,
    ErrorResponse,
    ChatStreamResponseEvent,
    ChatStreamResponseComplete,
)
from .sender import send_outgoing
from app.runtime.session_manager import SessionManager

logger = logging.getLogger("websocket_endpoint")

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
    session.resolve_schema(msg.requestId, msg.schema)

@handles(AddOperatorAndLinksResponse)
async def on_add_operator_response(ws: WebSocket, sm: SessionManager, msg: AddOperatorAndLinksResponse) -> None:
    sid = sm.current_sid
    if not sid:
        return
    session = sm.get(sid)
    if not session:
        return
    session.resolve_add_operator(msg.requestId, msg.status)

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

# Public exports
HANDLERS = _HANDLERS