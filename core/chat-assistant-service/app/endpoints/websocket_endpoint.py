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

import json
import logging
from fastapi import APIRouter, WebSocket
from pydantic import ValidationError
from starlette.websockets import WebSocketDisconnect

from messaging.protocol import (
    parse_incoming,
    ErrorResponse,
)
from messaging.sender import send_outgoing
from runtime.session_manager import SessionManager
from messaging.handlers import HANDLERS

router = APIRouter()
logger = logging.getLogger("websocket_endpoint")

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

            handler = HANDLERS.get(type(msg))
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