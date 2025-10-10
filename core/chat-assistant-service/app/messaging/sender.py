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

import logging
from pydantic import BaseModel
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from .protocol import Outgoing, OutgoingAdapter

logger = logging.getLogger("websocket_endpoint")

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