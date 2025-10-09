# Licensed to the Apache Software Foundation (ASF) ...
import asyncio
import logging
import uuid
from typing import Any, Dict, List, AsyncGenerator

from agents import Agent, Runner, OpenAIResponsesModel, Model, AsyncOpenAI, SQLiteSession
from agents.extensions.visualization import draw_graph
from openai.types.responses import ResponseTextDeltaEvent, ResponseCompletedEvent
from starlette.websockets import WebSocket

from agents import trace
from app.texera_bot.agent_factory import AgentFactory
from app.texera_bot.settings import Settings
from app.texera_bot.tool_registry import ToolRegistry

settings = Settings()
logger = logging.getLogger(__name__)


class AgentSession:
    """
    Manages the lifetime, context, and agent graph for a single chat session.
    Owns its session_id, tracing, and session memory (via Agents SDK Sessions).
    """

    def __init__(
            self,
            model: str | Model = OpenAIResponsesModel(
                model="gpt-4o-mini", openai_client=AsyncOpenAI()
            ),
            # optionally allow persistence by passing a DB path later if you want:
            # db_path: str | None = None,
    ):
        self.model = model

        # ID & trace are owned here
        self.session_id: str = str(uuid.uuid4())
        self._trace_cm = trace(f"Texera Workflow Builder [{self.session_id}]")
        self._trace_cm.__enter__()  # open span

        # Native session memory (no manual to_input_list needed)
        # For persistent storage, you can do: SQLiteSession(self.session_id, "conversations.db")
        self._session = SQLiteSession(self.session_id)

        # Internal coordination state for tool callbacks
        self._schema_futures: Dict[str, asyncio.Future] = {}
        self._add_op_futures: Dict[str, asyncio.Future] = {}

        # Optional local read cache (handy if other parts of your app want to inspect history)
        self._context_cache: List[Dict[str, Any]] = []

        self._current_dag: List[Any] = []

    # ---- lifecycle ----------------------------------------------------------

    def close(self):
        """Close tracing and any other held resources."""
        if self._trace_cm:
            self._trace_cm.__exit__(None, None, None)

    async def get_context_items(self) -> List[Dict[str, Any]]:
        """Fetch the full conversation items from the session backend."""
        return await self._session.get_items()

    # ---- agent construction -------------------------------------------------

    def _make_agents(self, websocket: WebSocket) -> Agent:
        # ToolRegistry needs the websocket to send tool-related requests;
        # keep that dependency internal to the session layer.
        registry = ToolRegistry(
            self,  # parent session for callbacks
            websocket,
            self._schema_futures,
            self._add_op_futures,
            self._current_dag,
        )
        get_schema, add_ops, get_current_dag = registry.build()

        factory = AgentFactory(
            settings=settings,
            openai_client=AsyncOpenAI(),
            graph_drawer=draw_graph,
            get_schema=get_schema,
            add_ops=add_ops,
            get_current_dag=get_current_dag,
        )
        return factory.build()

    # ---- resolvers for WS messages -----------------------------------------

    def resolve_schema(self, request_id: str, schema: Any | None) -> bool:
        fut = self._schema_futures.get(request_id)
        if fut and not fut.done():
            fut.set_result(schema)
            return True
        return False

    def resolve_add_operator(self, request_id: str, status: str | None) -> bool:
        fut = self._add_op_futures.get(request_id)
        if fut and not fut.done():
            fut.set_result(status)
            return True
        return False

    # ---- streaming chat (now using Sessions) --------------------------------

    async def stream_chat(
            self, websocket: WebSocket, user_message: str
    ) -> AsyncGenerator[str, None]:
        agent = self._make_agents(websocket)

        # Sessions API keeps history for us; just pass session=self._session
        result = Runner.run_streamed(
            agent,
            input=user_message,
            session=self._session,  # <-- native session memory
        )

        async for evt in result.stream_events():
            if evt.type == "raw_response_event":
                if isinstance(evt.data, ResponseTextDeltaEvent):
                    yield evt.data.delta
                elif isinstance(evt.data, ResponseCompletedEvent):
                    # optional: refresh our local cache after an assistant turn completes
                    try:
                        self._context_cache = await self._session.get_items()
                    except Exception:
                        logger.exception("Failed to refresh session items cache")
                else:
                    pass

        # optional: also refresh cache at the very end (covers non-standard flows)
        try:
            self._context_cache = await self._session.get_items()
        except Exception:
            logger.exception("Failed to refresh session items cache at end of stream")