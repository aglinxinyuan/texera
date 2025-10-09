# Licensed to the Apache Software Foundation (ASF) ...
import asyncio
import logging
import uuid
from typing import Any, Dict, List, AsyncGenerator

from agents import Agent, Runner, OpenAIResponsesModel, Model, AsyncOpenAI
from agents.extensions.visualization import draw_graph
from openai.types.responses import ResponseTextDeltaEvent, ResponseCompletedEvent
from starlette.websockets import WebSocket

from agents import trace  # tracing is owned here now
from app.services.agent_as_tools.texera_bot.agent_factory import AgentFactory
from app.services.agent_as_tools.texera_bot.settings import Settings
from app.services.agent_as_tools.texera_bot.tool_registry import ToolRegistry

settings = Settings()
logger = logging.getLogger(__name__)


class AgentSession:
    """
    Manages the lifetime, context, and agent graph for a single chat session.
    Owns its session_id and tracing lifecycle. Exposes resolvers so the WS layer
    never touches internals.
    """

    def __init__(
            self,
            model: str | Model = OpenAIResponsesModel(
                model="gpt-4o-mini", openai_client=AsyncOpenAI()
            ),
    ):
        self.model = model
        # ID & trace are owned here
        self.session_id: str = str(uuid.uuid4())
        self._trace_cm = trace(f"Texera Workflow Builder [{self.session_id}]")
        self._trace_cm.__enter__()  # open span

        # Internal coordination state
        self._schema_futures: Dict[str, asyncio.Future] = {}
        self._add_op_futures: Dict[str, asyncio.Future] = {}
        self._context: List[Dict[str, Any]] = []
        self._current_dag: List[Any] = []

    # ---- lifecycle ----------------------------------------------------------

    def close(self):
        """Close tracing and any other held resources."""
        if self._trace_cm:
            self._trace_cm.__exit__(None, None, None)

    @property
    def context(self) -> List[Dict[str, Any]]:
        return self._context

    # ---- agent construction -------------------------------------------------

    def _make_agents(self, websocket: WebSocket) -> Agent:
        # ToolRegistry needs the websocket to send tool-related requests;
        # we keep that dependency internal to the session layer.
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

    # ---- streaming chat -----------------------------------------------------

    async def stream_chat(
            self, websocket: WebSocket, user_message: str
    ) -> AsyncGenerator[str, None]:
        manager = self._make_agents(websocket)
        conversation = self._context + [{"role": "user", "content": user_message}]

        result = Runner.run_streamed(
            manager,
            input=conversation,
            max_turns=100,
        )

        async for evt in result.stream_events():
            if evt.type == "raw_response_event":
                if isinstance(evt.data, ResponseTextDeltaEvent):
                    yield evt.data.delta
                elif isinstance(evt.data, ResponseCompletedEvent):
                    # persist context at completion boundaries
                    self._context = result.to_input_list()
                else:
                    pass

        if result.is_complete and result.final_output:
            self._context = result.to_input_list()