from __future__ import annotations

import json
import os
import sys
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class MCPDataClient:
    def __init__(
        self,
        api_key: str,
        server_command: str | None = None,
        server_args: list[str] | None = None,
        server_cwd: str | None = None,
        server_env: dict[str, str] | None = None,
    ) -> None:
        self.api_key = api_key
        self.session_token: str | None = None
        self._exit_stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None

        project_root = Path(__file__).resolve().parents[1]
        self._server_params = StdioServerParameters(
            command=server_command or sys.executable,
            args=server_args or ["-m", "server.app"],
            cwd=server_cwd or str(project_root),
            env=server_env or os.environ.copy(),
        )

    async def __aenter__(self) -> MCPDataClient:
        self._exit_stack = AsyncExitStack()
        read_stream, write_stream = await self._exit_stack.enter_async_context(stdio_client(self._server_params))
        self._session = await self._exit_stack.enter_async_context(ClientSession(read_stream, write_stream))
        await self._session.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._exit_stack is not None:
            await self._exit_stack.aclose()
        self._exit_stack = None
        self._session = None

    async def connect(self, agent_id: str, org_id: str, version: str = "0.1.0") -> dict[str, Any]:
        response = await self._call_tool(
            "connect",
            {
                "api_key": self.api_key,
                "agent_id": agent_id,
                "org_id": org_id,
                "version": version,
            },
        )
        session_token = response.get("session_token")
        if not isinstance(session_token, str) or not session_token:
            raise RuntimeError("Connect response did not include a valid session token.")
        self.session_token = session_token
        return response

    async def capabilities(self) -> dict[str, Any]:
        return await self._call_tool("capabilities", self._require_session_args())

    async def register_dataset(self, dataset_id: str, file_path: str) -> dict[str, Any]:
        return await self._call_tool(
            "register_dataset",
            self._require_session_args() | {"dataset_id": dataset_id, "file_path": file_path},
        )

    async def list_columns(self, dataset_id: str) -> dict[str, Any]:
        return await self._call_tool("list_columns", self._require_session_args() | {"dataset_id": dataset_id})

    async def query(self, dataset_id: str, sql_query: str) -> dict[str, Any]:
        return await self._call_tool(
            "query",
            self._require_session_args() | {"dataset_id": dataset_id, "sql_query": sql_query},
        )

    async def generate_query(self, dataset_id: str, query_intent: dict[str, Any]) -> dict[str, Any]:
        return await self._call_tool(
            "generate_query",
            self._require_session_args() | {"dataset_id": dataset_id, "query_intent": query_intent},
        )

    async def execute_intent(self, dataset_id: str, query_intent: dict[str, Any]) -> dict[str, Any]:
        return await self._call_tool(
            "execute_intent",
            self._require_session_args() | {"dataset_id": dataset_id, "query_intent": query_intent},
        )

    async def tool(self, tool_name: str, params: dict[str, Any]) -> dict[str, Any]:
        return await self._call_tool(
            "tool",
            self._require_session_args() | {"tool_name": tool_name, "params": params},
        )

    async def list_tools(self) -> dict[str, Any]:
        if self._session is None:
            raise RuntimeError("MCP client session is not initialized.")
        response = await self._session.list_tools()
        return response.model_dump()

    async def _call_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._session is None:
            raise RuntimeError("MCP client session is not initialized.")

        result = await self._session.call_tool(tool_name, arguments)
        if getattr(result, "isError", False):
            content = getattr(result, "content", []) or []
            messages: list[str] = []
            for item in content:
                text_value = getattr(item, "text", None)
                if isinstance(text_value, str):
                    messages.append(text_value)
            if messages:
                raise RuntimeError("\n".join(messages))
            raise RuntimeError(f"MCP tool call failed: {tool_name}")

        structured = getattr(result, "structuredContent", None)
        if isinstance(structured, dict):
            return structured

        content = getattr(result, "content", []) or []
        text_chunks: list[str] = []
        for item in content:
            text_value = getattr(item, "text", None)
            if isinstance(text_value, str):
                text_chunks.append(text_value)

        if len(text_chunks) == 1:
            try:
                parsed = json.loads(text_chunks[0])
            except json.JSONDecodeError:
                return {"text": text_chunks[0]}
            if isinstance(parsed, dict):
                return parsed
            return {"value": parsed}

        if text_chunks:
            return {"text": "\n".join(text_chunks)}

        return {}

    def _require_session_args(self) -> dict[str, str]:
        if not self.session_token:
            raise RuntimeError("Session token not available. Call connect first.")
        return {"session_token": self.session_token}
