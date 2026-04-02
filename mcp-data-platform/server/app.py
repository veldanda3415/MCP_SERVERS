from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from server.models import ExecuteIntentRequest, GenerateQueryRequest, QueryRequest
from server.query_engine import build_limited_query, build_sql_from_intent
from server.registry import DatasetRegistry
from server.session_store import SessionStore

mcp = FastMCP("MCP Data Platform Phase 0")
registry = DatasetRegistry()
sessions = SessionStore(ttl_hours=8)
MAX_QUERY_ROWS = 1000
SERVER_VERSION = "0.1.0"
CAPABILITIES: list[str] = [
    "connect",
    "capabilities",
    "register_dataset",
    "list_columns",
    "query",
    "generate_query",
    "execute_intent",
    "tool",
]
CAPABILITY_DESCRIPTIONS: dict[str, str] = {
    "connect": "Authenticate agent API key and issue a scoped session token.",
    "capabilities": "Return the MCP tool capabilities for this server.",
    "register_dataset": "Register a tabular dataset (CSV, Excel, Parquet, JSON, SQLite) for deterministic querying.",
    "list_columns": "Get discovered columns and inferred types for a dataset.",
    "query": "Run read-only SQL against the active dataset alias `dataset`.",
    "generate_query": "Generate deterministic SQL from structured intent without execution.",
    "execute_intent": "Validate structured intent and execute deterministic read-only SQL.",
    "tool": "Call MCP-style tools through a single generic tool wrapper.",
}


def _expected_api_key() -> str:
    return os.getenv("MCP_API_KEY", "demo-key")


def _ensure_api_key(api_key: str) -> None:
    if api_key != _expected_api_key():
        raise ValueError("Invalid API key.")


def _ensure_session(session_token: str) -> None:
    try:
        sessions.validate(session_token)
    except KeyError as exc:
        raise ValueError(str(exc)) from exc


@mcp.tool()
def connect(api_key: str, agent_id: str, org_id: str, version: str = "0.1.0") -> dict[str, Any]:
    _ensure_api_key(api_key)
    record = sessions.create_session(agent_id=agent_id, org_id=org_id)
    return {
        "session_token": record.session_token,
        "session_expires_at": record.expires_at.isoformat(),
        "server_version": SERVER_VERSION,
        "client_version": version,
        "capabilities": CAPABILITIES,
    }


@mcp.tool()
def capabilities(session_token: str) -> dict[str, Any]:
    _ensure_session(session_token)
    return {
        "capabilities": CAPABILITIES,
        "tool_descriptions": CAPABILITY_DESCRIPTIONS,
    }


@mcp.tool()
def register_dataset(session_token: str, dataset_id: str, file_path: str) -> dict[str, Any]:
    _ensure_session(session_token)
    response = registry.register_dataset(dataset_id, file_path)
    return response.model_dump()


@mcp.tool()
def list_columns(session_token: str, dataset_id: str) -> dict[str, Any]:
    _ensure_session(session_token)
    response = registry.get_schema(dataset_id)
    return response.model_dump()


@mcp.tool()
def query(session_token: str, dataset_id: str, sql_query: str) -> dict[str, Any]:
    _ensure_session(session_token)
    parsed = QueryRequest(dataset_id=dataset_id, sql_query=sql_query)
    sql = build_limited_query(parsed.sql_query, MAX_QUERY_ROWS)
    response = registry.execute_user_query(parsed.dataset_id, sql)
    return response.model_dump()


@mcp.tool()
def generate_query(session_token: str, dataset_id: str, query_intent: dict[str, Any]) -> dict[str, Any]:
    _ensure_session(session_token)
    parsed = GenerateQueryRequest(dataset_id=dataset_id, query_intent=query_intent)
    record = registry.get_record(parsed.dataset_id)
    available_columns = {column.name for column in record.columns}
    generated = build_sql_from_intent(parsed.query_intent, available_columns, record.table_name, MAX_QUERY_ROWS)
    return {
        "dataset_id": parsed.dataset_id,
        "generated_sql": generated.sql,
        "query_intent": parsed.query_intent.model_dump(),
        "validated": True,
    }


@mcp.tool()
def execute_intent(session_token: str, dataset_id: str, query_intent: dict[str, Any]) -> dict[str, Any]:
    _ensure_session(session_token)
    parsed = ExecuteIntentRequest(dataset_id=dataset_id, query_intent=query_intent)
    record = registry.get_record(parsed.dataset_id)
    available_columns = {column.name for column in record.columns}
    generated = build_sql_from_intent(parsed.query_intent, available_columns, record.table_name, MAX_QUERY_ROWS)
    response = registry.execute_sql(parsed.dataset_id, generated.sql)
    return response.model_dump()


@mcp.tool()
def tool(session_token: str, tool_name: str, params: dict[str, Any]) -> dict[str, Any]:
    _ensure_session(session_token)
    handlers: dict[str, Any] = {
        "register_dataset": register_dataset,
        "list_columns": list_columns,
        "query": query,
        "generate_query": generate_query,
        "execute_intent": execute_intent,
        "capabilities": capabilities,
    }
    handler = handlers.get(tool_name)
    if handler is None:
        raise ValueError(f"Unknown tool: {tool_name}")
    return handler(session_token=session_token, **params)


if __name__ == "__main__":
    mcp.run()
