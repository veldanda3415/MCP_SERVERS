from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ConnectRequest(BaseModel):
    agent_id: str = Field(min_length=1)
    org_id: str = Field(min_length=1)
    version: str = Field(default="0.1.0", min_length=1)


class ConnectResponse(BaseModel):
    session_token: str
    session_expires_at: str
    server_version: str
    capabilities: list[str]


class CapabilitiesResponse(BaseModel):
    capabilities: list[str]
    tool_descriptions: dict[str, str]


class DatasetRegistrationRequest(BaseModel):
    dataset_id: str = Field(min_length=1)
    file_path: str = Field(min_length=1)


class QueryRequest(BaseModel):
    dataset_id: str = Field(min_length=1)
    sql_query: str = Field(min_length=1)


class QueryFilter(BaseModel):
    column: str = Field(min_length=1)
    op: Literal[
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "in",
        "between",
        "like",
        "is_null",
        "is_not_null",
    ]
    value: Any | None = None


class OrderByClause(BaseModel):
    column: str = Field(min_length=1)
    direction: Literal["ASC", "DESC", "asc", "desc"] = "ASC"


class QueryIntent(BaseModel):
    intent_type: Literal["aggregate", "select", "filter"]
    measures: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    filters: list[QueryFilter] = Field(default_factory=list)
    order_by: list[OrderByClause] = Field(default_factory=list)
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class ExecuteIntentRequest(BaseModel):
    dataset_id: str = Field(min_length=1)
    query_intent: QueryIntent


class GenerateQueryRequest(BaseModel):
    dataset_id: str = Field(min_length=1)
    query_intent: QueryIntent


class ToolRequest(BaseModel):
    tool_name: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)


class ColumnInfo(BaseModel):
    name: str
    type: str


class DatasetRegistrationResponse(BaseModel):
    dataset_id: str
    file_path: str
    table_name: str
    row_count: int
    columns: list[ColumnInfo]


class DatasetSchemaResponse(BaseModel):
    dataset_id: str
    row_count: int
    columns: list[ColumnInfo]


class QueryResponse(BaseModel):
    dataset_id: str
    generated_sql: str
    row_count: int
    columns: list[ColumnInfo]
    rows: list[list[Any]]


class GenerateQueryResponse(BaseModel):
    dataset_id: str
    generated_sql: str
    query_intent: QueryIntent
    validated: bool = True


class HealthResponse(BaseModel):
    status: str
    registered_datasets: int
