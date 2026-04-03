from __future__ import annotations

import re
from dataclasses import dataclass

from server.models import OrderByClause, QueryFilter, QueryIntent

AGGREGATE_PATTERN = re.compile(r"^(SUM|AVG|COUNT|MIN|MAX)\((\*|[A-Za-z_][A-Za-z0-9_]*)\)$", re.IGNORECASE)
ADDITIVE_AGGREGATE_PATTERN = re.compile(
    r"^(SUM|AVG)\(([A-Za-z_][A-Za-z0-9_]*(?:\s*\+\s*[A-Za-z_][A-Za-z0-9_]*)+)\)$",
    re.IGNORECASE,
)
COUNT_DISTINCT_PATTERN = re.compile(r"^COUNT_DISTINCT\(([A-Za-z_][A-Za-z0-9_]*)\)$", re.IGNORECASE)
GLOBAL_AVG_SENTINEL_PATTERN = re.compile(r"^__GLOBAL_AVG__\(([A-Za-z_][A-Za-z0-9_]*)\)$")
PROHIBITED_SQL_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|TRUNCATE|MERGE|CREATE|ALTER|DROP|RENAME|EXEC|EXECUTE|CALL|COPY|LOAD|ATTACH|DETACH)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class GeneratedQuery:
    sql: str


def validate_sql_query(sql_query: str) -> str:
    cleaned = sql_query.strip().rstrip(";").strip()
    if not cleaned:
        raise ValueError("SQL query cannot be empty.")
    if ";" in cleaned:
        raise ValueError("Only a single SQL statement is allowed.")
    if PROHIBITED_SQL_PATTERN.search(cleaned):
        raise ValueError("Only read-only SELECT queries are allowed.")
    if not cleaned.lower().startswith(("select", "with")):
        raise ValueError("Query must start with SELECT or WITH.")
    return cleaned


def build_limited_query(sql_query: str, max_rows: int) -> str:
    cleaned = validate_sql_query(sql_query)
    return f"SELECT * FROM ({cleaned}) AS limited_result LIMIT {max_rows}"


def build_sql_from_intent(intent: QueryIntent, available_columns: set[str], table_name: str, max_rows: int = 1000) -> GeneratedQuery:
    dimensions = _validate_dimensions(intent.dimensions, available_columns)
    measures, measure_alias_map, measure_expression_map = _validate_measures(intent.measures, available_columns)
    where_filters, having_filters = _build_filters(
        filters=intent.filters,
        available_columns=available_columns,
        measure_alias_map=measure_alias_map,
        measure_expression_map=measure_expression_map,
        table_name=table_name,
        aggregate_mode=intent.intent_type == "aggregate",
    )

    if intent.intent_type in {"select", "filter"} and measures:
        raise ValueError("Select and filter intents cannot include aggregate measures.")
    if intent.intent_type == "aggregate" and not measures:
        raise ValueError("Aggregate intents require at least one measure.")
    if not dimensions and not measures:
        raise ValueError("At least one dimension or measure is required.")

    distinct_clause = "DISTINCT " if intent.intent_type in {"select", "filter"} and not measures else ""

    select_parts: list[str] = []
    for dimension in dimensions:
        select_parts.append(f'{quote_identifier(dimension)} AS {quote_identifier(dimension)}')
    for expression, alias in measures:
        select_parts.append(f"{expression} AS {quote_identifier(alias)}")

    where_clause = ""
    if where_filters:
        where_clause = f" WHERE {' AND '.join(where_filters)}"

    group_by_clause = ""
    if measures and dimensions:
        group_by_clause = " GROUP BY " + ", ".join(quote_identifier(column) for column in dimensions)

    having_clause = ""
    if having_filters:
        having_clause = f" HAVING {' AND '.join(having_filters)}"

    order_by_clause = _build_order_by(intent.order_by, dimensions, measure_alias_map)
    limit = min(intent.limit, max_rows)
    offset_clause = f" OFFSET {intent.offset}" if intent.offset else ""

    sql = (
        f"SELECT {distinct_clause}{', '.join(select_parts)} "
        f"FROM {quote_identifier(table_name)}"
        f"{where_clause}"
        f"{group_by_clause}"
        f"{having_clause}"
        f"{order_by_clause}"
        f" LIMIT {limit}{offset_clause}"
    )
    return GeneratedQuery(sql=sql)


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _validate_dimensions(dimensions: list[str], available_columns: set[str]) -> list[str]:
    validated: list[str] = []
    for column in dimensions:
        if column not in available_columns:
            raise ValueError(f"Unknown dimension column: {column}")
        validated.append(column)
    return validated


def _validate_measures(
    measures: list[str],
    available_columns: set[str],
) -> tuple[list[tuple[str, str]], dict[str, str], dict[str, str]]:
    validated: list[tuple[str, str]] = []
    alias_map: dict[str, str] = {}
    expression_map: dict[str, str] = {}
    for measure in measures:
        candidate = measure.strip()
        distinct_match = COUNT_DISTINCT_PATTERN.match(candidate)
        if distinct_match:
            raw_column = distinct_match.group(1)
            if raw_column not in available_columns:
                raise ValueError(f"Unknown measure column: {raw_column}")
            expression = f"COUNT(DISTINCT {quote_identifier(raw_column)})"
            alias = f"count_distinct_{re.sub(r'[^A-Za-z0-9_]+', '_', raw_column).strip('_')}"
            validated.append((expression, alias))
            alias_map[measure] = alias
            alias_map[alias] = alias
            expression_map[alias] = expression
            continue

        additive_match = ADDITIVE_AGGREGATE_PATTERN.match(candidate)
        if additive_match:
            function_name, raw_expression = additive_match.groups()
            normalized_function = function_name.upper()
            raw_columns = [part.strip() for part in raw_expression.split("+")]
            for raw_column in raw_columns:
                if raw_column not in available_columns:
                    raise ValueError(f"Unknown measure column: {raw_column}")
            quoted_expression = " + ".join(quote_identifier(raw_column) for raw_column in raw_columns)
            expression = f"{normalized_function}({quoted_expression})"
            alias = _expression_measure_alias(normalized_function, raw_columns)
            validated.append((expression, alias))
            alias_map[measure] = alias
            alias_map[alias] = alias
            expression_map[alias] = expression
            continue

        match = AGGREGATE_PATTERN.match(candidate)
        if not match:
            raise ValueError(f"Unsupported measure expression: {measure}")
        function_name, raw_column = match.groups()
        normalized_function = function_name.upper()
        if raw_column != "*" and raw_column not in available_columns:
            raise ValueError(f"Unknown measure column: {raw_column}")
        expression = f"{normalized_function}({raw_column if raw_column == '*' else quote_identifier(raw_column)})"
        alias = _measure_alias(normalized_function, raw_column)
        validated.append((expression, alias))
        alias_map[measure] = alias
        alias_map[alias] = alias
        expression_map[alias] = expression
    return validated, alias_map, expression_map


def _measure_alias(function_name: str, column_name: str) -> str:
    if column_name == "*":
        return f"{function_name.lower()}_all"
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", column_name).strip("_")
    return f"{function_name.lower()}_{sanitized}"


def _expression_measure_alias(function_name: str, columns: list[str]) -> str:
    joined = "_".join(columns)
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", joined).strip("_")
    if len(sanitized) > 80:
        sanitized = sanitized[:80].rstrip("_")
    return f"{function_name.lower()}_{sanitized}"


def _build_filters(
    filters: list[QueryFilter],
    available_columns: set[str],
    measure_alias_map: dict[str, str],
    measure_expression_map: dict[str, str],
    table_name: str,
    aggregate_mode: bool,
) -> tuple[list[str], list[str]]:
    where_clauses: list[str] = []
    having_clauses: list[str] = []
    for item in filters:
        target = ""
        aggregate_target = False
        if aggregate_mode and item.column in measure_alias_map:
            alias = measure_alias_map[item.column]
            if alias not in measure_expression_map:
                raise ValueError(f"Unsupported aggregate filter target: {item.column}")
            target = measure_expression_map[alias]
            aggregate_target = True
        else:
            if item.column not in available_columns:
                raise ValueError(f"Unknown filter column: {item.column}")
            target = quote_identifier(item.column)

        rhs_global_avg = _global_avg_rhs(item.value, available_columns, table_name)
        rhs_literal = sql_literal(item.value)
        op = item.op
        if op == "eq":
            clause = f"{target} = {rhs_global_avg if rhs_global_avg else rhs_literal}"
        elif op == "neq":
            clause = f"{target} != {rhs_global_avg if rhs_global_avg else rhs_literal}"
        elif op == "gt":
            clause = f"{target} > {rhs_global_avg if rhs_global_avg else rhs_literal}"
        elif op == "gte":
            clause = f"{target} >= {rhs_global_avg if rhs_global_avg else rhs_literal}"
        elif op == "lt":
            clause = f"{target} < {rhs_global_avg if rhs_global_avg else rhs_literal}"
        elif op == "lte":
            clause = f"{target} <= {rhs_global_avg if rhs_global_avg else rhs_literal}"
        elif op == "like":
            clause = f"{target} LIKE {rhs_literal}"
        elif op == "in":
            if not isinstance(item.value, list) or not item.value:
                raise ValueError("The 'in' operator requires a non-empty list value.")
            values = ", ".join(sql_literal(value) for value in item.value)
            clause = f"{target} IN ({values})"
        elif op == "between":
            if not isinstance(item.value, list) or len(item.value) != 2:
                raise ValueError("The 'between' operator requires a two-item list value.")
            clause = f"{target} BETWEEN {sql_literal(item.value[0])} AND {sql_literal(item.value[1])}"
        elif op == "is_null":
            clause = f"{target} IS NULL"
        elif op == "is_not_null":
            clause = f"{target} IS NOT NULL"
        else:
            raise ValueError(f"Unsupported filter operator: {op}")

        if aggregate_target:
            having_clauses.append(clause)
        else:
            where_clauses.append(clause)
    return where_clauses, having_clauses


def _global_avg_rhs(value: object, available_columns: set[str], table_name: str) -> str | None:
    if not isinstance(value, str):
        return None
    match = GLOBAL_AVG_SENTINEL_PATTERN.match(value.strip())
    if not match:
        return None
    column_name = match.group(1)
    if column_name not in available_columns:
        raise ValueError(f"Unknown global average column: {column_name}")
    return f"(SELECT AVG({quote_identifier(column_name)}) FROM {quote_identifier(table_name)})"


def _build_order_by(order_by: list[OrderByClause], dimensions: list[str], measure_alias_map: dict[str, str]) -> str:
    if not order_by:
        return ""
    allowed_dimensions = {dimension: dimension for dimension in dimensions}
    clauses: list[str] = []
    for item in order_by:
        direction = item.direction.upper()
        if direction not in {"ASC", "DESC"}:
            raise ValueError(f"Unsupported sort direction: {item.direction}")
        if item.column in measure_alias_map:
            target = quote_identifier(measure_alias_map[item.column])
        elif item.column in allowed_dimensions:
            target = quote_identifier(allowed_dimensions[item.column])
        else:
            raise ValueError(f"Unsupported order_by target: {item.column}")
        clauses.append(f"{target} {direction}")
    return " ORDER BY " + ", ".join(clauses)
