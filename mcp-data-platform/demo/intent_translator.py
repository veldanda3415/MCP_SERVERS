from __future__ import annotations

import json
import os
import re
from typing import Any

import requests


def strip_code_fence(value: str) -> str:
    stripped = value.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 2:
            return "\n".join(lines[1:-1]).strip()
    return stripped


def normalize_cli_token(value: str) -> str:
    cleaned = value.strip()
    if cleaned and cleaned[0] in {"'", '"'}:
        cleaned = cleaned[1:]
    if cleaned and cleaned[-1] in {"'", '"'}:
        cleaned = cleaned[:-1]
    return cleaned.strip()


def normalize_question_text(value: str) -> str:
    cleaned = value.strip()
    if cleaned.startswith("<") and cleaned.endswith(">") and len(cleaned) > 2:
        cleaned = cleaned[1:-1]
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def contains_any(value: str, phrases: list[str]) -> bool:
    return any(phrase in value for phrase in phrases)


def normalize_column_name(column_name: str) -> str:
    return column_name.strip().lower().replace("_", " ")


def is_numeric_type(type_name: str) -> bool:
    token = type_name.lower()
    return any(part in token for part in ["int", "decimal", "numeric", "double", "float", "real", "bigint", "smallint"])


def infer_metric_column(normalized_question: str, columns: list[dict[str, Any]]) -> str | None:
    preferred_terms = ["amount", "price", "cost", "revenue", "sales", "score", "value", "count", "bytes", "latency", "duration", "volume"]
    numeric_columns = [
        column for column in columns if isinstance(column, dict) and is_numeric_type(str(column.get("type", "")))
    ]
    if not numeric_columns:
        return None

    for term in preferred_terms:
        if term not in normalized_question:
            continue
        for column in numeric_columns:
            column_name = str(column.get("name", ""))
            if term in normalize_column_name(column_name):
                return column_name

    for column in numeric_columns:
        column_name = str(column.get("name", ""))
        if normalize_column_name(column_name) in normalized_question:
            return column_name

    return str(numeric_columns[0].get("name", "")) or None


def infer_dimension_column(normalized_question: str, columns: list[dict[str, Any]], avoid: set[str] | None = None) -> str | None:
    avoid = avoid or set()
    text_columns = [
        column
        for column in columns
        if isinstance(column, dict)
        and str(column.get("name", "")) not in avoid
        and not is_numeric_type(str(column.get("type", "")))
    ]
    if not text_columns:
        return None

    for column in text_columns:
        column_name = str(column.get("name", ""))
        normalized_column = normalize_column_name(column_name)
        if normalized_column in normalized_question:
            return column_name
        singular = normalized_column[:-1] if normalized_column.endswith("s") else normalized_column
        plural = f"{normalized_column}s" if not normalized_column.endswith("s") else normalized_column
        if singular in normalized_question or plural in normalized_question:
            return column_name

    preferred_dimensions = ["product", "customer", "student", "region", "category", "status", "name", "host", "ip", "date"]
    for key in preferred_dimensions:
        if key not in normalized_question:
            continue
        for column in text_columns:
            column_name = str(column.get("name", ""))
            if key in normalize_column_name(column_name):
                return column_name

    return str(text_columns[0].get("name", "")) or None


def infer_identifier_columns(normalized_question: str, columns: list[dict[str, Any]], metric_column: str | None) -> list[str]:
    preferred_tokens = ["name", "id", "ip", "host", "source", "destination", "product", "customer", "student", "region", "date", "class", "status"]
    ranked: list[tuple[int, str]] = []
    avoid = {metric_column} if metric_column else set()

    for index, column in enumerate(columns):
        column_name = str(column.get("name", ""))
        if not column_name or column_name in avoid or is_numeric_type(str(column.get("type", ""))):
            continue

        normalized_column = normalize_column_name(column_name)
        score = 0
        if normalized_column in normalized_question:
            score += 100
        if any(token in normalized_column for token in preferred_tokens):
            score += 25
        if any(token in normalized_question and token in normalized_column for token in preferred_tokens):
            score += 25
        score += max(0, 10 - index)
        ranked.append((score, column_name))

    ranked.sort(key=lambda item: (-item[0], item[1]))
    selected = [column_name for _, column_name in ranked[:3]]
    if not selected and columns:
        fallback = infer_dimension_column(normalized_question, columns, avoid=avoid)
        if fallback:
            selected = [fallback]
    return selected


def infer_any_column(normalized_question: str, columns: list[dict[str, Any]], preferred_tokens: list[str] | None = None) -> str | None:
    preferred_tokens = preferred_tokens or []
    scored: list[tuple[int, str]] = []

    for index, column in enumerate(columns):
        column_name = str(column.get("name", ""))
        if not column_name:
            continue
        normalized_column = normalize_column_name(column_name)
        score = 0
        if normalized_column in normalized_question:
            score += 100
        singular = normalized_column[:-1] if normalized_column.endswith("s") else normalized_column
        if singular and singular in normalized_question:
            score += 40
        if any(token in normalized_question and token in normalized_column for token in preferred_tokens):
            score += 25
        score += max(0, 10 - index)
        scored.append((score, column_name))

    if not scored:
        return None
    scored.sort(key=lambda item: (-item[0], item[1]))
    return scored[0][1]


AGGREGATE_MEASURE_PATTERN = re.compile(r"^(sum|avg|count|min|max)\((\*|[A-Za-z_][A-Za-z0-9_]*)\)$", re.IGNORECASE)
GENERIC_AGGREGATE_PATTERN = re.compile(r"^(sum|avg|count|min|max)\((.+)\)$", re.IGNORECASE)
NESTED_AGGREGATE_PATTERN = re.compile(r"^(sum|avg|count|min|max)\((sum|avg|count|min|max)\((.+)\)\)$", re.IGNORECASE)


def measure_is_count_like(measure: str) -> bool:
    candidate = measure.strip()
    if not candidate:
        return False
    if candidate.upper().startswith("COUNT_DISTINCT("):
        return True
    aggregate_match = AGGREGATE_MEASURE_PATTERN.match(candidate)
    if not aggregate_match:
        return False
    func_name, _ = aggregate_match.groups()
    return func_name.lower() == "count"


def normalize_intent(intent: dict[str, Any]) -> dict[str, Any]:
    intent_type = str(intent.get("intent_type") or "select").lower()
    if intent_type not in {"aggregate", "select", "filter"}:
        intent_type = "aggregate" if intent.get("measures") else "select"

    dimensions = [str(value) for value in intent.get("dimensions", []) if isinstance(value, str) and value.strip()]

    normalized_measures: list[str] = []
    measure_by_raw_column: dict[str, str] = {}
    for measure in intent.get("measures", []):
        if not isinstance(measure, str):
            continue
        candidate = measure.strip()
        if not candidate:
            continue
        nested_match = NESTED_AGGREGATE_PATTERN.match(candidate)
        if nested_match:
            outer_function, _, inner_expression = nested_match.groups()
            candidate = f"{outer_function.upper()}({inner_expression.strip()})"

        aggregate_match = AGGREGATE_MEASURE_PATTERN.match(candidate)
        if aggregate_match:
            func_name, raw_column = aggregate_match.groups()
            normalized = f"{func_name.upper()}({raw_column})"
            normalized_measures.append(normalized)
            if raw_column != "*":
                measure_by_raw_column[raw_column] = normalized
            continue

        generic_aggregate_match = GENERIC_AGGREGATE_PATTERN.match(candidate)
        if generic_aggregate_match:
            func_name, raw_expression = generic_aggregate_match.groups()
            normalized = f"{func_name.upper()}({raw_expression.strip()})"
            normalized_measures.append(normalized)
            continue

        if intent_type == "aggregate":
            normalized = f"SUM({candidate})"
            normalized_measures.append(normalized)
            measure_by_raw_column[candidate] = normalized

    normalized_filters: list[dict[str, Any]] = []
    for item in intent.get("filters", []):
        if not isinstance(item, dict):
            continue
        column = item.get("column") or item.get("field")
        operator = item.get("op") or item.get("operator")
        if not isinstance(column, str) or not column.strip() or not isinstance(operator, str) or not operator.strip():
            continue
        normalized_filters.append({"column": column, "op": operator, "value": item.get("value")})

    normalized_order_by: list[dict[str, str]] = []
    for item in intent.get("order_by", []):
        if not isinstance(item, dict):
            continue
        column = item.get("column") or item.get("field")
        if not isinstance(column, str) or not column.strip():
            continue
        if intent_type == "aggregate" and column in measure_by_raw_column:
            column = measure_by_raw_column[column]
        direction = str(item.get("direction") or "ASC").upper()
        if direction not in {"ASC", "DESC"}:
            direction = "ASC"
        normalized_order_by.append({"column": column, "direction": direction})

    try:
        limit = int(intent.get("limit", 100))
    except (TypeError, ValueError):
        limit = 100
    limit = max(1, min(limit, 1000))

    try:
        offset = int(intent.get("offset", 0))
    except (TypeError, ValueError):
        offset = 0
    offset = max(0, offset)

    if intent_type in {"select", "filter"}:
        normalized_measures = []

    return {
        "intent_type": intent_type,
        "measures": normalized_measures,
        "dimensions": dimensions,
        "filters": normalized_filters,
        "order_by": normalized_order_by,
        "limit": limit,
        "offset": offset,
    }


def repair_intent_for_question(question: str, intent: dict[str, Any], columns: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_question = normalize_question_text(question).lower()
    asks_highest = contains_any(normalized_question, ["highest", "max", "maximum", "top", "best", "largest"])
    asks_lowest = contains_any(normalized_question, ["lowest", "min", "minimum", "bottom", "least"])
    asks_comparative = contains_any(normalized_question, ["higher", "lower", "greater", "smaller"])
    asks_count = contains_any(normalized_question, ["how many", "number of", "count", "total people", "total passengers"])
    asks_distinct = contains_any(normalized_question, ["different", "distinct", "unique"])
    asks_existence = contains_any(normalized_question, ["exist", "exists", "available", "present", "there are", "there is"])
    asks_anomaly = contains_any(normalized_question, ["unusual", "above average", "anomaly", "anomalous"])
    asks_combined_error = contains_any(normalized_question, ["combined error", "combined errors", "combined error rate", "combined error rates"])
    asks_high_error_low_traffic = contains_any(normalized_question, ["high error", "higher error"]) and contains_any(
        normalized_question,
        ["low traffic", "lower traffic", "low bytes", "lower bytes", "low volume", "lower volume"],
    )
    requests_entity = contains_any(normalized_question, ["which", "who", "what", "whose"])
    requests_rollup = contains_any(normalized_question, ["total", "sum", "average", "avg", "count"])
    metric_column = infer_metric_column(normalized_question, columns)
    numeric_column_names = {
        str(column.get("name", ""))
        for column in columns
        if isinstance(column, dict) and is_numeric_type(str(column.get("type", "")))
    }
    ranking_metric_column = metric_column
    order_by_items = intent.get("order_by", []) if isinstance(intent.get("order_by"), list) else []
    if order_by_items and isinstance(order_by_items[0], dict):
        order_column = order_by_items[0].get("column")
        if isinstance(order_column, str) and order_column in numeric_column_names:
            ranking_metric_column = order_column
    intent_dimensions = intent.get("dimensions", []) if isinstance(intent.get("dimensions"), list) else []
    for candidate in intent_dimensions:
        if isinstance(candidate, str) and candidate in numeric_column_names:
            ranking_metric_column = candidate
            break
    has_non_count_measures = any(
        isinstance(measure, str) and not measure_is_count_like(measure)
        for measure in intent.get("measures", [])
    )
    column_names = [str(column.get("name", "")) for column in columns if isinstance(column, dict) and str(column.get("name", ""))]
    error_rate_columns = [
        name
        for name in column_names
        if "error rate" in normalize_column_name(name) or "error_rate" in normalize_column_name(name)
    ]

    def find_numeric_column_by_tokens(tokens: list[str]) -> str | None:
        for column in columns:
            if not isinstance(column, dict):
                continue
            column_name = str(column.get("name", ""))
            if not column_name:
                continue
            if not is_numeric_type(str(column.get("type", ""))):
                continue
            normalized_name = normalize_column_name(column_name)
            if any(token in normalized_name for token in tokens):
                return column_name
        return None

    def preferred_service_dimension() -> str | None:
        service_column = infer_any_column(normalized_question, columns, preferred_tokens=["service"])
        if isinstance(service_column, str) and service_column:
            return service_column
        fallback_service_columns = [name for name in column_names if normalize_column_name(name) in {"service", "protocol", "name"}]
        if fallback_service_columns:
            return fallback_service_columns[0]
        return infer_dimension_column(normalized_question, columns)

    if asks_high_error_low_traffic:
        service_column = preferred_service_dimension()
        error_column = find_numeric_column_by_tokens(["serror", "rerror", "error rate", "error"])
        if not error_column and error_rate_columns:
            error_column = error_rate_columns[0]
        traffic_column = find_numeric_column_by_tokens(["src bytes", "src_bytes", "bytes", "traffic", "volume"])
        if not traffic_column:
            for name in column_names:
                if normalize_column_name(name) in {"src bytes", "src_bytes", "bytes", "traffic", "volume"}:
                    traffic_column = name
                    break
        if service_column and error_column and traffic_column:
            error_measure = f"AVG({error_column})"
            traffic_measure = f"AVG({traffic_column})"
            return {
                "intent_type": "aggregate",
                "measures": [error_measure, traffic_measure],
                "dimensions": [service_column],
                "filters": [
                    {"column": error_measure, "op": "gt", "value": f"__GLOBAL_AVG__({error_column})"},
                    {"column": traffic_measure, "op": "lt", "value": f"__GLOBAL_AVG__({traffic_column})"},
                ],
                "order_by": [{"column": error_measure, "direction": "DESC"}],
                "limit": 100,
                "offset": 0,
            }

    if asks_anomaly:
        service_column = preferred_service_dimension()
        anomaly_column = find_numeric_column_by_tokens(["serror", "rerror", "error rate", "error"])
        if not anomaly_column and error_rate_columns:
            anomaly_column = error_rate_columns[0]
        if service_column and anomaly_column:
            measure = f"AVG({anomaly_column})"
            return {
                "intent_type": "aggregate",
                "measures": [measure],
                "dimensions": [service_column],
                "filters": [{"column": measure, "op": "gt", "value": f"__GLOBAL_AVG__({anomaly_column})"}],
                "order_by": [{"column": measure, "direction": "DESC"}],
                "limit": 100,
                "offset": 0,
            }

    if asks_combined_error and error_rate_columns:
        service_column = preferred_service_dimension()
        if service_column:
            combined_measure = f"AVG({' + '.join(error_rate_columns)})"
            raw_limit = intent.get("limit", 5)
            try:
                limit = max(1, min(int(raw_limit), 1000))
            except (TypeError, ValueError):
                limit = 5
            return {
                "intent_type": "aggregate",
                "measures": [combined_measure],
                "dimensions": [service_column],
                "filters": [],
                "order_by": [{"column": combined_measure, "direction": "DESC"}],
                "limit": limit,
                "offset": 0,
            }

    if asks_count and asks_distinct:
        hinted_column = infer_any_column(normalized_question, columns, preferred_tokens=["fare", "price", "ticket", "name", "id"])
        if not hinted_column:
            hinted_column = intent.get("dimensions", [None])[0] if isinstance(intent.get("dimensions"), list) else None
        if isinstance(hinted_column, str) and hinted_column:
            return {
                "intent_type": "aggregate",
                "measures": [f"COUNT_DISTINCT({hinted_column})"],
                "dimensions": [],
                "filters": intent.get("filters", []),
                "order_by": [],
                "limit": 1,
                "offset": 0,
            }

    if asks_count and asks_existence and not contains_any(
        normalized_question,
        ["surviv", "alive", "and how many", "total people", "total passengers", "in total", "overall"],
    ):
        hinted_column = infer_any_column(normalized_question, columns)
        if not hinted_column:
            hinted_column = intent.get("dimensions", [None])[0] if isinstance(intent.get("dimensions"), list) else None
        if isinstance(hinted_column, str) and hinted_column and hinted_column in {
            str(column.get("name", "")) for column in columns if isinstance(column, dict)
        }:
            return {
                "intent_type": "aggregate",
                "measures": [f"COUNT_DISTINCT({hinted_column})"],
                "dimensions": [],
                "filters": intent.get("filters", []),
                "order_by": [],
                "limit": 1,
                "offset": 0,
            }

    if asks_count and intent.get("intent_type") == "aggregate" and (not intent.get("measures") or has_non_count_measures):
        survival_column = infer_any_column(normalized_question, columns, preferred_tokens=["surviv", "alive", "status"])
        if survival_column and contains_any(normalized_question, ["surviv", "alive"]):
            return {
                "intent_type": "aggregate",
                "measures": ["COUNT(*)"],
                "dimensions": [survival_column],
                "filters": intent.get("filters", []),
                "order_by": [{"column": survival_column, "direction": "ASC"}],
                "limit": 10,
                "offset": 0,
            }
        return {
            "intent_type": "aggregate",
            "measures": ["COUNT(*)"],
            "dimensions": [],
            "filters": intent.get("filters", []),
            "order_by": [],
            "limit": 1,
            "offset": 0,
        }

    if intent.get("intent_type") in {"select", "filter"} and not intent.get("measures") and ranking_metric_column and (asks_highest or asks_lowest or asks_comparative):
        dimension_names = [value for value in intent_dimensions if isinstance(value, str) and value]
        non_metric_dimensions = [value for value in dimension_names if value != ranking_metric_column]
        if not non_metric_dimensions:
            direction = "DESC" if asks_highest else "ASC"
            dimensions = infer_identifier_columns(normalized_question, columns, ranking_metric_column)
            if ranking_metric_column not in dimensions:
                dimensions.append(ranking_metric_column)
            return {
                "intent_type": "select",
                "measures": [],
                "dimensions": dimensions,
                "filters": intent.get("filters", []),
                "order_by": [{"column": ranking_metric_column, "direction": direction}],
                "limit": 1,
                "offset": 0,
            }

        primary_order_by = order_by_items[0] if order_by_items and isinstance(order_by_items[0], dict) else None
        order_column = primary_order_by.get("column") if isinstance(primary_order_by, dict) else None
        order_direction = str(primary_order_by.get("direction") or "ASC").upper() if isinstance(primary_order_by, dict) else "ASC"
        if order_direction not in {"ASC", "DESC"}:
            order_direction = "ASC"
        if isinstance(order_column, str) and order_column in numeric_column_names and order_column not in dimension_names:
            raw_limit = intent.get("limit", 1)
            try:
                limit = max(1, min(int(raw_limit), 1000))
            except (TypeError, ValueError):
                limit = 1
            return {
                "intent_type": "select",
                "measures": [],
                "dimensions": dimension_names + [order_column],
                "filters": intent.get("filters", []),
                "order_by": [{"column": order_column, "direction": order_direction}],
                "limit": limit,
                "offset": 0,
            }

    if intent.get("intent_type") == "aggregate" and intent.get("measures") and intent.get("dimensions") and asks_comparative:
        repaired = dict(intent)
        repaired["limit"] = 1
        if not repaired.get("order_by"):
            direction = "DESC" if contains_any(normalized_question, ["higher", "greater"]) else "ASC"
            first_measure = repaired["measures"][0] if isinstance(repaired["measures"], list) and repaired["measures"] else None
            if isinstance(first_measure, str) and first_measure:
                repaired["order_by"] = [{"column": first_measure, "direction": direction}]
        return repaired

    if not ranking_metric_column or requests_rollup or not requests_entity:
        return intent
    if not (asks_highest or asks_lowest or asks_comparative):
        return intent
    if intent.get("intent_type") != "aggregate":
        return intent

    direction = "DESC" if asks_highest else "ASC"
    dimensions = infer_identifier_columns(normalized_question, columns, ranking_metric_column)
    if ranking_metric_column not in dimensions:
        dimensions.append(ranking_metric_column)

    return {
        "intent_type": "select",
        "measures": [],
        "dimensions": dimensions,
        "filters": intent.get("filters", []),
        "order_by": [{"column": ranking_metric_column, "direction": direction}],
        "limit": 1,
        "offset": 0,
    }


def rule_based_question_to_intent(question: str, columns: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    normalized = normalize_question_text(question).lower()
    if not normalized:
        raise ValueError("Question cannot be empty.")

    columns = columns or [
        {"name": "date", "type": "VARCHAR"},
        {"name": "region", "type": "VARCHAR"},
        {"name": "product", "type": "VARCHAR"},
        {"name": "amount", "type": "DOUBLE"},
        {"name": "status", "type": "VARCHAR"},
    ]

    asks_highest = contains_any(normalized, ["highest", "max", "maximum", "top", "best"])
    mentions_amount = contains_any(normalized, ["amount", "revenue", "sales", "value"])
    mentions_product = contains_any(normalized, ["product", "products", "item", "items"])
    asks_list = contains_any(normalized, ["list", "show", "what", "which", "present", "available", "have"])
    mentions_region = "region" in normalized
    mentions_orders = contains_any(normalized, ["order", "orders", "transaction", "transactions", "records"])
    metric_column = infer_metric_column(normalized, columns)

    if asks_highest and metric_column and ("date" in normalized or "when" in normalized):
        date_column = infer_dimension_column("date", columns, avoid={metric_column}) or "date"
        return {
            "intent_type": "select",
            "measures": [],
            "dimensions": [date_column, metric_column],
            "filters": [],
            "order_by": [{"column": metric_column, "direction": "DESC"}],
            "limit": 1,
            "offset": 0,
        }

    if mentions_region and mentions_amount and contains_any(normalized, ["total", "sum", "by", "per"]):
        metric_column = metric_column or "amount"
        dimension_column = infer_dimension_column("region", columns, avoid={metric_column}) or "region"
        filters: list[dict[str, Any]] = []
        if "completed" in normalized:
            status_column = infer_dimension_column("status", columns, avoid={metric_column}) or "status"
            filters.append({"column": status_column, "op": "eq", "value": "completed"})
        if "q1" in normalized:
            date_column = infer_dimension_column("date", columns, avoid={metric_column}) or "date"
            filters.append({"column": date_column, "op": "between", "value": ["2026-01-01", "2026-03-31"]})
        return {
            "intent_type": "aggregate",
            "measures": [f"SUM({metric_column})"],
            "dimensions": [dimension_column],
            "filters": filters,
            "order_by": [{"column": f"SUM({metric_column})", "direction": "DESC"}],
            "limit": 100,
            "offset": 0,
        }

    if asks_list and mentions_orders:
        filters: list[dict[str, Any]] = []
        if "completed" in normalized:
            status_column = infer_dimension_column("status", columns) or "status"
            filters.append({"column": status_column, "op": "eq", "value": "completed"})
        if "west" in normalized:
            region_column = infer_dimension_column("region", columns) or "region"
            filters.append({"column": region_column, "op": "eq", "value": "West"})

        select_dimensions: list[str] = []
        for column in columns:
            column_name = str(column.get("name", ""))
            if column_name:
                select_dimensions.append(column_name)
            if len(select_dimensions) >= 5:
                break
        if not select_dimensions:
            raise ValueError("No selectable columns found in dataset schema.")

        order_column = infer_dimension_column(normalized, columns) or select_dimensions[0]
        return {
            "intent_type": "select",
            "measures": [],
            "dimensions": select_dimensions,
            "filters": filters,
            "order_by": [{"column": order_column, "direction": "ASC"}],
            "limit": 25,
            "offset": 0,
        }

    if mentions_product and asks_highest and mentions_amount:
        metric_column = metric_column or "amount"
        product_column = infer_dimension_column("product", columns, avoid={metric_column}) or infer_dimension_column(normalized, columns, avoid={metric_column})
        if not product_column:
            raise ValueError("Could not infer a grouping column for highest-value query.")
        return {
            "intent_type": "aggregate",
            "measures": [f"MAX({metric_column})"],
            "dimensions": [product_column],
            "filters": [],
            "order_by": [{"column": f"MAX({metric_column})", "direction": "DESC"}],
            "limit": 1,
            "offset": 0,
        }

    if mentions_product and asks_list:
        product_column = infer_dimension_column("product", columns) or infer_dimension_column(normalized, columns)
        if not product_column:
            raise ValueError("Could not infer a categorical column to list from the dataset schema.")
        return {
            "intent_type": "select",
            "measures": [],
            "dimensions": [product_column],
            "filters": [],
            "order_by": [{"column": product_column, "direction": "ASC"}],
            "limit": 100,
            "offset": 0,
        }

    revenue_match = re.search(r"total revenue(?: for)? (?P<region>north|south|east|west)", normalized)
    if revenue_match:
        region = revenue_match.group("region").capitalize()
        metric_column = metric_column or "amount"
        region_column = infer_dimension_column("region", columns, avoid={metric_column}) or "region"
        return {
            "intent_type": "aggregate",
            "measures": [f"SUM({metric_column})"],
            "dimensions": [region_column],
            "filters": [{"column": region_column, "op": "eq", "value": region}],
            "order_by": [],
            "limit": 10,
            "offset": 0,
        }

    if asks_highest and metric_column:
        dimension_column = infer_dimension_column(normalized, columns, avoid={metric_column} if metric_column else set())
        if metric_column and dimension_column:
            return {
                "intent_type": "aggregate",
                "measures": [f"MAX({metric_column})"],
                "dimensions": [dimension_column],
                "filters": [],
                "order_by": [{"column": f"MAX({metric_column})", "direction": "DESC"}],
                "limit": 1,
                "offset": 0,
            }

    if asks_list:
        dimension_column = infer_dimension_column(normalized, columns)
        if dimension_column:
            return {
                "intent_type": "select",
                "measures": [],
                "dimensions": [dimension_column],
                "filters": [],
                "order_by": [{"column": dimension_column, "direction": "ASC"}],
                "limit": 100,
                "offset": 0,
            }

    raise ValueError(
        "Rule-based demo translator could not map the question. Add a new rule here or plug in an agent-side LLM."
    )


class IntentTranslator:
    def __init__(self, provider: str, claude_model: str, columns: list[dict[str, Any]]) -> None:
        self.provider = provider
        self.claude_model = claude_model
        self.columns = columns
        self.column_names = [column["name"] for column in columns if isinstance(column, dict) and "name" in column]
        self._anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        self._anthropic_base_url = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/")
        self.last_source = "unknown"

    def describe(self) -> str:
        if self.provider == "rules":
            return "rules"
        if self.provider == "claude":
            return f"claude ({self.claude_model})"
        if self._anthropic_api_key:
            return f"auto -> claude ({self.claude_model})"
        return "auto -> rules"

    def llm_readiness(self) -> tuple[bool, str]:
        if self.provider == "rules":
            return False, "rules mode selected"
        if not self._anthropic_api_key:
            return False, "ANTHROPIC_API_KEY is not set"
        return True, "configured"

    def question_to_intent(self, question: str) -> dict[str, Any]:
        if self.provider == "rules":
            self.last_source = "rules"
            return repair_intent_for_question(question, normalize_intent(rule_based_question_to_intent(question, self.columns)), self.columns)
        if self.provider == "claude":
            self.last_source = f"claude:{self.claude_model}"
            return repair_intent_for_question(question, normalize_intent(self._question_to_intent_with_claude(question)), self.columns)
        if self._anthropic_api_key:
            try:
                self.last_source = f"claude:{self.claude_model}"
                return repair_intent_for_question(question, normalize_intent(self._question_to_intent_with_claude(question)), self.columns)
            except Exception as exc:  # noqa: BLE001
                print(f"Claude translation failed, falling back to rules: {exc}")
                self.last_source = "rules(fallback)"
                return repair_intent_for_question(question, normalize_intent(rule_based_question_to_intent(question, self.columns)), self.columns)
        self.last_source = "rules"
        return repair_intent_for_question(question, normalize_intent(rule_based_question_to_intent(question, self.columns)), self.columns)

    def update_columns(self, columns: list[dict[str, Any]]) -> None:
        self.columns = columns
        self.column_names = [column["name"] for column in columns if isinstance(column, dict) and "name" in column]

    def _format_columns_for_prompt(self) -> str:
        formatted_columns: list[str] = []
        for column in self.columns:
            if not isinstance(column, dict):
                continue
            name = column.get("name")
            if not isinstance(name, str) or not name:
                continue
            column_type = column.get("type")
            if isinstance(column_type, str) and column_type:
                formatted_columns.append(f"{name} ({column_type})")
            else:
                formatted_columns.append(name)
        return ", ".join(formatted_columns)

    def _build_claude_examples(self) -> str:
        examples = [
            {
                "question": "how many passengers boarded",
                "intent": {
                    "intent_type": "aggregate",
                    "measures": ["COUNT(*)"],
                    "dimensions": [],
                    "filters": [],
                    "order_by": [],
                    "limit": 1,
                    "offset": 0,
                },
            },
            {
                "question": "list products",
                "intent": {
                    "intent_type": "select",
                    "measures": [],
                    "dimensions": ["product"],
                    "filters": [],
                    "order_by": [{"column": "product", "direction": "ASC"}],
                    "limit": 1000,
                    "offset": 0,
                },
            },
            {
                "question": "how many different products",
                "intent": {
                    "intent_type": "aggregate",
                    "measures": ["COUNT_DISTINCT(product)"],
                    "dimensions": [],
                    "filters": [],
                    "order_by": [],
                    "limit": 1,
                    "offset": 0,
                },
            },
        ]
        lines = ["Examples:"]
        for example in examples:
            lines.append(f"Question: {example['question']}")
            lines.append(json.dumps(example["intent"], separators=(",", ":")))
        return "\n".join(lines)

    def _build_claude_rules(self) -> str:
        return "\n".join(
            [
                "Rules:",
                "1. Return JSON only. Do not add markdown, commentary, or code fences.",
                "2. intent_type must be exactly one of: aggregate, select, filter.",
                "3. Use only column names that appear in the dataset columns list.",
                "4. For row-count questions such as 'how many', 'number of rows', 'number of passengers', or 'total passengers', use measures=['COUNT(*)'].",
                "5. Do not use SUM, AVG, MIN, or MAX on identifier columns such as id, passengerid, orderid, customerid, row numbers, or names.",
                "6. For unique or distinct counting questions such as 'how many different products', use measures=['COUNT_DISTINCT(column)'].",
                "7. For listing entities or values such as 'what products are present' or 'list regions', use intent_type='select', put the listed fields in dimensions, and leave measures empty.",
                "8. Use SUM, AVG, MIN, or MAX only for true numeric metrics such as amount, fare, sales, price, quantity, score, or latency.",
                "9. If the question asks for the highest or lowest entity by a raw metric, prefer select with order_by on the metric column; use aggregate only when a grouped rollup is explicitly requested.",
                "10. filters items must use keys: column, op, value. op must be one of: eq, neq, gt, gte, lt, lte, in, between, like, is_null, is_not_null.",
                "11. Use limit <= 1000 and offset >= 0.",
            ]
        )

    def _build_claude_user_prompt(self, question: str) -> str:
        sections = [
            f"Dataset columns: {self._format_columns_for_prompt()}",
            f"Question: {question}",
            self._build_claude_rules(),
            self._build_claude_examples(),
            "Return exactly one JSON object with keys: intent_type, measures, dimensions, filters, order_by, limit, offset.",
        ]
        return "\n\n".join(sections)

    def _question_to_intent_with_claude(self, question: str) -> dict[str, Any]:
        if not self._anthropic_api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is required when --intent-provider=claude.")
        payload = {
            "model": self.claude_model,
            "max_tokens": 600,
            "temperature": 0,
            "system": (
                "You convert a user question into a strict JSON query_intent object for a deterministic MCP server. "
                "You must follow the dataset columns and rules exactly. Prefer semantically correct intents over plausible-looking aggregates. "
                "Return exactly one JSON object and nothing else."
            ),
            "messages": [
                {
                    "role": "user",
                    "content": self._build_claude_user_prompt(question),
                }
            ],
        }
        response = requests.post(
            f"{self._anthropic_base_url}/v1/messages",
            headers={
                "x-api-key": self._anthropic_api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        if response.status_code >= 400:
            detail = response.text[:500]
            if response.status_code == 404:
                raise RuntimeError(
                    "Anthropic endpoint returned 404. Check ANTHROPIC_BASE_URL, outbound network access, or API key scope. "
                    f"Response: {detail}"
                )
            raise RuntimeError(f"Anthropic call failed ({response.status_code}): {detail}")
        response.raise_for_status()
        body = response.json()
        text_parts: list[str] = []
        for block in body.get("content", []):
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))
        raw_text = "\n".join(text_parts).strip()
        raw_text = strip_code_fence(raw_text)
        try:
            intent = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Claude did not return valid JSON intent. Raw output: {raw_text}") from exc
        if not isinstance(intent, dict):
            raise RuntimeError("Claude response must be a JSON object.")
        return intent