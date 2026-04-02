from __future__ import annotations

import asyncio
import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from client.mcp_client import MCPDataClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase 0 demo agent for the MCP data platform.")
    parser.add_argument("--server-url", default="http://127.0.0.1:8000")
    parser.add_argument("--api-key", default=os.getenv("MCP_API_KEY", "demo-key"))
    parser.add_argument(
        "--intent-provider",
        choices=["auto", "rules", "claude"],
        default="auto",
        help="How to produce structured intent in the agent layer.",
    )
    parser.add_argument(
        "--claude-model",
        default=os.getenv("CLAUDE_MODEL", "claude-haiku-4-5"),
        help="Claude model name for agent-side translation.",
    )
    parser.add_argument(
        "--dataset-path",
        default=os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sales.csv"),
    )
    parser.add_argument("--dataset-id", default="sales_demo")
    parser.add_argument("--agent-id", default="phase0_demo_agent")
    parser.add_argument("--org-id", default="demo_org")
    return parser


def main() -> None:
    asyncio.run(async_main())


async def async_main() -> None:
    args = build_parser().parse_args()
    async with MCPDataClient(api_key=args.api_key) as client:
        connect_result = await client.connect(agent_id=args.agent_id, org_id=args.org_id)
        capabilities = await client.capabilities()
        current_dataset_id = args.dataset_id
        await client.register_dataset(dataset_id=current_dataset_id, file_path=args.dataset_path)
        schema = await client.list_columns(current_dataset_id)
        translator = IntentTranslator(
            provider=args.intent_provider,
            claude_model=args.claude_model,
            columns=schema["columns"],
        )
        known_datasets = {current_dataset_id}

        print("Connected session:", connect_result["session_token"])
        print("Session expires at:", connect_result["session_expires_at"])
        print("Server capabilities:", ", ".join(capabilities["capabilities"]))
        print("Registered dataset:", current_dataset_id)
        print("Columns:", ", ".join(column["name"] for column in schema["columns"]))
        print("Intent provider:", translator.describe())
        llm_ready, llm_reason = translator.llm_readiness()
        print("LLM readiness:", "ready" if llm_ready else f"not ready ({llm_reason})")
        print(
            "Commands: status | schema | sql <query> | nl2sql <question> | preview <question> | ask <question> "
            "| register <dataset_id> <csv_path> | use <dataset_id> | datasets | exit"
        )

        while True:
            try:
                raw = input("demo> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not raw:
                continue
            if raw.lower() in {"exit", "quit"}:
                break
            if raw == "datasets":
                print("Known datasets:", ", ".join(sorted(known_datasets)))
                print("Current dataset:", current_dataset_id)
                continue

            if raw == "status":
                llm_ready, llm_reason = translator.llm_readiness()
                print("Current dataset:", current_dataset_id)
                print("Intent provider:", translator.describe())
                print("LLM readiness:", "ready" if llm_ready else f"not ready ({llm_reason})")
                print("Known datasets:", ", ".join(sorted(known_datasets)))
                continue

            if raw.startswith("register "):
                parts = raw.split(maxsplit=2)
                if len(parts) != 3:
                    print("Usage: register <dataset_id> <csv_path>")
                    continue
                _, new_dataset_id, csv_path = parts
                new_dataset_id = normalize_cli_token(new_dataset_id)
                csv_path = normalize_cli_token(csv_path)
                csv_path = os.path.expanduser(csv_path)
                if not os.path.isabs(csv_path):
                    csv_path = str((PROJECT_ROOT / csv_path).resolve())
                try:
                    await client.register_dataset(dataset_id=new_dataset_id, file_path=csv_path)
                    current_dataset_id = new_dataset_id
                    known_datasets.add(new_dataset_id)
                    schema = await client.list_columns(current_dataset_id)
                    translator.update_columns(schema["columns"])
                    print(f"Registered and switched to dataset: {current_dataset_id}")
                except Exception as exc:  # noqa: BLE001
                    print(f"Registration failed: {exc}")
                continue

            if raw.startswith("use "):
                parts = raw.split(maxsplit=1)
                if len(parts) != 2:
                    print("Usage: use <dataset_id>")
                    continue
                requested_dataset_id = parts[1]
                try:
                    schema = await client.list_columns(requested_dataset_id)
                    current_dataset_id = requested_dataset_id
                    known_datasets.add(requested_dataset_id)
                    translator.update_columns(schema["columns"])
                    print(f"Switched to dataset: {current_dataset_id}")
                except Exception as exc:  # noqa: BLE001
                    print(f"Cannot switch dataset: {exc}")
                continue

            if raw == "schema":
                print(json.dumps(await client.list_columns(current_dataset_id), indent=2))
                continue

            if raw.startswith("sql "):
                try:
                    result = await client.query(current_dataset_id, raw[4:])
                    print_result(result)
                except Exception as exc:  # noqa: BLE001
                    print(f"Query failed: {exc}")
                continue

            if raw.startswith("nl2sql "):
                try:
                    intent = translator.question_to_intent(raw[7:])
                    preview = await client.generate_query(current_dataset_id, intent)
                    print("Intent source:", translator.last_source)
                    print("Structured intent:")
                    print(json.dumps(intent, indent=2))
                    print("Generated SQL:")
                    print(preview["generated_sql"])
                except Exception as exc:  # noqa: BLE001
                    print(exc)
                continue

            if raw.startswith("preview "):
                try:
                    intent = translator.question_to_intent(raw[8:])
                    print("Intent source:", translator.last_source)
                    print(json.dumps(await client.generate_query(current_dataset_id, intent), indent=2))
                except Exception as exc:  # noqa: BLE001
                    print(exc)
                continue

            if raw.startswith("ask "):
                try:
                    intent = translator.question_to_intent(raw[4:])
                    print("Intent source:", translator.last_source)
                    print("Structured intent:")
                    print(json.dumps(intent, indent=2))
                    result = await client.execute_intent(current_dataset_id, intent)
                    print_result(result)
                except Exception as exc:  # noqa: BLE001
                    print(exc)
                continue

            print("Unsupported command.")


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

    def _question_to_intent_with_claude(self, question: str) -> dict[str, Any]:
        if not self._anthropic_api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is required when --intent-provider=claude.")
        payload = {
            "model": self.claude_model,
            "max_tokens": 600,
            "temperature": 0,
            "system": (
                "You convert user questions into a strict JSON query_intent object for a deterministic MCP server. "
                "Return JSON only with keys: intent_type, measures, dimensions, filters, order_by, limit, offset."
            ),
            "messages": [
                {
                    "role": "user",
                    "content": (
                        "Dataset columns: "
                        + ", ".join(self.column_names)
                        + "\nQuestion: "
                        + question
                        + "\nConstraints: intent_type in [aggregate, select, filter]; "
                        "filters items use keys column, op, value; op in [eq, neq, gt, gte, lt, lte, in, between, like, is_null, is_not_null]. "
                        "Use limit <= 1000 and offset >= 0."
                    ),
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
    preferred_terms = ["amount", "price", "cost", "revenue", "sales", "score", "value", "count", "bytes", "latency", "duration"]
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
        aggregate_match = AGGREGATE_MEASURE_PATTERN.match(candidate)
        if aggregate_match:
            func_name, raw_column = aggregate_match.groups()
            normalized = f"{func_name.upper()}({raw_column})"
            normalized_measures.append(normalized)
            if raw_column != "*":
                measure_by_raw_column[raw_column] = normalized
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
    asks_highest = contains_any(normalized_question, ["highest", "max", "maximum", "top", "best"])
    asks_lowest = contains_any(normalized_question, ["lowest", "min", "minimum", "bottom", "least"])
    asks_count = contains_any(normalized_question, ["how many", "number of", "count", "total people", "total passengers", "total"])
    asks_distinct = contains_any(normalized_question, ["different", "distinct", "unique"])
    requests_entity = contains_any(normalized_question, ["which", "who", "what", "whose"])
    requests_rollup = contains_any(normalized_question, ["total", "sum", "average", "avg", "count"])
    metric_column = infer_metric_column(normalized_question, columns)

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

    if asks_count and intent.get("intent_type") == "aggregate" and not intent.get("measures"):
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

    if not metric_column or requests_rollup or not requests_entity:
        return intent
    if not (asks_highest or asks_lowest):
        return intent
    if intent.get("intent_type") != "aggregate":
        return intent

    direction = "DESC" if asks_highest else "ASC"
    dimensions = infer_identifier_columns(normalized_question, columns, metric_column)
    if metric_column not in dimensions:
        dimensions.append(metric_column)

    return {
        "intent_type": "select",
        "measures": [],
        "dimensions": dimensions,
        "filters": intent.get("filters", []),
        "order_by": [{"column": metric_column, "direction": direction}],
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


def print_result(result: dict[str, Any]) -> None:
    print("Generated SQL:")
    print(result["generated_sql"])
    print("Rows returned:", result["row_count"])
    if not result["rows"]:
        return
    header = " | ".join(column["name"] for column in result["columns"])
    print(header)
    print("-" * len(header))
    for row in result["rows"]:
        print(" | ".join(str(value) for value in row))


if __name__ == "__main__":
    main()
