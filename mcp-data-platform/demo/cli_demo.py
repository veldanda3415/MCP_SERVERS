from __future__ import annotations

import asyncio
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from client.mcp_client import MCPDataClient
from demo.intent_translator import IntentTranslator, normalize_cli_token


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
