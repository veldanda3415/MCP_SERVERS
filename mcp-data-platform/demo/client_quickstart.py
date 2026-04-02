from __future__ import annotations

import asyncio
import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from client.mcp_client import MCPDataClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Quick client walkthrough for MCP Data Platform Phase 0.")
    parser.add_argument("--server-url", default="http://127.0.0.1:8000")
    parser.add_argument("--api-key", default=os.getenv("MCP_API_KEY", "demo-key"))
    parser.add_argument("--dataset-id", default="sales_quickstart")
    parser.add_argument("--agent-id", default="phase0_quickstart_agent")
    parser.add_argument("--org-id", default="demo_org")
    parser.add_argument(
        "--dataset-path",
        default=str(PROJECT_ROOT / "data" / "sales.csv"),
    )
    return parser


def main() -> None:
    asyncio.run(async_main())


async def async_main() -> None:
    args = build_parser().parse_args()
    async with MCPDataClient(api_key=args.api_key) as client:
        print("0) Connect + get capabilities")
        connected = await client.connect(args.agent_id, args.org_id)
        print(json.dumps(connected, indent=2))
        print(json.dumps(await client.capabilities(), indent=2))

        print("1) Register dataset")
        registered = await client.register_dataset(args.dataset_id, args.dataset_path)
        print(json.dumps(registered, indent=2))

        print("2) List columns")
        schema = await client.list_columns(args.dataset_id)
        print(json.dumps(schema, indent=2))

        print("3) Direct SQL query")
        sql_result = await client.query(
            args.dataset_id,
            "select region, sum(amount) as total_amount from dataset group by region order by total_amount desc",
        )
        print(json.dumps(sql_result, indent=2))

        print("4) Structured intent preview + execute")
        query_intent = {
            "intent_type": "aggregate",
            "measures": ["SUM(amount)"],
            "dimensions": ["region"],
            "filters": [{"column": "status", "op": "eq", "value": "completed"}],
            "order_by": [{"column": "SUM(amount)", "direction": "DESC"}],
            "limit": 100,
            "offset": 0,
        }
        print(json.dumps(await client.generate_query(args.dataset_id, query_intent), indent=2))
        print(json.dumps(await client.execute_intent(args.dataset_id, query_intent), indent=2))


if __name__ == "__main__":
    main()
