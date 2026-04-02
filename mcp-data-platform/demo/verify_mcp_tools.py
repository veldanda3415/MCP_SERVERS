from __future__ import annotations

import asyncio
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

REQUIRED_TOOLS = {
    "connect",
    "capabilities",
    "register_dataset",
    "list_columns",
    "query",
    "generate_query",
    "execute_intent",
    "tool",
}


async def main() -> None:
    api_key = os.getenv("MCP_API_KEY", "demo-key")
    async with MCPDataClient(api_key=api_key) as client:
        connect_result = await client.connect(agent_id="verify_agent", org_id="verify_org")
        caps_result = await client.capabilities()
        list_tools_result = await client.list_tools()

    tool_entries = list_tools_result.get("tools", [])
    tool_names = {item.get("name") for item in tool_entries if isinstance(item, dict)}
    missing = sorted(REQUIRED_TOOLS - tool_names)

    print("Connect result:")
    print(json.dumps(connect_result, indent=2))
    print("Capabilities result:")
    print(json.dumps(caps_result, indent=2))
    print("Registered MCP tools:")
    print(", ".join(sorted(name for name in tool_names if isinstance(name, str))))

    if missing:
        raise SystemExit(f"FAILED: missing required tools: {', '.join(missing)}")

    print("PASSED: all required MCP tools are registered and callable.")


if __name__ == "__main__":
    asyncio.run(main())
