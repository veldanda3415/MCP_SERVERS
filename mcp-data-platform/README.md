# MCP Query Engine (Phase 0, MCP SDK)

A lightweight MCP (Model Context Protocol) query engine that enables AI agents to query structured data in a deterministic and read-only way.

- Server runtime: `mcp.server.fastmcp.FastMCP`
- Transport: MCP stdio
- Agent reasoning: outside MCP (rule-based or Claude in demo)
- MCP responsibility: deterministic query planning + safe execution

## Disclaimer (Phase 0)

This repository is a **Phase 0 demo** intended to validate architecture and core MCP workflows.

- It is optimized for correctness and learnability over production scale.
- Agent-side NL translation is heuristic/prompt-driven and still evolving.
- Behavior is deterministic at execution layer, but NL understanding quality depends on translator rules/prompting.

## Why This Exists

Most AI-driven data querying approaches suffer from:

- Non-deterministic SQL generation
- Direct exposure of raw data and credentials
- No reusable execution layer across agents

This project addresses that by:

- Separating reasoning (agent/LLM) from execution (MCP server)
- Enforcing read-only, safe SQL execution
- Translating intent to deterministic SQL

## Architecture

```text
User -> Agent -> LLM (optional) -> MCP ClientSession -> FastMCP Server -> DuckDB -> Dataset
```

Lifecycle in this repo:

1. Agent calls `connect` (API key + metadata)
2. Agent receives `session_token`
3. Agent calls `capabilities`
4. Agent registers dataset(s) and executes tools

## Project Layout

```text
mcp-data-platform/
├── server/                      # FastMCP server + query/registry/session logic
├── client/                      # MCP SDK transport wrapper
├── demo/                        # CLI + quickstart + verification scripts
├── data/                        # Example datasets
├── tests/                       # Smoke and stability tests
├── FUTURE_ENHANCEMENTS_PLANNED.md
├── requirements.txt
└── README.md
```

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Optional `.env`:

```dotenv
MCP_API_KEY="demo-key"
ANTHROPIC_API_KEY="<your_anthropic_key>"
CLAUDE_MODEL="claude-haiku-4-5"
ANTHROPIC_BASE_URL="https://api.anthropic.com"
```

## MCP Tools Implemented

- `connect`
- `capabilities`
- `register_dataset`
- `list_columns`
- `query`
- `generate_query`
- `execute_intent`
- `tool` (generic dispatcher)

All tools except `connect` require `session_token`.

## Run Demo Agent

```powershell
$env:MCP_API_KEY = "demo-key"
python demo/cli_demo.py
```

Optional Claude-backed translation:

```powershell
$env:MCP_API_KEY = "demo-key"
$env:ANTHROPIC_API_KEY = "<your_anthropic_key>"
python demo/cli_demo.py --intent-provider claude
```

## Real Examples

### Example A: Sales Aggregation (`data/sales.csv`)

```text
demo> register sales_demo data/sales.csv
demo> ask total revenue by region for completed orders in q1
```

Expected behavior:

- Intent type is `aggregate`
- Measure uses `SUM(amount)`
- Filters include status and date range where available
- Output returns one row per region ordered by total amount

### Example B: Titanic Count (`data/Titanic-Dataset.csv`)

```text
demo> register titanic data/Titanic-Dataset.csv
demo> ask how many passengers boarded in titanic
```

Expected behavior:

- Intent type is `aggregate`
- Measure is `COUNT(*)` (not `SUM(PassengerId)`)
- Output returns total passenger count

### Example C: Stock Ranking Without Ticker (`data/final_stock_prices.csv`)

```text
demo> register stock data/final_stock_prices.csv
demo> ask which stock has highest volume
```

Dataset note:

- This sample has no ticker/symbol column.
- The translator falls back to identifier-like context (for example `Date`) with `Volume` for ranking.
- Best semantic question for this dataset is: `which date has highest volume`.

## Integration Guide

Use this section if you want to plug this MCP server/client into your own agent runtime.

### 1) Programmatic Client Integration

Use `MCPDataClient` as transport wrapper and keep reasoning in your agent layer.

```python
import asyncio
from client.mcp_client import MCPDataClient


async def main() -> None:
	async with MCPDataClient(api_key="demo-key") as client:
		await client.connect(agent_id="my_agent", org_id="my_org")
		await client.register_dataset("sales", "data/sales.csv")
		schema = await client.list_columns("sales")
		intent = {
			"intent_type": "aggregate",
			"measures": ["SUM(amount)"],
			"dimensions": ["region"],
			"filters": [],
			"order_by": [{"column": "SUM(amount)", "direction": "DESC"}],
			"limit": 100,
			"offset": 0,
		}
		result = await client.execute_intent("sales", intent)
		print(schema)
		print(result)


asyncio.run(main())
```

### 2) Integration Contract (Recommended)

- Keep MCP client transport-only.
- Keep NL translation/intent policy outside the client (agent layer).
- Always validate/normalize intent before execution.
- Treat SQL tool as deterministic execution endpoint, not reasoning surface.

### 3) Dataset Onboarding Checklist

- Ensure at least one identifier-like column exists (`id`, `name`, `symbol`, `ticker`, etc.).
- Ensure numeric metrics are clearly typed.
- Verify schema and row count with `schema` command after registration.
- Add sample questions and expected intent/output for regression tests.

## Quickstart Script

```powershell
$env:MCP_API_KEY = "demo-key"
python demo/client_quickstart.py
```

Demonstrates:

1. `connect`
2. `capabilities`
3. `register_dataset`
4. `list_columns`
5. `query`
6. `generate_query`
7. `execute_intent`

## Verification

Automated verification:

```powershell
$env:MCP_API_KEY = "demo-key"
python demo/verify_mcp_tools.py
pytest -q
```

Manual MCP Inspector:

```powershell
npx @modelcontextprotocol/inspector
```

Inspector server launch settings:

- command: `python`
- args: `-m server.app`
- cwd: project root
- env: `MCP_API_KEY=demo-key`

## Future Enhancements Planned

See `FUTURE_ENHANCEMENTS_PLANNED.md` for prioritized backlog items and contributor-ready scope.

## Notes

- MCP server execution is deterministic and read-only.
- NL translation remains in the agent layer by design.
- This repo is intended as a practical baseline for Phase 1 hardening and productionization.

## Example CLI DEMO with Test Data Set Trials:(Network Intrusion DataSet from Kaggle)

python.exe .\demo\cli_demo.py
Connected session: sess_afa170c98638457d8dccb010754189db
Session expires at: 2026-04-03T10:58:12.532616+00:00
Server capabilities: connect, capabilities, register_dataset, list_columns, query, generate_query, execute_intent, tool
Registered dataset: sales_demo
Columns: date, region, product, amount, status
Intent provider: auto -> claude (claude-haiku-4-5)
LLM readiness: ready
Commands: status | schema | sql <query> | nl2sql <question> | preview <question> | ask <question> | register <dataset_id> <csv_path> | use <dataset_id> | datasets | exit
demo> register network data/network_intrusion.csv   
Registered and switched to dataset: network
demo> schema                                     
{
  "dataset_id": "network",
  "row_count": 22544,
  "columns": [
    {
      "name": "duration",
      "type": "BIGINT"
    },
    {
      "name": "protocol_type",
      "type": "VARCHAR"
    },
    {
      "name": "service",
      "type": "VARCHAR"
    },
    {
      "name": "flag",
      "type": "VARCHAR"
    },
    {
      "name": "src_bytes",
      "type": "BIGINT"
    },
    {
      "name": "dst_bytes",
      "type": "BIGINT"
    },
    {
      "name": "land",
      "type": "BIGINT"
    },
    {
      "name": "wrong_fragment",
      "type": "BIGINT"
    },
    {
      "name": "urgent",
      "type": "BIGINT"
    },
    {
      "name": "hot",
      "type": "BIGINT"
    },
    {
      "name": "num_failed_logins",
      "type": "BIGINT"
    },
    {
      "name": "logged_in",
      "type": "BIGINT"
    },
    {
      "name": "num_compromised",
      "type": "BIGINT"
    },
    {
      "name": "root_shell",
      "type": "BIGINT"
    },
    {
      "name": "su_attempted",
      "type": "BIGINT"
    },
    {
      "name": "num_root",
      "type": "BIGINT"
    },
    {
      "name": "num_file_creations",
      "type": "BIGINT"
    },
    {
      "name": "num_shells",
      "type": "BIGINT"
    },
    {
      "name": "num_access_files",
      "type": "BIGINT"
    },
    {
      "name": "num_outbound_cmds",
      "type": "BIGINT"
    },
    {
      "name": "is_host_login",
      "type": "BIGINT"
    },
    {
      "name": "is_guest_login",
      "type": "BIGINT"
    },
    {
      "name": "count",
      "type": "BIGINT"
    },
    {
      "name": "srv_count",
      "type": "BIGINT"
    },
    {
      "name": "serror_rate",
      "type": "DOUBLE"
    },
    {
      "name": "srv_serror_rate",
      "type": "DOUBLE"
    },
    {
      "name": "rerror_rate",
      "type": "DOUBLE"
    },
    {
      "name": "srv_rerror_rate",
      "type": "DOUBLE"
    },
    {
      "name": "same_srv_rate",
      "type": "DOUBLE"
    },
    {
      "name": "diff_srv_rate",
      "type": "DOUBLE"
    },
    {
      "name": "srv_diff_host_rate",
      "type": "DOUBLE"
    },
    {
      "name": "dst_host_count",
      "type": "BIGINT"
    },
    {
      "name": "dst_host_srv_count",
      "type": "BIGINT"
    },
    {
      "name": "dst_host_same_srv_rate",
      "type": "DOUBLE"
    },
    {
      "name": "dst_host_diff_srv_rate",
      "type": "DOUBLE"
    },
    {
      "name": "dst_host_same_src_port_rate",
      "type": "DOUBLE"
    },
    {
      "name": "dst_host_srv_diff_host_rate",
      "type": "DOUBLE"
    },
    {
      "name": "dst_host_serror_rate",
      "type": "DOUBLE"
    },
    {
      "name": "dst_host_srv_serror_rate",
      "type": "DOUBLE"
    },
    {
      "name": "dst_host_rerror_rate",
      "type": "DOUBLE"
    },
    {
      "name": "dst_host_srv_rerror_rate",
      "type": "DOUBLE"
    }
  ]
}
demo> ask how many services present
Intent source: claude:claude-haiku-4-5
Structured intent:
{
  "intent_type": "aggregate",
  "measures": [
    "COUNT_DISTINCT(service)"
  ],
  "dimensions": [],
  "filters": [],
  "order_by": [],
  "limit": 1,
  "offset": 0
}
Generated SQL:
SELECT COUNT(DISTINCT "service") AS "count_distinct_service" FROM "dataset_c112e88173d4" LIMIT 1
Rows returned: 1
count_distinct_service
----------------------
64
demo> ask which services have high error rate but low traffic
Intent source: claude:claude-haiku-4-5
Structured intent:
{
  "intent_type": "aggregate",
  "measures": [
    "AVG(serror_rate)",
    "AVG(src_bytes)"
  ],
  "dimensions": [
    "service"
  ],
  "filters": [
    {
      "column": "AVG(serror_rate)",
      "op": "gt",
      "value": "__GLOBAL_AVG__(serror_rate)"
    },
    {
      "column": "AVG(src_bytes)",
      "op": "lt",
      "value": "__GLOBAL_AVG__(src_bytes)"
    }
  ],
  "order_by": [
    {
      "column": "AVG(serror_rate)",
      "direction": "DESC"
    }
  ],
  "limit": 100,
  "offset": 0
}
Generated SQL:
SELECT "service" AS "service", AVG("serror_rate") AS "avg_serror_rate", AVG("src_bytes") AS "avg_src_bytes" FROM "dataset_c112e88173d4" GROUP BY "service" HAVING AVG("serror_rate") > (SELECT AVG("serror_rate") FROM "dataset_c112e88173d4") AND AVG("src_bytes") < (SELECT AVG("src_bytes") FROM "dataset_c112e88173d4") ORDER BY "avg_serror_rate" DESC LIMIT 100
Rows returned: 47
service | avg_serror_rate | avg_src_bytes
-----------------------------------------
nntp | 0.47619047619047616 | 0.2857142857142857
netbios_ssn | 0.4 | 0.0
gopher | 0.38 | 0.0
systat | 0.375625 | 0.0
rje | 0.375 | 0.0
mtp | 0.37375 | 0.0
netbios_dgm | 0.36 | 0.0
link | 0.35609756097560974 | 0.0
netstat | 0.34615384615384615 | 0.0
kshell | 0.3333333333333333 | 0.0
finger | 0.3330882352941177 | 3.0073529411764706
echo | 0.3245945945945946 | 0.0
daytime | 0.32142857142857145 | 0.0
Z39_50 | 0.31955555555555554 | 0.0
whois | 0.3165 | 0.0
domain | 0.31019607843137254 | 122.50980392156863
ctf | 0.30902439024390244 | 0.0
ssh | 0.3080769230769231 | 0.0
netbios_ns | 0.3055555555555556 | 0.0
name | 0.302972972972973 | 0.0
vmnet | 0.3023255813953488 | 0.0
nnsp | 0.2957142857142857 | 0.0
iso_tsap | 0.29270833333333335 | 0.0
sql_net | 0.285 | 0.0
exec | 0.2777777777777778 | 0.0
supdup | 0.277037037037037 | 0.0
shell | 0.27625 | 0.0
csnet_ns | 0.27235294117647063 | 0.0
hostnames | 0.2652173913043478 | 0.0
uucp_path | 0.2617391304347826 | 0.0
time | 0.25 | 0.0
courier | 0.25 | 0.0
imap4 | 0.24183006535947713 | 4.4183006535947715
bgp | 0.2408695652173913 | 0.0
klogin | 0.23809523809523808 | 0.0
login | 0.23655172413793102 | 4.206896551724138
pop_2 | 0.2361538461538462 | 0.0
discard | 0.23076923076923078 | 0.0
telnet | 0.22921279212792123 | 118.43542435424354
http_443 | 0.21638888888888888 | 0.0
efs | 0.21212121212121213 | 0.0
ldap | 0.21052631578947367 | 0.0
private | 0.2091914537075825 | 16.848973607038122
printer | 0.20454545454545456 | 0.0
uucp | 0.2 | 0.12
auth | 0.1644776119402985 | 3.7761194029850746
remote_job | 0.15 | 0.0
demo> ask compare error rates between tcp and udp services
Intent source: claude:claude-haiku-4-5
Structured intent:
{
  "intent_type": "aggregate",
  "measures": [
    "AVG(serror_rate)",
    "AVG(rerror_rate)",
    "AVG(dst_host_serror_rate)",
    "AVG(dst_host_rerror_rate)"
  ],
  "dimensions": [
    "protocol_type"
  ],
  "filters": [
    {
      "column": "protocol_type",
      "op": "in",
      "value": [
        "tcp",
        "udp"
      ]
    }
  ],
  "order_by": [
    {
      "column": "protocol_type",
      "direction": "ASC"
    }
  ],
  "limit": 1000,
  "offset": 0
}
Generated SQL:
SELECT "protocol_type" AS "protocol_type", AVG("serror_rate") AS "avg_serror_rate", AVG("rerror_rate") AS "avg_rerror_rate", AVG("dst_host_serror_rate") AS "avg_dst_host_serror_rate", AVG("dst_host_rerror_rate") AS "avg_dst_host_rerror_rate" FROM "dataset_c112e88173d4" WHERE "protocol_type" IN ('tcp', 'udp') GROUP BY "protocol_type" ORDER BY "protocol_type" ASC LIMIT 1000
Rows returned: 2
protocol_type | avg_serror_rate | avg_rerror_rate | avg_dst_host_serror_rate | avg_dst_host_rerror_rate
-------------------------------------------------------------------------------------------------------
tcp | 0.12289459745762661 | 0.2846822033898292 | 0.11621345338982995 | 0.2747409957627169
udp | 0.0 | 7.630675314765357e-05 | 0.0019572682182373135 | 0.0189202594429607

These are successfull cases however we can enhance this to reach 95% to 99% success rate if we fine tune.

