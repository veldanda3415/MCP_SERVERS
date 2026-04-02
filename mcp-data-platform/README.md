# MCP Data Platform Phase 0 (MCP SDK)

This Phase 0 implementation is now MCP SDK native.

- Server runtime: `mcp.server.fastmcp.FastMCP`
- Transport: MCP stdio
- Agent reasoning: outside MCP (rule-based or Claude in demo)
- MCP responsibility: deterministic, read-only query planning and execution

## Architecture

```text
User -> Agent -> LLM (optional) -> MCP ClientSession -> FastMCP Server -> DuckDB -> CSV
```

Lifecycle used in this repo:

1. Agent calls MCP tool `connect` (API key + agent/org metadata)
2. Agent receives `session_token`
3. Agent calls MCP tool `capabilities`
4. Agent registers datasets and executes deterministic tools

## Project Layout

```text
mcp-data-platform/
├── server/            # FastMCP server + query/registry/session logic
├── client/            # MCP SDK client wrapper
├── data/              # Sample CSV files
├── demo/              # Demo scripts (interactive and quickstart)
├── tests/             # MCP SDK smoke tests
├── requirements.txt
└── README.md
```

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Optional `.env` support (already wired in demo scripts):

```dotenv
MCP_API_KEY="demo-key"
ANTHROPIC_API_KEY="<your_anthropic_key>"
CLAUDE_MODEL="claude-haiku-4-5"
ANTHROPIC_BASE_URL="https://api.anthropic.com"
```

If `.env` is present, you do not need to export these variables manually.

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

## Run Demo Agent (MCP Native)

```powershell
$env:MCP_API_KEY = "demo-key"
python demo/cli_demo.py
```

Optional Claude in agent layer:

```powershell
$env:MCP_API_KEY = "demo-key"
$env:ANTHROPIC_API_KEY = "<your_anthropic_key>"
python demo/cli_demo.py --intent-provider claude
```

Quick LLM smoke test:

```powershell
@('nl2sql show top product by amount','exit') | python demo/cli_demo.py --intent-provider claude
```

Expected signal:

- `Intent source: claude:<model_name>`
- `Generated SQL:` appears without fallback warning

If LLM translation fails:

- verify `ANTHROPIC_API_KEY` is valid and active
- verify `CLAUDE_MODEL` is available for your account
- keep `--intent-provider auto` to gracefully fallback to rules

`--intent-provider auto` behavior:

- Uses Claude when `ANTHROPIC_API_KEY` is available
- Falls back to rule-based mapping if Claude call fails

Example CLI commands:

- `datasets`
- `register sales_jan .\data\sales.csv`
- `use sales_jan`
- `schema`
- `nl2sql total revenue by region for completed orders in q1`
- `preview total revenue by region for completed orders in q1`
- `ask total revenue by region for completed orders in q1`
- `sql select region, sum(amount) as total_amount from dataset group by region order by total_amount desc`

## Run Quickstart Script

```powershell
$env:MCP_API_KEY = "demo-key"
python demo/client_quickstart.py
```

This script demonstrates:

1. `connect`
2. `capabilities`
3. `register_dataset`
4. `list_columns`
5. `query`
6. `generate_query`
7. `execute_intent`

## Verify With New Datasets

1. Prepare a new CSV file.
2. Run `python demo/cli_demo.py`.
3. Register and switch dataset:

```text
register my_data C:\path\to\my_data.csv
use my_data
schema
```

Supported file types for `register`:

- CSV: `.csv`
- Excel: `.xlsx`, `.xls`
- Parquet: `.parquet`, `.pq`
- JSON: `.json`, `.jsonl`, `.ndjson`
- SQLite DB: `.db`, `.sqlite`, `.sqlite3`

SQLite table selection syntax:

```text
register net_logs C:\path\to\network.sqlite::logs
```

If table is not specified for SQLite, the first user table (alphabetical) is used.

1. Verify direct SQL path:

```text
sql select count(*) as row_count from dataset
```

1. Verify NL -> intent -> SQL path:

```text
nl2sql total revenue by region
preview total revenue by region
ask total revenue by region
```

Expected signals:

- Dataset registers successfully with non-zero row count
- `schema` matches CSV headers
- Direct SQL returns rows and blocks non-SELECT statements
- `preview` and `ask` use the same deterministic SQL for the same intent

## Run Tests

```powershell
pytest -q
```

## Verify MCP Tools

Automated MCP tool verification:

```powershell
$env:MCP_API_KEY = "demo-key"
python demo/verify_mcp_tools.py
```

This checks:

- MCP session can connect
- capabilities tool is callable
- MCP tool registry includes all expected tools

Manual inspector-based verification:

1. Install Node.js if not installed.
2. Run MCP Inspector:

```powershell
npx @modelcontextprotocol/inspector
```

3. In Inspector, configure stdio server launch:

- command: python
- args: -m server.app
- cwd: project root of this repository
- env: MCP_API_KEY=demo-key

4. Use Inspector UI to:

- list tools
- call connect
- call capabilities
- call register_dataset and list_columns
- call generate_query and execute_intent

## Notes

- This implementation is now true MCP SDK, not HTTP endpoint emulation.
- The MCP server itself does not do NL interpretation.
- NL translation stays in the agent layer for scalability and deterministic execution boundaries.
