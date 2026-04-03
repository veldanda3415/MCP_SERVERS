# MCP Query Engine – Phase 0 Release (v0.0.0)

**Release Date:** April 3, 2026  
**Release Type:** Experimental / Phase 0  
**Status:** Reference Implementation (Proof of Concept)

---

## Overview

**MCP Query Engine** is a deterministic query execution layer for AI agents using the Model Context Protocol (MCP). This Phase 0 release provides:

- **Deterministic SQL generation** from structured natural language intents
- **Agent-side intent translation** with rule-based and Claude-backed inference paths
- **MCP server/client connectivity** with session-oriented workflows
- **CSV dataset support** with end-to-end validation
- **Regression test coverage** for ranking, aggregation, comparative, and anomaly patterns

This is an **experimental reference implementation** designed to demonstrate safe query generation patterns for agent-grade applications. It is not production-hardened for scale or throughput.

---

## What's New in 0.0.0

### Core Features
- ✅ MCP SDK native stdio transport and session management
- ✅ Deterministic SQL generation with validation-before-execution
- ✅ Agent-side intent module (rules-based + optional Claude integration)
- ✅ Support for complex intents: ranking, anomaly detection, dual-filter comparatives
- ✅ Nested aggregate expression normalization (e.g., `SUM(SUM(expr))` → `AVG(expr1 + expr2)`)
- ✅ HAVING clauses with global-average subquery patterns
- ✅ Distinct counting and service existence queries

### Test Coverage
- ✅ Runtime stability tests (23 passing)
  - Ranking with dimension preservation
  - Anomaly detection with HAVING
  - High-error-low-traffic dual filters
  - Combined error aggregation patterns
  - Order-by metric auto-inclusion
  - Service existence counting
- ✅ Query engine aggregate tests (additive aggregates, subquery HAVING)
- ✅ API smoke tests (end-to-end flows)
- ✅ Dataset registration format tests

### Verified Data Paths
- CSV registration and schema inspection
- Direct SQL execution
- Intent-driven SQL generation
- Complete `register → schema → translate → generate → execute` workflows

---

## Installation

### Prerequisites
- Python 3.11+
- pip or poetry
- Virtual environment (recommended)

### Setup

1. **Clone the repository:**
   ```bash
   git clone <repo-url>
   cd mcp-data-platform
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv .venv
   # On Windows:
   .\.venv\Scripts\Activate.ps1
   # On macOS/Linux:
   source .venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Optional: Anthropic API key for Claude-backed translation**
   ```bash
   # Create .env file in project root
   echo "ANTHROPIC_API_KEY=sk-..." >> .env
   echo "CLAUDE_MODEL=claude-haiku-4-5" >> .env
   ```

---

## Quick Start

### 1. Start the MCP Server

```bash
cd mcp-intent-execution-engine/mcp-data-platform
python -m server.app
# Server running on http://127.0.0.1:8000
```

### 2. Run the CLI Demo

```bash
python demo/cli_demo.py --intent-provider auto --dataset-path data/sales.csv
```

### 3. Example Interactions

**Register a dataset:**
```
demo> register titanic data/Titanic-Dataset.csv
Registered and switched to dataset: titanic
```

**Inspect schema:**
```
demo> schema
{
  "columns": [
    {"name": "PassengerId", "type": "integer"},
    {"name": "Survived", "type": "integer"},
    ...
  ]
}
```

**Ask a question (nl2sql):**
```
demo> nl2sql which passenger class has the highest survival rate
Intent source: rules
Structured intent:
{
  "intent": "select",
  "dimensions": ["pclass"],
  "measures": ["survival_rate"],
  "order_by": {"measure": "survival_rate", "direction": "desc"},
  "limit": 1
}
Generated SQL:
SELECT pclass, AVG(survived) AS survival_rate
FROM titanic
GROUP BY pclass
ORDER BY survival_rate DESC
LIMIT 1
```

**Execute a full ask:**
```
demo> ask show me services with error rates above average
Intent source: rules
Structured intent: {...}
Generated SQL: SELECT ... HAVING ... > (SELECT AVG(...) FROM ...)
Rows returned: 5
service_name | error_rate
...
```

---

## Documentation

- **[README.md](README.md)** – Setup, capabilities, real examples, integration guide
- **[CHANGELOG.md](CHANGELOG.md)** – Detailed feature list, fixes, and known limitations
- **[CONTRIBUTING.md](CONTRIBUTING.md)** – Development guidelines, testing, issue/PR processes
- **[FUTURE_ENHANCEMENTS_PLANNED.md](FUTURE_ENHANCEMENTS_PLANNED.md)** – Roadmap for Phase 0.2+

---

## Architecture Overview

### Three-Layer Design

```
┌─────────────────────────────────────┐
│  Agent (Your Application)           │
│  ├─ Intent generation logic         │
│  └─ Intent Translator module        │
├─────────────────────────────────────┤
│  MCP Transport (stdio)              │
│  ├─ Session management             │
│  └─ Capability discovery           │
├─────────────────────────────────────┤
│  Query Engine (MCP Server)          │
│  ├─ Deterministic SQL generation   │
│  ├─ Dataset registration           │
│  ├─ Intent → SQL execution         │
│  └─ DuckDB backend (CSV validated) │
└─────────────────────────────────────┘
```

### Key Separation: Reasoning vs. Execution

- **Agent-side (your responsibility):** Intent inference, confidence scoring, clarification prompts
- **MCP-side (our responsibility):** Deterministic SQL generation with pre-execution validation

---

## Test Execution

### Run All Tests

```bash
pytest tests/ -v --tb=short
```

### Run Specific Test Suites

```bash
# Runtime stability (23 tests)
pytest tests/test_runtime_stability.py -v

# Query engine aggregates (3 tests)
pytest tests/test_query_engine_aggregates.py -v

# API smoke tests
pytest tests/test_api_smoke.py -v

# Dataset registration formats
pytest tests/test_dataset_registration_formats.py -v
```

---

## Known Limitations

### Scope Boundaries (Phase 0)
- ⚠️ **UI/CLI productization:** Not included (use as reference only)
- ⚠️ **Authentication framework:** Not included (implement at agent layer)
- ⚠️ **Data sources:** CSV only (SQLite/Postgres planned for Phase 0.2)
- ⚠️ **Scaling:** Not tested for large datasets or high throughput
- ⚠️ **Error handling:** Read-only guardrails in place; auditing patterns TBD

### Known Issues
- Complex nested joins require explicit intent specification (not auto-inferred)
- Subqueries in WHERE clauses limited to deterministic patterns (anomaly, global-avg filtering)
- Timestamp/date arithmetic not yet expression-evaluated (pass-through only)

---

## Integration Guide

### Using the Python Client

```python
from client.mcp_client import MCPDataClient
import asyncio

async def example():
    async with MCPDataClient(api_key="your-key") as client:
        # Connect session
        result = await client.connect(agent_id="my_agent", org_id="my_org")
        
        # Register dataset
        await client.register_dataset(
            dataset_id="sales",
            file_path="data/sales.csv"
        )
        
        # Get schema
        schema = await client.list_columns("sales")
        
        # Translate intent and execute
        intent = {
            "intent": "select",
            "dimensions": ["region"],
            "measures": ["total_sales"],
            "limit": 10
        }
        result = await client.execute_intent("sales", intent)
        print(result["rows"])

asyncio.run(example())
```

### Using the Intent Translator (Agent-Side)

```python
from demo.intent_translator import IntentTranslator

translator = IntentTranslator(
    provider="auto",  # or "rules" or "claude"
    claude_model="claude-haiku-4-5",
    columns=[
        {"name": "product_id", "type": "string"},
        {"name": "sales", "type": "float"},
        {"name": "error_count", "type": "integer"},
    ]
)

intent = translator.question_to_intent("show me top 5 products by sales")
print(intent)
# {
#   "intent": "select",
#   "dimensions": ["product_id"],
#   "measures": ["total_sales"],
#   "order_by": {"measure": "total_sales", "direction": "desc"},
#   "limit": 5
# }

# LLM readiness (optional)
llm_ready, reason = translator.llm_readiness()
if not llm_ready:
    print(f"Claude integration not available: {reason}")
```

---

## Supported Query Patterns

### Ranking (Top-N)
```
"show me top 5 services by error rate"
→ SELECT ... ORDER BY error_rate DESC LIMIT 5
```

### Aggregation with Filtering
```
"show me services with error rates above average"
→ SELECT ... HAVING error_rate > (SELECT AVG(error_rate) FROM ...)
```

### Comparative Analysis
```
"show me services with high error rates and low traffic"
→ SELECT ... HAVING error_rate > threshold AND traffic < threshold
```

### Counting & Existence
```
"how many services exist"
→ SELECT COUNT(DISTINCT service_name) FROM ...
```

### Anomaly Detection
```
"show unusual services with above-average errors"
→ SELECT ... HAVING error_rate > (SELECT AVG(error_rate) FROM ...) AND anomaly_score > threshold
```

---

## Feedback & Contributions

### Report Issues
- Use GitHub Issues with the label `phase-0-feedback`
- Include: query intent, generated SQL, error message, dataset sample

### Contributing Code
- See [CONTRIBUTING.md](CONTRIBUTING.md) for pull request guidelines
- Maintain backward compatibility; add tests for new features
- Follow PEP8; run `pytest` before submitting

### Questions?
- Check [FUTURE_ENHANCEMENTS_PLANNED.md](FUTURE_ENHANCEMENTS_PLANNED.md) for planned features
- Review test examples in `tests/` for pattern inspiration

---

## Next Steps (Phase 0.2+)

- 📋 Verify SQLite/Postgres data paths
- 🔍 Add semantic validation with confidence scoring
- 📊 Extend test coverage for scale/throughput
- 🔐 Implement structured logging and error taxonomy
- 🎯 Build public benchmark corpus for translation accuracy

---

## License

See [LICENSE](LICENSE) file for details.

---

**MCP Query Engine – Phase 0**  
*A deterministic query execution layer for safe AI agent development.*

**Version:** 0.0.0  
**Latest Update:** April 3, 2026  
**Status:** Experimental / Reference Implementation
