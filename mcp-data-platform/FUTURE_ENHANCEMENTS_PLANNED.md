# Future Enhancements Planned

## Purpose
Capture what Phase 0 currently supports, what has been verified, and what to improve next for query generation and deterministic execution.

## Current Status
Phase 0 is functionally usable for deterministic dataset querying with MCP tools and agent-side intent translation.

### Supported in Phase 0
- MCP server is implemented and callable through stdio transport.
- MCP client and server connectivity is implemented end-to-end.
- Session lifecycle is implemented: `connect` -> `session_token` -> tool execution.
- Dataset registration and schema introspection are supported.
- SQL generation from structured intent is supported.
- Deterministic execution boundaries are enforced (read-only, SELECT-oriented execution).
- Agent-side question-to-intent translation is available (rules and Claude-based paths).

### What Has Been Verified
- CSV datasets are verified and used in demos/tests.
- Schema listing, direct SQL execution, and intent execution are verified.
- Representative question patterns are verified:
  - list/select questions
  - count and distinct-count questions
  - aggregate and ranking questions
  - anomaly-style and comparative patterns with deterministic SQL generation

### Verification to Add Next
- SQLite dataset verification
- Postgres dataset verification
- Extended regression sets for mixed-schema and larger datasets

## Scope of Improvements

Note: UI, CLI polish, and authentication are user-side responsibilities for now.
This document focuses on query generation quality and deterministic execution behavior.

### P0.1 Intent Quality and Safety (High Priority)
1. Add dataset-level identifier policy
- Goal: Explicitly define preferred identifier columns per dataset (for example ticker, symbol, name, id, date fallback).
- Why: Removes ambiguity for "which ..." questions and prevents metric-only answers.
- Deliverable: Config-driven identifier priority used by translator/repair flow.

2. Add ambiguity handling for missing identifiers
- Goal: Return clear clarification when question asks for entity but dataset has no entity column.
- Why: Better UX than silently returning only metric values.
- Deliverable: Standard user message + fallback strategy.

3. Add stronger intent validation before execution
- Goal: Validate semantic consistency, not only schema shape.
- Checks:
  - count questions should map to COUNT(*) or COUNT_DISTINCT
  - SUM/AVG/MIN/MAX should avoid identifier-like fields
  - highest/lowest entity questions should include at least one identifier dimension when available
  - anomaly and comparative questions should map to deterministic aggregate patterns
- Deliverable: pre-execution validator with actionable error/warning path.

### P0.2 Prompt and Translation Reliability (High Priority)
1. Introduce prompt profile per dataset domain
- Goal: Use small domain hints (finance, sales, network, education) to reduce misclassification.
- Deliverable: domain-aware prompt augmentation.

2. Add offline translation evaluation set
- Goal: Maintain a benchmark corpus of NL questions and expected intents.
- Deliverable: test fixtures + pass/fail report in CI.

3. Add translator confidence scoring
- Goal: Estimate confidence of produced intent and route low-confidence intents through rules fallback or clarification.
- Deliverable: confidence heuristic with threshold settings.

### P0.3 Deterministic Execution Hardening (Medium Priority)
1. Strengthen aggregate expression handling
- Goal: Support expected aggregate expression patterns while rejecting unsafe/ambiguous constructs.
- Deliverable: explicit expression grammar + validation errors with actionable messages.

2. Expand HAVING and subquery-safe patterns
- Goal: Support deterministic anomaly/comparative patterns (above-average, high/low threshold) with strict templates.
- Deliverable: bounded support for approved HAVING/global-average forms.

3. Improve order-by target normalization
- Goal: Avoid order-by failures by aligning selected dimensions/aliases with deterministic ordering rules.
- Deliverable: pre-build normalization rules and tests.

### P0.4 Observability and Verification (Medium Priority)
1. Structured logs for translation lifecycle
- Stages: raw question -> model intent -> normalized intent -> repaired intent -> SQL.
- Deliverable: optional JSON log output for debugging.

2. Error taxonomy for query generation and execution
- Goal: Track common failure classes (parse errors, semantic repairs, unsupported expressions, execution errors).
- Deliverable: counters/events for deterministic pipeline quality tracking.

3. Repro bundle for failed runs
- Goal: Save minimal repro artifact for bug reports.
- Deliverable: serialized question, schema snapshot, intent versions, and generated SQL.

### P0.5 Data Source Verification Expansion (Medium Priority)
1. SQLite verification track
- Goal: Validate the same deterministic behavior with SQLite-backed datasets.
- Deliverable: tests + sample scenarios equivalent to CSV coverage.

2. Postgres verification track
- Goal: Validate deterministic generation/execution assumptions with Postgres-backed datasets.
- Deliverable: connector assumptions, schema edge-case tests, and parity checks.

### P0.6 Architecture Cleanup (Medium Priority)
1. Keep MCP client transport-only boundary
- Confirmed direction: no NL reasoning in client layer.
- Deliverable: architecture note and contribution guidance.

2. Modularize agent logic further
- Suggested modules:
  - prompt_builder
  - intent_normalizer
  - intent_repair
  - intent_validator
- Deliverable: internal package structure with tests.

## Proposed Implementation Order
1. Identifier policy + ambiguity handling.
2. Semantic intent validator.
3. Deterministic expression/having hardening.
4. SQLite/Postgres verification tracks.
5. Translation evaluation dataset and CI gate.
6. Observability and failure taxonomy.

## Acceptance Criteria
- Entity-ranking questions return identifier + metric when available.
- Ambiguous questions without identifiers produce explicit clarification.
- Translation benchmark reaches agreed pass threshold (for example >= 90% exact intent match on core scenarios).
- Repair/fallback behavior is visible through logs and test coverage.
- MCP client remains transport-only and reusable across entrypoints.
- CSV and SQLite/Postgres verification suites pass for deterministic core scenarios.

## Out of Scope for Phase 0
- Full autonomous schema learning.
- Cost/latency optimization for large-scale production traffic.
- Multi-turn conversational memory and context carryover beyond current CLI session.
- UI/CLI productization and authentication framework ownership.

## Notes for Contributors
- Prioritize deterministic behavior over cleverness.
- Prefer explicit schema-driven rules over broad heuristics.
- Keep MCP server execution deterministic; retain reasoning in agent layer.
