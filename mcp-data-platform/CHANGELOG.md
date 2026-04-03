# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning for pre-1.0 releases.

## [0.1.0] - 2026-04-03

Release type: Phase 0 (Experimental)
Project display name: MCP Query Engine
Maintainers: Project Owners

### Added
- MCP SDK native server and client connectivity over stdio.
- Session-oriented tool flow: connect, capabilities, dataset registration, schema inspection, query execution.
- Deterministic SQL generation from structured intents.
- Agent-side intent translation module with rule-based and Claude-backed paths.
- Support for combined aggregate expressions and anomaly-oriented aggregate filtering patterns.
- Runtime stability tests for ranking, counting, distinct counting, and comparative/anomaly scenarios.

### Changed
- Refactored translator logic out of CLI into a dedicated module for cleaner boundaries.
- Improved ranking repairs to include identifier context with metrics where needed.
- Strengthened intent normalization/repair for count, distinct, comparative, and top-N questions.
- Expanded aggregate query handling with deterministic HAVING/global-average compatible patterns.

### Fixed
- Environment path issues from moved workspace virtual environments.
- Duplicate values in list-style select outputs by applying DISTINCT where appropriate.
- Incorrect count-style intents produced by LLM translation in common cases.
- Order-by failures when metric columns were not included in selected dimensions.

### Verified
- CSV data path validated end-to-end (register, schema, direct SQL, generated SQL, execute intent).
- Targeted test suites passing for runtime stability and aggregate query generation.

### Known Limitations
- UI/CLI productization and auth framework are out of Phase 0 scope.
- SQLite/Postgres verification tracks are planned but not yet completed.
- This release is not production-hardened for scale/throughput.
