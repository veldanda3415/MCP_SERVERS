# Contributing to MCP Data Platform

Thanks for your interest in contributing.

## How to Contribute

1. Fork the repository
2. Create a feature branch:
   git checkout -b feature/your-feature-name
3. Make your changes
4. Add/update tests
5. Submit a Pull Request

---

## Guidelines

- Keep changes small and focused
- Write clear and simple code
- Maintain deterministic behavior
- Keep reasoning in the agent layer and execution in MCP server/client layers
- Do not introduce write operations to query execution paths

---

## Code Style

- Python 3.11+
- Follow PEP8
- Keep public APIs backward compatible unless clearly documented in PR

---

## Testing

Run:

```bash
pytest -q
```

For this project specifically, run tests from `mcp-data-platform/`:

```bash
cd mcp-data-platform
pytest -q
```

If you touch translator or prompt behavior, include/update targeted tests in `tests/test_runtime_stability.py`.

---

## Reporting Issues

Include:
- Steps to reproduce
- Expected vs actual behavior
- Dataset/sample input used
- Relevant logs or traceback

---

## Feature Requests

Include:
- Problem statement
- Proposed solution

---

## Pull Request Checklist

- [ ] Changes are scoped and documented
- [ ] Tests added/updated and passing
- [ ] README/docs updated if behavior changed
- [ ] No secrets/credentials added to repo
