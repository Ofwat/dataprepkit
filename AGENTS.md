# AGENTS

## Primary Guidance
- Keep all code changes as small and focused as possible; prefer the lowest line count needed to solve the task.
- Keep the codebase as lean as possible; avoid unnecessary files or bloat when implementing requirements.
- Respect existing structure and avoid adding new files unless explicitly requested.
- Use the strictest Python style guidance (PEP 8, strict linters) when editing and assume VSCode defaults.
- Run or explain the relevant verification steps when touching functionality.
- Prefer a simple, easy-to-follow modular codebase; split responsibilities cleanly and keep helpers focused per module.

## Interactions
- When asked to refactor or improve code, point out risks/next steps, but do not invent large rewrites without a follow-up ask.
- Keep comments and documentation concise so they stay readable.
- Always mention tests or checks you performed in your final response.
- When the user mentions public API endpoints, add tests that exercise those behaviors and favor unions or shared fixtures to minimize duplicated test code.
- Avoid focusing on testing private API branches; prioritize public surface behavior when recommending tests.
- Follow a test-driven development approach: propose/tests public behavior before implementing or modifying the supporting code path.
- Assume Fabric SQL Database is the production target for SCD2 workloads, but rely on SQLite (or DuckDB) for local/unit testing to keep feedback loops fast.
