# Release and API Plan

This repository is being stabilized in two steps:

## 1.0: Compatibility-preserving polish

Goal: make the current behavior reliable enough to support as a stable release
without changing the public surface.

Focus areas:

- Keep existing function names, signatures, defaults, and return types stable.
- Fix behavior mismatches surfaced by the test suite.
- Keep dialect-specific SQL behavior consistent for existing users.
- Replace ad hoc output like `print()` with structured logging.
- Tighten packaging and dependency hygiene where it does not change runtime
  behavior.
- Add or update tests around public behavior before changing internals.

## 2.0: Public API cleanup

Goal: reduce the supported surface and make the package easier to understand
and maintain, even if that requires breaking changes.

Likely changes:

- Introduce a smaller official facade for supported entrypoints.
- Move internal helpers behind private modules.
- Centralize shared SQL, quoting, and schema helpers.
- Simplify configuration shapes where the current API is too broad.
- Remove compatibility shims only after a deliberate deprecation window.

## Working rule

- For 1.0 work, prefer wrapper-based refactors and behavior-preserving edits.
- For 2.0 work, prefer simplification over compatibility.

## Current stance

- The core ETL workflows are strong enough to keep building on.
- The codebase is not yet shaped like a fully polished public library API.
- The safe path is to stabilize 1.0 first, then redesign the surface in 2.0.
