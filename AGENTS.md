# AGENTS.md

## Repo-specific rules
- Implement in the root `cc-reporter` service. Use sibling modules in this repo only as references.
- When Maven dependency semantics or Thrift / Fistful protocol details matter, use `protocol-knowledge` against the relevant `pom.xml` before making behavioral claims.
- Keep schema, Thrift IDL, CSV spec, and implementation aligned when changing shared contracts.
