# AGENTS.md

## Repo-specific rules
- Implement in the root `cc-reporter` service. Use sibling modules in this repo only as references.
- Build and test `cc-reporter` with Java 25. If local Maven defaults to another JDK, switch `JAVA_HOME` explicitly before making build-compatibility claims.
- For Maven phases that trigger `pg-embedded-plugin` (`initialize`, `compile`, `test`, or the IntelliJ Maven runner command), run outside sandbox restrictions; embedded Postgres can fail inside sandbox with shared-memory permission errors that do not reproduce in the normal local environment.
- Known-good local shell setup is `JAVA_HOME=/Users/karleowne/Library/Java/JavaVirtualMachines/temurin-25.0.2/Contents/Home` with `PATH=$JAVA_HOME/bin:$PATH` before invoking Maven or the IntelliJ-bundled `mvn`.
- ignore JaCoCo
- Use Lombok in this project for boilerplate reduction where behavior stays unchanged; default to
  `@RequiredArgsConstructor` for DI classes and move constructor setup logic into config beans when practical. @Data for
  getters\setters
- prefer scenario-oriented integration tests instead of utility-only tests; keep shared Spring/DB/file-storage test harness in the package-level base classes/config and keep lifecycle assertions in dedicated scenario test classes.
- Preserve and extend the repository's package structure by semantic ownership: place each class in the narrowest package that matches its domain responsibility and neighbors, and prefer splitting mixed flows into focused subpackages over adding new classes to broad catch-all packages.
- When Maven dependency semantics or Thrift / Fistful protocol details matter, use `protocol-knowledge` against the relevant `pom.xml` before making behavioral claims.
- Keep schema, Thrift IDL, CSV spec, and implementation aligned when changing shared contracts.
- If continuity docs exist for the active task, treat them as the restore surface after pause, resume, or context compaction.
- If execution state becomes uncertain, re-read the continuity docs that exist for the active task before reopening decisions or widening scope. This may include `EXECUTION_INPUT.md`, `INVARIANTS.md`, `PROJECT_STATE.md`, and the active handoff doc(s).
- checkstyle.LineLength <= 120, checkstyle.VariableDeclarationUsageDistance <= 3, npath < 200 if possible, var type is priority
- use -Dcheckstyle.skip=true
