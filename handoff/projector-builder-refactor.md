# projector-builder-refactor

## Scope
- Refactor projector-side current-state assembly so payment and withdrawal projection code is easier to read, evolve, and extend.
- Replace constructor-like / setter-heavy row assembly with projector-local builder-based composition where it improves clarity.
- Keep the track bounded to model assembly and projector / mapper readability, not to behavioral changes in report contracts.

## Thin continuity layer
- Restore surface for this track only:
  `EXECUTION_INPUT.md` -> `INVARIANTS.md` -> `PROJECT_STATE.md` -> `TEMP_PAYMENT_CURRENT_NOTES.md` -> this file.
- Primary truth files for decisions:
  `pom.xml`,
  `src/main/java/dev/vality/ccreporter/ingestion/PaymentEventProjector.java`,
  `src/main/java/dev/vality/ccreporter/ingestion/WithdrawalEventProjector.java`,
  `target/generated-sources/dev/vality/ccreporter/domain/tables/pojos/PaymentTxnCurrent.java`,
  `target/generated-sources/dev/vality/ccreporter/domain/tables/pojos/WithdrawalTxnCurrent.java`,
  `src/test/java/dev/vality/ccreporter/integration/IngestionSerializedEventsIntegrationTest.java`,
  `src/test/java/dev/vality/ccreporter/integration/IngestionToReportLifecycleIntegrationTest.java`.
- If continuity becomes uncertain after pause/resume/compaction, re-read the files above before changing projector assembly shape.

## Current state
- `PaymentEventProjector` is already large and mixes event decoding, field extraction, patch semantics, and row assembly.
- Current `PaymentTxnCurrent` assembly is positional / setter-heavy, which makes new field additions awkward and error-prone.
- Real payment fixture work already expanded the projector with:
  `payment_tool_type`,
  `trx_id` fallback from proxy state,
  temporary FX field population,
  and `error_summary`.
- The same readability pressure exists on the withdrawal side for generic projector assembly concerns.
- Primary truth no longer supports the initial Lombok assumption for this track:
  `pom.xml` has no Lombok dependency or annotation processor configured.
  Builder-oriented refactors must therefore use projector-local fluent builders unless and until build tooling changes.

## Why this track exists
- Projector logic is now carrying enough field mapping that readability and extension cost matter.
- A builder-based assembly layer makes it easier to add or reorder fields without long positional factory methods.
- The refactor should reduce incidental complexity before any further current-state expansion work such as replay semantics or
  dominant-name enrichment.

## Design target
- Introduce projector-local fluent builders around generated `jOOQ` POJOs where that meaningfully improves row construction.
- Start with payments:
  create a projector-friendly assembly layer around `PaymentTxnCurrent`,
  build rows through fluent builder composition,
  and shrink `PaymentEventProjector` into smaller extraction helpers.
- Apply the same refactoring style to withdrawals for the generic parts:
  builder-based row composition,
  helper extraction,
  and reduction of setter-heavy code.

## Current result
- `PaymentEventProjector` now routes each payment change type through small helper methods backed by
  `PaymentCurrentUpdateBuilder`; the old positional factory method is gone.
- `WithdrawalEventProjector` now uses the same pattern via `WithdrawalCurrentUpdateBuilder`.
- `WithdrawalSessionEventProjector` now reuses the withdrawal builder for transaction-bound current-state updates, so the
  withdrawal side no longer has a stray setter-heavy `WithdrawalTxnCurrent` assembly path.
- Java 25 verification passed on the track-defining test slice:
  `IngestionSerializedEventsIntegrationTest`,
  `IngestionToReportLifecycleIntegrationTest`.

## Constraints
- Do not widen generated `jOOQ` POJOs into arbitrary business-domain inheritance without a clear projector-local reason.
- Do not mix readability refactors with unrelated contract changes in the same step.
- Preserve current ingestion semantics while reshaping assembly code.
- Apply generic improvements to withdrawals too; do not leave the repo with one polished projector and one stale one.
- Do not add Lombok just to satisfy the original handoff direction unless primary truth changes first.

## Next step
- No further work is required for this track unless future projector expansion reintroduces setter-heavy assembly.

## Done when
- Payment and withdrawal current-state projectors assemble rows through a consistent builder-oriented pattern.
- `PaymentEventProjector` no longer relies on the current monolithic positional assembly style.
- The refactor is covered by existing serialized-event and end-to-end ingestion/report integration tests.

## Status
- Done.
