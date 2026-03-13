# dominant-name-materialization

## Scope
- Introduce local materialized dominant projections for human-readable names used by CCR reporting:
  `shop_name`, `provider_name`, `terminal_name`, and analogous withdrawal-side display-name enrichments where applicable.
- Keep the track bounded to local enrichment tables, ingestion of dominant-derived updates, and read-time joining from CCR-owned
  state.
- Do not turn CCR into a runtime client of `daway`'s database.

## Thin continuity layer
- Restore surface for this track only:
  `EXECUTION_INPUT.md` -> `INVARIANTS.md` -> `PROJECT_STATE.md` -> `TEMP_PAYMENT_CURRENT_NOTES.md` -> this file.
- Primary truth files for decisions:
  `src/main/resources/db/migration/V1__init.sql`,
  `src/main/java/dev/vality/ccreporter/report/ReportCsvService.java`,
  `src/main/java/dev/vality/ccreporter/dao/PaymentCurrentDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/WithdrawalCurrentDao.java`,
  `src/test/resources/payment.sql`,
  `../refs/daway/src/main/resources/db/migration/V1__init.sql`,
  `../refs/daway/src/main/java/dev/vality/daway/handler/dominant/impl/ShopHandler.java`,
  `../refs/daway/src/main/java/dev/vality/daway/handler/dominant/impl/ProviderHandler.java`,
  `../refs/daway/src/main/java/dev/vality/daway/handler/dominant/impl/TerminalHandler.java`.
- If continuity becomes uncertain after pause/resume/compaction, re-read the files above before changing the enrichment plan.

## Current state
- CCR schemas already contain `shop_name`, `provider_name`, and `terminal_name` fields in current-state tables.
- Real payment events do not provide a trustworthy event-native source for those names.
- CCR currently leaves explicit projector `TODO`s and stores ids without reliable display-name enrichment.
- Reference truth from `daway` shows those names are locally materialized from dominant snapshots:
  `dw.shop.details_name`, `dw.provider.name`, `dw.terminal.name`.
- Withdrawal-side review against `daway` also shows that `wallet_name` is dominant-backed:
  `dw.wallet.wallet_name` is written by `WalletHandler` from dominant `WalletConfig.data.name`.
- The intended reference is not `daway` DB as a runtime dependency, but `daway`'s dominant ingestion pattern:
  dominant events -> local lookup tables -> local joins in reporting queries.

## Why this track exists
- The current schema and filter/search model expect display names, but the Kafka payment / withdrawal event streams do not carry
  them consistently enough to populate CCR directly.
- Joining at report time against `daway` DB would create a hard runtime dependency on another service's schema and availability.
- The cleaner architecture is to keep a CCR-owned local dominant projection and enrich CCR queries from local tables.

## Design target
- Add CCR-owned lookup tables to `V1__init.sql` for the required dominant name projections.
- Populate them from a dedicated dominant ingestion path or another CCR-owned synchronization pipeline, not from cross-service DB
  reads on the reporting hot path.
- Keep `payment_txn_current` and `withdrawal_txn_current` as id-centric projections and fetch names through local joins or a local
  enrichment query when building reports.

## Reference model from daway
- Use `daway` as the implementation example for how dominant-backed names are materialized locally.
- The concrete files to mirror conceptually are:
  `../refs/daway/src/main/resources/db/migration/V1__init.sql`,
  `../refs/daway/src/main/java/dev/vality/daway/handler/dominant/impl/ShopHandler.java`,
  `../refs/daway/src/main/java/dev/vality/daway/handler/dominant/impl/ProviderHandler.java`,
  `../refs/daway/src/main/java/dev/vality/daway/handler/dominant/impl/TerminalHandler.java`.
- The point of this reference is:
  find which dominant event payloads feed the local tables,
  which ids are used as stable lookup keys,
  and which name fields are considered authoritative.
- CCR should copy this architecture in a narrower form, only for the names required by CCR reports and filters.

## What must be extracted from daway first
- The exact dominant input source:
  Kafka topic names, consumer wiring, or any intermediate ingestion abstraction that `daway` uses before data reaches the handlers.
- The exact entity/event types that feed each lookup:
  shop, provider, terminal, and any withdrawal-relevant dominant entities if they are needed later.
- The stable CCR-side lookup keys:
  confirm whether the authoritative keys are the same ids already stored in `payment_txn_current` / `withdrawal_txn_current`.
- The authoritative name fields:
  for example `details_name` vs `name`, and whether any normalization or fallback rules exist in `daway`.
- The update semantics:
  overwrite rules, tombstone/delete behavior if any, and whether the local projection is snapshot-like or patch-like.
- The minimum payload CCR needs to persist:
  raw id + display name may be enough, or there may be version/update metadata worth keeping for replay/debugging.

## Expected CCR data flow
- Dominant events arrive through a CCR-owned ingestion path, preferably a Kafka consumer dedicated to dominant snapshots/updates.
- CCR writes those events into local lookup tables such as:
  `ccr.shop_lookup`,
  `ccr.provider_lookup`,
  `ccr.terminal_lookup`,
  `ccr.wallet_lookup`
  or an equivalent naming scheme chosen in `V1__init.sql`.
- `payment_txn_current` and `withdrawal_txn_current` remain id-based current-state tables and do not become the primary storage for
  authoritative names.
- Report-building and read/query paths resolve names locally by joining current-state ids to the CCR lookup tables.
- Search/filter behavior that currently relies on `*_name` or `*_search` must remain local to CCR after the join/enrichment step.

## Lookup-table intent
- `shop` lookup should map the payment/withdrawal `shop_id` key to the dominant-backed shop display name.
- `provider` lookup should map `provider_id` to provider display name.
- `terminal` lookup should map `terminal_id` to terminal display name.
- `wallet` lookup should map `wallet_id` to wallet display name for withdrawals.
- Withdrawal-side dominant enrichment therefore needs, at minimum:
  `provider_name`,
  `terminal_name`,
  `wallet_name`.
- If withdrawals later prove they need additional dominant-backed names, add separate lookup tables rather than overloading
  the existing ones.
- Keep these tables small, explicit, and lookup-oriented; they are not a second copy of full `daway` domain storage.

## Withdrawal-specific reference in daway
- `wallet_name` should be treated as a first-class part of this track, not as a generic future possibility.
- The reference implementation is:
  `../refs/daway/src/main/java/dev/vality/daway/handler/dominant/impl/WalletHandler.java`
  plus
  `../refs/daway/src/main/java/dev/vality/daway/dao/dominant/impl/WalletDaoImpl.java`
  and
  `../refs/daway/src/main/resources/db/migration/V45__add_wallet.sql`.
- In `daway`, wallet lookup rows are written from dominant `WalletConfigObject` / `WalletConfig`, with:
  `wallet_id = wallet config ref id`
  and
  `wallet_name = WalletConfig.data.name`.
- CCR should mirror this in a narrower form:
  materialize `wallet_id -> wallet_name` locally and use it to enrich `withdrawal_txn_current` consumers.

## Open work
- Decide the minimal lookup table set and keys:
  at least `shop`, `provider`, `terminal`, `wallet`;
  add more withdrawal-side tables only if CSV/API later proves it.
- Add the lookup tables to the base schema and generate `jOOQ` models for them.
- Define the ingestion source:
  dominant topic consumer is preferred;
  ad hoc direct reads from `daway` DB are allowed only as a temporary bootstrap tool, not as steady-state runtime behavior.
- Decide whether names should be copied into `payment_txn_current` / `withdrawal_txn_current` during write time or joined at read
  time from lookup tables. Current preference: keep lookup tables independent and join when reading reports.
- Update CSV/report query paths and current-state search behavior so name filters remain local to CCR.
- Apply the same design to withdrawals for non-payment-specific name enrichment.

## Recommended implementation order
1. Inspect `daway` dominant handlers and identify the exact dominant entities / event shapes that populate shop, provider, and terminal
   names there, plus wallet names for withdrawals.
2. Define the minimal CCR lookup schema in `V1__init.sql` with stable business keys and authoritative display-name fields:
   `shop_id -> shop_name`,
   `provider_id -> provider_name`,
   `terminal_id -> terminal_name`,
   `wallet_id -> wallet_name`.
3. Add a CCR-owned dominant ingestion path that materializes those lookup rows locally.
4. Update CCR read/report SQL so names are resolved through local joins, not through duplicated write-time copies in payment/withdrawal
   current-state projectors.
5. Only after local joins work, decide whether denormalized `*_name` copies are still worth keeping in current-state rows for search or
   export convenience.

## How withdrawals should use the lookup data
- `WithdrawalEventProjector` should continue to project stable ids from withdrawal events:
  `wallet_id`,
  `provider_id`,
  `terminal_id`.
- It should not invent or guess names from withdrawal events when the authoritative source is dominant-backed lookup data.
- Once local lookup tables exist, withdrawal read/report paths should resolve:
  `wallet_name` by `wallet_id`,
  `provider_name` by `provider_id`,
  `terminal_name` by `terminal_id`.
- If CCR later decides to denormalize names into `withdrawal_txn_current`, that should happen only after the lookup contract is proven,
  and the source of those denormalized fields must still be the local lookup tables rather than raw withdrawal events.
- The same rule applies to projector-side search columns:
  if `wallet_search` / `provider_search` / `terminal_search` need display-name content, derive that from CCR local lookups, not from
  ad hoc event payloads.

## Discovery checklist before implementation
- Identify the dominant topic names and message envelope used by `daway`.
- Identify the exact handler-to-table mapping for shop/provider/terminal/wallet.
- Write down one authoritative source field per CCR display-name column:
  `shop_name`,
  `provider_name`,
  `terminal_name`,
  `wallet_name`.
- Confirm whether withdrawals need any additional dominant-backed names beyond provider/terminal/wallet.
- Confirm whether CCR can reuse the same ids already present in current-state rows without any cross-id translation layer.
- Confirm whether local lookup rows should support hard delete, soft delete, or overwrite-only semantics.

## Explicit non-goals
- Do not query `daway` DB synchronously from CSV generation, report APIs, or Kafka projectors.
- Do not treat free-form payment or withdrawal event fields as a substitute for dominant-backed names.
- Do not make the first implementation broader than needed; the primary target set is
  `shop_name`,
  `provider_name`,
  `terminal_name`,
  `wallet_name`.

## Constraints
- Do not add a synchronous runtime dependency on `daway` DB for report generation.
- Do not invent fallback names from ids or free-form event fields when dominant is the authoritative source.
- Keep the enrichment model local to CCR and compatible with current CSV / Thrift contracts.

## Next step
- Inspect `daway` dominant event handlers first and write down the concrete CCR lookup schema plus the expected dominant input types
  before implementing any CCR-side tables or consumers.

## Done when
- CCR owns local lookup tables for required dominant-driven display names.
- Payment and withdrawal reports can resolve `*_name` fields locally without depending on foreign databases.
- The previous projector `TODO`s around missing name sources are closed by a documented local enrichment path.
