# Schema-aware Java sender: contract simplification proposals

Date: 2026-09-18

Status: proposal 1 is implemented (see the "One schema snapshot per table per batch" section of
[schema-aware-sender.md](schema-aware-sender.md)), with one deviation: there is no explicit refresh
operation. Batch-boundary adoption relies on ACK schema feedback and reconnects, because a caller cannot
distinguish a stale-snapshot rejection from invalid input and so has no sound rule for when to refresh.
Proposals 2 to 4 remain proposals for discussion, not approved contract or implemented behavior.

These ideas come from the second complexity review of client
`7a73e58e8d9354a045c9895c563e8009cd6ff8cf`, including the corresponding server paths inspected at
`6b0c992b18`. They deliberately question the contract rather than only refactoring its implementation.

## Recommendation

Simplify the promises before doing another mechanical cleanup. Much of the remaining complexity implements
server-compatible casting, automatic schema refresh, and adaptation while batches are being built. Those promises
are stronger than the core requirement:

> Validate types before creating, and potentially persisting, a data frame.

The proposed direction is typed writes checked against a stable schema snapshot, followed by the existing encoder
and persistence pipeline. Do not build a general conversion framework or another buffering lifecycle.

Start with proposal 1: extend the batch-boundary rule to schema versions and make refresh explicit. It offers a
substantial lifecycle simplification without first redesigning the conversion catalogue or wire protocol.

Validation remains relative to the chosen snapshot. It cannot guarantee eventual server acceptance after concurrent
DDL, permission changes, or other server-side failures. The current implementation does not provide that guarantee
either.

## 1. One schema snapshot per table per batch

### Assumption to change

Every new row should observe the latest cached schema, and a failed setter should automatically refresh metadata
before deciding which error to report.

Currently, a schema identity change can replace a table buffer and retain its previous generation even when the
relevant column is unchanged. This requires pending generations, additional flush collection and accounting, and
cleanup across reset, flush, and close. The one-refresh policy also tracks whether each failing operation already
obtained fresh metadata and compares its old and new targets.

### Proposed contract

- Pin one schema snapshot per table for the entire pending batch.
- Adopt another snapshot only after successful flush or explicit reset. A failed flush retains the existing snapshot.
- Make explicit refresh an empty-batch operation. Initial lookup and lookup on a cache miss still obtain metadata.
- Report local validation failures directly; do not perform a network lookup from the failed setter.
- Do not implicitly flush pending rows merely to adopt a new schema.

This is separate from the already-resolved legacy-to-modern transition. It applies the same boundary principle to
different schema versions within schema mode.

### Complexity that can disappear

`retiredTableBuffers`, multiple pending generations of one table, their flush/accounting/cleanup paths, per-operation
freshness bookkeeping, relevant-target comparison, and the automatic `SCHEMA_CHANGED` recovery path.

Layout replacement is still necessary when a schema changes: `QwpTableBuffer.reset()` retains column definitions.
The simplification is that replacement happens when the old buffer has no pending rows, rather than retaining both
layouts for a later flush.

### User impact

Applications must coordinate schema changes and refresh deliberately. An input made valid by DDL may continue to
be rejected until refresh. Earlier completed rows remain intact; refresh does not silently discard or rewrite them.

Evidence: [QwpWebSocketSender](../core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketSender.java),
especially `bindingForEffectiveWrite()`, `installSchemaBinding()`, `refreshAfterSchemaRejection()`, and
`collectNonEmptyTables()`. The existing
[`testUnrelatedVersionChangeKeepsOriginalRejectionAndRetainsOldRows()`](../core/src/test/java/io/questdb/client/test/cutlass/qwp/client/QwpSchemaSenderIntegrationTest.java)
demonstrates separate generations after an unrelated version change.

## 2. Keep schema identities out of new persisted data frames

### Assumption to change

Local schema validation requires a schema-extended data-frame format and schema feedback in write responses.

The current `(tableId, metadataVersion)` pair is not an acceptance precondition. The server processes wire values
normally; matching identities suppress schema feedback. A version mismatch alone does not reject the write or
prevent the server from applying another conversion.

### Proposed contract

Retain schema lookup and local validation, then emit ordinary QWP data frames. Replace automatic ACK/NACK schema
feedback with explicit refresh. Keep the validation snapshot in memory; replay does not need to reconstruct it.

This requires a coordinated client/server change, not merely ignoring feedback in the client. Schema lookup still
needs capability negotiation and authorization.

### Complexity that can disappear

- Schema identities and the additional data-frame encoding path.
- Schema-update lists and invalidate-all responses, including their ordering and size-limit rules.
- Schema-specific persisted-frame detection and replay capability restrictions for the new ordinary frames.

### User impact and migration constraints

Applications lose passive schema updates from writes and must refresh deliberately. Ordinary ACK, durable ACK,
terminal-NACK, and replay semantics must remain unchanged.

Do not equate ordinary framing with reverting all value encoding. In particular, preserve the forced GEOHASH null
bitmap needed to distinguish valid all-one values from sentinel NULL. Calling today's legacy encoder unchanged is
not an equivalent implementation.

Any already-persisted extended frames must retain a compatible reader/drainer until they are drained. Do not strip
identities, rewrite queued frames, discard data, or advance watermarks to simplify migration. That compatibility
requirement may delay deletion of old replay support.

Evidence: server
[`QwpIngressProcessorState.captureSchemaFeedback()`](../../core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressProcessorState.java);
client [QwpWebSocketEncoder](../core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketEncoder.java),
[WebSocketResponse](../core/src/main/java/io/questdb/client/cutlass/qwp/client/WebSocketResponse.java), and
[`QwpColumnWriter.encodeSchemaTable()`](../core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpColumnWriter.java).

## 3. Validate typed inputs instead of reproducing the server's casting catalogue

### Assumption to change

The client must accept essentially every intentional setter-to-column conversion that the server accepts, with
compatible parsing, formatting, rounding, and null behavior.

`stringColumn()` currently supports numeric parsing, timestamps, dates, UUIDs, decimals, geohashes, binary, and
other targets. Other setters implement reverse formatting conversions. This makes the binding a conversion engine,
not merely a validator.

### Proposed contract

Accept native typed inputs and a small, explicitly documented set of safe conversions. Reject arbitrary cross-family
casts before framing. For example:

- UUID columns require UUID input rather than UUID-looking strings.
- Timestamp columns use timestamp setters.
- Applications format numbers themselves when writing text columns.
- DOUBLE arrays remain the only supported array element type; validate the rank against the target.

Keep value-dependent validation where type compatibility alone is insufficient: decimal precision and scale,
timestamp overflow, numeric range where conversions remain, and array shape/rank constraints. Narrowing the cast
catalogue must not defer these checks to the server.

### Complexity that can disappear

Temporal text conversion alone accounts for the dedicated 818-line parser and 187-line formatter at the reviewed
revision. Removing those conversions makes both files unnecessary. A narrower catalogue also removes many binding
switch branches, formatting/parsing helpers, scratch sinks, and compatibility-specific test cases.

Do not delete shared utilities merely because one consumer disappears. The existing typed APIs may still need
parsing or representation normalization; the target is implicit cross-family casting, not all conversion code.

### User impact

Some currently accepted setter/column combinations become local errors. Applications must parse or format explicitly
and choose the appropriate setter. DATE needs a deliberate mapping because `Sender` has no `dateColumn()` method.

This is the largest compatibility change, but also the largest reduction in conversion code. Agree on the retained
conversion list before implementation rather than growing it opportunistically during the rewrite.

Evidence: [QwpSchemaBinding](../core/src/main/java/io/questdb/client/cutlass/qwp/protocol/QwpSchemaBinding.java),
[QwpSchemaTimestampParser](../core/src/main/java/io/questdb/client/cutlass/qwp/protocol/QwpSchemaTimestampParser.java),
and [QwpSchemaTimestampFormatter](../core/src/main/java/io/questdb/client/cutlass/qwp/protocol/QwpSchemaTimestampFormatter.java).

## 4. Require a known target schema for validated writes

### Assumption to change

The same validated path must also support old servers, fresh-process offline ingestion, and automatic table/column
creation without known target metadata.

The current asynchronous cold-start path deliberately accepts legacy writes without metadata. It cannot satisfy an
unconditional validate-before-framing guarantee. A confirmed missing table or column likewise has no existing target
type to validate against; inference is a separate contract.

### Proposed contract

- Cold-start writes wait or fail until metadata is available.
- Cached schemas may still support offline buffering.
- Unknown tables and columns require provisioning and refresh instead of inference and automatic creation.
- If old-server compatibility remains, expose it as explicitly unchecked behavior outside the validated contract,
  not as an automatic fallback within it.

### Complexity that can disappear

Missing-schema inference and adoption, unknown target identities, and opportunistic legacy-to-schema transitions
within the validated path.

### User impact

Less zero-setup ingestion, no fresh-process offline ingestion without metadata, and explicit schema provisioning.
Do not compensate automatically by adding a persisted metadata cache: that introduces another format and freshness
lifecycle that needs its own justification.

Evidence: [`QwpWebSocketSender.bindingForEffectiveWrite()`](../core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketSender.java),
the missing-target branches in [QwpSchemaBinding](../core/src/main/java/io/questdb/client/cutlass/qwp/protocol/QwpSchemaBinding.java),
and `testAsyncWritesLegacyOfflineThenUsesSchemaAfterSupportingConnect()` in
[QwpSchemaSenderIntegrationTest](../core/src/test/java/io/questdb/client/test/cutlass/qwp/client/QwpSchemaSenderIntegrationTest.java).

## Validation timing and complexity worth keeping

The core requirement permits type errors at `at()`/`atNow()` rather than at every setter. Allow that flexibility if it
lets validation reuse the existing row-commit and rollback machinery. Do not introduce a second store of original
inputs merely to delay validation.

Avoid moving all validation to flush: recovering one rejected row is substantially easier than reconstructing a
rejected batch. Every encoding path, including auto-flush and close, must still be protected before it creates a
data frame.

Keep immutable snapshots, bounded lookup with cancellation, authorization checks, row rollback, parameter/range
validation, and unchanged-byte replay. Those mechanisms support the actual requirement or existing delivery safety.

## Review validation and limits

The review ran 252 focused client tests on Java 8 at the reviewed revision; all passed. That validates the current
contract, not these proposed replacements. The proposals were derived from source and contract analysis, not an
implemented prototype or measured performance comparison.

Each adopted contract change needs explicit regression coverage for its new acceptance/error behavior, pending-row
retention, auto-flush, transaction boundaries, and replay compatibility as applicable. No production implementation
was changed as part of this review or this document.
