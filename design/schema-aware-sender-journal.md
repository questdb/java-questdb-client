# Schema-aware sender implementation journal

North star: [schema-aware sender design](schema-aware-sender.md), revision 59
(revision 9 at kickoff; source-backed clarifications recorded below).

This journal records decisions, evidence, review outcomes and remaining work.
The design defines the intended product contract; this journal must not imply
that an unimplemented part already works. Corrections are recorded explicitly.

Current status: iterations 1 through 2.37 are accepted for their recorded scopes, including typed
microsecond/nanosecond conversion (D020), broader timestamp units and `Instant`
with the approved range/precision rules (D021), and text/symbol conversions
(D023). Boolean conversion and strict ASCII text-to-boolean parsing are accepted
in D024. D030 revises D022's sequencing: public Sender integration is accepted
on the unreleased branch; full conversion coverage still gates release.
FLOAT/DOUBLE input conversion is accepted under the approved D026 rules,
including consistent NaN missing values and mathematical integer range checks.
STRING-to-numeric conversion is accepted in D027, preserving target-specific
syntax and rejecting LONG text overflow under the approved exception.
BINARY targets and exception-safe string/binary append rollback are accepted
in D029. LONG/UUID/floating text output is accepted in D039–D041; LONG and
STRING input to ordinary timestamps is accepted in D042–D043. Timestamp-to-text
with checked normalization is accepted in D044. Native CHAR and STRING-to-CHAR
are accepted in D045; STRING-to-LONG256 is accepted in D046, STRING-to-GEOHASH
in D047, and all 18 logical native decimal-to-decimal pairs in D048.
LONG-to-decimal is accepted in D049, STRING-to-decimal in D050,
FLOAT/DOUBLE-to-decimal in D051 and native-decimal-to-text in D053.
Negotiated discovery, lookup/cache coordination, ACK/NACK feedback, pinned
schema framing and safe SF recovery/replay are implemented and tested.
Non-owning bindings provide local UUID, LONG-to-numeric, primitive/`Instant`
timestamp, text/symbol, boolean, FLOAT/DOUBLE, STRING numeric, BINARY-target and
native decimal-to-decimal, LONG/STRING/FLOAT/DOUBLE-to-decimal and native-
decimal-to-text conversion over caller-owned table buffers. Iteration 2.38 adds
the textual decimal overload to all decimal and text targets and is locally
validated but not committed.
Review corrections are recorded in D016 and D019; the approved LONG null rule
is recorded in D013.

Automatic row-boundary schema adoption and ordinary Sender conversion are
implemented and tested in iteration 2.15. The original 47-method failure ledger
is fully resolved after iteration 2.36, but the complete feature is not yet
releasable because the wider conversion inventory remains open.
D030 records the original 58, D032 the 56-test baseline, D033 the 54-test
inference result and D034 the current diagnostic repair.
The 2.15 scoped gates passed 410 client tests plus two packaging
checks, 222 server tests and three seeded repeats of the eight new E2Es.
Ordinary Sender can recover extended backlog and enforce its required
capability. Remaining conversion and release-validation work stays explicit
in the design's implementation-status section.

Iteration 2.16 validates file-backed process-restart recovery through ordinary
Sender without manually seeded frames. The final server gate passed 230 tests;
the two new recovery cases also passed three seeded repeats. Client production
code and the accepted 2.15 artifact are unchanged. D031 records scope and evidence.

Iteration 2.17's first repair group is accepted: independent legacy conversion
comparisons are restored, with 251 scoped tests and three repeats of the 25
affected tests passing. The broad 333-test selection still has 56 failures/errors;
functional fixes remain ahead. D032 separates fixture, product and environment
evidence and records the reviewed inference plan.

Iteration 2.18 implements confirmed-missing inference for supported setters using
the existing binding and buffer. The final gates passed 414 client tests plus
two packaging checks, 261 server tests and three seeded repeats of 16 integration
and recovery tests. The original 333-test selection has 54 failures/errors, with
two removals and no new failing methods. D033 records scope and evidence.

Iteration 2.19 repairs seven stale diagnostic expectations without production
changes. The 268-test server gate and three repeats of 23 integration/recovery
cases passed. The original 333-test selection has 47 failures/errors, with
exactly seven removals and no additions. D034 records independent legacy
rejection coverage, typed local recovery, evidence and remaining decisions.

Iteration 2.20 characterizes LONG/UUID null-sentinel text behavior without
production changes. Three new tests and the 271-test server gate pass, as do
three repeats of 26 integration/recovery cases. The broad 47-method failure set
is unchanged. D035 corrects the predicted LONG text result and records a
proposed, not yet approved, source-null policy for these five conversions.

D036 records the [remaining-failures plan](schema-aware-sender-remaining-failures-plan.md),
including all 47 baseline methods and the source-backed distinction between
nine test repairs and 38 missing-conversion cases. No further implementation
iteration or new test run is accepted by this planning entry.

Iteration 2.21 accepts phase 1a: five expectation/fixture repairs and one new
public Sender floating-boundary test. The 277-test server gate and three
32-test repeats pass. The original 333-test diagnostic now has 42 failures,
with exactly the five planned removals and no additions. Production code and
the client artifact are unchanged. D037 records acceptance and remaining work.

Iteration 2.22 accepts phase 1b: four direct-buffer fixtures now send explicit
legacy frames with exact wire and original stored-value assertions. The 281-test
gate and three 36-test repeats pass. The original 333-test diagnostic has 38
failing methods, with exactly the four planned removals and no additions.
All nine phase-1 test repairs are complete; production code and the client
artifact remain unchanged. D038 records acceptance and the conversion backlog.

## Working agreement — 2026-09-09

The primary agent owns architecture, scope, integration and acceptance. SOL
subagents receive bounded implementation, source-validation and independent
review assignments. Authors self-review; a separate reviewer and adversarial
reviewer challenge important changes before an iteration is accepted. Reviews
are evidence, not votes: findings must be resolved or explicitly justified.

Each iteration follows this loop:

1. State the smallest observable behavior being delivered and its non-goals.
2. Verify load-bearing assumptions in current source and existing tests.
3. Delegate implementation with explicit file ownership and acceptance tests.
4. Run the real client/server E2E path and relevant regression tests.
5. Review correctness, failure sequences, simplicity and developer experience.
6. Record findings, fixes, exact validation and what the iteration did not prove.

Prefer existing components and direct control flow. Do not introduce a framework
for a single use case. Keep producer state producer-owned and replay immutable.
Review allocations, synchronization and repeated work on the steady-state write
path, but do not claim performance from inspection alone. Changes to a hot path
need a measured, reproducible comparison before performance claims are accepted.

Tests use public API outcomes, protocol contracts, deterministic SQL results and
delivery behavior. No reflection, private-field inspection, test-only production
hooks, visibility changes solely for tests or implementation call-count checks.
Use bounded event-based coordination. A mocked peer is useful for wire faults,
not a replacement for real-server ingestion tests.

No commits, pushes or external coordination are part of the current instruction.
Preserve pre-existing work; serialize builds sharing generated outputs.

## Carried-forward product decisions

- Early detection means converting supplied values against the server schema
  before buffering, not merely comparing setter types or an acceptance mask.
- Keep QWP v1. New clients work with old servers in legacy mode and always
  request schema support. Confirmation enables schema mode without an opt-out.
- Upgrade is one-way for a sender. Subsequent connections must support the
  extension. No send-time transformation, downgrade encoder or SF rewriting.
- An in-progress row and completed blocks retain their encoding snapshot.
  Adopt new schema or legacy-to-schema mode only at a real row boundary.
- A local conversion/lookup failure cancels the partial row, not completed
  rows. A refreshed schema can require a whole-row retry; never splice a new
  converter into an already partially converted row.
- Server DDL and auto-creation races remain server-authoritative. Metadata
  feedback is not acceptance, permission to retry a terminal NACK or ACK progress.
- Schema mode uses target missing-value semantics; legacy mode preserves the
  existing behavior. Conversion parity for supplied values needs shared vectors.
- Ingestion schema discovery follows write authorization, not SELECT permission.
- Schema caches grow on demand, with the agreed million-entry ceiling. No
  subscriptions, polling or eager million-entry allocation.
- A real E2E test works from the first iteration. Negotiation, conversion
  families, schema evolution and recovery are added and tested incrementally.

## D001 — Start with the existing real-server test infrastructure

Date: 2026-09-09. Status: accepted; iteration 1 completed and verified.

Iteration 1 establishes a small public `Sender` → real QuestDB → SQL baseline
that subsequent schema work can extend. Use the sibling server's existing QWP
E2E harness, not a new embedded server framework in the standalone client.
Production wire and conversion behavior remain unchanged in this iteration.

The baseline must exercise a precreated target schema and matching typed values.
Additional cases should establish meaningful row/flush and local-error recovery
contracts, without pretending that local STRING-to-UUID conversion exists yet.

Reason: a green path through the actual client artifact, transport, WAL ingestion
and SQL is the earliest useful integration gate. It is not proof of the proposed
schema extension. Existing adjacent tests remain part of validation.

Starting workspace:

- Client: `/home/jara/devel/oss/java-questdb-client`, HEAD `981bdb02`.
  Pre-existing untracked file: `design/schema-aware-sender.md`.
- Server: `/home/jara/devel/oss/questdb`, HEAD `12a33d651e`.
  Pre-existing untracked directory: `core/rust/qdbr/parquet2/`; out of scope.
- Toolchain: Corretto JDK 25.0.4, Maven 3.9.11, Linux amd64.
- `/tmp` has limited free space. Use task-owned temporary test storage on the
  workspace filesystem if needed; do not delete unrelated temporary files.

Assignments:

- `baseline_implementation` (SOL): implement the narrow real-server E2E baseline;
  no production or build-configuration edits.
- `architecture_validation` (SOL): verify source assumptions and recommend the
  smallest subsequent lookup/binding slice; read-only initially.
- `adversarial_review` (SOL): challenge failure sequences and compatibility
  assumptions independently; read-only.
- Primary architect: journal, build/artifact provenance, scope and acceptance.

## D002 — Test the actual standalone client artifact

Date: 2026-09-09. Status: accepted and verified.

The server also contains a separate `java-questdb-client` checkout. Do not assume
that source is the client under test. Build/install the standalone workspace's
`questdb-client:1.3.10-SNAPSHOT`, then run the server tests without activating its
`local-client` reactor profile. Check the packaged manifest, artifact hash and
Surefire classpath. No source copies or build-configuration edits are needed.

Verified artifact SHA-256 (workspace JAR and installed JAR match):
`b9f547437a66825e11da10d31bd89b8e4092c49c30b4f2ed4b09bc9af58df969`.
Manifest commit: `981bdb02a471f3b290c89b8e78cbc422610e329e`.

## D003 — Interpret review findings against iteration scope

Date: 2026-09-09. Status: accepted.

The adversarial review confirms that current code does not yet have first-write
mode selection, immutable schema generations or lookup gating. These are planned
production changes, not defects introduced by the baseline test. Do not inflate
the first iteration to implement all of them.

Two findings do refine the next brief:

- Minimal request/confirmation classification must accompany the first real
  schema lookup. A lookup cannot be sent to an unconfirmed peer. Milestone 3
  adds the broader compatibility/transition matrix; it does not authorize an
  unsafe handshake shortcut in milestone 2.
- A matching-type SQL success cannot demonstrate that schema lookup/binding
  occurred. The first schema slice must also observe the wire contract and a
  controlled lookup failure at the public setter, before any row is accepted.

Do not add a generic rollback framework merely because a reviewer suggested
one. Establish the required A/invalid-B/C behavior across generations first;
use the smallest producer-owned state transition that satisfies it.

## D004 — Make duplicate semantics explicit per mode

Date: 2026-09-09. Status: source-verified; implementation remains future work.

`QwpTableBuffer.getOrCreateColumn()` calls `lookupColumn(name, type)` before the
already-written check. `lookupColumn()` checks the bound type first. Thus current
legacy behavior ignores same-type duplicates but rejects a different setter
type, even when the column already has a value in that row.

Schema mode's first-value-wins rule must check whether the target column was
already supplied before converting a second value, including another setter
type. Legacy mode must preserve its existing type-sensitive behavior. Add
black-box tests for both, including a valid UUID followed by an ignored malformed
UUID string in schema mode. Do not describe this as universally unchanged
duplicate behavior.

SOL updated the design to revision 10 with this distinction and D003's minimal
confirmation prerequisite. These are source-backed clarifications of the agreed
modes, not a new opt-out, downgrade path or expansion of the first iteration.

## Iteration 1 evidence and review

Implemented by SOL: one method in the existing server `QwpSenderE2ETest`,
`testTypedUuidPreservesCompletedRowAfterLocalError`. The scenario is precreated
UUID target, completed A, partially written B with a local type error, explicit
flush, valid C omitting B's marker, flush/close, exact SQL containing only A/C.

Executed from the standalone client workspace:

```sh
mvn -B -pl core -Dtest=QwpTableBufferTest,QwpWebSocketEncoderTest,QwpWebSocketSenderTest -Dmaven.javadoc.skip=true install
```

Result: PASS, 151 focused unit tests and 2 `JarPackagingIT` tests, no failures,
errors or skips. The resulting snapshot is installed for the server E2E run.

Executed from the server workspace:

```sh
mvn -B -pl core '-Dtest=QwpSenderE2ETest#testTypedUuidPreservesCompletedRowAfterLocalError+testUuid,QwpWebSocketTypeConversionE2ETest,QwpWebSocketSenderReceiverTest#testColumnTypeMismatchThrowsClientSide' -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-aware-tests.lOeKVW -Dmaven.compiler.showWarnings=false test
```

Initial result: FAIL, 59 tests, 1 setup error, no assertion failures or skips.
All 58 existing adjacent tests passed. The new test's
`connectWs(port, 0, 0, 0)` configuration was rejected with
`disabling auto-flush is not supported for WebSocket protocol`.

Reflection: author and architect both assumed that the helper's all-off
translation was a supported configuration. The real artifact test disproved
that assumption before any schema code was added. Fix the test through supported
public configuration, keeping A demonstrably buffered at the local error. Do not
change production configuration rules or rely on timing to make the test pass.

The corrected public configuration uses row threshold 2, disabled byte trigger
and the largest accepted interval (`Integer.MAX_VALUE - 1` milliseconds). Only A
completes before the first flush; B fails in its setter. Review the actual
interval rule, not the helper's apparent ability to disable every trigger.

Second run of the same 59-test selection: 58 adjacent tests passed; the new
test reached SQL verification but failed `Expected no timestamp but found ts,
idx=2`. The stronger `.returns(...)` assertion also validates cursor metadata;
the query needs an explicit designated-timestamp expectation. Keep that stronger
assertion rather than reverting to `.returnsOnce(...)`.

Independent and adversarial source reviews accept the A/B/C test shape and
confirmed meaningful leak detection and UUID limb-order coverage. They did not
catch the missing query metadata expectation. Source-review approval therefore
does not complete the acceptance gate; executed tests remain mandatory.

Architect review adds one further discriminator: use public
`flushAndGetSequence()` results to prove that each explicit flush actually
publishes data. Final SQL after `close()` alone could conceal a no-op flush.
Assert a nonnegative first FSN and increasing second FSN, not an exact number
or internal call count. This also checks the claim that A was still buffered.

Final focused command uses the same server build options above with
`-Dtest=QwpSenderE2ETest#testTypedUuidPreservesCompletedRowAfterLocalError`.
Result: PASS, 1 test, no failures, errors or skips. The actual test was compiled
and executed against the freshly installed standalone client artifact.

Both SOL reviewers accepted the final test independently after the FSN and
timestamp assertions were added. The final architecture check confirmed those
FSN comparisons are public publication semantics, not internal counters. The
adversarial check distinguishes publication (FSNs), delivery (close/ACK drain)
and stored state (SQL after WAL application).

Final regression rerun used the same 59-test selection and temporary directory,
with `surefire:test` after the final focused `test` command had compiled the
current sources. Result: PASS, 59 tests, no failures, errors or skips: 2 sender
E2E tests, 1 receiver-side local-error test and all 56 existing type-conversion
E2E tests. Log: server `core/target/schema-aware-tests.lOeKVW/final-regression.log`.

SOL then ran three serial repetitions of the final compiled baseline through
`surefire:test`, using explicit `(fuzz.s0, fuzz.s1)` seed pairs `(1, 2)`,
`(42, 43)` and `(1234567, 7654321)`. Each run exited 0 with 1 test and no failures,
errors or skips. The primary architect independently checked the logs, including
the recorded seeds and test summaries. These vary the existing harness's seeded
fragmentation; they are not a claim of exhaustive fuzz coverage.

Repeat command, substituting each seed pair:

```sh
mvn -B -pl core '-Dtest=QwpSenderE2ETest#testTypedUuidPreservesCompletedRowAfterLocalError' -Dfuzz.s0=1 -Dfuzz.s1=2 -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-aware-tests.lOeKVW surefire:test
```

Logs are retained in the server's task-owned
`core/target/schema-aware-tests.lOeKVW/`: `final-focused.log`,
`final-regression.log` and `repeat-1.log` through `repeat-3.log`.

Acceptance: iteration 1 is complete. The final gates are 151 client unit tests,
2 packaged-JAR tests, 59 selected real-server tests and 3 extra seeded baseline
runs, plus independent and adversarial SOL review. Only the single server test
method and the client design/journal changed. No production or build
configuration changes, commits, performance claims or claims of completed schema
behavior. The unrelated Rust directory remains untouched.

## Next iteration candidate

Schema lookup plus matching-type binding on the same real E2E path. Exact scope
and prerequisites will be selected from source validation and adversarial
findings. Do not implement the entire conversion matrix or rewrite replay as
prerequisites for that first schema-aware write.

Source-backed constraints for the next implementation brief:

- Start with one precreated LONG target plus its timestamp handling. Include
  minimal handshake confirmation and a classified lookup failure at a setter;
  successful matching-type SQL alone is insufficient evidence of schema use.
- Reuse the existing cursor I/O loop for a bounded control-request exchange.
  No second query client/socket or shared-socket lock. Controls must not enter
  SF or consume data FSNs; classify schema replies before ordinary ACK parsing.
- Obtain WAL metadata through the sequencer-aware engine metadata API, not a
  reader snapshot that may lag WAL DDL. Authorize ingestion-schema disclosure
  with table-write authorization before emitting it.
- Keep immutable lookup metadata separate from producer-owned target bindings.
  Bind/cache at row/layout boundaries; avoid per-value wrapper allocations or
  a generic converter hierarchy for the first matching-type case.
- Assign and review bounded control-frame encodings before implementation.
  Full table identities, ACK feedback and conversion families remain later
  slices; their absence must be explicit, with no premature release/advertisement
  of the complete extension.

## D005 — Subdivide discovery from high-level Sender activation

Date: 2026-09-09. Status: accepted; supersedes the immediate LONG-binding
candidate above, not the final design contract.

Source review exposed a staging problem with the proposed LONG-only activation:
once the high-level Sender enters schema mode, other existing conversion paths
must either gain converters, fail explicitly or silently pass through. The last
choice violates the design; implementing every converter would make this a large
iteration. Merely turning on negotiation without binding is not schema mode.

Iteration 2.1 therefore implements the negotiated discovery control plane and
production client codecs, exercised over the real low-level WebSocket client
against the real server. The ordinary Sender does not request this capability
yet and retains its existing behavior. There is no user disable switch, fake
server, build-time test switch or test-only production hook. The full extension
is still unreleased and must not be advertised as complete. Activation remains
a subsequent milestone-2 increment with explicit supported-conversion behavior.

This is a smaller increment than the initial commentary proposed. It delivers
real discovery, authorization and wire/lifecycle tests while keeping the existing
public Sender E2E path green; it does not claim setter-time detection yet.

Assignments:

- `schema_server_slice` (SOL): server handshake, discovery, bounded response
  lifecycle and dedicated real-server E2E tests.
- `schema_client_slice` (SOL): low-level handshake opt-in, production request/
  response codec and immutable metadata, with contract tests. No Sender or SF
  changes and no unused producer/I/O mailbox ahead of its first caller.
- `architecture_validation` (SOL): independent adversarial review of protocol,
  authorization, sequence/ACK isolation, bounds and response backpressure.
- Primary architect: wire decisions, scope, integration, source verification,
  journal and final acceptance; second review of all final diffs.

## D006 — Keep discovery separate from data sequencing

Date: 2026-09-09. Status: frozen for iteration 2.1 implementation.

Use the ordinary QWP1 twelve-byte header and version 1, with a reserved
`FLAG_CONTROL = 0x20`, zero tables and exact payload length. No other flag may
be combined with CONTROL. This flag is not the future persisted `FLAG_SCHEMA`.

All multibyte numbers are little-endian. Request payload:

```text
kind:u8 = 1 (DESCRIBE), requestId:i64 > 0, nameByteLen:u16, tableName:utf8
```

Response payload:

```text
kind:u8 = 2 (SCHEMA), requestId:i64 > 0, result:u8
result: 0 KNOWN, 1 MISSING, 2 DENIED, 3 UNAVAILABLE, 4 TOO_LARGE
KNOWN only:
  tableId:i32, metadataVersion:i64, designatedIndex:i16, columnCount:u16
  repeated columnCount times:
    nameByteLen:u16, name:utf8, serverEncodedType:i32,
    paramsLen:u16, params:bytes
```

The server type is the full encoded server ColumnType integer, not the QWP wire
type or a masked base tag. It preserves existing timestamp, decimal, geohash and
array parameters without a second lossy mapping. Freeze this representation as
part of the schema-control contract. The separate length-delimited extension
parameter area is empty in this increment; unknown extension bytes can be
retained/skipped by the client. Future converters interpret known target types;
the transport codec does not reject a schema merely for an unfamiliar type.

Bounds: frames at most 1 MiB, at most 2,048 active columns, at most 1,024 extension
parameter bytes per column, names at most 381 UTF-8 bytes and 127 UTF-16 units.
Validate UTF-8 strictly and follow existing identifier rules. The initial agent
suggestion of a 127-byte limit was rejected: it would exclude valid multibyte
names. Reject truncation, trailing bytes, conflicting flags and overflow before
allocation. Oversized schemas return TOO_LARGE, not MISSING.

Skip dropped columns and remap the designated timestamp index into the compact
active-column list; -1 means no designated timestamp. Return schema identity and
columns from one sequencer-aware metadata snapshot. Authorize insertion before
opening/serializing known-table metadata; write-only users need no SELECT grant.
Only an absent table token yields MISSING; metadata-open/drop races yield
UNAVAILABLE. Denials carry no schema.

Recognize control traffic before allocating a data message sequence. It must
not pass through data processing, WAL commit, data ACK bookkeeping or SF. A
dedicated parked schema-response state owns bounded bytes until fully sent;
completion resumes ordinary pending ACK/durable-ACK processing. Tests must
interleave controls and data/deferred writes and exercise fragmented responses.

Low-level client API uses one-way `requestQwpSchema()` and
`isQwpSchemaEnabled()`; there is no high-level Sender option. Request and confirm
headers follow the design. Missing confirmation is unsupported; malformed
confirmation is an error. Ordinary clients request nothing new in this increment.

Refinements accepted during implementation review:

- The 1 MiB ceiling includes the QWP header. Effective response size is also
  bounded by the existing raw send buffer, including its WebSocket header.
  Precompute using overflow-safe arithmetic; return TOO_LARGE before allocating
  or serializing a schema that cannot fit. Reuse the current send machinery;
  no additional growable reply queue or application-level chunking in 2.1.
- Unknown length-delimited extension bytes are skipped, not allocated/stored
  per column. Full encoded ColumnType already preserves all current parameters.
  Remove the speculative `getColumnParams()` API until a real consumer needs it.
- Validate request table-name syntax, but do not apply setter syntax bans to
  returned column names: authorized existing SQL metadata must round-trip.
- Recognized malformed controls close with WebSocket protocol error 1002, not
  a data NACK or UNAVAILABLE. Detect the control flag as soon as its byte is
  available, including a truncated header. Live read-only metadata lookup yields
  UNAVAILABLE without mutating role-change/data-delivery state.

## Iteration 2.1 evidence and review

The following records implementation and review chronologically. Intermediate
green runs are not acceptance; final acceptance and executed gates appear below.

Pre-build review findings being resolved:

- Main review: the schema-send catch also enclosed subsequent ACK sends. An ACK
  blocked by backpressure could therefore have its saved ACK state overwritten
  with RESUME_SCHEMA. Restrict the catch to the schema send and test interleaving.
- Main and independent review: substring-based HTTP header scanning could treat
  a prefixed header name or another header's value as schema confirmation. Match
  whole header names at line boundaries and reject duplicate confirmations.
- Independent review: malformed-field tests wrote at incorrect byte offsets,
  sometimes rejecting for an unrelated reason. Correct offsets and retain
  targeted tests that fail if the particular bound is removed.
- Main review: a blanket catch(Throwable) hid VM/programming failures as schema
  unavailability. Map expected lookup failures explicitly instead.
- Independent review: include the new schema-send state in close-time draining,
  and apply the configured output-buffer cap before full schema serialization.

These are findings in unfinished drafts, not accepted behavior. They must be
resolved and validated before the increment is marked complete.

Interim evidence (not acceptance): the focused standalone client install passed
189 unit tests and 2 packaged-JAR tests, with no failures, errors or skips. The
primary architect verified the Maven summaries and that workspace/installed JAR
SHA-256 values match:
`546873e4d03b9e98ba24aa3b9d3a25ed793369a8ab30d4aedb9861b1752feb61`.
Log: client `core/target/schema-client-2.1-rerun.RrLlpw/focused-install.log`.
The installed artifact contains the final `RESULT_DENIED` API and skips unknown
extension parameters. Manifest commit remains the working-tree base `981bdb02`;
the hash, not that base commit alone, identifies the changed artifact.

Independent SOL source review accepts both production diffs after the response
state, close-draining, header parsing and exception-mapping corrections. The
architect's second review requested two additional codec tests: every truncated
prefix with a repaired envelope length, and the maximum multibyte response name.
Dropping a byte while retaining the old payload length only tests envelope
rejection; it does not demonstrate safety at each deeper field boundary.

The first real discovery E2E run exposed invalid CREATE TABLE syntax in the
new test, not a production failure. After adding `PARTITION BY DAY`, the live
negotiated request/response passed. An extended rerun also passed immediate WAL
ALTER metadata freshness and full encoded DECIMAL type. Authorization,
malformed/unnegotiated traffic, deferred-commit isolation and forced response
backpressure remain mandatory before acceptance.

Additional bounded-allocation review: do not size the temporary encoded-name
array from historical metadata column slots, which include tombstones. Count
active columns first, reject above 2,048, and allocate only for that bounded
active set. Compute the compact timestamp index during that count. This is a
resource bound, not a measured performance claim.

The extra client codec tests passed: 191 focused unit tests, no failures, errors
or skips. Log verified by the architect:
`core/target/schema-client-review.oAb6mA/focused-test.log`. This was a test-only
rerun (`test`, not `install`); the installed production artifact is unchanged.
An agent initially reported a nonexistent log directory; the architect checked
the filesystem and corrected the evidence before recording it here.

Interim server regression passed 96 tests, no failures, errors or skips, against
the installed standalone artifact. The architect checked the Surefire classpath
and per-class summaries in server
`core/target/schema-aware-discovery.compile/final-regression.log`. This includes
three discovery tests and 93 adjacent tests, but not the still-unwritten
authorization and interleaving cases. A filename containing `final` does not
make this the final acceptance run.

Test-plan reflection: the initial interleaving proposal reused a large network
relay. The production low-level encoder and WebSocket client can express
DATA → DESCRIBE → commit directly, so a relay is unnecessary for that case.
Also, the existing configured send-fragmentation path deterministically parks
the response when it exceeds the chunk size, even with a writable kernel socket.
Use those existing seams for schema-response resume. Testing an ACK blocked
immediately after a fully sent schema response additionally requires a small
schema response and deterministically coalesced input; merely sending two frames
quickly is not evidence that the server processed them in one read.

Authorization E2E passed two tests over the real client/server connection:
write-only discovery succeeds despite SELECT denial, and INSERT denial returns
DENIED with no schema identity/columns. Log checked by the architect: server
`core/target/schema-acl.z7FCbt/acl-e2e.log`. The fixture uses the existing
SecurityContextFactory boundary; no production hook or visibility change.
Review tightened the SELECT-denial precondition to an authorization exception,
and ensured helper-created clients close if their handshake fails.

Review of the first state-test draft found two false-positive risks despite its
green run. A schema-response barrier proves the server handled the preceding
frame, but does not prove a wrongly committed WAL transaction was applied;
drain WAL before asserting zero visible rows. Also, chunk-size 1 proves schema
send resumption but cannot exercise an ACK blocking inside the initial schema
handler: the schema itself always parks first. Keep separate tests for these
distinct response states and require the observed ordering appropriate to each.

The complete new schema matrix subsequently passed 11 tests: six discovery/
protocol/deferred/backpressure cases, two authorization cases and three bounds/
read-only cases. Log verified by the architect: server
`core/target/schema-aware-discovery.compile/full-schema-matrix-final.log`.
Public SQL creation of 2,049 columns succeeds in this checkout, so the active
column-limit test reaches discovery's TOO_LARGE result without internal metadata
fabrication. A 512-byte send buffer rejects a wider response while allowing a
subsequent small lookup on the same connection. A controlled live read-only
engine boundary yields UNAVAILABLE, then KNOWN after re-promotion; this is not
an exhaustive endpoint-failover test.

The initial coalescing socket fixture failed test compilation because its
constructor exposed SLF4J through a module the server tests do not read. The
final response-order fixture uses a small raw loopback WebSocket peer with
production QWP encoding/decoding. No build configuration was changed. An early
two-write fixture also allowed the ACK to arrive before the control; the final
fixture writes the combined pipeline and explicitly asserts SCHEMA → ACK →
next SCHEMA. One TCP write is not a universal one-read transport guarantee;
the observed response order is the test's discriminator.

## Iteration 2.1 acceptance — 2026-09-09

Accepted after SOL implementation, independent/adversarial source review,
cross-review of the final test fixtures, and the architect's source/evidence
checks. No outstanding blocker remains within this increment's scope.

Client commands (standalone client workspace):

```sh
mvn -B -pl core -Dtest=QwpSchemaProtocolTest,WebSocketClientSchemaNegotiationTest,WebSocketClientTest,WebSocketClient421RoleHeaderTest,QwpWebSocketEncoderTest,QwpTableBufferTest,QwpWebSocketSenderTest,QwpWebSocketSenderMultiEndpointTest,QwpQueryClientUpgradeStatusTest -Djava.io.tmpdir=core/target/schema-client-2.1-rerun.RrLlpw -Dmaven.javadoc.skip=true install
mvn -B -pl core -Dtest=QwpSchemaProtocolTest,WebSocketClientSchemaNegotiationTest,WebSocketClientTest,WebSocketClient421RoleHeaderTest,QwpWebSocketEncoderTest,QwpTableBufferTest,QwpWebSocketSenderTest,QwpWebSocketSenderMultiEndpointTest,QwpQueryClientUpgradeStatusTest -Dmaven.javadoc.skip=true -Djava.io.tmpdir=core/target/schema-client-review.oAb6mA test
```

The install passed 189 unit tests plus 2 packaging tests; the final test-only
additions raised the focused unit selection to 191, all passing. Logs and the
unchanged installed artifact hash are recorded above. JDK Unsafe/removal and
native-access warnings were present; no build failure was hidden by them.

Fresh final server regression, after restoring the correct production source:

```sh
mvn -B -pl core '-Dtest=QwpSchemaDiscoveryE2ETest,QwpSchemaDiscoveryAuthorizationE2ETest,QwpSchemaDiscoveryLimitsE2ETest,QwpWebSocketProtocolTest,QwpUpgradeRejectFragmentationTest,QwpServerCloseDrainTest,QwpAckSeqTxnCoverageBlackBoxTest,QwpWebSocketTypeConversionE2ETest,QwpSenderE2ETest#testTypedUuidPreservesCompletedRowAfterLocalError+testUuid,QwpWebSocketSenderReceiverTest#testColumnTypeMismatchThrowsClientSide' -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-aware-discovery.compile test
```

Result: PASS, 104 tests, no failures, errors or skips. This used the full `test`
lifecycle to compile current main/test sources, without the `local-client`
profile. The architect checked the standalone installed-JAR classpath and log
`core/target/schema-aware-discovery.compile/final-regression-104.log`.

Three additional serial repetitions used the freshly compiled sources:

```sh
mvn -B -pl core -Dtest=QwpSchemaDiscoveryE2ETest,QwpSchemaDiscoveryAuthorizationE2ETest,QwpSchemaDiscoveryLimitsE2ETest -Dfuzz.s0=1 -Dfuzz.s1=2 -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-aware-discovery.compile surefire:test
```

Seed pairs `(1,2)`, `(42,43)` and `(1234567,7654321)` each passed all 11 tests,
with no failures, errors or skips. Logs: `schema-repeat-1-2.log`,
`schema-repeat-42-43.log`, `schema-repeat-1234567-7654321.log` in the same server
directory. These are stability checks, not exhaustive fragmentation coverage.

Adversarial mutation check: temporarily move the pending ACK pumps back into
the schema-send catch. The completed-schema/chunk-24 test then fails as intended:
the next DESCRIBE receives a duplicate 11-byte ACK rather than SCHEMA. The
observed exception is `Malformed QWP schema response: invalid frame length: 11`.
Log: `mutation-ack-catch.log` in the same directory. Restore with the exact inverse
patch before the final regression. The restored upgrade processor's SHA-256
matches its pre-mutation value, independently checked by the architect:
`ec0e15bc6c8cc79be96f4a39152ad302596772f6a8f0996daf4ddd9147e5c9c0`.
This confirms the test detects the reviewed state-loss bug, not just happy-path
response delivery. No deliberate mutation remains in the workspace.

Scope at handoff: discovery control frames and low-level client negotiation/
codecs are complete. Normal Sender activation, local target conversion, schema
bindings, ACK/NACK schema feedback, persisted framing and replay changes remain
future work. No send-time transformation, opt-out, subscription/cache framework
or public feature-completion claim was introduced. No performance improvement
is claimed. No commits or pushes were made; unrelated Rust files and the accepted
iteration-1 baseline were preserved. Both repositories pass `git diff --check`.

## Next increment

Integrate lookup with producer-owned immutable schema bindings and a first real
setter-time failure E2E. Select the activation boundary and supported conversion
slice from source before changing the high-level Sender. Do not silently pass
through conversions merely to enable negotiation, and do not expand this into a
send-time transformation or replay rewrite.

## D007 — Test the conversion component before Sender activation

Date: 2026-09-09. Status: accepted and verified in iteration 2.2.

Do not activate normal Sender merely to exercise a converter. Its flush path
encodes and durably appends bytes before I/O sends them. Activating schema mode
therefore also needs coherent first-use negotiation, row boundaries, frame
identities, reconnect and persisted-backlog compatibility. Adding FLAG_SCHEMA
but deferring its recovery checks would violate the design's first-framing-change
gate. Implementing all of that alongside the first converter is too large a
single increment.

Instead add the genuine production `QwpSchemaRowBuffer` component, holding one
immutable server snapshot and a private sparse existing `QwpTableBuffer`. Its
first supported pairs are STRING-to-UUID, typed UUID-to-UUID and matching LONG.
Unknown targets, missing columns and unsupported non-null pairs fail explicitly,
not through raw-value fallback. Null string input is an effective target NULL
for the supported targets, matching current QWP setter semantics. `atNow()`
retains server-assigned time; explicit timestamp conversion is not in this slice.

The E2E path is real DESCRIBE → immutable binding → locally converted A / invalid
B / valid C → existing typed UUID/LONG QWP encoding → real server ACK and SQL.
Observe typed UUID bytes, not merely successful server-side parsing. Failures
cancel B and its newly introduced buffer columns, not completed rows. Duplicate
suppression uses the target column before validating/converting a second input,
including cross-setter duplicates. Keep the mutable storage private and expose
only row operations and encoding; no per-value wrapper allocation or new queue.

This is deliberately conversion-component E2E, not full schema mode. It does
not transmit schema identities, activate Sender, add a lookup mailbox/cache or
change SF. The existing type definitions faithfully describe the already
converted bytes; no STRING value is passed through as a substitute for UUID
conversion. Design revision 12 makes this staging distinction explicit.

Reflection: initial agent briefs coupled any schema-bound component to the new
persisted flag. The architect challenged that coupling: wire identities enable
feedback and snapshot accounting, but are not required to test a standalone
local converter honestly. Independent review agreed after separating component
behavior from activation claims. Another brief proposed rejecting a stale
identity; this was rejected outright. The design says identity is descriptive,
not CAS, and a version mismatch alone must not reject otherwise valid data.

Assignments:

- `schema_client_slice` (SOL): row component, classified schema exceptions,
  minimal UUID conversion helper if needed, codec refinement and component tests.
- `schema_server_slice` (SOL): real-server component/UUID conformance E2E tests;
  no server production or framing changes.
- `architecture_validation` (SOL): independent/adversarial API, ownership,
  rollback, unknown-parameter and test-discrimination review; read-only.
- Primary architect: scope, source verification, design/journal, integration,
  artifact provenance and final acceptance. Build slots remain serialized.

## D008 — Preserve unknown extension presence for the first real consumer

Date: 2026-09-09. Status: accepted for iteration 2.2.

Iteration 2.1 correctly skipped opaque per-column extension bytes without
allocating/storing them. A converter now needs one additional fact: whether
such bytes were present. Otherwise an unfamiliar extension on a familiar base
type could be silently ignored when writing that column.

Retain a boolean per column and expose
`hasColumnExtensionParameters(index)`. Continue skipping the bytes and keep the
snapshot immutable. Reject use of a parameter-bearing column with
UNSUPPORTED_FEATURE; allow unrelated supported columns. Known parameters remain
packed in the full ColumnType integer. The first converter compares exact
supported types, not masked tags that could discard unfamiliar flags. This is
a consumer-driven refinement, not restoration of speculative byte arrays/API.

## D009 — Compare UUID conversion with QWP ingestion, not SQL casts

Date: 2026-09-09. Status: source-verified and runtime parity verified in iteration 2.2.

Server `WalColumnarRowAppender.putStringToUuidColumn()` checks the null bitmap,
then calls strict UUID length/dash/hex parsing. An empty non-null string fails.
`SqlUtil.implicitCastStrAsUuid()` instead treats an empty string as null, so it
is not the correct conformance oracle. Java's UUID parser is not substituted.

Reuse the narrow server parsing rules with the client's existing hexadecimal
parser. Parse both limbs before append. The public input order is `(lo, hi)`;
existing `ColumnBuffer.addUuid(hi, lo)` writes low then high. Preserve the native
sentinel limitation: both UUID limbs equal to Long.MIN_VALUE represent NULL;
a single such limb does not. Matching LONG likewise preserves its null sentinel.

Use one versioned UUID vector corpus, vendored byte-identically into both test
resource trees, so each repository's tests are hermetic. Verify equal hashes.
Compare acceptance/rejection and stored values against actual legacy QWP string
ingestion as well as the new local component. Invalid input must never become
a successful null merely because a different SQL conversion would accept it.

## Iteration 2.2 evidence and review

Development evidence follows; final acceptance is recorded below. Do not infer
high-level Sender activation from component completion.

Read-only baseline, against the previously installed 2.1 client and compiled
server sources:

```sh
mvn -B -pl core '-Dtest=QwpWebSocketTypeConversionE2ETest#testStringToUuidColumn+testUuidToStringColumn+testUuidToVarcharColumn' -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-binding-baseline.bAkuPn surefire:test
```

PASS: three tests, no failures, errors or skips. Log checked by the architect:
server `core/target/schema-binding-baseline.bAkuPn/uuid-baseline.log`.

Draft review caught input-pair validation running before duplicate suppression.
Fix by resolving the actual target wire type, asking the existing sparse buffer
for that target column, and honoring its duplicate result before validating the
input pair. Do not introduce a parallel row-stamp array or another row tracker;
one existing bookkeeping source suffices. Other draft checks cover closed-state
reuse, table/column validation, ambiguous schema names and failure cleanup.

Keep the design's five exception reasons. In this component, confirmed MISSING
means inference is not implemented, and TOO_LARGE means discovery cannot supply
the bounded schema; both are UNSUPPORTED_FEATURE with distinct diagnostics.
DENIED is ACCESS_DENIED and transient UNAVAILABLE is SCHEMA_UNAVAILABLE.
Do not invent staging-only public reason codes or call these INVALID_VALUE.
Reserved SCHEMA_CHANGED is not emitted until actual refresh/rebinding exists.

Independent review also caught a test-client leak if the handshake failed before
the helper returned, and a conformance gap: only invalid strings initially went
through the legacy server converter. The helper now closes on construction
failure. The E2E test compares every accepted value, explicit null and native
sentinel through both local binary UUID encoding and actual server VARCHAR-to-
UUID ingestion; each invalid vector gets a fresh stream and must receive a NACK.

First client build passed 200 unit tests and two packaging tests. The architect
checked the log and matching workspace/installed JAR hash. Before accepting the
slice, test review requested stronger wire assertions: an encoder returning a
positive length alone does not prove row retention, null encoding or rollback.
The server test must assert exactly the A/C rows and their effective columns,
so a failed-row-only column cannot remain unnoticed as an all-null definition.
Client tests must likewise inspect observable bytes after partial-row rejection,
cancel/reset and explicit nulls. Final evidence follows after these checks run.

Server validation caught two test authoring mistakes before acceptance. The
first compile lacked required `WebSocketFrameHandler.onClose()` callbacks. The
first executed run then passed two of four tests; the other two expected `NaN`
for LONG nulls, while this server's SQL renderer returns `null`. The architect
checked both actual/expected outputs: the UUIDs, retained A/C rows and values
matched, and only the null text differed. Fix the expected text, not production
conversion semantics, and rerun the tests and regression selection.

The client build also caught a server/client API difference (`NumericException`
uses `instance()` in this client, not `INSTANCE`) and a malformed test resource
with literal `\\t` text instead of actual tab separators. Both were corrected
before the successful install. Failure logs are retained as
`core/target/schema-row-buffer.2.2/focused-install.log` and
`focused-install-rerun.log`; the architect checked the actual failures.

## Iteration 2.2 acceptance — 2026-09-09

Accepted after author self-review, independent SOL adversarial/source review,
architect source review and direct checks of build logs, artifact provenance
and corpus hashes. No concrete blocker remains within the component scope.

Client install (200 unit tests plus two packaging tests, all passing):

```sh
mvn -B -pl core -Dtest=QwpSchemaRowBufferTest,QwpSchemaProtocolTest,WebSocketClientSchemaNegotiationTest,WebSocketClientTest,WebSocketClient421RoleHeaderTest,QwpWebSocketEncoderTest,QwpTableBufferTest,QwpWebSocketSenderTest,QwpWebSocketSenderMultiEndpointTest,QwpQueryClientUpgradeStatusTest -Djava.io.tmpdir=core/target/schema-row-buffer.2.2 -Dmaven.javadoc.skip=true install
```

Log: `core/target/schema-row-buffer.2.2/focused-install-final.log`.
After test-only strengthening, the same selection passed **203 tests** on final
sources with `test` instead of `install` and temporary directory
`core/target/schema-row-buffer-final.2.2`. Log:
`core/target/schema-row-buffer-final.2.2/final-focused-test.log`.
The intermediate component/codec-only run passed 21 tests; its log is
`core/target/schema-row-buffer-tests.2.2/strengthened-test.log`.
All green runs have zero failures, errors and skips. Production did not change
after installation; the final test-only changes did not require reinstallation.

Workspace and installed `questdb-client-1.3.10-SNAPSHOT.jar` SHA-256:
`eeec24a05e186cb22c9b063008a5febf1bf5b57bd640d8fe9f24d82669d26251`.
The manifest records base commit `981bdb02a471f3b290c89b8e78cbc422610e329e`;
the hash, not that base commit alone, identifies this dirty-worktree artifact.
Server Surefire reports explicitly reference the installed standalone artifact
under `/home/jara/.m2/repository/org/questdb/questdb-client/1.3.10-SNAPSHOT/`.
No embedded-client profile was used.

Fresh server regression, against server base
`12a33d651e51e2682e7a448c8db5168fc72dfad3` plus the task changes:

```sh
mvn -B -pl core '-Dtest=QwpSchemaRowBufferE2ETest,QwpSchemaDiscoveryE2ETest,QwpSchemaDiscoveryAuthorizationE2ETest,QwpSchemaDiscoveryLimitsE2ETest,QwpWebSocketProtocolTest,QwpUpgradeRejectFragmentationTest,QwpServerCloseDrainTest,QwpAckSeqTxnCoverageBlackBoxTest,QwpWebSocketTypeConversionE2ETest,QwpSenderE2ETest#testTypedUuidPreservesCompletedRowAfterLocalError+testUuid,QwpWebSocketSenderReceiverTest#testColumnTypeMismatchThrowsClientSide' -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-row-buffer-server test
```

PASS: **108 tests**, zero failures, errors or skips. Log:
server `core/target/schema-row-buffer-server/final-regression.log`.
The four new E2E tests also passed separately in `new-e2e-final.log`.
Three additional serial runs used `-Dtest=QwpSchemaRowBufferE2ETest`,
`-Dfuzz.s0=<first> -Dfuzz.s1=<second>` and `surefire:test` against those compiled
sources. Seed pairs `(1,2)`, `(42,43)` and `(1234567,7654321)` each passed all
four tests. Logs: `repeat-1-2.log`, `repeat-42-43.log` and
`repeat-1234567-7654321.log` in the same server directory. These are stability
checks, not an exhaustive fuzzing or performance claim.

The shared 20-case corpus is at test resource
`io/questdb/client/cutlass/qwp/uuid-string-conformance.tsv` in each repository.
Both copies have SHA-256
`c0ce3c5836cc58e1960dbcadbcfd9d53df0d842af796c76d25d942259cec90e4`.
Tests inspect public protocol bytes and deterministic SQL, without reflection,
private-state hooks or runtime cross-repository resource dependencies.

Delivered scope: immutable schema-bound row construction, STRING-to-UUID,
typed UUID/LONG writes, explicit target nulls, duplicate-first semantics,
partial-row rollback and stable classified exceptions. Opaque extension presence
is retained without storing its bytes; unknown used types are rejected locally.
The mutable row storage remains private and reuses existing row bookkeeping.

Remaining scope: ordinary Sender activation and lookup coordination, inferred
schemas, further conversion families, schema identities in persisted frames,
ACK/NACK schema feedback, snapshot transitions and one-way-upgrade/recovery
integration. Caller-owned response correlation and snapshot acquisition remain
explicit component preconditions. No send-time transformation, feature opt-out,
subscription machinery or parallel row-state tracker was introduced.
No existing Sender, I/O, encoder, SF or server production behavior changed in
iteration 2.2. Both worktrees pass `git diff --check`; unrelated work remains
preserved. No commits or pushes were made.

Next architectural gate: define the smallest pinned-frame/recovery increment
before activating Sender. Introducing persisted schema framing must include
negotiation and replay/recovery checks in the same increment. Continue to keep
the real component and public-Sender baseline E2E paths green throughout.

## D010 — Persisted schema framing and replay before Sender activation

Date: 2026-09-09. Status: accepted in iteration 2.3; evidence below.

After the user asked what unchanged ordinary Sender behavior meant, the
architect checked whether this increment could safely activate it. Source
review and independent SOL review found that UUID/LONG-only activation would
either reject the other existing conversions globally on supporting servers or
silently pass values through. The latter violates the design; the former is
not an acceptable ordinary-Sender regression. Do not use a per-table, SF or
configuration exception as an implicit feature opt-out.

Complete the next prerequisite instead: extended table framing and safe SF
recovery. This is still not high-level schema activation, and is not described
as the final prerequisite: lookup coordination, conversion coverage, snapshot
transitions and feedback remain. The public Sender baseline stays green.

Wire decision, captured in design revision 13: `FLAG_SCHEMA=0x02`; after each
table name, kind 0 means unknown, or kind 1 precedes a non-negative i32 table
ID and i64 metadata version, little-endian. Reject invalid kinds/fields,
SCHEMA+CONTROL and schema-flagged table-less frames. Reject unnegotiated schema
frames before data processing. Stale identities remain descriptive, not CAS.
ACK metadata feedback is not needed to parse/persist these identities and is
not implemented in this slice.

The encoder gets explicit per-operation schema methods. Do not turn the
existing mutable flags setting into a sticky schema mode that can relabel
later legacy data. A message has one layout; reject mixing legacy/schema table
addition methods. Split messages retain the original flag and table bytes.
The schema row component passes its immutable snapshot identity to the encoder;
there is no new mutable identity field on QwpTableBuffer merely for serialization.

Recovery derives its requirement in the existing ordered frame scan, before
any reconnect can send dictionary catch-up or a legacy prefix. Any unacknowledged
retained extended frame requires support, conservatively including a deferred
tail; acknowledged residue does not. Foreground and background/drainer paths
must enforce the requirement for their own slot on every connection. A mismatch
retains bytes and watermarks and must not become quarantine, stripping or
downgrade. Fresh ordinary Sender still does not request the extension in this
staging increment; schema-required replay does request it. No second SF scan or
new persistent side file is planned.

Assignments: SOL client author owns explicit encoding and SF/reconnect safety;
SOL server author owns bounded parsing/ingress and real-server tests; independent
SOL reviewer challenges lifecycle, compatibility and test discrimination.
The architect owns the contract, journal, source checks and final acceptance.
Tests must cover actual disk recovery and byte-identical replay, not just flag
getters, and preserve the existing component and public-Sender E2E coverage.

## Iteration 2.3 development evidence

Pre-edit compiled baseline, run without recompiling while authors prepared the
new source changes:

```sh
mvn -B -pl core '-Dtest=RecoveredFrameAnalysisTest,CursorWebSocketSendLoopOrphanTailTest,BackgroundDrainerOrphanTailTest,CursorWebSocketSendLoopForegroundReconnectPolicyTest,CursorWebSocketSendLoopReconnectLeakTest' -Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-framing-baseline.LGBRLN surefire:test
```

PASS: 27 tests, zero failures/errors/skips. Log:
`core/target/schema-framing-baseline.LGBRLN/baseline.log`. This establishes the
unchanged recovery baseline; it is not validation of the new source edits.

Source review found local deferred-tail retirement in both BackgroundDrainer
and the cursor loop before any connection. The conservative schema-required
tail rule must gate that self-acknowledgment too, otherwise an incompatible
endpoint can coincide with watermark advancement and deletion without sending
any bytes. Keep legacy-tail retirement unchanged. A supporting connection may
still abort the orphaned transaction normally; tests must not expect deferred
rows without a commit to be ingested.

Draft encoder review found transient mutation of configured flags around header
writing. A header allocation failure could leave FLAG_SCHEMA set for a later
legacy encode. Pass computed header flags explicitly instead, including the
existing delta-dictionary path. Reject schema/table-add layout mixing and
schema-flagged table-less construction; do not add another buffering framework.

Independent and architect review caught schema-capability errors being routed
through the existing durable-ACK mismatch budget. That budget can quarantine an
orphan and report data loss, which is wrong for schema-required bytes. Schema
mismatch must retain/retry until stop or a compatible endpoint, and reset—not
consume—the unrelated durable-capability episode. Preserve typed errors through
endpoint exhaustion so the correct policy sees them.

One-way support is owner-wide, not just a flag on an individual reconnect
factory. A new legacy-only orphan connection cannot go to an old server after
the owning sender required/confirmed schema support; the reverse ordering also
matters. Keep the recovered per-slot requirement and one shared monotonic sender
requirement. Existing legacy connections need not be forcibly closed solely for
another connection's upgrade, but every subsequent connection must respect the
shared requirement. Tests need both foreground/background orderings.

The architect and independent reviewer also found a publication race: checking
the requirement only at the start of trySendOne allows the producer to publish
the first extended frame after that check. Recheck after observing the complete
published frame, before delta processing or send. The producer's requirement
write precedes ring publication, so this ordering makes the guard effective.
Keep the earlier guard for retirement/catch-up paths. Do not add private test
barriers to force this exact instruction interleaving; combine public live-
append coverage with the explicit memory-ordering argument.

Structural cost budget: a known identity adds 13 bytes per table block, unknown
adds one byte, never per row. Recovery folds one boolean into its existing
ordered scan; it does not reread the SF log. Capability checks are per frame or
connection, not per value. These are source/protocol observations, not measured
throughput, latency or allocation-improvement claims.

A custom reconnect factory may ignore the new default requireSchema callback.
The background drainer must check the returned connection inside its typed
retry loop, using the actual recovered engine requirement. Checking only at
send-loop startup would let a capability mismatch escape through generic setup
failure handling and mark the slot failed. Independent review accepted the
real-network regression: a factory returns old-server connections twice, stop
terminates retries, no frames or data-loss errors occur, and reopening the disk
slot proves both watermarks and schema-required backlog were retained.

First integrated client install passed 294 unit tests and two packaging checks,
with zero failures, errors or skips. Log:
`core/target/schema-framing-final.2.3/focused-install.log`. The architect checked
the aggregate results and matching workspace/installed main-JAR SHA-256:
`6e1d9067594db8afd70c96d0cbd7b999eed56a18c1a16663c686d6bce1832340`.
This artifact unblocked fresh server compilation; it is not final acceptance.
Explicit owner-wide negotiation and split/deferred identity-preservation tests
are still required. The real-network tests already cover incompatible peers
that also lack durable ACK, exact mixed legacy/schema replay after recovery,
and deferred-tail retention followed by normal local abort on a supporting
connection. No new white-box production hooks were introduced.

Fresh server validation against that standalone artifact passed 265 tests:

```sh
mvn -B -pl core '-Dtest=QwpSchemaIdentityParserTest,QwpSchemaIdentityE2ETest,QwpSchemaRowBufferE2ETest,QwpSchemaDiscoveryE2ETest,QwpSchemaDiscoveryAuthorizationE2ETest,QwpSchemaDiscoveryLimitsE2ETest,QwpWebSocketProtocolTest,QwpUpgradeRejectFragmentationTest,QwpServerCloseDrainTest,QwpAckSeqTxnCoverageBlackBoxTest,QwpWebSocketTypeConversionE2ETest,QwpSenderE2ETest#testTypedUuidPreservesCompletedRowAfterLocalError+testUuid,QwpWebSocketSenderReceiverTest#testColumnTypeMismatchThrowsClientSide,QwpUdpMalformedTest,QwpUdpInsertTest' -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-identity-server test
```

Log: server `core/target/schema-identity-server/final-regression.log`.
The architect verified zero failures, errors and skips, and the standalone
`/home/jara/.m2/repository/org/questdb/questdb-client/1.3.10-SNAPSHOT/questdb-client-1.3.10-SNAPSHOT.jar`
in the identity E2E Surefire runtime classpath. The earlier fresh focused run
passed 61 tests; log `core/target/schema-identity-server/focused.log`.
Three further serial runs used `-Dtest=QwpSchemaIdentityE2ETest,QwpSchemaRowBufferE2ETest`,
`-Dfuzz.s0=<first> -Dfuzz.s1=<second>` and `surefire:test` against those compiled
sources. Seeds `(1,2)`, `(42,43)` and `(1234567,7654321)` each passed nine tests.
Logs: `core/target/schema-identity-server/repeats/repeat-<first>-<second>.log`.
These are stability checks, not exhaustive fuzzing. UDP regression is included
because the shared parser now understands schema framing; both ordinary and
Linux batched UDP receivers must reject it without WebSocket negotiation.

Test review rejected version-zero/table-less pseudo-frames as positive schema
recovery fixtures and a fixed 100 ms checkpoint sleep. Use real encoder output
for both schema frames and the retained legacy suffix, with lengths matching
the allocations. Observe the public ACK watermark with a bounded wait before
reopening. Deliberately malformed delta-parser fixtures remain synthetic.
These corrections strengthen the evidence; a green run of the earlier fixtures
alone does not establish valid-frame recovery correctness.

## Iteration 2.3 acceptance

Date: 2026-09-09. Accepted by the architect after SOL author self-review,
independent source/adversarial review, and final-source runtime verification.
No remaining finding blocks the frozen scope. Both worktrees remain uncommitted;
unrelated server `core/rust/qdbr/parquet2/` was not touched.

Final client command:

```sh
mvn -B -pl core '-Dtest=QwpSchemaProtocolTest,WebSocketClientSchemaNegotiationTest,WebSocketClientTest,WebSocketClient421RoleHeaderTest,QwpWebSocketEncoderTest,QwpTableBufferTest,QwpWebSocketSenderTest,QwpWebSocketSenderMultiEndpointTest,QwpQueryClientUpgradeStatusTest,QwpSchemaRowBufferTest,RecoveredFrameAnalysisTest,CursorWebSocketSendLoopOrphanTailTest,BackgroundDrainerOrphanTailTest,CursorWebSocketSendLoopForegroundReconnectPolicyTest,CursorWebSocketSendLoopReconnectLeakTest,QwpSchemaReplayNetworkTest,CursorWebSocketSendLoopDurableAckTest,BackgroundDrainerDurableAckRetryTest' -Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-framing-acceptance.2.3 -Dmaven.javadoc.skip=true test
```

PASS: **298 tests**, zero failures, errors or skips. Log:
`core/target/schema-framing-acceptance.2.3/focused-test.log`.
The preceding final-source run passed 297 before the live-append test was added;
log `core/target/schema-framing-final-source.2.3/focused-test.log`.
The two packaging checks passed during the earlier install recorded above.
Production did not change after that install; final edits were test-only.
The architect rechecked the matching workspace and installed main-JAR SHA-256:
`6e1d9067594db8afd70c96d0cbd7b999eed56a18c1a16663c686d6bce1832340`.
The manifest still records base commit
`981bdb02a471f3b290c89b8e78cbc422610e329e`; the hash identifies the dirty-worktree
artifact. Server validation remains the fresh **265-test** gate recorded above,
with three additional nine-test seeded E2E passes against this same artifact.

The final five-test client network suite passed three more serial runs using
`-Dtest=QwpSchemaReplayNetworkTest`, `surefire:test` and
`-Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-framing-repeats.2.3/run-<n>`.
Logs: `core/target/schema-framing-repeats.2.3/run-1.log`, `run-2.log`, `run-3.log`.
No seed or forced-interleaving claim applies to these repetitions.

The last live-append test starts the actual cursor loop on an already-connected
legacy peer with an empty disk engine, then publishes the first valid extended
frame through appendBlocking. It observes repeated connection attempts, no
binary frames at the peer, no terminal/quarantine outcome, unchanged ACK and
publication watermarks, and retained schema-required backlog after reopening.
Together with the post-publication guard's memory-ordering argument, this covers
the important transition without a private barrier or new production test hook.
The acknowledged-residue fixture also explicitly proves FSN 0 remains represented
in the recovered segment before checking that it does not require schema mode.

Delivered: immutable encoding identity survives table framing, splitting and
disk replay; negotiated server parsing accepts descriptive stale identities;
unnegotiated WS and all UDP ingress reject extended framing; foreground and
background reconnects share a one-way requirement; capability mismatch retains
the backlog rather than transforming, quarantining or acknowledging it away.
Existing legacy wire bytes and durable-ACK failure policy retain regression
coverage. No protocol-version bump, send-time transformation, feature switch,
second recovery scan or persistent capability side file was introduced.

Reflection: persisted-format changes reach beyond parsing. Startup, orphan-tail
self-acknowledgment, mixed backlog prefixes, shared connection ownership and
producer/I/O publication all need the same safety contract. The small explicit
encoder API and existing recovery scan were sufficient; a parallel buffering or
recovery framework was unnecessary. Source review found the lifecycle mistakes
before acceptance; valid-frame and real-network tests now guard their outcomes.
No throughput or latency improvement was measured or claimed.

Remaining product work is explicit: ordinary-setter activation, producer-owned
lookup coordination, inferred schemas, the remaining conversion families,
snapshot transitions, and ACK/NACK schema feedback. Next, inventory conversion
coverage against server source and choose a bounded family with real-server
conformance tests. Do not activate the UUID/LONG-only component globally or
introduce an implicit opt-out/raw fallback to make incomplete activation pass.

## D011 — LONG-input numeric conversion family

Date: 2026-09-09. Status: accepted in iteration 2.4 on 2026-09-10, with the
sentinel parity assumption corrected by D012 and the approved rule in D013.

Three SOL discovery briefs proposed different slices: STRING/LONG to INT/LONG,
STRING/LONG to all integer widths, and LONG to all six scalar numeric targets.
The architect selected the last: extend the existing schema row component's
longColumn to BYTE, SHORT, INT, LONG, FLOAT and DOUBLE. This follows one coherent
server dispatch without introducing distinct string grammars or a private SHORT
parser in the same increment. The narrower text proposals remain useful future
work, not rejected product functionality.

Source authority: server `cairo/wal/WalColumnarRowAppender.putIntegerToNumericColumn`
and `cutlass/qwp/protocol/QwpFixedWidthColumnCursor.isCurrentValueSentinelNull`.
The architect read both after the independent reviewer identified the shared
dispatch. Source LONG_NULL is classified before range checks or casts. Correct
the initial client brief's suggestion to treat every raw LONG as an ordinary
number: Long.MIN_VALUE must become target missing/null, not overflow or a large
negative FLOAT/DOUBLE. BYTE/SHORT store zero for null; INT/LONG use their null
sentinels and floating targets use NaN. A non-null LONG equal to Integer.MIN_VALUE
is within range and becomes the INT null sentinel after narrowing. Rejecting
that value would diverge from current QWP behavior.

BYTE, SHORT and INT use inclusive Java signed ranges. LONG is identity.
FLOAT/DOUBLE use the same Java casts as ingestion; precision loss is accepted,
including 16,777,217 to float 16,777,216 and 9,007,199,254,740,993 to double
9,007,199,254,740,992. Encode the target wire type directly. No fallback that
leaves the server to perform the new conversion is permitted.

Keep target-to-wire resolution small and shared so an already-written column
ignores duplicate setters before pair/range/value validation, including a
different existing setter on a newly supported target. The existing explicit
null behavior of stringColumn(null) may cover the new supported targets;
non-null numeric string parsing remains UNSUPPORTED_FEATURE unless the write
is an ignored duplicate. Used unknown/parameterized targets remain explicit
limitations, not INVALID_VALUE. Range failures cancel the entire partial row
and its new-only bindings, preserving completed rows through the existing
rollback machinery. No extra row-state tracker or converter framework.

Assignments: SOL client author owns QwpSchemaRowBuffer, unit tests and canonical
shared vectors; SOL server author owns real-server tests and an identical
vendored resource, with no server production changes; independent SOL reviewer
challenges parity, sentinels, duplicate ordering, rollback and evidence quality.
Client and server Maven runs remain serialized within their repositories. Server
tests use the freshly installed standalone client, never the embedded-client
profile. The architect owns this journal, scope and final acceptance.

Acceptance requires explicit expected wire values/types/null bits and SQL
results, not only equality between two paths. Exercise accepted and rejected
vectors through both local target-typed conversion and legacy raw LONG server
conversion; rejected server batches use an isolated connection. Include integer
boundaries and one-outside values, floating precision boundaries, source nulls,
target sentinel collisions, omissions, duplicate-before-overflow and A/invalid-B/C
rollback with B-only columns absent. Keep the accepted framing/recovery and
ordinary Sender E2E regressions green. Tests use public component/wire/SQL
contracts, no reflection, production test hooks or arbitrary sleeps.

This is new functional coverage, not a performance optimization or bottleneck
claim. Check successful conversion paths for per-value object allocation and
unnecessary scans; do not claim measured throughput/latency improvements.
Deferred: non-null text parsing, floating-source conversions, timestamps,
decimals, arrays, all other families, lookup coordination and ordinary Sender
activation. Design revision 14 only clarifies the historical staging paragraph:
component E2E need not initially transmit identities, but accepted iteration 2.3
now does. The component's stale framing Javadoc will be corrected with its edit.

## Iteration 2.4 development evidence

Pre-edit component baseline:

```sh
mvn -B -pl core -Dtest=QwpSchemaRowBufferTest -Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-numeric-baseline.2.4 -Dmaven.javadoc.skip=true test
```

PASS: 12 tests, zero failures/errors/skips. Log:
`core/target/schema-numeric-baseline.2.4/baseline.log`. Maven ran the normal
lifecycle but found compiled classes up to date; this checks the accepted
pre-edit component, not the new conversion implementation.

The architect found a null-ordering bug in the first draft. Generalized wire-type
resolution also accepts UUID for the existing UUID setter; an unconditional
LONG_NULL shortcut therefore accidentally accepted LONG-to-UUID as UUID null.
Validate that the source/target pair is supported after duplicate suppression
but before interpreting the source null sentinel. Unsupported pairs remain
unsupported even for LONG_NULL. A duplicate LONG_NULL after a valid UUID value
must still be ignored. Dedicated regressions cover both outcomes and rollback.

This differs from the intentional stringColumn(null) behavior frozen in D011:
that existing explicit-null operation is allowed on all supported numeric
targets. Non-null numeric strings are still unimplemented. Independent review
challenged the scope; the architect retained the documented target-owned null
rule and required null/omission coverage rather than introducing a source-type
restriction accidentally.

The initial 29-vector corpus was expanded to 47 before freezing. It includes
ordinary values, signed boundaries, one-outside rejections, sentinel collisions,
positive and negative precision boundaries and LONG_MIN+1. FLOAT/DOUBLE expected
results are literal IEEE bit patterns, not production casts in the oracle.
The architect independently checked all 21 non-null floating expectations using
BigInt significands, discarded-bit comparison and ties-to-even rounding; all
matched, without using Java or JavaScript floating-point casts. The <NULL>
token denotes the target missing representation, explicitly SQL zero for
BYTE/SHORT. Canonical client resource: `io/questdb/client/cutlass/qwp/long-to-numeric.tsv`.
Frozen SHA-256: `0b574bc3b53fb7cc027b0cd2a9fc8d63f75a99d35f6f7f206d7bd21a70334f64`.

Review also requires tests that distinguish completed-row preservation from
new-only-column rollback: the invalid row must introduce a separate column that
does not appear in either completed row, and its wire definition must disappear.
Duplicate coverage includes all six targets and both other existing setters,
not just same-setter repetition. Used extension parameters must be checked on a
newly supported type too. Range diagnostics name the target type as well as its
raw code and retain the stable non-retryable reason without echoing input values.

Early focused runs failed because the test oracle conflated two valid INT null
encodings. Source LONG_NULL and explicit string(null) produce a null bitmap;
a non-null LONG equal to INT_MIN narrows to four sentinel bytes with no bitmap.
Both read as SQL NULL, but the wire assertion must distinguish them. The
architect traced `schema-numeric-focused.2.4d/focused.log` to that fixture error
and required an input-aware assertion while preserving correct production
conversion. Negative DOUBLE IEEE
goldens also require unsigned hexadecimal parsing, not Long.decode. Include
the vector case ID in failures so these distinctions are directly diagnosable.
The server E2E uses its public cursor's effective-null semantics and is not a
substitute for these exact client wire assertions.

Client final-source focused verification passed 20 tests; log:
`core/target/schema-numeric-focused-final.2.4/focused.log`. The earlier
`schema-numeric-focused.2.4` through `2.4e` logs retain the failed fixture
assertions and unsigned-bit parsing error described above. Those fixture
corrections did not change production conversion behavior.

Final standalone client regression and installation:

```sh
mvn -B -pl core '-Dtest=QwpSchemaProtocolTest,WebSocketClientSchemaNegotiationTest,WebSocketClientTest,WebSocketClient421RoleHeaderTest,QwpWebSocketEncoderTest,QwpTableBufferTest,QwpWebSocketSenderTest,QwpWebSocketSenderMultiEndpointTest,QwpQueryClientUpgradeStatusTest,QwpSchemaRowBufferTest,RecoveredFrameAnalysisTest,CursorWebSocketSendLoopOrphanTailTest,BackgroundDrainerOrphanTailTest,CursorWebSocketSendLoopForegroundReconnectPolicyTest,CursorWebSocketSendLoopReconnectLeakTest,QwpSchemaReplayNetworkTest,CursorWebSocketSendLoopDurableAckTest,BackgroundDrainerDurableAckRetryTest' -Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-numeric-acceptance.2.4 -Dmaven.javadoc.skip=true install
```

PASS: **306 unit tests plus two packaging checks**, zero failures/errors/skips.
Log: `core/target/schema-numeric-acceptance.2.4/focused-install.log`.
The architect verified aggregate results and matching workspace/installed
`questdb-client-1.3.10-SNAPSHOT.jar` SHA-256:
`847c13d172067281cae5149604c2585eb3392ae0028be48429cd4a93bace29e4`.
The client author also verified byte identity with cmp. Installed tests-JAR
SHA-256: `955dda25657168fb0375edc6d4a09bcb98a6e45af35a6e0bb639f8c70e7286ce`.
Three additional serial runs used `-Dtest=QwpSchemaRowBufferTest`, `surefire:test`
and `-Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-numeric-repeats.2.4/run-<n>`.
Each passed all 20 tests. Logs:
`core/target/schema-numeric-repeats.2.4/run-1.log`, `run-2.log`, `run-3.log`.
Independent review accepted final client and server source; real-server runtime
validation is still required before accepting iteration 2.4.

## D012 — Hold acceptance: null-sentinel parity depends on legacy block contents

Date: 2026-09-09. Status: source and real-server confirmed compatibility issue;
the hold was resolved by the user decision in D013 on 2026-09-10. The evidence
below records why the original parity assumption could not be accepted.

The first server runtime run also exposed the INT_MIN bitmap/sentinel assertion
mistake. Inspecting why the public cursor reported a present value led the
architect to a deeper issue: QwpFixedWidthColumnCursor.advanceRow only invokes
isCurrentValueSentinelNull when there is no null bitmap. D011's statement that
source LONG_NULL is always classified before conversion was incomplete.

Ordinary QwpWebSocketSender.longColumn creates a nullable LONG column but calls
addLong with the raw value. An omitted row in the same column/block causes a
bitmap to be emitted. A supplied LONG_MIN value with its bitmap bit clear is
then treated as a present number: narrowing to BYTE/SHORT/INT fails its range
check; FLOAT/DOUBLE casts produce a finite large negative value. Without that
bitmap, the same supplied bits are treated as null. The LONG-to-LONG same-width
path still stores the LONG null sentinel. This is block-context-dependent
legacy behavior, not an ordinary value-only converter.

Independent review confirmed the chain through Sender, QwpTableBuffer,
QwpFixedWidthColumnCursor and WalColumnarRowAppender. The 47-vector legacy
corpus contains supplied values but no omitted row in the same column, so it
does not establish parity for this discriminator. Existing LONG setter Javadoc
does not promise sentinel normalization, and the design's intentional omission
change does not by itself authorize changing a supplied LONG_MIN value.

The architect recommends a fixed schema-mode rule: source null sentinels mean
null before buffering, independently of other rows. Preserve legacy client and
server behavior. Do not add block-dependent conversion, send-time transformation
or a server behavior change merely to make this test agree. This is an explicit
compatibility exception to supplied-value parity and needs user agreement before
the design is revised or iteration 2.4 is accepted. Current client code already
implements the proposed rule but remains a dormant component; ordinary Sender
activation is still absent. The server author is adding a real-server diagnostic
with raw LONG_MIN and omission in the same block; no server production edit is
authorized by this finding.

Runtime confirmation: the fresh server gate initially passed 268 tests (the
previous 265 plus three numeric E2E tests); log
`core/target/schema-numeric-server.2.4/full-regression.log`. This was before the
mixed-bitmap discriminator and is not final acceptance evidence. Three added
diagnostic tests in QwpSchemaLongNumericE2ETest now intentionally fail the
previous parity assumption:

- BYTE receives an out-of-range SCHEMA_MISMATCH NACK for the block.
- FLOAT stores `-9.223372E18` for supplied LONG_MIN, followed by NULL for omission.
- DOUBLE stores `-9.223372036854776E18`, followed by NULL for omission.

Log: server `core/target/schema-numeric-server.2.4/legacy-min-bitmap-diagnostic.log`.
The architect verified all three failures and their actual server outcomes.
The diagnostic methods are `testLegacyLongMinWithBitmapNullToByteMatchesSchemaConversion`,
`testLegacyLongMinWithBitmapNullToFloatMatchesSchemaConversion` and
`testLegacyLongMinWithBitmapNullToDoubleMatchesSchemaConversion`. They preserve
the failed assumption visibly; do not silently weaken them while claiming
parity. Source is frozen, server seeded repeats stopped, and no server production
changes were made. Once the user chooses the contract, update these tests to
assert the agreed legacy/schema distinction or the agreed parity rule and rerun
the complete acceptance gate. The client 306+2 gate and artifact remain valid
evidence for the proposed component, not a claim that the product contract is met.

## D013 — Approved: consistent LONG null handling in schema mode

Date: 2026-09-10. Status: user approved; accepted and verified in iteration 2.4.

The user agreed that adding an empty row should not change another row's value
and accepted the recommendation that the new client should not reproduce the
server inconsistency. Adopt the previously proposed rule: for supported numeric
targets, LONG_MIN is source null before buffering, independent of block contents.
Legacy behavior remains unchanged. This approval is not authorization for a
separate legacy server fix or ordinary Sender activation.

Design revision 15 records the rule as an explicit exception to supplied-value
conversion parity. Target missing representation remains zero for BYTE/SHORT
and NULL for INT/LONG/FLOAT/DOUBLE. Duplicate-first behavior and input/target
support validation precede normalization; LONG_NULL must not enable an otherwise
unsupported pair. The already-reviewed client component implements this rule;
no additional production change is needed to resolve the hold.

Replace the three failed parity assumptions with deliberate compatibility tests.
They must separately prove the preserved legacy outcomes and the stable schema
outcomes with and without omission in the same column/block. Include the full
six-target numeric family, exact wire representations and independent SQL
expectations. Preserve the failed diagnostic log as evidence of the decision;
do not describe the resulting green tests as full legacy parity.

SOL server author owns those test changes and the fresh real-server gate;
SOL client author revalidates the unchanged artifact and client gate; independent
SOL reviewer challenges the contract, source and assertions. The architect owns
the design, journal and final acceptance. Keep per-repository Maven runs
serialized, use the installed standalone client, and preserve unrelated work.
No server production changes, send-time transformation, extra mode switch or
per-row allocation mechanism are introduced. A legacy-server bug fix remains
separate work with its own compatibility review.

## Iteration 2.4 acceptance — approved null contract

Date: 2026-09-10. Status: accepted. The D012 hold is closed by D013 and the
fresh checks below, not by claiming full legacy conversion parity.

The final server test replaces the three false-parity diagnostics with
`testLongMinSchemaNullIsStableWhileLegacyDependsOnBlockBitmap`. It covers all
six numeric targets, each with and without an omitted row, separately for
legacy and schema encoding. Wire assertions retain legacy TYPE_LONG and raw
LONG_MIN bits, prove when a bitmap exists and whether the value is present,
and contrast schema encoding's target type and explicit missing-value bit.
SQL assertions independently verify stable schema missing values and the
preserved legacy range errors or finite FLOAT/DOUBLE values. The existing
47-vector corpus, partial-row rollback, duplicate-first behavior and explicit
null/omission checks remain in the gate. The failed D012 log is retained.

Fresh client verification, from the standalone client repository:

```sh
mvn -B -pl core '-Dtest=QwpSchemaProtocolTest,WebSocketClientSchemaNegotiationTest,WebSocketClientTest,WebSocketClient421RoleHeaderTest,QwpWebSocketEncoderTest,QwpTableBufferTest,QwpWebSocketSenderTest,QwpWebSocketSenderMultiEndpointTest,QwpQueryClientUpgradeStatusTest,QwpSchemaRowBufferTest,RecoveredFrameAnalysisTest,CursorWebSocketSendLoopOrphanTailTest,BackgroundDrainerOrphanTailTest,CursorWebSocketSendLoopForegroundReconnectPolicyTest,CursorWebSocketSendLoopReconnectLeakTest,QwpSchemaReplayNetworkTest,CursorWebSocketSendLoopDurableAckTest,BackgroundDrainerDurableAckRetryTest' -Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-numeric-final-verify.2.4 -Dmaven.javadoc.skip=true verify
```

PASS: **306 selected unit tests plus two packaging checks**, zero
failures/errors/skips. Log:
`core/target/schema-numeric-final-verify.2.4/focused-verify.log`.
No client source edits or reinstall were needed for this decision.

Fresh real-server gate, from the server repository, without `local-client`:

```sh
mvn -B -pl core '-Dtest=QwpSchemaIdentityParserTest,QwpSchemaIdentityE2ETest,QwpSchemaRowBufferE2ETest,QwpSchemaLongNumericE2ETest,QwpSchemaDiscoveryE2ETest,QwpSchemaDiscoveryAuthorizationE2ETest,QwpSchemaDiscoveryLimitsE2ETest,QwpWebSocketProtocolTest,QwpUpgradeRejectFragmentationTest,QwpServerCloseDrainTest,QwpAckSeqTxnCoverageBlackBoxTest,QwpWebSocketTypeConversionE2ETest,QwpSenderE2ETest#testTypedUuidPreservesCompletedRowAfterLocalError+testUuid,QwpWebSocketSenderReceiverTest#testColumnTypeMismatchThrowsClientSide,QwpUdpMalformedTest,QwpUdpInsertTest' -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-numeric-server.2.4 test
```

PASS: **269 selected tests**, zero failures/errors/skips. Log:
`core/target/schema-numeric-server.2.4/final-regression.log`.
The same command with only `-Dtest=QwpSchemaLongNumericE2ETest` first passed
all four numeric tests; log:
`core/target/schema-numeric-server.2.4/contract-focused-wire.log`.

Three additional serial server runs used
`-Dtest=QwpSchemaLongNumericE2ETest,QwpSchemaRowBufferE2ETest`, `surefire:test`,
the same absolute temporary directory, and seed pairs supplied through
`-Dfuzz.s0`/`-Dfuzz.s1`: `(1,2)`, `(42,43)`, `(1234567,7654321)`.
Each passed **eight tests**, zero failures/errors/skips. Logs under
`core/target/schema-numeric-server.2.4/repeats/`:
`repeat-1-2.log`, `repeat-42-43.log`, `repeat-1234567-7654321.log`.

The architect rechecked aggregate logs, final wire assertions and the Surefire
XML classpath, which uses the installed standalone client from
`/home/jara/.m2/repository/org/questdb/questdb-client/1.3.10-SNAPSHOT/`.
Workspace and installed main JARs retain matching SHA-256
`847c13d172067281cae5149604c2585eb3392ae0028be48429cd4a93bace29e4`.
Both repositories' shared corpus retains SHA-256
`0b574bc3b53fb7cc027b0cd2a9fc8d63f75a99d35f6f7f206d7bd21a70334f64`.
Independent SOL review accepted revision 15, final source and runtime evidence
with no remaining blocker. Both repositories pass `git diff --check`.

Reflection: a corpus of individual supplied values was insufficient to prove
legacy parity because missing values elsewhere in a block change decoding.
Future conversion slices must vary omission/explicit-null context as well as
individual boundary values. Keep exceptions explicit in the contract instead
of letting batching determine conversion results.

This completes the bounded LONG-input numeric component iteration. Ordinary
Sender setters remain unactivated; no legacy server production behavior,
send-time transformation or mode switch changed. No commits were made.
The next conversion family remains separate work requiring its own bounded
brief, source validation and real-server conformance tests.

## D014 — Record integration simplification opportunities

Date: 2026-09-10. Status: recorded at the user's request; implementation unchanged.

Design revision 16 adds a separate "Simplification opportunities" section after
the progress reflection: reuse Sender-owned row storage and lifecycle, keep
lookups with the existing I/O owner, share schema metadata/payload machinery,
and make current implementation status easy to find. These are integration
guidelines, not completed refactors or authorization for partial activation.
Pinned snapshots, compatibility, bounded requests and delivery rules remain
unchanged. The implementation-status checklist is recorded as an opportunity;
this documentation change does not add it or select the next implementation
slice.

## D015 — Schema feedback before further conversion families

Date: 2026-09-10. Status: accepted. The fresh SOL review reopened the initial
acceptance; D016 records the correction, independent review and final regression
evidence that restored it. Wire contract and scope remain unchanged.

Following the progress reflection, the user asked to continue. Prioritize the
schema-update path over another isolated converter family. This bounded slice
adds real server ACK/NACK feedback and a production client decoder, exercised
with the existing conversion component. Ordinary Sender activation, latest-
schema cache ownership/adoption, lookup coordination and automatic row-boundary
transitions remain future work. Do not add an unused callback/cache framework
or activate only selected setters to disguise those gaps.

Root and three SOL agents inspected response parsing, metadata discovery,
cumulative ACKs, deferred commit groups, partial sends and schema-required SF
replay before freezing the contract. Current table identities are parsed and
persisted but are not consumed by ingestion; this slice gives them a feedback
purpose, never a version precondition. Negotiated legacy blocks also have an
unknown identity and can receive feedback.

Revision 17 defines status mode bits: `0x00` unchanged response, `0x80` complete
update list, `0xC0` invalidate all future lookup-cache entries, `0x40` reserved.
Low six bits retain the ordinary status. Feedback requires negotiation;
durable ACKs never carry it. Named, length-delimited entries reuse the SCHEMA
payload with request ID zero reserved for write feedback. DESCRIBE remains
positive-ID. Full validation precedes ACK/NACK handling and metadata exposure.

Simplification chosen after adversarial review: if the complete metadata set
cannot fit the 1 MiB UPDATES-payload and raw-response-buffer bounds, send
invalidation instead of paginating or repeating cumulative ACKs. Encoding the
mode in existing status bits makes invalidation exactly the base response's
length. This avoids even a one-byte capacity regression and requires no new
send continuation. It affects future lookup cache entries only, never pinned
rows, persisted bytes, delivery watermarks or terminal-error policy.

Collect bounded names/identities; obtain coherent, freshly authorized snapshots
when serializing responses. Keep successful ACK-prefix feedback separate from
rejected-frame/deferred-group feedback. Preserve both across backpressure under
the existing response ownership; clear only the fully sent set. NACK metadata
may cover authorized tables encountered in rejected work, not unrelated tables
or metadata owned by a pending earlier ACK. Unavailable/unauthorized complete
feedback may use nameless invalidation without changing the data outcome.

Assignments:

- SOL server author: scoped server metadata/feedback state and wire writes,
  real-server feedback/evolution, authorization, bounds, ACK ordering and
  partial-send tests. No WAL conversion or UDP behavior change.
- SOL client author: shared schema payload codec, negotiation-aware response
  decoding, existing foreground/background replay consumers, malformed-response
  and delivery-regression tests. No alternate Sender API or unused observer.
- Independent SOL reviewer: adversarial wire, ownership, bounds, compatibility
  and final source/evidence review, read-only and separate from authors.
- Architect: design, journal, scope, source assumptions and final acceptance.

Per-repository builds remain serialized. Use the installed standalone client,
never the server's `local-client` profile, and verify artifact identity. Preserve
all existing dirty work and the unrelated server Rust checkout. This is
functional protocol work, not a performance optimization: no bottleneck or
speedup claim, speculative tuning or new per-row conversion work is authorized.
Performance remains unmeasured and must not be inferred from test time;
performance optimization would require the hardware-first measurement gate.

Pre-acceptance clarification: root challenged applying the 1 MiB limit to
INVALIDATE_ALL. An existing base ACK can exceed 1 MiB when the configured
response buffer and table set are large. The new limit therefore applies to
the complete UPDATES payload only; NONE and INVALIDATE_ALL keep existing base
bounds. Independent review agreed. Add paired codec checks for a large valid
base ACK and its same-length invalidation form, while rejecting oversized
UPDATES. This preserves the fallback's zero-added-size guarantee without
introducing a global receive cap.

## Iteration 2.5 development evidence

Source baselines remain client `981bdb02a471f3b290c89b8e78cbc422610e329e` and
server `12a33d651e51e2682e7a448c8db5168fc72dfad3`, plus the preserved dirty
changes from accepted iterations. Root verified both HEADs and the installed
artifact used below. All builds use repository-local temporary storage because
the shared `/tmp` has little space; unrelated files are not removed.

Client pre-edit response/replay/durable baseline passed 41 tests:
`core/target/schema-feedback-baseline.2.5/baseline.log`. The first focused run
failed the old unknown-status fixture using `0xFF`, which is now an explicitly
negotiated response mode rather than an unqualified legacy error. The fixture
now uses unknown base status `0x3F`; separate tests reject unnegotiated modes.
This is a protocol-reservation correction, not removal of unknown-base-status
coverage. Failed log: `core/target/schema-feedback-focused.2.5/focused.log`.
The subsequent focused gate passed 58 tests:
`core/target/schema-feedback-focused3.2.5/focused.log`.

The initial standalone install passed 331 selected tests plus two packaging
checks; log `core/target/schema-feedback-install.2.5/install.log`. Workspace
and installed main JARs match SHA-256
`e9e9d93f63a1a9a25b33163a6db4a027152bb8c174030b965450bb64211e6825`.
This supplies the server with the production decoder. Network tests and final
source validation were still being extended afterward; this is not iteration
acceptance evidence by itself.

Construction review found and required corrections before acceptance:

- A reset block had landed in `onAckBlocked`, clearing rejected/deferred
  metadata before copying ACK ownership. Keep connection reset separate from
  the parked ACK snapshot; test a blocked ACK followed by a distinct NACK.
- Table-writer acquisition can fail before append, for example for a non-WAL
  table. Capture encountered-table feedback on that path too, without masking
  the original failure or adding metadata reads to matching successful writes.
- Preflight declared update count against available minimum entry bytes before
  allocation; validate table names with server-compatible case-insensitive
  duplicate checks and existing name rules.
- A size-limit rejection test must contain an otherwise complete valid UPDATES
  response; a missing trailer would reject independently of the limit. Use
  valid, distinct table names in the large base-response compatibility fixture.
- WebSocket short-payload capacity must clamp at 125 bytes for 128/129-byte
  buffers, before the larger header becomes necessary.

These are in-progress review findings, not defects in an accepted iteration.
Final runtime gates and independent stable-source review remain required.

Client canonical final-source verification passed 333 selected tests plus two
packaging checks; log `core/target/schema-feedback-canonical.2.5/verify.log`.
Three serial runs of `QwpSchemaFeedbackResponseTest,QwpSchemaReplayNetworkTest`
each passed 15 tests; logs
`core/target/schema-feedback-repeats.2.5/run-{1,2,3}/test.log`.
The production artifact hash remains `e9e9d93f...`; no reinstall occurred while
the server used it. Independent client review accepted source and evidence.
The malformed-response network test uses an observable second replay attempt
and bounded close configuration, then reopens public SF state to verify
`ackedFsn=-1`, retained published frames and no quarantine. Earlier unbounded
test-harness attempts are not counted as successful validation.

Initial real-server checks passed five identity E2Es and two new feedback E2Es;
logs `core/target/schema-feedback-server.2.5/identity-test.log` and
`feedback-e2e.log`. The exact-capacity test sends a 252-byte base ACK plus its
four-byte WebSocket header through a 256-byte response buffer, preserving both
table transaction entries and SQL rows with same-length invalidation. The
pre-writer non-WAL failure returns authorized current metadata and no rows.
These are partial gates, not final server acceptance.

Independent server review also found that resumed NACK completion did not
release its metadata ownership like immediate completion; this was corrected.
Root challenged a proposed later-unrelated-NACK test: the existing unresolved
sequence guard prevents that ordinary data sequence on the same connection.
The reviewer agreed to test reachable partial-NACK completion and retained
delivery behavior, not invent a private-state test or claim a demonstrated leak.

After the client gate completed, the client SOL author took ownership of the
new server `QwpSchemaFeedbackAuthorizationE2ETest.java` only, in parallel with
the server author's remaining lifecycle tests. Server production and Maven
remain solely server-author-owned; the independent reviewer remains read-only.

The client author subsequently added a separate evolution E2E and handed both
test files to the server author before combined compilation. Initial combined
runs exposed fixture errors: invalid WAL DDL, duplicate frame-handler methods,
using a wire type constant for a metadata type, and an old response helper that
did not pass negotiated capability. These failures are preserved in the
`feedback-matrix-*.log` files, not counted as successful gates. Root also
required ordered DESCRIBE barriers before revocation or ALTER, rather than
assuming that sending DATA proves server processing. Native encoder pointers
must be read after encoding, which can grow the underlying buffer.

Revocation tests corrected a load-bearing assumption: the existing commit path
checks INSERT authorization again. Revoking it after deferred ingestion can
produce a SECURITY_ERROR NACK, not a successful ACK. Test the existing data
outcome and nameless metadata invalidation; do not change authorization to make
the original fixture expectation pass.

Root challenged the claim that the existing live-table limit also bounded
pending feedback. A proposed rowless deferred/drop/eviction sequence appeared
to allow historical names to accumulate, and root temporarily authorized an
explicit bounded accumulator with an invalidation marker. Both author and
reviewer initially accepted the counterexample. Further source tracing then
disproved its premise on the WebSocket path: the independent stale-entry scan
is UDP-only. WebSocket lookup evicts under the same requested name, while a new
distinct name hits the table ceiling; rename and buffered-drop eviction paths
reject the frame. The captured table token also does not silently follow pure
renames. No unbounded WebSocket sequence was established.

Correction and final decision: withdraw that blocker and the proposed new
accumulator. Restore the simpler direct maps, bounded by the configured table
ceiling plus at most the terminal failing name. Document this source dependency
and revisit it if cache eviction changes. Do not add an invalidation latch or
new test obligations merely to defend the earlier hypothesis. The temporary
helper compiled but is not part of the accepted change. This corrects root's
earlier user-facing claim of a reachable gap as well.

One independently verified simplification remains: remove the redundant parked
ACK metadata copy. The existing raw send buffer already owns the exact serialized
bytes; resuming that send needs no retained or resampled schema objects. Keep
pending rejected-work metadata separate until its NACK is serialized. Final
server runtime and review acceptance remain pending.

The corrected server component matrix passed 26 tests in
`core/target/schema-feedback-server.2.5/feedback-matrix-7.log`. In particular,
the mixed authorization test preserves the existing partial-commit outcome:
the earlier authorized table retains its row, the denied table has none, and
the NACK exposes no named schema. This is not an all-or-nothing transaction
guarantee. The evolution E2E verifies INT bytes encoded before ALTER, LONG
metadata in the stale write's ACK, byte-identical re-encoding from the old
component, and a subsequent LONG wire value above INT_MAX with no redundant
feedback. The combined gate still precedes final cleanup and broad regression.

Canonical client verification command (client repository; selected tests, not
the entire project suite):

```sh
mvn -B -pl core '-Dtest=QwpSchemaProtocolTest,QwpSchemaFeedbackResponseTest,WebSocketResponseTest,WebSocketClientSchemaNegotiationTest,WebSocketClientTest,WebSocketClient421RoleHeaderTest,QwpWebSocketEncoderTest,QwpTableBufferTest,QwpWebSocketSenderTest,QwpWebSocketSenderMultiEndpointTest,QwpQueryClientUpgradeStatusTest,QwpSchemaRowBufferTest,RecoveredFrameAnalysisTest,CursorWebSocketSendLoopOrphanTailTest,BackgroundDrainerOrphanTailTest,CursorWebSocketSendLoopForegroundReconnectPolicyTest,CursorWebSocketSendLoopReconnectLeakTest,QwpSchemaReplayNetworkTest,CursorWebSocketSendLoopDurableAckTest,BackgroundDrainerDurableAckRetryTest' -Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-feedback-canonical.2.5 -Dmaven.javadoc.skip=true verify
```

Each client repeat uses the same command shape with
`-Dtest=QwpSchemaFeedbackResponseTest,QwpSchemaReplayNetworkTest`, `test` rather
than `verify`, and its own absolute temporary directory under
`core/target/schema-feedback-repeats.2.5/run-{1,2,3}`.

The first broad server gate ran 280 tests with two failures, both in existing
LONG-numeric E2Es whose response helper negotiated schema but still invoked
the legacy decoder overload. Negotiated legacy table blocks correctly return
feedback for their unknown identity. Correct the test helper to pass connection
capability; do not suppress production feedback or weaken SQL/wire assertions.
Failed gate preserved as `core/target/schema-feedback-server.2.5/final-canonical.log`.
The other 278 tests, including all new feedback cases, passed; the complete
corrected gate and seeded repetitions are still required.

## Iteration 2.5 final acceptance — 2026-09-10

Accepted by the architect after the independent SOL review returned ACCEPT and
both authors froze their source. No remaining source or targeted-runtime blocker.
This accepts the bounded feedback slice, not automatic schema-mode Sender.

- Client canonical gate: **333 selected tests plus two packaging checks**, zero
  failures/errors/skips. The seven network replay tests are included in 333,
  not an additional count. Three serial feedback/replay repeats passed **15
  tests each**. Logs and command are recorded above.
- Server canonical gate: **280 selected tests**, zero failures/errors/skips.
  Log: `core/target/schema-feedback-server.2.5/final-canonical-2.log`.
- Server focused repeats: **19 tests each**, zero failures/errors/skips, with
  seeds `(1,2)`, `(42,43)` and `(1234567,7654321)`. Logs:
  `core/target/schema-feedback-server.2.5/repeats/repeat-1-2.log`,
  `repeat-42-43.log` and `repeat-1234567-7654321.log`.
- Root reverified the matching workspace/installed client JAR SHA-256
  `e9e9d93f63a1a9a25b33163a6db4a027152bb8c174030b965450bb64211e6825`.
  Surefire XML points to the installed standalone `.m2` artifact, not
  `local-client`, and confirms the final seed pair and repository-local temp
  directory. Both repositories pass `git diff --check`.

Canonical server command (server repository):

```sh
mvn -B -pl core -DargLine='-ea -Dfile.encoding=UTF-8 -Djava.io.tmpdir=/home/jara/devel/oss/questdb/core/target/schema-feedback-server.2.5/tmp -XX:+UseParallelGC --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=io.questdb' '-Dtest=QwpSchemaIdentityParserTest,QwpSchemaIdentityE2ETest,QwpSchemaRowBufferE2ETest,QwpSchemaLongNumericE2ETest,QwpSchemaDiscoveryE2ETest,QwpSchemaDiscoveryAuthorizationE2ETest,QwpSchemaDiscoveryLimitsE2ETest,QwpSchemaFeedbackE2ETest,QwpSchemaFeedbackAuthorizationE2ETest,QwpSchemaFeedbackEvolutionE2ETest,QwpWebSocketProtocolTest,QwpUpgradeRejectFragmentationTest,QwpServerCloseDrainTest,QwpAckSeqTxnCoverageBlackBoxTest,QwpWebSocketTypeConversionE2ETest,QwpSenderE2ETest#testTypedUuidPreservesCompletedRowAfterLocalError+testUuid,QwpWebSocketSenderReceiverTest#testColumnTypeMismatchThrowsClientSide,QwpUdpMalformedTest,QwpUdpInsertTest' test
```

Server repetitions reuse the identical `-DargLine`, replace the selection with
`-Dtest=QwpSchemaFeedbackE2ETest,QwpSchemaFeedbackAuthorizationE2ETest,QwpSchemaFeedbackEvolutionE2ETest,QwpSchemaRowBufferE2ETest,QwpSchemaLongNumericE2ETest`,
use `surefire:test`, and add `-Dfuzz.s0=1 -Dfuzz.s1=2`, then `42/43`, then
`1234567/7654321`, serially. Passing `java.io.tmpdir` in the fork's JVM arguments
is important: earlier server runs still used `/tmp` despite a Maven-level
property. Final XML verifies the intended location. No unrelated temporary
files were deleted.

Delivered behavior is demonstrated through real negotiated writes and observable
wire/SQL outcomes: matching identities omit feedback; stale/unknown identities
return current authorized metadata; oversized or denied complete feedback uses
nameless invalidation; deferred groups coalesce names; describes and subsequent
ACKs agree after ALTER; partial sends preserve separate ACK/NACK contents; old
INT bytes remain unchanged while a later component encodes LONG. Malformed
feedback cannot release persisted replay data. Metadata does not change ordinary
or durable ACK coverage, rejection policy, or partial-commit semantics.

Simplicity decisions retained: shared schema payload codec, one complete update
set or same-length invalidation, response-time snapshots, and no duplicate
metadata owner for an already serialized ACK. The speculative accumulator was
removed, and its disproved motivating sequence is recorded above rather than
presented as a fixed defect.

Remaining work: latest-schema cache ownership/adoption, lookup coordination,
automatic row-boundary transitions, the remaining conversion families and
coherent activation in ordinary Sender. The next architecture slice should
resolve producer-owned row/block integration using the existing buffer owner;
do not grow another independent row store or activate a subset of setters.
Tests are a targeted regression matrix, not exhaustive proof of every existing
close/durable permutation. Performance is unmeasured; no throughput or latency
claim follows from these functional checks. No commits or pushes were made;
unrelated work, including the server Rust checkout, was preserved.

## Fresh independent SOL review — 2026-09-10

At the user's request, a new SOL agent reviewed iteration 2.5 independently,
without the implementation conversation. It inspected the design, current
client/server source, tests and recorded execution evidence. Root separately
reverified artifact identity, test counts, seeds and temporary paths. No Maven
rerun, implementation edit or commit was performed during this review.

Verdict: reopen iteration 2.5 acceptance for one **P2** finding. Root agrees.
`QwpSchemaProtocol.decodePayload` checks column-name length and UTF-8 but does
not apply column-name rules or reject case-insensitive duplicates. A schema
containing `x` and `X`, or an illegal name such as `a/b`, can therefore pass the
shared decoder. `WebSocketResponse.readFeedback` trusts that decoded schema;
the successful ACK path can then advance replay progress. Duplicate detection
in `QwpSchemaRowBuffer` occurs later and does not protect the ACK boundary.

The reviewer reproduced duplicate-column acceptance against the verified
installed client JAR: `decoded=2 names=x,X`. Acceptance of the enclosing ACK and
the replay-watermark consequence were traced in source, not reproduced through
a network replay. No data-loss incident or malformed response from the current
server was demonstrated. The issue is the promised rejection of malformed
metadata before delivery progress; existing replay tests cover malformed
framing, not these invalid column definitions.

Recommended correction, not implemented in this review: validate server-compatible
column names and case-insensitive uniqueness in the shared schema decoder. Add
DESCRIBE and ACK-feedback rejection tests plus a public replay-network case
showing the malformed schema leaves `ackedFsn == -1`. Rerun the affected gates
before restoring acceptance.

No other blocking defect was found. A suspected combined-response 1 MiB sizing
mismatch was disproved: the caller subtracts the base response from the capped
complete-payload budget before encoding feedback. Missing-table create-permission
reporting was not promoted to a defect: MISSING reports absence, not permission
to create, and actual creation still enforces authorization. Automatic cache
adoption and ordinary Sender row transitions remain outside the reviewed slice.

## D016 — Validate schema column names at the decoding boundary

Date: 2026-09-10. Status: accepted and verified; iteration 2.5 acceptance restored.
The user asked to fix the P2 without extra complexity.

Put column-name validity and case-insensitive uniqueness checks in the existing
shared decoder loop. Reuse `TableUtils` and an existing bounded hash collection;
do not add a validator framework, configuration, wire field, lookup state or
quadratic scan. Both DESCRIBE and write feedback then share the same rejection
boundary. This is schema-metadata work, not per-row conversion validation.

Root verified that `QwpSchemaResponse` is constructed only by the decoder and
its constructor is package-private. Once decoding guarantees valid unique names,
remove the later duplicate-error branch from `QwpSchemaRowBuffer`; retain the
name-to-column binding map it actually needs. Do not retain another name index
in the immutable schema just to avoid a short-lived decoder collection.

The new SOL author owns the two scoped client production files and existing
codec/feedback/replay tests. The fresh SOL reviewer remains independent and
read-only. Root owns architecture, journal and acceptance. Preserve all earlier
dirty work and failed-run evidence; no commits, ordinary Sender activation,
other converters or server implementation changes are part of this fix.

Require failing tests on the old decoder, then a green focused gate: direct
DESCRIBE and ACK/NACK feedback reject duplicate/illegal column names before
publishing any metadata. Preserve valid Unicode, spaces and boundary-length
names. Reuse the public replay-network harness to show semantically malformed
feedback retains the published backlog with `ackedFsn == -1`, rather than just
asserting a parser exception. No reflection, private-state hooks or fixed sleeps.
Revalidate the client aggregate and real-server matrix with the newly installed
standalone artifact before restoring iteration 2.5 acceptance. Tests remain
functional evidence; no performance claim is made.

D016 construction evidence: the initial focused red run failed the new direct
DESCRIBE and feedback checks, but the new network retention test still passed
with the old decoder. Root traced this to the existing feedback test helper:
its latch counted two replies, while the seeded backlog contains two frames.
Both replies could be sent on the first connection, followed by immediate
sender close before either ACK was processed. The earlier description of this
as an observable second replay attempt was therefore too strong.

Require replies on distinct peer connections before closing the sender. This
observes an actual reconnect without production hooks, and strengthens both
the existing truncated-feedback case and the new invalid-column case. Verify
that the corrected semantic network test fails against the previous installed
artifact, without restoring production source to manufacture the red run.
This is test coordination, not additional production state.

The first post-fix focused run also failed native library extraction because
the configured JVM temporary directory did not exist (`File.createTempFile`
reported ENOENT). Creating the scoped directory corrected the environment;
no native or production-code change was required. Preserve that failed log
alongside the rerun; it is not acceptance evidence.

The first completed aggregate run found one stale row-buffer test expecting the
later duplicate-name exception. Remove that redundant test: the decoder tests
now own this invariant, and invalid schemas cannot reach the row buffer.
This changes the final selected client count from 337 to 336 without dropping
coverage. An earlier incomplete aggregate log is also retained and is not
counted as a passing gate.

### D016 final verification and acceptance

The production change is confined to the shared schema decoder and removal of
the redundant row-buffer check. The decoder reuses the existing
`LowerCaseCharSequenceIntHashMap`, bounded by the existing 2,048-column limit.
Its case comparison and `TableUtils` name rules match the server. There is no
new production type, protocol field, setting or retained state. The row buffer
still has its required binding map. Server source is unchanged by D016.

The final public-Sender network test fails against the previous installed client
JAR, SHA-256
`e9e9d93f63a1a9a25b33163a6db4a027152bb8c174030b965450bb64211e6825`.
The old JAR precedes compiled test classes on the classpath. It accepts the
malformed feedback and stays on one connection; the test fails specifically at
`invalid schema feedback must be rejected and replayed`, not native startup.
With the fix, two distinct peer connections are observed and reopening the
published backlog verifies `ackedFsn == -1`, `publishedFsn == 1` and no quarantine.
The direct codec and whole-ACK/NACK tests also reject illegal names and exact,
ASCII-case and Unicode-case duplicates, while retaining valid name boundaries.

Final evidence (all successful gates have zero failures, errors and skips):

- Client: **336 selected tests and two packaging checks** in
  `core/target/schema-feedback-name-fix/canonical-pass.log`.
- Client feedback/replay repeats: **17 tests each, three runs**, in the same
  directory's `repeat-1.log`, `repeat-2.log` and `repeat-3.log`.
- Old-artifact negative proof: `final-network-old-installed-red.log`; initial
  focused codec/feedback negative proof: `focused-red.log`. Failed environment
  runs and the stale-test aggregate failure remain separate from acceptance.
- Server: **280 selected tests** in
  `core/target/schema-feedback-name-fix-server/canonical.log`.
- Server feedback/conversion repeats: **19 tests each**, seeds `1/2`, `42/43`
  and `1234567/7654321`, in that directory's corresponding `repeat-*.log` files.

The installed standalone client and workspace JAR have identical SHA-256:
`9eb4c6716a3dd65c8c4bf6e2de92427f80a7c7a2dc4840aadfbdb75322064fa0`.
Root verified this before and after the server runs, and verified the installed
JAR on the server test classpath. The server did not use `-Plocal-client`.
Client HEAD remains `981bdb02a471f3b290c89b8e78cbc422610e329e`; server HEAD remains
`12a33d651e51e2682e7a448c8db5168fc72dfad3`, both with the ongoing task's uncommitted
changes. Builds were serialized per repository. Test JVMs used existing,
absolute repository-local temporary directories, including forked JVM options.

Exact commands, including the old-artifact classpath reference and per-run JVM
temporary paths, are recorded in the local
[run manifest](../core/target/schema-feedback-name-fix/run-manifest.md). Client
and server test selections match D015; the client aggregate includes the new
tests and removal of the redundant row-buffer test described above.

The independent SOL reviewer accepted the final source, client and server
evidence. Root independently verified the gates and artifact provenance before
restoring acceptance. Both repositories pass `git diff --check`. This closes
the P2, not the remaining latest-schema adoption or ordinary Sender activation
work. No commits, pushes or performance claims were made.

## D017 — Separate schema conversion from buffer ownership

Date: 2026-09-10. Status: accepted and verified; iteration 2.6.

The user approved the next integration step. Root's preceding recommendation
overstated how soon automatic INT-to-LONG adoption could be exercised through
ordinary Sender. Read-only SOL review and root's source trace found no
production consumer for decoded schema updates, no first-use DESCRIBE
coordinator, and no complete Sender conversion/inference integration. Starting
with a legacy probe and adopting feedback would bypass required first-use
lookup and constitute the prohibited partial activation. Do not manufacture
that E2E by adding an opt-in or test-only Sender mode.

Deliver the ownership prerequisite first: make the conversion component bind
to an existing `QwpTableBuffer`, without allocating, closing, resetting,
completing or encoding a second row store. Keep one owner for buffer lifetime,
row completion, cancellation, byte/row accounting and encoding. Preserve the
existing UUID and LONG numeric conversion contracts and real-server coverage.
No borrowing/owning flag, alternate buffer queue, cache scaffold or ordinary
Sender activation belongs in this increment. Pin schema identity to the bytes
at the owner boundary; a borrowed converter must not provide a way to mix two
snapshots in a non-empty buffer.

Before editing, root read the hardware-first-performance skill and its required
USE/hardware references; SOL performed a read-only capability preflight. There
is no existing benchmark for the schema component's fixed-work write/encode
path. The available Sender latency benchmark exercises a different, non-schema
path, and the host's current system-wide I/O pressure needs workload-correlated
analysis before any resource classification. The skill's optimization study
cannot be applied cleanly to this bounded functional task without creating a
separate measurement project. Fallback: no performance optimization or claimed
bottleneck/speedup, no change to ordinary Sender's row setters, source review of
ownership/allocation changes, and functional regression evidence only. Encoding
entry points gain guards for newly schema-bound buffers; do not describe that
as zero code change on the existing encoding path or as measured zero overhead.

Root owns this brief, documentation and acceptance; SOL implementation and an
independent SOL review remain separate. Preserve all prior dirty work, serialize
builds per repository, use existing absolute repository-local JVM temp paths,
install the standalone client before real-server validation, and never use
the server's `-Plocal-client` profile. No commits or pushes are authorized.

D017 API decision: replace the unreleased owning `QwpSchemaRowBuffer` with
`QwpSchemaBinding(QwpTableBuffer, QwpSchemaResponse)`. The owner creates and
closes the table buffer, completes rows, and cancels/rolls back a partial row
when a setter fails. The binding only resolves, converts and appends values.
Do not retain a compatibility wrapper that owns another buffer lifecycle.

The table buffer holds its current binding reference. Attach only to a wholly
empty, unbound layout, after validating/constructing the binding. `reset()`
preserves the attachment and column definitions; `clear()` detaches before
cleanup. Each binding setter checks that it is still current before touching
the buffer. Root rejected the proposed separate layout-lease object and
generation counter: reference identity already detects stale bindings, and the
binding already holds the immutable schema. Do not duplicate schema identities
or introduce a reattachment API for previously invalidated bindings.

Schema encoding derives identity from the buffer's binding. Existing explicit
identity entry points remain valid for unbound protocol buffers and bound
buffers with the exact pinned identity; reject mismatches before changing the
encoder. Legacy encoding rejects a bound buffer rather than stripping its pin.
For bound buffers, partial rows cannot be encoded; an empty single-table encode
returns zero without modifying the encoder, while appending an empty bound table
to a predeclared multi-table frame is rejected. Existing unbound-buffer encoding
contracts remain unchanged. These are owner-misuse errors, not value-conversion
failures or reasons to cancel another row automatically.

Real-server tests must use this explicit owner lifecycle and retain their SQL,
wire and shared-vector assertions. Evolve a caller-owned buffer from INT to
LONG only after retaining its old encoded bytes, clearing its old layout and
binding the new schema. This proves the ownership prerequisite and immutable
old bytes; it is explicitly not automatic feedback adoption by Sender.

### Iteration 2.6 development and review evidence

The first migrated client gate ran 19 tests and found three callers still
assuming the old facade's automatic rollback. The replacement binding correctly
left cancellation to the owner; the tests needed explicit owner cancellation
and uncommitted-column rollback. Keep their original row/value expectations,
and rename the key regression to describe owner rollback. Preserve the failed
run as `core/target/schema-binding-2.6/logs/focused.log`, not acceptance evidence.
The completed focused suite has 23 tests, including reference-based stale
binding rejection for all three setters while a newer row remains intact,
same-snapshot clear/rebind, reset/layout retention, failed-constructor reuse,
and byte-level encoder checks for bound-buffer framing, identity and partial/
empty-row errors.

The independent SOL reviewer accepted the reference-identity attachment and
verified the final guard ordering. Root required rejection before the
`encodeWithDeltaDict` convenience method starts a new frame, and preserved
matching explicit-identity encoding as byte-identical to derived encoding.
No lease object, counter, additional schema representation or owning adapter
was needed. Root also required explicit owner cleanup in failed-constructor
tests and verified that the same buffer accepts a valid binding afterward.

Three existing real-server E2E classes were migrated without changing server
production code. The first build stopped at test compilation because a renamed
buffer local shadowed a table-cursor local; no tests ran. The corrected focused
gate passed nine tests. Root restored the pre-existing old-frame re-encoding
equality assertion before clear/rebind and strengthened wire assertions to
check exact old/new table identities, not merely non-negative identities.
SQL, UUID/numeric corpus, null semantics and completed-row preservation checks
remain intact. These tests still exercise the production components over a real
connection; they do not pretend to exercise automatic ordinary Sender adoption.

An incremental-build rename can leave obsolete classes in the artifact. Root
required source, compiled-output and JAR checks for the removed owning facade.
No stale compiled files were found, so no cleanup or quarantine was needed.
Earlier logs were preserved, and neither repository was blanket-cleaned.

Design revision 18 adds a short current implementation-status section, as
suggested by D014, so the intended contract cannot be mistaken for a completed
feature. Detailed history and test evidence remain here.

### Iteration 2.6 final acceptance — 2026-09-10

Accepted by root following independent SOL source/adversarial review and
completed client/server gates. This accepts the non-owning binding prerequisite,
not automatic schema adoption or normal Sender activation.

- Client final canonical gate: **340 selected tests plus two packaging checks**,
  zero failures/errors/skips. The 23 binding tests are included in 340. Log:
  `core/target/schema-binding-2.6/logs/canonical-final.log`.
- Server focused migrated E2Es: **9 tests**, then canonical **280 tests**, all
  green. Logs: `core/target/schema-binding.2.6-server/focused3-rerun.log` and
  `canonical280.log` in the server repository.
- Three server feedback/conversion repeats: **19 tests each**, seeds `1/2`,
  `42/43`, `1234567/7654321`, zero failures/errors/skips. Logs in the same server
  directory: `repeat-1-2.log`, `repeat-42-43.log`,
  `repeat-1234567-7654321.log`.

Client canonical command, run from the client repository:

```sh
mvn -B -pl core '-Dtest=QwpSchemaProtocolTest,QwpSchemaFeedbackResponseTest,WebSocketResponseTest,WebSocketClientSchemaNegotiationTest,WebSocketClientTest,WebSocketClient421RoleHeaderTest,QwpWebSocketEncoderTest,QwpTableBufferTest,QwpWebSocketSenderTest,QwpWebSocketSenderMultiEndpointTest,QwpQueryClientUpgradeStatusTest,QwpSchemaBindingTest,RecoveredFrameAnalysisTest,CursorWebSocketSendLoopOrphanTailTest,BackgroundDrainerOrphanTailTest,CursorWebSocketSendLoopForegroundReconnectPolicyTest,CursorWebSocketSendLoopReconnectLeakTest,QwpSchemaReplayNetworkTest,CursorWebSocketSendLoopDurableAckTest,BackgroundDrainerDurableAckRetryTest' -Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-binding-2.6/tmp -Dmaven.javadoc.skip=true verify
```

The focused client command selects `QwpSchemaBindingTest` and uses `test` with
the same temporary path; install used
`mvn -B -pl core -DskipTests -Dmaven.javadoc.skip=true install` after the first
green canonical gate. Final test-only constructor assertions were subsequently
verified in `canonical-final.log`, without changing or reinstalling the main
production artifact. Client JVM reports confirm the repository-local temporary
directory; the command relies on existing POM fork options and does not add a
separate temporary-directory option to `argLine`.

Server selectors, JVM flags, temporary paths and seeded commands are recorded
in the local [server run manifest](/home/jara/devel/oss/questdb/core/target/schema-binding.2.6-server/run-manifest.md).
The installed standalone client was used, never `-Plocal-client`; root verified
the test classpath, final seed/temporary path, and before/after artifact hashes.
The workspace and installed main JAR share SHA-256:
`f14a8793c882bcbf5515c911376c823d8addd17e7013ed9d6f35858173b3b3d0`.
Both contain `QwpSchemaBinding` and no `QwpSchemaRowBuffer` class.

HEADs remain client `981bdb02a471f3b290c89b8e78cbc422610e329e` and server
`12a33d651e51e2682e7a448c8db5168fc72dfad3`, with ongoing uncommitted task changes.
Both repositories pass `git diff --check`. Only the three scoped server test
files changed in this increment; no server production code, commits or pushes.
Functional tests do not establish a performance improvement or zero overhead.

Next dependency: a real first-use DESCRIBE/feedback handoff and latest-schema
cache, with connection invalidation, bounded waits and producer-only adoption.
Do not add a feedback-only legacy probe or enable a subset of ordinary Sender
setters. The remaining conversion/inference contract and owner integration must
be coherent before automatic schema mode is activated.

## D018 — Iteration 2.7: one lookup handoff and one latest-schema cache

Date: 2026-09-10. Status: accepted; final verification below.

The next functional slice connects existing schema codecs and ACK/NACK feedback
to the real cursor I/O loop. Root owns architecture, decisions and acceptance;
SOL owns client implementation, real-server tests and independent/adversarial
review. Ordinary Sender activation and row-boundary adoption remain out of
scope. This must produce an observable real-loop E2E, not another unused codec
or a feedback-only legacy probe.

Decisions and source-checked simplifications:

- One producer needs one outstanding lookup, not a general request queue. A
  coordinator monitor protects the request and a lazy insertion-order cache
  capped at 1,000,000 entries. Do not combine a concurrent map, eviction queue,
  atomic request slot and separate state machine without a demonstrated need.
- Low-level `CursorWebSocketSendLoop.resolveSchema(name, timeoutMillis)` uses
  the cache or issues DESCRIBE; `refreshSchema` forces discovery for subsequent
  conversion revalidation. Return known or confirmed-missing immutable
  responses. Existing typed schema errors distinguish access denial,
  unavailable lookup and unsupported capability/size. Preserve interruption.
  Neither method changes a row, binding, buffer, SF log or ACK watermark.
- The deadline includes queueing, connection wait, control send and response.
  Loss releases the current waiter; a later request can wait for reconnect
  within its own deadline. Installing a replacement connection clears latest
  entries, not pinned snapshots or a newer unsent request. Close and terminal
  exit release waiters before waiting for worker shutdown. IDs are never
  reused; late or unmatched replies are discarded, not opportunistically cached.
- Cache known and confirmed missing results. Do not cache denial/unavailability;
  forced refresh evicts the previous entry on admission instead of duplicating
  cleanup across every failure branch. A failure never restores that entry;
  independently received newer feedback remains usable. Preserve known entries
  during an outage for offline row construction. Use QuestDB-compatible character folding, not
  locale-sensitive whole-string lowercasing.
- A proposed SCHEMA/durable-ACK discriminator collision was disproved before
  implementation: `KIND_SCHEMA=2` is inside the QWP control envelope, whereas
  durable ACK starts with status 2. The full control message starts with QWP
  magic. Strict envelope routing needs no protocol change or decoder fallback.
- Root and both reviewing SOL passes verified server response ordering:
  `QwpIngressUpgradeProcessor.handleSchemaControl` sends its sampled response
  before pending ACKs; blocked responses finish before later response handling;
  `trySendAck` and `sendErrorResponse` sample current metadata when serializing.
  Pending feedback retains names rather than stale snapshots. Therefore use
  receive order; a client wire-sequence barrier would incorrectly suppress
  newer metadata attached to an ACK for older data. No per-table revision
  comparison or new server ordering field is needed.
- The existing WebSocket send path already handles whole-frame partial writes.
  Send one control request at a loop boundary with its remaining deadline;
  recycle after a transport/partial-send failure before sending another frame.
  Do not introduce a second socket owner or send-time data transformation.

Validation brief: public loop/network tests for cold/hit/refresh, case aliases,
missing versus failures, feedback/invalidation ordering, control/durable ACK
interleaving, timeout/cancellation/late replies, connection and close lifecycle,
and unchanged data sequence/watermarks. Real-server E2E must obtain metadata
through this loop, publish bound data, alter INT to LONG, receive updated cache
metadata through ACK, clear/rebind the same buffer, and verify exact wire
identities and SQL values. No reflection, private-field inspection, test-only
production hooks or fixed sleeps. Client/server Maven runs remain serialized
per repository, with repository-local temporary paths and installed standalone
client artifact provenance; never use the server local-client profile.

Performance boundary: the hardware-first skill informs bounded I/O work and
allocation/ownership scrutiny. This is functional integration, not a measured
optimization. No isolated schema-coordination workload has yet established
throughput, latency or allocation cost; do not claim a speedup or zero overhead.
Keep the benchmark study separate from this bounded implementation.

### Iteration 2.7 implementation and review checkpoint — 2026-09-10

The client implementation adds package-private `QwpSchemaCoordinator`, the two
documented low-level loop methods, and a byte-array WebSocket send overload
that reuses the existing whole-frame send path. No ordinary Sender setter,
row/buffer ownership or SF engine change was needed. The coordinator retains
one request object and its sending-client identity; there is no separate epoch,
atomic request state machine, callback adapter, subscription or sequence barrier.

Root and independent SOL review corrected draft lifecycle races before final
validation: closed coordinators must reject cache hits and later feedback;
connection loss must be reported once through the common reconnect boundary;
pre-completing a failed send could otherwise cancel a subsequent request twice.
Close releases waiters before joining the I/O worker. The control-send path
propagates JVM errors to the existing fatal-exit handling. Deadline arithmetic
uses elapsed nanoseconds rather than assuming `nanoTime()` has a positive
origin. Request validation/ID assignment happens before publication, and table
name length is bounded before normalization.

Cache storage is lazy. Invalidation drops the map reference rather than
walking up to a million entries while holding the monitor. Feedback is decoded
completely before publication, with bounded per-name monitor work; a close
guard prevents late publication. Ordinary responses without metadata bypass
the coordinator, and an idle loop checks a volatile pending flag without
taking the coordinator monitor. These are source-level ownership/work bounds,
not measured throughput, latency or allocation results.

Independent SOL source/adversarial review accepted the final production code
and test refinements with no remaining reported blockers. Root additionally
required explicit failed-refresh eviction coverage and a genuinely negotiated
durable-ACK test, not merely injection of a stray durable ACK into ordinary mode.
The real-server test uses existing one-byte send fragmentation, real cursor
engine/loop APIs, exact INT/LONG wire identities, ACK-derived cache lookup,
same-buffer clear/rebind and SQL values `42`, `43`, `2147483648`.

Client gates verified by root:

- Final focused coordinator/lifecycle suite: **12 tests**, zero failures/errors/
  skips. Log: `core/target/schema-coordination-2.7/logs/focused-post-coverage.log`.
- Final canonical gate: **352 selected tests plus two packaging checks**, zero
  failures/errors/skips. Log:
  `core/target/schema-coordination-2.7/logs/canonical-final.log`.
- The earlier 351-test canonical gate and installation succeeded before the
  final test-only refinements. The main artifact was not rewritten while the
  server tests ran. Workspace and installed standalone client JAR SHA-256:
  `20308e3deb22bbf2a66355afb8fcc9529022cb8b4499c4e5730ee8b2c0fe5d8f`.

The initial client compile failed on incorrect existing getter names; the first
combined run also exposed an invalid never-started-loop test setup (null client
without a reconnect factory). A later test compile had incorrect FSN getter
names. These were corrected, their logs retained, and they are not acceptance
evidence. No reflection, private-field inspection, test-only production hook
or fixed sleep was added. Constructor/close failure cleanup in the real-server
fixture was corrected to use try-with-resources.

The client canonical command is the iteration 2.6 selector plus
`CursorWebSocketSchemaCoordinatorTest,CursorWebSocketSchemaLifecycleTest`, with
`-Djava.io.tmpdir=/home/jara/devel/oss/java-questdb-client/core/target/schema-coordination-2.7/tmp`.
It uses the same `mvn -B -pl core ... -Dmaven.javadoc.skip=true verify` lifecycle.
Focused runs select those two new classes with `test`; installation uses
`mvn -B -pl core -DskipTests -Dmaven.javadoc.skip=true install`.
Surefire confirms the absolute project-local temporary directory.

The full one-million-entry eviction/resource stress fixture was **not run**.
The fixed ceiling and eldest-entry removal are source-reviewed; runtime memory
and latency at that scale remain unmeasured. This does not weaken the configured
ceiling or justify adding a test-only production capacity switch.

Server focused/aggregate/repeat acceptance remains pending at this checkpoint.

### Iteration 2.7 final acceptance — 2026-09-10

Accepted by root after independent SOL source/adversarial review, final test
review and direct verification of the client/server logs and artifact hashes.
This accepts the lookup/cache coordination slice, not ordinary Sender activation
or the complete schema-directed encoding feature.

- Client: **352 selected tests plus two packaging checks**, all green; the
  **12** new coordinator/lifecycle tests are included in 352. Final evidence:
  `core/target/schema-coordination-2.7/logs/canonical-final.log`.
- Server fragmented-response focused gate: **8 tests**, all green. Evidence:
  `core/target/schema-coordination.2.7-server/focused-fragmented.log` in the
  server repository. An earlier unfragmented focused run also passed eight
  tests; the fragmented run supersedes it for acceptance.
- Server canonical gate: **281 tests**, all green. Evidence in the same server
  directory: `canonical281.log`.
- Three server feedback/conversion repeats: **20 tests each**, seeds `1/2`,
  `42/43`, `1234567/7654321`, all green. Evidence in the same directory:
  `repeat-1-2.log`, `repeat-42-43.log`, `repeat-1234567-7654321.log`.

All accepted runs have zero failures, errors and skips. Exact server selectors,
JVM arguments and run-specific temporary paths are recorded in the
[server run manifest](/home/jara/devel/oss/questdb/core/target/schema-coordination.2.7-server/run-manifest.md).
Root verified the final Surefire classpath uses the installed standalone
client, the final seed pair and the absolute repository-local temporary path.
The installed and workspace JAR hashes remained
`20308e3deb22bbf2a66355afb8fcc9529022cb8b4499c4e5730ee8b2c0fe5d8f`
before and after server validation. The server local-client profile was not
used. Main production bytes were unchanged by the final test-only refinements.

Both worktrees pass `git diff --check`. HEADs remain client
`981bdb02a471f3b290c89b8e78cbc422610e329e` and server
`12a33d651e51e2682e7a448c8db5168fc72dfad3`. Existing unrelated changes were
preserved. No server production code changed in this iteration; only the
existing evolution E2E was extended there. No commits or pushes were made.

Reflection: the existing server serialization guarantee removed the need for
client ordering barriers, and actual connection identity removed the need for
an epoch counter. The coordinator fits the existing I/O owner; no second
transport, generic queue or owning row facade was added. Subsequent slices
should reuse this path for remaining conversions and producer-owned
row-boundary adoption, not add another lookup/cache layer. Full million-entry
stress and performance characterization remain explicitly unmeasured, as noted
above. Do not activate a subset of ordinary Sender setters or advertise the
full extension before the conversion and inference contract is coherent.

## D019 — Iteration 2.7.1: reject stale replies before schema parsing

Date: 2026-09-10. Status: accepted after client/server validation and independent review.

A fresh SOL review after D018 acceptance identified an uncovered case: the I/O
handler decoded the entire schema body before correlating its request ID. A
malformed but identifiable reply to an expired lookup could therefore recycle
the connection and fail a newer lookup. The existing late-reply test supplied
only valid metadata. The earlier green gates did not establish this case.

The user approved the smallest correction using existing request state. Root
owns the brief, design/journal and acceptance; SOL owns implementation and
regression tests, a separate SOL pass owns independent review, and the server
validation owner reruns existing real E2Es against the newly installed client.

Decisions:

- Factor fixed control-envelope validation into a small protocol helper that
  safely returns the positive request ID without parsing the schema body.
  Reuse validation in the full decoder rather than maintaining divergent
  header parsers. Header, kind and ID occupy 21 bytes; this can identify an
  absent-result reply, but the full schema decoder must still reject it.
  Unsafe/unidentifiable framing retains the existing connection-failure path.
- Use one synchronized, side-effect-free predicate over the existing request,
  sending-client identity, ID and deadline. Check before body decoding and
  again if decoding fails. Keep decoding outside the monitor so timeout and
  close are not delayed by that work. Keep the existing successful-response
  check before metadata publication.
- The final positive predicate check accepts a malformed reply as a live
  protocol failure. Cancellation before that check wins and the stale error
  is ignored; cancellation afterward does not retroactively undo the accepted
  protocol failure. No claimed-request state, epoch or additional counter is
  needed.
- Reject the proposed alternative of pre-completing the request before
  calling `fail()`: it would wake caller A, allow request B to queue, and then
  let the common connection-loss handler cancel B too. The existing timeout
  and connection-loss paths retain sole ownership of their notifications.
- No new classes, queues, callbacks, protocol version or capability changes.
  Ordinary Sender activation, schema conversion, cache policy and SF/ACK
  semantics remain outside this fix.

Validation starts with observable regressions against the unfixed code, with
the failing log retained. Cover malformed identifiable expired replies both
without a pending lookup and while a newer lookup waits; the connection and
new lookup must remain usable. Current malformed bodies and unidentifiable
headers must still fail. Add fixed-header/ID boundary checks while preserving
full-decoder truncation and feedback tests. Use real scripted-peer traffic,
bounded waits and observable results, not reflection, test-only production
hooks or artificial parser delays. The precise timeout-during-decoding race
also requires source review of the post-failure predicate; do not invent a
deterministic runtime claim if that scheduling point is not exercised.

The hardware-first skill was read for this I/O-sensitive change. Its bottleneck
study is not a fit for this narrowly authorized correctness repair; no
production optimization is attempted. The functional fallback is unchanged
ownership, bounded header inspection, parsing outside the monitor and no
performance or allocation-improvement claim. No benchmark study or broader
performance work is added to this fix.

### Iteration 2.7.1 client acceptance — 2026-09-10

Root and independent SOL review accepted the production fix and strengthened
regressions. Server validation remains pending at this checkpoint.

The initial behavioral RED ran after the unused header helper was factored in,
but before changing handler/coordinator behavior. Exactly the two new
stale-malformed-reply tests failed because the old handler decoded their bodies
and triggered connection loss. The matching-active malformed case still passed.
Evidence: `core/target/schema-reply-fix-2.7.1/logs/initial-red.log`.

Review strengthened two tests before final acceptance:

- Sending a stale frame immediately before a new lookup did not prove the I/O
  thread consumed it with no pending request. A following schema-feedback frame
  on the same connection now provides an observable barrier: zero-time cache
  probes cannot send a lookup, and observing the feedback with request count
  still one establishes receive order. Only then does the next lookup begin.
- A schema-unavailable error alone could have come from the lookup deadline.
  The matching-malformed test now uses an effectively unbounded lookup deadline
  and separately waits, within a bounded test deadline, for the public transport
  error. This also respects the ordering between waiter release and terminal
  error publication. New expiry tests allow one second for request delivery
  instead of depending on the I/O thread running within 25 ms.

Final client gates, all with zero failures, errors and skips:

- Focused codec/lifecycle: **20 tests**, `focused-strengthened.log`.
- Coordinator/lifecycle repeats: **15 tests each**, `repeat-1.log`,
  `repeat-2.log`, `repeat-3.log`.
- Canonical: **356 selected tests plus two packaging checks**,
  `canonical-strengthened-final.log`. Earlier `canonical.log` and
  `canonical-final.log` predate the final test strengthening and are superseded.
- Installation: `install.log`.

All logs are under `core/target/schema-reply-fix-2.7.1/logs/`. Runs were serialized
and used the absolute client-repository-local `core/target/schema-reply-fix-2.7.1/tmp`
directory, verified in Surefire XML. The canonical command retains D018's
selector and `verify` lifecycle. No test classes were removed from the gate.

The workspace and installed standalone client JARs both hash to
`b5b2f5300c7d391dae99b7f9904848c13cc77d8c22c4c4d9f11f06d86de06f94`.
The installed artifact is frozen during server validation. Production changes
are limited to the protocol helper, the existing coordinator predicate and the
I/O response handler; only protocol/lifecycle tests changed alongside them.
The precise cancellation-during-decoding interleaving remains source-reviewed,
not deterministically forced by a runtime test. No new state or test hooks were
needed. Both worktrees pass `git diff --check`; no commits or pushes were made.

### Iteration 2.7.1 final acceptance — 2026-09-10

Accepted after root independently verified the final client/server logs,
standalone artifact hashes and Surefire runtime configuration, alongside SOL's
independent source and adversarial test review.

- Server focused discovery, authorization, limits, feedback and evolution:
  **19 tests**, all green, `logs/focused.log`.
- Server canonical: **281 tests**, all green, `logs/canonical281.log`.
- Server feedback/conversion repeats: **20 tests each**, seeds `1/2`, `42/43`,
  `1234567/7654321`, all green, `logs/repeat-1-2.log`,
  `logs/repeat-42-43.log`, `logs/repeat-1234567-7654321.log`.

These server logs live under
`/home/jara/devel/oss/questdb/core/target/schema-reply-fix.2.7.1-server/`.
Exact selectors and JVM arguments are recorded in the
[server run manifest](/home/jara/devel/oss/questdb/core/target/schema-reply-fix.2.7.1-server/run-manifest.md).
All accepted runs have zero failures, errors and skips. The final Surefire
classpath uses the installed standalone client, not the local-client profile;
its final seed pair and absolute repository-local temporary path were checked.
The workspace and installed client JARs retain the `b5b2f530...` hash recorded
above before and after server validation.

No server source changes were needed. Both worktrees pass `git diff --check`.
HEADs remain client `981bdb02a471f3b290c89b8e78cbc422610e329e` and server
`12a33d651e51e2682e7a448c8db5168fc72dfad3`; unrelated changes were preserved.
No commits or pushes were made.

Reflection: the missing step was checking existing request identity before
interpreting a reply, not adding lifecycle state. Keeping that check
side-effect-free also avoids premature waiter notification. Most review-driven
changes were to make test ordering and failure assertions conclusive. This
accepts only the stale-reply repair; ordinary Sender activation and remaining
conversion/row-boundary work remain outside this iteration.

## D020 — Iteration 2.8: typed microsecond/nanosecond conversion

Date: 2026-09-10. Status: accepted after independent review and client/server validation.

The user approved proceeding with timestamp conversion. Root recommended a
bounded conversion slice, with real-server tests from the beginning. Independent
SOL client, server and adversarial reconnaissance narrowed the slice to typed
wire-unit conversion; the broader timestamp family remains unfinished.

Frozen scope:

- Add `QwpSchemaBinding.timestampColumn(name, long, ChronoUnit)` for MICROS and
  NANOS inputs into known, non-designated TIMESTAMP_MICRO/TIMESTAMP_NANO columns.
  Encode the target's timestamp wire type and converted value directly.
- Reuse the existing timestamp drivers after the two-unit support check. No new
  converter framework, binding lifecycle, buffer owner, lookup state or protocol
  change. Caller-owned row cancellation and column rollback remain unchanged.
- Nanos-to-micros uses Java division by 1000, truncating toward zero, not floor.
  Micros-to-nanos rejects values outside the server's exact multiplication
  bounds. Same-unit conversion preserves the supplied bits.
- Timestamp cursor nullness is bitmap-only. Do not copy the separate LONG-input
  source-null rule: supplied `Long.MIN_VALUE` is rejected for micro-to-nano,
  becomes `-9223372036854775` for nano-to-micro, and reads as SQL NULL when
  stored unchanged. Explicit nulls through the existing null-input path and
  omissions use the target null representation. Verify bitmap/no-bitmap cases.
- Preserve first-value-wins before input-unit validation or conversion.
  Unsupported non-null units report `UNSUPPORTED_FEATURE`; a null unit and
  arithmetic overflow report `INVALID_VALUE`. These errors are not flush retries.
- Reject designated timestamp targets at the shared target-support boundary,
  including the null-input path. Their bounds and server-assigned-time semantics
  belong with producer-owned row completion, not this ordinary-column slice.
- Defer other ChronoUnits, `Instant`, DATE, text parsing, LONG-to-timestamp,
  `at`/`atNow`, automatic row-boundary adoption and ordinary Sender activation.
  This is a component extension, not complete timestamp support or a release.

Source authority is QWP ingress, not SQL casts:

- Server `QwpWalAppender` routes timestamp precision mismatches to
  `WalColumnarRowAppender.putTimestampColumnWithConversion`; matching precision
  uses the existing direct-copy path.
- The conversion method checks multiplication bounds and divides toward zero.
  `QwpTimestampColumnCursor.advanceRow` obtains nullness only from the bitmap.
  The initial server reconnaissance mistakenly treated the primitive minimum as
  source-null; root cross-check and independent review corrected that before
  implementation.
- Existing `QwpWebSocketSender.toMicros` multiplies wider units unchecked;
  its `Instant` overload truncates to micros. Reusing the more permissive
  timestamp-driver API without a scope gate would silently tighten overflow
  handling or retain extra precision. Those changes require a separate
  compatibility decision. Timestamp-to-DATE is not a supported server wire pair.

Validation brief: shared exact-value TSV vectors across client and server,
covering all four unit/target pairs, negative fractional cutoffs, multiplication
endpoints and adjacent failures, MIN/MAX, explicit/omitted nulls and unrelated
bitmap rows. Assert target wire types, exact values and SQL stored epoch values;
compare with legacy timestamp-wire ingestion rather than SQL conversion rules.
Add A/invalid-B/C rollback, removal of failed-row-only columns, cross-setter
duplicates, unsupported/name/parameter/designated failures and buffer lifecycle
tests. Exercise plain/compressed timestamps where the existing fixtures permit.
Keep existing conversion, lookup, negotiation and replay regression gates green;
extend persisted target-byte coverage without adding another transport owner.
No reflection, private-state tests, test-only production hooks or fixed sleeps.

Root owns the design, journal, scope and acceptance. SOL owns client code/tests
and serialized client Maven; a second SOL owns server tests and serialized server
Maven; a third independently reviews source, arithmetic and observable coverage.
The client owns the shared corpus. Coordinate installation so server tests use
one frozen standalone Maven JAR, never the server's local-client profile.
Task evidence directories are client `core/target/schema-timestamps-2.8/` and
server `core/target/schema-timestamps.2.8-server/`, with absolute local JVM temps.

Baseline client HEAD is `981bdb02a471f3b290c89b8e78cbc422610e329e`; server HEAD is
`12a33d651e51e2682e7a448c8db5168fc72dfad3`. Workspace/installed standalone client
JARs both hash to
`b5b2f5300c7d391dae99b7f9904848c13cc77d8c22c4c4d9f11f06d86de06f94`.
Both repositories contain prior accepted, uncommitted work. Preserve it and the
unrelated server Rust work; no commits or pushes are authorized by this slice.

The hardware-first skill was read because this extends the write path. Its
bottleneck/optimization gate is not a fit for a functional conversion extension;
use the established functional fallback, with no production optimization or
performance/zero-allocation claim. Reuse existing buffer ownership and conversion
utilities. Hardware cost and scale measurements remain unmeasured, not implied
by passing functional tests.

### Iteration 2.8 client acceptance and early real E2E — 2026-09-10

Root and independent SOL review accepted the bounded client implementation.
`timestampColumn` reuses the two existing drivers, writes the exact target type
and adds no state. Full-type matching preserves nano precision bits; unsupported
targets and duplicate suppression precede arithmetic. The shared designated
guard also rejects the existing null-input path. Caller-owned row lifecycle,
ordinary Sender, protocol, cache and replay implementation are unchanged.

The shared corpus grew to **48 vectors**. Root and the independent reviewer each
checked it using exact BigInt arithmetic rather than production conversion
utilities: zero mismatches. Client/server resource mirrors are byte-identical,
SHA-256 `696d832cd692d5ff8765ac265070a8af6fcfcdc8c4cc87dc5967d72f4d946ab6`.

Client gates have zero failures, errors and skips:

- Focused binding: **26 tests**, `logs/focused-final.log`.
- Final canonical: **359 selected tests plus two packaging checks**,
  `logs/canonical-final.log`, with the final 48-vector resource.
- Installation: `logs/install.log`; workspace and installed standalone JARs
  hash to `909f1e039f6528067545b56477bdb8beab829a7c37f8151e9fb6ddfe6ff960ce`.
  The final client verify ran without reinstalling while server validation used
  the frozen installed artifact; both hashes remained equal afterward.

Client logs are under `core/target/schema-timestamps-2.8/`. The canonical selector
is D019's unchanged selector: the three new tests live in `QwpSchemaBindingTest`.
Surefire confirms the absolute client-repository-local task temporary directory.
The first two focused runs failed because new test readers omitted timestamp
encoding/bitmap bytes and expected a never-bound column in the sparse layout.
These were fixture mistakes, not product failures or a claimed behavioral RED;
their logs remain as development evidence. The existing layout/encoder was not
changed to satisfy them.

The first real-server timestamp E2E passed **3 tests** in
`core/target/schema-timestamps.2.8-server/logs/focused.log`. The expanded source
then passed **6 tests** in `logs/focused-final.log`, adding schema-bound MIN and
bitmap cases, actual Gorilla encoding and recovered timestamp delivery.

Root review requested exact per-row wire and SQL checks for the compressed case,
using nanos input with a nonzero sub-micro remainder. Count/min/max alone could
hide incorrect middle values. The SF smoke proves exact target encoding before
persistence and recovered delivery with the expected value, not byte-identical
replay by itself. Existing generic replay tests separately check byte identity;
the independent review's stronger wording was corrected. Final server gates
will run after these small test refinements. No server production change is
needed, and no performance claim follows from these functional results.

### Iteration 2.8 final acceptance — 2026-09-10

Accepted by root after independent SOL source/adversarial review and direct
verification of final logs, corpus equality, artifact hashes and runtime
configuration. All accepted runs have zero failures, errors and skips:

- Client canonical: **359 selected tests plus two packaging checks**,
  `core/target/schema-timestamps-2.8/logs/canonical-final.log`.
- Server refined timestamp E2E: **6 tests**, `logs/focused-final2.log`.
  This supersedes the earlier three-test and six-test focused runs for final
  test-source acceptance.
- Server canonical: **287 tests**, `logs/canonical287.log`.
- Server conversion/feedback repeats: **26 tests each**, seeds `1/2`, `42/43`,
  `1234567/7654321`, logs `repeat-1-2.log`, `repeat-42-43.log` and
  `repeat-1234567-7654321.log` under `logs/`.

Server evidence lives under
`/home/jara/devel/oss/questdb/core/target/schema-timestamps.2.8-server/`.
Exact selectors, JVM flags and temporary directories are recorded in the
[server run manifest](/home/jara/devel/oss/questdb/core/target/schema-timestamps.2.8-server/run-manifest.md).
The final Surefire report identifies the installed standalone client, final
seed pair and absolute task-local JVM temp path. No local-client profile was
used. The workspace and installed JARs retained the full `909f1e03...` hash
recorded above throughout server validation, and the two 48-vector corpus
files remained byte-identical.

The production change is limited to `QwpSchemaBinding`: one timestamp input
method, existing driver reuse, exact target wire mapping and the shared
designated-target support guard. The client binding tests, shared timestamp
corpus and new server E2E test provide the additional coverage. No server
production change, new converter abstraction, state or buffer owner was needed.
Both worktrees pass `git diff --check`. HEADs remain the baseline revisions
recorded above; prior/unrelated work is preserved. No commits or pushes were made.

Reflection: typed wire-unit conversion is a useful complete subproblem. It
does not justify claiming all timestamp overloads work. Source-backed review
prevented an unchecked assumption about timestamp source nulls and prevented
existing driver reuse from silently redefining larger-unit/Instant behavior.
Wire and SQL checks establish conversion before ingestion; exact per-row
compressed checks and recovered-delivery coverage supplement that contract.
The next timestamp work needs an explicit overflow/precision decision for
broader input normalization. Designated timestamp handling remains tied to
producer-owned row completion, and ordinary Sender activation remains deferred
until the full conversion/inference contract is coherent.

## D021 — Decide broader timestamp input semantics before implementation

Date: 2026-09-10. Status: contract approved by the user; iteration 2.9 accepted.

The user asked to continue after D020. Root inspected the next timestamp slice
and requested independent SOL compatibility review. The design explicitly
requires an overflow/precision decision before implementing broader units and
`Instant`; continuing implementation does not silently resolve that product
choice. No production code or tests changed in this step.

Independent SOL review agrees with the recommendation and the need for explicit
approval. It also confirmed the intermediate-overflow issue in both timestamp
drivers and the need to preserve the existing unit allowlist.

Current source establishes two differences from direct target conversion:

- `QwpWebSocketSender.toMicros` multiplies MILLIS through DAYS unchecked.
  For example, `Long.MAX_VALUE` seconds wraps to `-1000000` microseconds instead
  of reporting overflow. Root verified the arithmetic independently with BigInt.
- Its `Instant` overload first converts to micros, even for a server nano
  column. An instant 999 nanoseconds after the epoch consequently becomes zero
  on that path; direct nano conversion would retain 999.

Root recommends the following contract for schema mode only, subject to user
approval:

- Reject inputs outside the target timestamp's range with `INVALID_VALUE`
  before append, rather than preserving accidental arithmetic wraparound.
- Preserve all `Instant` precision supported by the target: retain nanos for
  TIMESTAMP_NS, and retain existing `Instant`-to-micro rounding for TIMESTAMP.
  Do not change the already-approved primitive NANOS-to-MICROS truncation rule.
- Support the existing QWP unit set: NANOS, MICROS, MILLIS, SECONDS, MINUTES,
  HOURS and DAYS. Do not accidentally admit additional units through the
  timestamp drivers' generic Duration fallback.
- Preserve duplicate suppression before conversion/argument validation. A null
  `Instant` reports `INVALID_VALUE`, not an implicit timestamp null.
- Keep old-server/legacy behavior, designated timestamp handling and ordinary
  Sender activation outside this slice. The changed overflow acceptance and
  added nano precision must be documented as intentional schema-mode behavior.

Implementation warning discovered before delegation: the current timestamp
drivers multiply epoch seconds with `Math.multiplyExact` before adding the
positive fractional part. Near the negative limit that intermediate product
can overflow even when the final timestamp fits. For example, epoch seconds
`-9223372037` and nano fraction `145224193` represent `Long.MIN_VALUE + 1`
nanoseconds. Root's BigInt check confirms the final value is representable;
blindly reusing `NanosTimestampDriver.from(Instant)` would reject it. If this
contract is approved, require range-correct arithmetic and boundary tests for
the final combined value, without introducing BigInteger into the write path.

The next step is one user decision on the recommended schema-mode overflow and
precision contract. The accepted 2.8 artifact and all existing behavior remain
unchanged while that decision is pending.

### Approval and iteration 2.9 scope

The user approved the recommendation with "ok". Design revision 22 now records
the strict target-range and full-nanosecond `Instant` contract. The pending
decision language above records the earlier pause, not the current status.

Implement only broader units and `Instant` on the existing non-owning binding.
Reuse the checked primitive drivers after an explicit unit gate, but keep the
range-correct `Instant` arithmetic local to schema conversion. Do not change the
shared drivers, legacy Sender, designated timestamp handling, protocol or row
ownership. This avoids broadening a schema-only decision into unrelated APIs.

SOL assignments: client implementation/component tests and serialized client
builds; separate real-server E2E/shared vectors and serialized server builds;
independent arithmetic, compatibility and adversarial review. Root owns design,
integration and acceptance. Preserve the existing 2.8 corpus and add explicit
new input vectors, wire assertions, SQL boundary/precision checks, local-error
rollback, legacy comparisons and an `Instant` recovered-delivery case.

The hardware-first performance skill was consulted because this changes a
write path. This is a functional correctness increment, not a bottleneck or
optimization study; its hardware-measurement gate cannot establish this feature's
contract. Keep arithmetic bounded and avoid new per-write state or BigInteger.
Hardware limits, throughput and allocation rates remain unmeasured; make no
performance improvement or zero-allocation claims. Production optimization is
outside this slice.

### Simplification and pre-test review

The author initially proposed checking floor-divided long bounds and handling
the lowest representable second separately. Root recommended a smaller formula:
for negative seconds, evaluate `(seconds + 1) * scale + (fraction - scale)`
with checked multiplication/addition; otherwise evaluate `seconds * scale +
fraction` with the same checks. The normalized fraction is in `[0, scale)`, so
both terms in the negative branch are non-positive. An intermediate underflow
therefore cannot hide a representable final result. No special boundary table
or new converter state is needed.

SOL independently checked the argument and boundary arithmetic. Root also
compared checked arithmetic with an exact BigInt oracle over 200,140 boundary
and deterministic sampled cases at both scales (seed 20260910): zero mismatches.
This is an independent arithmetic check, not a substitute for component or SQL
tests. Initial independent production review found no correctness or scope issue;
test and final acceptance review remain pending.

### Implemented behavior and validation

The production change is confined to `QwpSchemaBinding`: extend the primitive
unit gate and add the `Instant` overload plus one small arithmetic helper. No
new schema state, buffer owner, protocol field or send-time work was introduced.
The common timestamp drivers and `QwpWebSocketSender` are byte-for-byte unchanged
from this iteration's starting source hashes. The 48-row 2.8 unit corpus is also
unchanged.

The new shared `timestamp-inputs.tsv` has 102 cases: zero, signed values and both
range boundaries for each newly supported unit/target pair; primitive minimum
longs; `Instant` extrema, negative fractions, target precision and exact long
boundaries. Root added two precision-edge requirements during review: the extra
999 nanos within the boundary microsecond must not cause rejection. The minimum
microsecond remains a wire-present minimum long/SQL NULL; the maximum remains a
finite maximum long. Independent SOL proleptic-Gregorian/BigInt verification of
all final vectors found zero mismatches.

Component tests assert target wire types/values, typed errors, duplicate-before-
validation ordering, partial-row and failed-column rollback, designated-target
rejection and stale-binding rejection through the new overload. The real-server
corpus test checks each accepted encoded value before sending and the resulting
SQL value; rejected inputs fail locally. The ordinary Sender test deliberately
pins the old results: maximum-long seconds wrap to -1000000000 in a nano column,
and an `Instant` 999 nanos after the epoch becomes zero. Schema conversion does
not reproduce those legacy normalization rules.

The existing SF test now also encodes an `Instant` as exactly 123 nanos before
append, closes the cursor engine, then drains through a recovering Sender and
checks SQL. It proves pre-persistence conversion and recovered value preservation,
not exact replayed-byte identity by itself. Existing generic replay tests retain
that separate byte-identity coverage.

Validation used serialized Maven within each repository, an installed standalone
client JAR (never `-Plocal-client`) and absolute project-local JVM temp directories.
Root verified final log results, matching corpora, stable main JAR hashes and
unchanged legacy/common-driver source hashes. All accepted runs have zero
failures, errors or skips:

- Client focused: 89 tests with the final corpus; 29 binding tests after the
  final stale-binding assertion.
- Client canonical after final review: 362 selected tests plus 2 packaging checks.
- Server focused: 8 timestamp E2E tests, including the existing six scenarios.
- Server canonical: 289 selected tests.
- Server seeded repeats: 28 tests each with seeds 1/2, 42/43 and
  1234567/7654321.
- Both repositories: `git diff --check` clean.

Exact commands, selectors, runtime identities and log paths:

- Client: `core/target/schema-timestamps-2.9/run-manifest.md`;
  final broad log `logs/canonical-post-review-final.log`.
- Server: sibling `questdb/core/target/schema-timestamps.2.9-server/run-manifest.md`;
  final broad log `logs/canonical289.log` and three `logs/repeat-*.log` files.
- Client HEAD: `981bdb02a471f3b290c89b8e78cbc422610e329e`;
  server HEAD: `12a33d651e51e2682e7a448c8db5168fc72dfad3`, both with the existing
  schema worktree changes. JDK: Corretto 25.0.4; Maven: 3.9.11.
- Workspace/installed main JAR SHA-256:
  `5ae6591c5ba90a29f4b743bfba0259488b4e16c0c76f59a09b29def745de08e2`.
- Both copies of the new corpus SHA-256:
  `f46a23969089a41ea38bf1d801f0176e8c2fd9fef9a91c71f471a64deae169df`.

An initial focused client run exposed an obsolete 2.8 test expectation that
MILLIS was unsupported; it now tests unsupported WEEKS. This was a fixture update
for the approved scope, not a production regression. A server run was interrupted
during compilation while the final corpus additions were synchronized; its
replacement completed successfully. Neither superseded run is counted as an
acceptance gate. The main JAR was not reinstalled after test-only additions;
server tests use its unchanged production bytes and their final mirrored corpus.

Final independent source/test review accepted with no findings after the corpus
and new overload guards were synchronized. A second SOL adversarial review of
the client implementation also found no issue. The client author independently
cross-reviewed the server assertions and found no false-green gap against the
stated scope. That review confirmed the limited SF claim above and distinguished
the two explicit legacy-output tests from exhaustive legacy equivalence; the
broader unchanged-code claim also rests on the source-hash checks.

Root accepts iteration 2.9. No commits or pushes were made. This remains a
conversion component, not activated schema mode in ordinary Sender. Timestamp
text parsing, designated timestamp row completion, remaining conversion families,
inference, automatic row-boundary adoption and coherent Sender activation remain
explicitly unfinished. The useful simplification is the bounded arithmetic
helper and reuse of the existing primitive drivers, not a new converter framework
or duplicated row lifecycle.

## D022 — Validate Sender integration prerequisites before activation

Date: 2026-09-10. Status: source-backed readiness review complete; activation deferred.

The user explicitly required sufficient context and assumption validation before
integrating senders. Root traced the current Sender build, row completion,
rollback, buffering, encoding, publication and schema APIs against design
revision 22. Three SOL agents independently inventoried setter/server conversion
coverage, negotiation/recovery and adversarial row/schema transitions. No
production code, tests or build artifacts changed; no tests were rerun. This
journal entry records the findings under the standing decision-log agreement.

### Confirmed gaps, not assumptions of readiness

- **The binding is not a replacement for the Sender write surface.** Its public
  inputs are LONG, STRING, UUID and timestamp overloads; non-null STRING currently
  converts only to UUID. Even ordinary STRING/VARCHAR, SYMBOL, BOOLEAN and DOUBLE
  identity writes are not covered. Compare `QwpSchemaBinding.java:95-243,324-373`
  with `QwpWebSocketSender.java:1118-1248,1571-1738,2285-2484,2892-3017` and the
  server's `QwpWalAppender.java:522-885`. Complete the agreed accepted conversion
  matrix, not just method signatures or identity writes, before activation.
- **Existing setters are not interchangeable for nulls.** Decimal, array and
  IPv4-text nulls are early no-ops; binary null is rejected. Preserve the explicitly
  retained no-op rules. Schema-mode cross-setter duplicate suppression is an
  intentional change and must run before value conversion; do not accidentally
  impose all legacy mismatch/validation ordering on schema mode.
- **Inference and designated timestamps are unfinished.** The binding rejects
  missing tables/columns and designated columns (`QwpSchemaBinding.java:297-332,
  407-415`). Sender currently creates its inferred layout and completes rows via
  `at`/`atNow` (`QwpWebSocketSender.java:1016-1055,2924-2952,3111-3127`). Confirmed
  missing metadata may use inference; unavailable metadata must never do so.
  `at` needs the pinned target unit; a setter-free `atNow` still needs resolution.
- **Normal construction does not yet select schema mode.** `buildAndConnect`
  requests the extension only when schema is already required
  (`QwpWebSocketSender.java:3378-3379`). Existing required-capability latches protect
  recovery, but are not a substitute for initial mode selection. ASYNC currently
  starts with no client and marks Sender connected after starting its I/O loop
  (`:4047-4064,4176`); that flag does not prove a successful handshake. The future
  first effective write must wait for initial negotiation. Only a successful
  old-server handshake permits legacy writes; a timeout/offline server does not.
- **A table selection is not a row boundary.** Repeating the current `table()`
  returns immediately (`:2924-2929`), and another row may start without calling
  `table()` again. Detect the first effective operation from actual row state,
  pin once, and keep that snapshot through completion/cancellation. Clear both
  designated timestamp caches when replacing a layout, not only on table switch.
- **Existing flush is publication, not just sealing.** Sender retains one current
  buffer per table, encodes legacy frames (`:4335-4353`), then immediately appends
  through `sealAndSwapBuffer` (`:5154-5200`). `QwpTableBuffer.reset()` preserves
  column definitions and binding (`:359-367`); `clear()` destroys them and data
  (`:123-136`). Neither alone retains old completed rows while adopting new types.
  Preserve old encoded blocks and existing pending accounting, symbol dictionary
  ordering, split-frame behavior and per-table transaction/commit boundaries.
- **Refresh is available but not integrated with row errors.** The loop exposes
  resolve/refresh (`CursorWebSocketSendLoop.java:1226-1239`); Sender calls neither.
  `SCHEMA_CHANGED` currently appears only in the reason enum. Existing evolution
  E2Es explicitly reset/clear/rebind their component buffers
  (`QwpSchemaFeedbackEvolutionE2ETest.java:76-134`); they do not prove automatic
  Sender adoption or whole-row retry after a cached conversion rejection.

API presence also does not prove successful server ingestion: current client
`longArray` overloads emit TYPE_LONG_ARRAY, but this server's mapping explicitly
rejects that wire type (`QwpWalAppender.java:124-125`), with a real-server rejection
test (`QwpSenderE2ETest.java:3628-3636`). Do not count it among working baseline
writes, or infer a new LONG_ARRAY conversion contract from the method name.
Validate the design's array-input contract in its own conversion slice.

### Shortcuts challenged during review

Root rejected two initial SOL recommendations: routing only the implemented
setters while retaining raw/server-side conversion for the rest, and permitting
ASYNC rows before initial negotiation. Both contradict revision 22. The agents
re-read the contract and corrected their recommendations. Existing legacy rows
may survive an old-to-new upgrade only after legacy mode was established by a
successful old-server connection; they are not permission for offline guessing.

The adversarial review also corrected an overly broad rejection of sealing on
schema change. Internal sealing is allowed; calling the current publishing
flush path unconditionally is not proven safe. A retained local encoding or
deliberately deferred publication could reuse existing machinery, but each must
prove ordering, accounting, reset/close, crash/recovery and backpressure behavior.
Do not choose a new per-table generation container or queue framework merely
because multiple encoding snapshots must coexist. Likewise, reuse the existing
request flag and mandatory-capability latches before adding negotiation state;
requesting support and requiring it are distinct semantics, not justification
for another owner or user-visible switch.

### Observable integration acceptance sequences

1. Buffer an INT/v10 row, learn LONG/v11, and write the next row without another
   `table()` call. Verify separate old/new wire types and identities plus SQL.
   Repeat with feedback arriving mid-row: that whole row stays on its old schema.
2. Exercise `at`, setter-free `atNow`, no-op/null inputs, duplicate inputs and
   cached timestamp references across row/layout transitions.
3. Reject against a cached schema and refresh once: unchanged target gives the
   invalid-value result; changed target/incarnation cancels the whole row and
   gives `SCHEMA_CHANGED`; refresh failure gives its actual reason. Earlier
   completed rows survive and the next whole-row attempt can succeed.
4. With all auto-flush triggers disabled, buffer rows across two tables and a
   schema change. Prove no unintended publication/commit, correct explicit-flush
   results and unchanged reset/crash behavior. Repeat with transactional
   auto-flush, capacity pressure, dictionary splitting and reconnect. Preserve
   existing per-table guarantees; do not claim new cross-table atomicity or
   stronger delivery guarantees after partial publish failures.
5. Test successful old/new handshakes, ASYNC first-write timeout, old-to-new
   upgrade with pending legacy rows, reconnect during a partial row and extended
   SF/background recovery. Never guess legacy, downgrade or relabel bytes.
6. Cover ordinary working setter/target pairs and confirmed missing-table/column
   inference alongside unavailable/denied metadata. No raw-value fallback or
   per-setter opt-out on supporting servers.

Recommendation: continue bounded converter/inference and row-lifecycle work
without activating normal Sender. Lock down existing Sender behavior with
observable characterization tests where needed. Public activation must land with
the complete integration matrix above, not ahead of it. Reuse the existing row
owner, schema coordinator, encoder and recovery path; choose any additional
storage/state only after those sequences establish why it is necessary.

## D023 — Text and symbol inputs reuse target-native text encoding

Date: 2026-09-10. Status: iteration 2.10 accepted after review and validation.

The user asked to continue after the integration-readiness review. The next
bounded slice covers six pairs on the existing non-owning binding: STRING and
SYMBOL inputs into exact STRING, VARCHAR and SYMBOL schema targets. Leave normal
Sender, inference, designated timestamps, other conversions and activation alone.
This fills ordinary text coverage without loosening the D022 activation gate.

Root and independent SOL source review verified:

- The server accepts all six pairs (`QwpWalAppender.java:698-805`). STRING and
  VARCHAR targets share TYPE_VARCHAR on QWP; SYMBOL retains TYPE_SYMBOL.
- Existing `ColumnBuffer.addString` encodes text into its offsets/data storage;
  `addSymbol` supports both the owner-backed global dictionary and a senderless
  local dictionary (`QwpTableBuffer.java:1197-1228,1855-1863`). Its constructor
  documentation overstates the need for a Sender, but the implementation already
  provides the needed local path. No new dictionary or owner is necessary.
- Both active client serialization paths replace individual unpaired UTF-16
  surrogates with literal `?`, preserve following characters and encode valid
  pairs normally (`OffHeapAppendMemory.java:166-202`,
  `NativeBufferWriter.putString`/`Utf8s.strCpyUtf8`). Preserve this behavior rather
  than substituting a parser/encoder or changing shared helpers.
- Null is distinct from empty. Text value case and whitespace must not be
  normalized; column-name case folding is unrelated. Distinct raw strings can
  occupy separate symbol IDs and still encode to the same replacement text.
- Text writes and dictionary insertion snapshot mutable inputs using existing
  encoding/copy operations. Tests must mutate a supplied CharSequence after the
  setter and verify the buffered value, not inspect ownership fields.

Implementation: extend `stringColumn` and the existing exact target-to-wire map,
then add `symbol(name, value)` with the same duplicate-first buffer lookup.
Preserve STRING-to-UUID and existing broader STRING-null behavior. The new SYMBOL
input accepts null only for its implemented text targets; reject other pairs
unless the value is already suppressed as a duplicate. Keep unknown full target
types, extension parameters and designated columns behind existing guards.

SOL owns client implementation/component tests/shared corpus, a separate real
server E2E implementation and serialized server builds, and independent source/
corpus/adversarial review. Root owns scope, final cross-review and acceptance.
The corpus records input UTF-16 units, expected target wire type/UTF-8 bytes and
expected SQL text units explicitly. Cover all six pairs, null/empty, Unicode,
malformed surrogate sequences, controls, dictionary collisions, duplicates,
rollback, mutable inputs and reset/clear. Also test existing global dictionary
encoding and SF recovery through production APIs, with no reflection, TestOnly
accessors, new hooks or fixed sleeps. Assert encoded bytes and real SQL, not just
successful sending. Existing corpora remain unchanged.

The hardware-first performance skill was consulted for the write-path change.
This is functional coverage, not production optimization: reuse existing storage
and serialization and add no new state/framework. No hardware bottleneck,
throughput improvement or allocation-rate claim is made. As in D021, measurement
gates are not a substitute for conversion correctness. Use absolute project-local
JVM temp storage; `/tmp` remains nearly full. Preserve all existing work and do
not commit or push.

Review clarified an existing encoder precondition: a Sender-owned symbol buffer
contains global IDs, so it must use the global dictionary message path. The
senderless convenience encoder uses a local dictionary and cannot encode that
buffer correctly. Do not conceal this by introducing another dictionary owner or
an automatic format conversion. The owner-backed test constructs a normal Sender,
lets the binding intern new values, then uses a matching explicit test dictionary
to assert the global delta and actual row IDs through the production encoder.
Only a prefix is registered before binding writes; registering every tested value
in advance would miss the insertion behavior. Distinct raw keys with identical
replacement text must retain distinct client IDs.

Coverage remains deliberately separated: the client fixture proves owner-backed
global encoding; real-server component and SF tests prove the existing local
dictionary path; ordinary Sender tests establish legacy value parity. Together
these do not prove activated schema-aware Sender delivery or global SF replay.
Those remain obligations of the D022 integration gate.

### Review and test corrections

Independent SOL review checked all 90 vectors with a separate UTF-16/UTF-8
oracle: zero mismatches. Root strengthened the tests to assert reset output on
the wire, enforce 90 corpus records and 15 records per conversion pair, and
preserve a non-null completed A row across failed B and completed C. Server
checks also distinguish explicit null and omission from empty text using raw
values and SQL `is null`, not the text printer's blank-cell rendering.

The broader server suite exposed an obsolete assertion in
`QwpSchemaRowBufferE2ETest`: STRING-to-STRING had deliberately been unsupported
in the earlier UUID-only slice. Preserve that test's unsupported-column rollback
coverage using LONG-to-STRING, which is still outside this binding's supported
conversions. Do not remove the assertion or turn off the regression suite.

The directly related `QwpTableBuffer` constructor Javadoc was corrected to
describe its global/local dictionary choice. This is the sole change to that
existing buffer class in this iteration: documentation only, with unchanged line
count, made after the client gates/install. The executable artifact remains
frozen for server validation; no reinstall occurs during server tests.

Test-authoring failures remain in the run manifests: Java-11-incompatible switch
expressions in a new helper, an incorrect close-flush configuration key, and the
SQL null-text printing expectation. These did not require production fixes.
The shared TSV intentionally has trailing delimiters on its six empty-text
records: these retain seven fields and must not be stripped as whitespace.

### Final validation and reflection

All final gates passed with zero failures, errors or skips:

- Client focused: 35 tests (29 existing binding tests and 6 text tests).
- Client canonical: 368 selected tests plus 2 packaging checks.
- Server focused: 8 tests across text and the existing row-buffer class.
- Server canonical: 293 selected tests.
- Server seeded repeats: 32 tests each with seeds 1/2, 42/43 and
  1234567/7654321.

The canonical selectors extend D021 with `QwpSchemaBindingTextTest` on the client
and `QwpSchemaTextE2ETest` on the server; the latter also joins all three seeded
repeats. Exact commands, runtime identities and superseded-run explanations are
in `core/target/schema-text-2.10/run-manifest.md` and sibling
`questdb/core/target/schema-text.2.10-server/run-manifest.md`. Final logs are
client `logs/canonical-final.log`; server `logs/focused-final3.log`,
`logs/canonical293-final.log` and the three `logs/repeat-*.log` files.

Client HEAD remains `981bdb02a471f3b290c89b8e78cbc422610e329e`; server HEAD remains
`12a33d651e51e2682e7a448c8db5168fc72dfad3`, with the existing dirty schema
worktrees preserved. Runtime: Corretto 25.0.4 and Maven 3.9.11. Builds were
serialized within each repository; server runs used the frozen installed client
JAR, never `-Plocal-client`, and absolute project-local JVM temp directories.

- Workspace/installed main JAR SHA-256:
  `572065d10739860e6b63d80f55f6834429f2b9134c360519d7e9aed094d08443`.
- Both 90-record text corpus copies SHA-256:
  `b5bdb1102a6d0887c8c81acc0eabf7519b56ac809f05bc505b44b92ccdbcaba7`.
- Root rechecked unchanged Sender, column writer, global dictionary and both
  UTF-8 serialization source hashes, and unchanged timestamp corpora. Restoring
  only the old constructor comment in a read-only stream reproduces the exact
  pre-iteration `QwpTableBuffer` source hash; all executable buffer code is
  unchanged. Both repositories pass `git diff --check`; untracked Java checks
  are clean, with the intentional TSV delimiters noted above.

Independent SOL source/test review accepted the final code, corpus, dictionary
fixture and corrected old test. The client author also cross-reviewed real-server
assertions; the server author independently reviewed the client conversion and
dictionary paths adversarially, finding no defect. Root checked the actual
wire/SQL assertions, logs, corpus hashes, final server classpath/temp/seed
properties and scope boundaries rather than treating review votes as acceptance
evidence.

Root accepts iteration 2.10. The useful simplification is reusing existing text
storage, symbol dictionaries and encoding paths: six conversions need no new
production state, generic converter framework or row owner. This is a functional
result, not a measured performance claim. Normal Sender remains unmodified by
this slice, and D022's conversion, inference, negotiation and row-boundary
integration requirements remain open. No commits or pushes were made.

## D024 — Boolean conversion uses target values and strict ASCII parsing

Date: 2026-09-10. Status: iteration 2.11 accepted after review and validation.

Continue with the BOOLEAN component family, not ordinary Sender integration.
Root and separate SOL source reviews confirmed a small complete scope:
`boolColumn(boolean)` into BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING
and VARCHAR, plus `stringColumn` into BOOLEAN. BOOLEAN-to-SYMBOL and other
unimplemented pairs remain unsupported. Preserve every existing conversion and
the broader `stringColumn(null)` rule; adding a BOOLEAN wire mapping must not
implicitly authorize other input methods.

### Source-backed contract and implementation boundary

- `QwpWalAppender.java:555-563,608-616,698-805` and
  `WalColumnarRowAppender.java:228-330` establish the nine boolean targets.
  Convert directly before append: boolean, exact numeric zero/one or lowercase
  ASCII text. Do not retain BOOLEAN wire for a numeric/text target and leave
  conversion to the server.
- `WalColumnarRowAppender.java:1241-1268` accepts only single-character `0`/`1`
  and ASCII-case-insensitive `true`/`false`. Whitespace, signs, alternate words,
  embedded NUL, Unicode lookalikes and malformed surrogate sequences are invalid.
  The existing client `Chars.equalsLowerCaseAscii` uses strict ASCII folding and
  can be reused without allocating strings or encoding temporary UTF-8. Java
  `Boolean.parseBoolean` and Unicode case-insensitive comparison have different
  acceptance rules and must not replace this contract.
- BOOLEAN storage has no distinct SQL null: the server maps wire null to false.
  BYTE/SHORT similarly store zero. Nullable numeric and text targets retain their
  own nulls. This follows the already approved target-native missing-value rule,
  not a new policy choice. Legacy boolean layouts instead materialize omitted
  values as false before server conversion, giving numeric zero or text `false`.
  Test both behaviors explicitly without changing legacy Sender.
- `ColumnBuffer.addBoolean` already stores ordinary boolean values; the column
  writer emits a row-null bitmap and then packs only non-null value bits.
  `QwpBooleanColumnCursor` advances its value index only for non-null rows.
  Reuse this representation and test across both byte and 64-row boundaries.

Only `QwpSchemaBinding` needs executable client changes: the bool setter, the
BOOLEAN target-to-wire case and the string parser case. Reuse the existing
buffer ownership, row cancellation, stale-binding guard and duplicate-first
lookup. No production changes to Sender, buffer, encoder, shared serializers,
server ingestion, lookup/cache coordination or protocol. No new conversion
framework, dictionary, fallback or feature switch.

SOL responsibilities: client implementation plus shared corpora and observable
wire tests; separate real-server E2E implementation/builds; independent and
adversarial source/corpus/test review. Root owns scope, assumption validation,
design/journal and acceptance. Planned corpora exhaust the ASCII case variants
of both accepted words and cover invalid tokens, all nine supplied-boolean
target pairs and explicit missing-value scenarios. Tests must include local
invalid-value errors, duplicate suppression before parsing, mutable input,
reset/clear, A-invalid-B-C row rollback and failed-row-only columns, legacy
accept/reject parity, intentional omission differences and recovered target
values. Use real wire and SQL observations, no reflection or TestOnly hooks.

The performance skill is applied as in D021/D023: this is functional coverage,
not production optimization, and no hardware bottleneck or speed/allocation-rate
claim is made. Source-level reuse is not a measured performance result. Keep
Maven serialized within each repository, install/freeze the client only after
client gates, then run server gates against that installed JAR without
`-Plocal-client`. Use absolute project-local temp paths; `/tmp` has about 621 MB
free. Preserve dirty worktrees, existing corpora and unrelated server files.
No commits or pushes.

Initial corpus review caught two ambiguous expectations before acceptance:
numeric target results must say zero/one, not reuse boolean `false`/`true`
labels; and explicit STRING null to BOOLEAN must identify a wire null, not a
stored false bit, even though SQL reads false. Keep the wire and SQL oracles
separate. Root also required the Unicode long-s spelling of `false` as a
rejection case: it specifically detects accidental Unicode case folding, unlike
an arbitrary non-ASCII typo. Packed tests must cross 64 non-null values as well
as 64 total rows; these are different indices when nulls are interspersed.

The final shared corpora contain 18 primitive-boolean target cases and 80 text
cases: all 50 accepted non-null ASCII spellings, one explicit null and 29 invalid
tokens. Root's independently constructed accepted-token set and target-value
oracle found zero mismatches; independent SOL review agrees. Client wire tests
use 137 rows with 69 supplied booleans, then reset to 70 all-null rows, checking
the raw null bitmap and compacted value bitmap separately. The server pattern
uses 137 rows with 93 supplied values; acceptance requires checking stored values
by row, not only aggregate counts. Float/double zero checks must compare exact
bits so negative zero cannot pass a delta-based assertion for positive zero.

Final source review keeps the omission comparison deliberately representative:
INT and STRING explicitly contrast schema target nulls with legacy zero/text
`false`. All nine targets still have supplied-value parity tests; the packed
BOOLEAN case and existing numeric-null tests cover their separate missing-value
rules. Do not describe the representative comparison as nine-target omission
coverage. A transient expanded draft was removed before the accepted runs.

The first client run exposed a raw null-bitmap fixture expectation error; the
first server build exposed missing required WebSocket close callbacks in the
test helpers. Fixes stay in tests, with unexpected close now failing explicitly.
The accepted server invalid-token tests send actual legacy VARCHAR frames and
require both SCHEMA_MISMATCH and the boolean-parse diagnostic. Generic failure
or timeout is not evidence of parser parity. Superseded development logs remain
identified in the run manifests.

### Final validation and reflection

All accepted runs passed with zero failures, errors or skips:

- Client focused: 41 tests, including all three binding test classes.
- Client canonical: 374 selected tests plus 2 packaging checks.
- Server focused: 6 new BOOLEAN E2E tests.
- Server canonical: 299 selected tests.
- Server seeded repeats: 38 tests each with seeds 1/2, 42/43 and
  1234567/7654321.

The canonical selectors extend D023 with `QwpSchemaBindingBooleanTest` on the
client and `QwpSchemaBooleanE2ETest` on the server; the latter also joins the
three seeded repeats. Commands, selectors and logs are recorded in client
`core/target/schema-boolean-2.11/run-manifest.md` and sibling
`questdb/core/target/schema-boolean.2.11-server/run-manifest.md`. Accepted logs
are client `logs/focused-final.log` and `logs/canonical-final.log`; server
`logs/focused-final.log`, `logs/canonical299.log` and the three
`logs/repeat-*.log` files. The initial server compile-only failure is not counted
as a test run or acceptance evidence.

Source HEADs remain client `981bdb02a471f3b290c89b8e78cbc422610e329e` and server
`12a33d651e51e2682e7a448c8db5168fc72dfad3`, with the dirty schema worktrees
preserved. Runtime is Corretto 25.0.4 with Maven 3.9.11. Server tests use the
standalone client installed after its final gates, never `-Plocal-client`, and
absolute repository-local JVM temp directories. Root verified the final Surefire
classpath, temp path and seed properties, not only the reported counts.

- Workspace/installed main JAR SHA-256, unchanged during server testing:
  `b1aa01621ae22cfc39b15a9b1385c82834f5999ab667fb2c2f6798bdeae4a86f`.
- Both copies of `boolean-to-target.tsv` SHA-256:
  `37038ba5b25a254b7b0e005ba449d3d652a5541734a98fdb9f5dc0dae0540581`.
- Both copies of `string-to-boolean.tsv` SHA-256:
  `1b8952d51862f68e80c23ea06111b3e4cfdee8a87cf37c3374d57d430b288c44`.

Root checked that the tracked production diffs in both repositories are
unchanged from the start of this iteration, along with the untracked server
schema-control source. The only executable production changes are in the
already-untracked client binding. Sender, buffer, column writer and all existing
corpora retain their pre-iteration hashes. Both repository diff checks and
explicit checks of the new untracked tests/resources are clean.

Independent SOL review accepted the conversion code, corpora and final test
contracts; the client author also cross-reviewed the server test, and the server
author independently audited the client implementation adversarially with no
blocker. Runtime gates remain necessary: source review alone did not establish
that test fixtures compiled or that their wire expectations were correct. The
accepted tests now assert actual encoded values, typed local failures, specific
server parsing failures and exact stored results, with no private hooks or
reflection.

Root accepts iteration 2.11. Reusing strict ASCII comparison and existing target
writers kept production changes limited to a setter, parser branch and type
mapping. No new state or conversion framework was needed, and no performance
improvement is claimed. The SF test proves conversion before persistence and
preservation of recovered values, not byte-identical replay by itself. Ordinary
Sender integration and the remaining D022 conversion/inference/row-boundary
requirements are still unfinished. No commits or pushes were made.

## D025 — Floating-point source semantics need an explicit compatibility decision

Date: 2026-09-10. Status: findings verified against source and real-server tests;
characterization accepted. The compatibility decision was subsequently approved
by the user in D026. This entry records the investigation, not implementation.

The next candidate slice is FLOAT/DOUBLE inputs into BYTE, SHORT, INT, LONG,
FLOAT and DOUBLE. Before assigning implementation, root and three SOL agents
traced ordinary Sender, the column encoder, QWP dispatch and server conversion.
The twelve server-supported numeric pairs are small enough to implement directly
in the binding, but supplied-value parity has two important edge cases.

### Source evidence and recommendation

- Ordinary `QwpWebSocketSender.floatColumn`/`doubleColumn` use bitmap-capable
  columns and append supplied values without NaN normalization. `ColumnBuffer`
  and the column writer preserve the raw value bytes. Crucially,
  `QwpColumnWriter.writeNullHeader` emits no bitmap when there are no omitted
  values; bitmap capability is not the same as a bitmap on the wire.
- `QwpFixedWidthColumnCursor.advanceRow` checks NaN as a null sentinel only
  without a bitmap. With a bitmap, a supplied NaN is non-null and reaches
  `WalColumnarRowAppender.checkDoubleToInteger`, which rejects it. Without a
  bitmap, BYTE/SHORT instead receive zero and INT/LONG receive SQL NULL. Adding
  an omitted row therefore changes whether another row is accepted. D013
  approved a stable source-null rule for LONG minimum only, not for NaN.
- `QwpWalAppender.isFixedTypeCoercionAllowed` confirms the six numeric targets;
  DATE and TIMESTAMP are not part of this floating-point slice. For integer
  targets, the server casts to long and compares the double round trip, then
  checks the narrower integer bounds. Exactly positive `2^63` casts to
  `Long.MAX_VALUE`, whose double representation rounds back to `2^63`, so the
  equality test accepts it. This is silent clamping of an out-of-range value,
  not exact conversion. Fractions and infinities fail the round-trip check.
- FLOAT/DOUBLE targets use normal widening/narrowing; finite DOUBLE overflow
  to FLOAT infinity and signed zero are separate behaviors. Do not accidentally
  ban them with a blanket finite-value or mathematical-range rule.

Root recommends NaN-as-source-null for supported numeric targets, consistent
with D013, and mathematical range checking for floating-point inputs to integer
targets. Preserve duplicate-first and supported-pair checks before null handling.
This means zero for BYTE/SHORT and NULL for nullable numeric targets; positive
`2^63` to LONG must fail locally instead of being clamped. Both are proposed
schema-mode exceptions to parity, not changes to legacy Sender or server
ingestion. No per-row state, batch scan, send-time conversion or framework is
needed. The exact negative LONG endpoint remains representable and follows the
target's existing SQL-null sentinel semantics; it is not a new source-null rule.

One independent reviewer initially called the twelve-pair slice coherent under
existing parity, then withdrew that conclusion after root challenged the
bitmap-free NaN case. A second review initially conflated bitmap-capable storage
with a serialized bitmap and corrected that claim against `writeNullHeader`.
These corrections matter: choosing either always-reject or always-null for NaN
changes one working legacy context. Reviews are evidence to check, not votes.
The independent review favors preserving the clamped endpoint for compatibility;
root favors rejecting it because early validation should not silently change
an out-of-range integer. User approval must settle the deliberate exception.

SOL server validation owns test-only real-server characterization using ordinary
Sender and exact stored values or server error messages. Root verified the
final tests and logs before presenting the decision. No client build/install,
server production change, ordinary Sender integration or new converter is part
of this investigation. The performance skill remains a functional-work guard:
no optimization, hardware bottleneck or speed/allocation-rate claim is made.
Existing worktrees and the frozen 2.11 client artifact are preserved.

### Characterization construction and review

Two tests were added to the existing server `QwpSenderE2ETest`, exercising
ordinary public Sender calls. NaN covers both FLOAT/DOUBLE sources and all four
integer targets, with and without an omitted row. The endpoint test checks
exactly positive `2^63`, the adjacent smaller representable source value and
the adjacent larger value, separately for FLOAT and DOUBLE. Assertions use
exact SQL values, missing-value predicates, specific SCHEMA_MISMATCH diagnostics
and zero committed rows from rejected blocks.

The first endpoint fixture mixed FLOAT and DOUBLE setters in one legacy layout
and failed locally, before the server conversion. Root caught the invalid
assumption; separate table layouts corrected the test. Root and adversarial
review also required explicit batching settings for NaN plus omission, so a
timer cannot flush the NaN row before the omitted row is appended. A dedicated
test helper pins the row threshold above two, disables the byte trigger and
sets the interval to the largest supported value. It does not alter the shared
rejection helper or production code. The first compile after this change used
a private superclass timeout constant; an explicit test timeout corrected it.
Error-message target names use locale-independent case conversion. Failed and
superseded intermediate runs are retained and are not final acceptance evidence.

### Final verification and handoff

The final two characterization tests passed, with zero failures, errors or
skips. Three repeats also passed both tests, using seeds 1/2, 42/43 and
1234567/7654321. Accepted server logs are
`core/target/schema-floating-feasibility/logs/final.log` and the three matching
`logs/repeat-*.log` files. The adjacent `run-manifest.md` records the selector,
JVM arguments, temporary paths and superseded runs. An intermediate compile
was interrupted for the locale fix; it is explicitly excluded from acceptance.

Root checked the final source, all four successful logs, the last Surefire XML
classpath/temp/seed properties and unchanged workspace/installed client JAR.
The final server source received independent adversarial SOL acceptance. No
reflection, TestOnly method or private-state hook is used. These checks establish
the two legacy behaviors; they do not validate an unimplemented schema converter
or replace its future full regression matrix.

- Client JAR SHA-256 remains
  `b1aa01621ae22cfc39b15a9b1385c82834f5999ab667fb2c2f6798bdeae4a86f`.
- Final `QwpSenderE2ETest.java` SHA-256:
  `9bcb321488b05e724eb90c8e5eb3f4ea205bec3b5716f7380fbe6fe8668a5ba6`.
- Client HEAD remains `981bdb02a471f3b290c89b8e78cbc422610e329e`; server HEAD
  remains `12a33d651e51e2682e7a448c8db5168fc72dfad3`. Tracked production diff
  hashes in both worktrees match the pre-investigation values, and the client
  binding is unchanged. Both repository diff checks are clean; earlier dirty
  work and the unrelated server Rust checkout are preserved.

Root accepts the characterization, not iteration 2.12. The design explicitly
keeps the two proposed parity exceptions pending. No conversion code, Sender
activation, legacy behavior, protocol, persisted bytes or server ingestion
was changed. No client reinstall, full canonical rerun, commit or push was
performed. Next action requires the user's decision on consistent NaN missing
values and rejection of floating-point inputs outside an integer target's range.

## D026 — Approved FLOAT/DOUBLE numeric conversion rules

Date: 2026-09-10. Status: user approved; iteration 2.12 accepted after review and
client/server validation.

The user answered "yes" to applying both recommended rules in schema mode while
leaving legacy behavior unchanged. NaN is source missing for supported numeric
targets, and floating-point input outside an integer target's mathematical range
fails locally. Design revision 26 makes both exceptions explicit; D025's
characterization remains the preserved legacy baseline.

Scope: add `floatColumn` and `doubleColumn` to the non-owning binding for BYTE,
SHORT, INT, LONG, FLOAT and DOUBLE. No text formatting/parsing, decimals, arrays,
DATE/TIMESTAMP conversion, inference, row-boundary adoption or Sender activation.
Executable production changes stay in `QwpSchemaBinding`; existing buffer,
encoder, ownership and rollback remain. One shared floating numeric helper is
acceptable: widening a FLOAT input to DOUBLE is exact for all non-NaN values,
and NaNs are normalized before append. Do not duplicate twelve target branches
or introduce a converter framework merely to keep separate setter names.

Preserve duplicate-first behavior, exact supported-pair/parameter/designated
checks before NaN normalization, and the input type in error context. NaN uses
the target missing representation, not a raw NaN payload. Integer conversion
rejects fractions and infinities and explicitly excludes positive `2^63` before
any long cast could silently saturate. Inclusive BYTE/SHORT/INT ranges and the
negative LONG endpoint remain. A finite conversion landing on INT/LONG minimum
is still a present target sentinel that reads SQL NULL, not a new source-null
policy. Floating targets retain server-equivalent non-NaN bits and rounding,
including negative zero and overflow/underflow when narrowing DOUBLE to FLOAT.

SOL responsibilities: client binding, shared raw-bit corpus and observable
client tests; separate real-server E2E author/build owner; independent source,
corpus and adversarial test reviewer. Root owns scope, independent boundary
oracle, design/journal and acceptance. The client corpus owner publishes its
format early so server tests can be written independently. Tests must assert
wire type, actual bitmap/value bytes and stored values separately; a cursor's
sentinel interpretation alone does not prove an explicit wire null.

Cover all twelve pairs; +/-zero, subnormal and finite extrema; integer edges
and adjacent representable floats; fractions and infinities; multiple NaN
payloads/signs; nulls with and without other omitted rows; duplicate invalid or
unsupported inputs; reset/clear/stale bindings; whole-row rollback preserving
A/C and removing failed-row-only columns; and converted bytes persisted and
recovered through SF. Legacy tests must separate preserved parity from the two
approved differences. No reflection, private hooks, TestOnly APIs or assertions
whose expected result is computed by the converter being tested.

Run focused and canonical client gates before installing/freezing its standalone
artifact. Only then run server gates against that installed JAR, never
`-Plocal-client`. Extend the 2.11 canonical selectors with the new floating
tests and retain both D025 legacy characterization methods. Repeat the server
schema matrix with the existing three seed pairs. Keep Maven serialized within
each repository and use absolute project-local JVM temp directories. Preserve
dirty worktrees, all prior corpora and unrelated files; no commits or pushes.

As in D024, the performance skill is used for functional-work discipline rather
than a production optimization: existing storage/writers are reused, measured
hardware behavior is unknown, and no bottleneck, throughput or allocation-rate
claim is made. Functional test counts are not performance evidence.

### Implementation and independent checks

The implementation uses two public setter methods and one shared floating
numeric path. It reuses the numeric target predicate with LONG conversion;
following root review, SOL consolidated the two identical allowlists. No buffer,
encoder, wire mapping, owner or server ingestion change was needed. Integer
checks use finite/whole-value validation and exact mathematical bounds, with
the LONG upper bound expressed as exclusive positive `2^63`.

The shared corpus is frozen at 350 raw-bit cases, SHA-256
`7194ec96006341cf5a5d9081fd75d8f1ab7a1a5813067ba80971d01df8957216`.
Its seven fields separate input bits, target type, wire type, wire bits and
stored value. Explicit wire null is distinct from a present INT/LONG sentinel
and from BYTE/SHORT's stored zero. Stored floating values use raw-bit expectations
to preserve negative zero and avoid relying on SQL text formatting.

Root's exact-IEEE BigInt oracle found eight incorrect draft SQL expectations:
NaN-to-BYTE/SHORT encodes an explicit null but stores zero, not SQL NULL. These
were corrected before acceptance. Additional vectors cover signaling NaN,
negative fractions, LONG/INT boundaries and adjacent representable inputs,
round-to-even ties, zero/subnormal and subnormal/normal transitions, and the
finite-to-infinity narrowing threshold. Input raw bits, not approximate numeric
case labels, are authoritative: FLOAT cannot distinguish integer values already
rounded to the same input before the setter call.

Both root's independent BigInt oracle and a separate SOL arithmetic check agree
on all 350 cases: 106 invalid, 36 explicit missing and 208 present values across
all twelve input/target pairs. Root's diagnostic script is retained at
`core/target/schema-floating-oracle-2.12/check-corpus.cjs`; its rounding logic
was separately cross-checked on 10,000 deterministic IEEE samples. That sample
validates the diagnostic oracle, not 10,000 executions of the new converter.

Root and SOL review strengthened duplicate tests to assert retained encoded
values rather than only successful encoding. The new client tests also check
actual null bitmap/value bytes, whole-row and failed-column rollback, reset and
stale binding behavior, unsupported targets before NaN handling, and input/target
error context. Independent client review found no remaining issue. The first
focused run passed 47 tests, including all six new floating tests; broader
client and real-server validation remain the acceptance gates below.

Server review required three additional observations before runtime acceptance:
test all 104 legacy-invalid raw frames, excluding only the two approved positive
`2^63` clamps; cover both NaN row contexts for both input types and all six
targets; and prove that a bitmap actually exists and contains zero compact
values for an all-missing column. A cursor null flag alone was insufficient,
because its bitmap-free sentinel interpretation is the original compatibility
problem. Once bitmap presence and compact value counts are checked, the existing
cursor's row flags and value reads can be reused without another wire parser.

Stored-value checks were corrected to recognize NaN as FLOAT/DOUBLE SQL null;
BYTE/SHORT omission cases assert actual zeros, not just non-null counts. The SF
test uses literal expected integer and floating bits before persistence and
after recovery, rather than describing converted values as byte-identity proof.
Independent server source review accepted these final contracts, with runtime
verification still required.

The first server focused run exposed an incorrect sparse-layout column index
in the new NaN fixture: only `value` was written, so its wire index is zero even
though it follows `case_id` in the SQL schema. Root caught the assumption during
review; the fixture now asserts the single encoded column's name and type. The
next focused run exposed an expected SQL-header mismatch for two unaliased
counts. These are test-fixture failures, not converter defects. Their logs are
retained separately from the final accepted runs. Source review did not replace
compilation and execution.

### Final validation and acceptance

All final gates passed with zero failures, errors or skips:

- Client focused: 47 tests, including 6 new floating tests.
- Client canonical: 380 tests plus 2 packaging checks.
- Server focused: 5 new floating E2E tests.
- Server canonical: 306 tests.
- Server seeded repeats: 45 tests each at seeds 1/2, 42/43 and
  1234567/7654321.

The client selector extends D024's 374 with `QwpSchemaBindingFloatingTest`.
The server selector extends D024's 299 with the five-test
`QwpSchemaFloatingNumericE2ETest` and both D025 legacy characterization methods
in `QwpSenderE2ETest`. The repeat selector likewise adds those seven tests to
D024's 38. Explicit method filters retain both older UUID checks in the
canonical run; adding methods to the source alone would not select them.

Client commands and logs are recorded in
`core/target/schema-floating-2.12/run-manifest.md`; accepted logs are
`logs/focused-initial.log`, `logs/canonical-final.log` and `logs/install.log`.
Server commands and logs are recorded under sibling
`questdb/core/target/schema-floating.2.12-server`; accepted logs are
`logs/focused-final2.log`, `logs/canonical306.log` and the three
`logs/repeat-*.log` files. The two failed focused fixture runs remain separate.

The shared 350-case corpus is byte-identical in both repositories. Server tests
consume every wire and stored-value field: 244 accepted cases, plus all 106
local invalid cases; 104 of those also receive specific legacy-frame NACKs,
while the two deliberately different LONG clamps are covered by D025. The NaN
matrix checks both inputs, all six targets and both row contexts. SF validates
target-native values before persistence and after recovery; byte-identity replay
remains covered by the existing replay suites, not inferred from this test.

Root verified the successful logs, printed seed values, Surefire's installed-JAR
classpath and project-local temp paths, and final artifact identity. Server
tests use the frozen standalone client without `-Plocal-client`; no reinstall
occurred during server testing. Runtime is Corretto 25.0.4 with Maven 3.9.11.

- Workspace/installed client JAR SHA-256:
  `8e6dfd80195889b30eb1d0a8466f5529a15a242bc19b1d6814b4ff4ee988497e`.
- Client `QwpSchemaBinding.java` SHA-256:
  `7ca784dc29c76d1ace2e2ebafef963c5e2d77939c485fdd5c2d04c104b4be53f`.
- Server `QwpSchemaFloatingNumericE2ETest.java` SHA-256:
  `178815e8e7dc046f0bcd1e76926047c0bf628d1b791345165e460833dc8389ee`.
- Client HEAD remains `981bdb02a471f3b290c89b8e78cbc422610e329e`; server HEAD
  remains `12a33d651e51e2682e7a448c8db5168fc72dfad3`.

Tracked production diff hashes in both repositories, the shared buffer/writer/
Sender hashes, the server schema-control source and all older corpora remain
unchanged. Only the previously untracked client binding has new executable
production changes in this iteration. Both tracked diff checks and explicit
new-file whitespace checks are clean. Dirty worktrees and unrelated server
Rust files are preserved; no commits or pushes were made.

Root accepts iteration 2.12. Independent SOL review accepted the client code,
corpus and tests, and the final server test contract; the server author also
audited the client conversion independently. Review strengthened evidence without
adding production state or a conversion framework. The performance skill kept
functional coverage separate from unmeasured performance claims. Ordinary
Sender activation, remaining conversion families, inference and producer-owned
row-boundary adoption are still deferred under D022. This is a completed
conversion component, not an activated or release-complete schema-aware Sender.

## D027 — STRING-to-numeric conversion, characterized before implementation

Date: 2026-09-10; accepted 2026-09-11. Status: iteration 2.13 accepted after
explicit LONG text overflow approval, review and client/server validation.

The next bounded slice adds STRING input to BYTE, SHORT, INT, LONG, FLOAT and
DOUBLE in the existing non-owning binding. It directly supports early rejection
of malformed or out-of-range values. Ordinary Sender activation, inference,
row-boundary adoption, numeric formatting and other conversion families remain
outside this increment. D022's integration gate is unchanged.

Start with real-server characterization using the previously accepted client
artifact. Similar parser implementations do not prove parity: the client reads
Java characters while ingestion reads encoded UTF-8. Explicitly test grammar,
Unicode, malformed surrogates and NUL as well as numeric boundaries. Do not
assume every non-ASCII input is rejected. Server acceptance and stored values
must be established before releasing the production patch.

Reuse direct target parsers and the existing column append/rollback machinery.
Do not route integer text through DOUBLE, add a converter framework, introduce
another buffer owner or change the legacy Sender/server to fit client behavior.
Preserve duplicate-first checks and existing target/parameter/designated guards.
Any newly discovered contract difference requiring a product choice must be
reported before silently changing acceptance.

SOL ownership is split between client binding/shared corpus/client tests,
server characterization/E2E/builds, and independent adversarial review. Root
owns architecture, independent evidence checks and this journal. Acceptance
requires shared cases with exact target wire values and SQL results, server
rejection parity, null/sentinel contexts, duplicate handling, partial-row
rollback preserving completed rows, and converted SF recovery. No reflection,
private hooks or test-only production APIs. Maven runs remain serialized per
repository and the installed standalone client is frozen during server gates.

The performance skill was read with both evidence references. This is a
functional extension, not a production optimization: no bottleneck is claimed
and no measured throughput/latency comparison is available. Its optimization
gate does not establish correctness for this work. Keep implementation scope
small and make no performance claim. Use absolute repository-local test temp
paths because shared `/tmp` is close to capacity; preserve unrelated files.

### Characterization findings before the production gate

Source review disproved the initial SHORT simplification: server `parseShort`
accepts only optional minus and ASCII digits, while `parseInt` accepts leading
plus and underscores. A strict grammar check followed by the existing integer
parser and short range check can preserve acceptance without another arithmetic
parser. This is a simplicity choice, not a measured performance improvement.

Root also reproduced an existing LONG parser overflow through the public
`Numbers.parseLong` API in the accepted 2.12 client artifact:
`"21000000000000000000"` becomes `2553255926290448384`; its negative counterpart
becomes the negative of that value. Values such as unsigned 64-bit maximum
reject, so checking only obvious boundary strings would miss the problem.
Client and server source use the same insufficient wrap-detection arithmetic.
Real-server ingestion confirmation is being run before a product recommendation.

Independent SOL review agrees that silently adding a checked parser would
violate the current supplied-value parity rule. Root recommends rejecting
mathematically overflowing LONG text in schema mode if ingestion confirms the
behavior, while preserving legacy ingestion and shared parsing utilities.
That would require an explicit compatibility decision; production edits remain
withheld. Preserve other established grammar independently of this arithmetic
decision, including LONG's existing suffix/separator behavior.

The real-server test confirmed all five supplied wrapping examples (positive
and negative 21e18, positive 25e18, 42e18 and 63e18). It sends legacy VARCHAR
wire values, requires successful write responses and checks the exact stored
LONG values. `testLegacyLongParserWrapCharacterization` passed 1/1 with zero
failures/errors/skips; evidence is in sibling
`questdb/core/target/schema-string-numeric.2.13-server/logs/wrap-characterization.log`.

The broader diagnostic characterization passed separately in
`logs/characterization2.log`. This records acceptance and stored values rather
than asserting a frozen conformance corpus; it is not converter acceptance.
Its first run, retained as `logs/characterization.log`, stopped on an incorrect
test assumption that every rejection used one status code. Production behavior
was not changed to make the diagnostic pass.

Root verified both successful logs and the unchanged standalone 2.12 artifact
SHA-256 `8e6dfd80195889b30eb1d0a8466f5529a15a242bc19b1d6814b4ff4ee988497e`.
Client binding, client tracked production diff, server tracked production diff
and server schema-control hashes also remain unchanged. No converter patch or
ordinary Sender activation has landed in 2.13. The user has been asked whether
to approve strict LONG text range rejection in schema mode. This iteration
remains incomplete pending that decision; the dedicated test preserves the
legacy baseline independently of the future choice.

The server build owner recorded commands and scope in
`core/target/schema-string-numeric.2.13-server/characterization-manifest.md`.
The diagnostic matrix has 366 observations, not a full grammar proof. Root
corrected a summary overstatement: rejection of `1L` and `0x10` does not prove
rejection of floating suffixes or complete hexadecimal floating literals.
Those remain explicit cases for the future shared corpus. All agents have
paused; no client source, tests, resources or build artifacts changed in 2.13.

### Approved LONG text range exception and resumed implementation

The user answered "Yes, proceed" to rejecting mathematically overflowing LONG
text in schema mode while leaving legacy behavior unchanged. Revision 27 records
this exception to supplied-value parity. Implementation is now released, not
accepted: all 2.13 converter and regression gates remain to be run.

Use a private checked LONG text parser in the existing binding if necessary;
do not change shared `Numbers`, ordinary Sender, or server ingestion. Preserve
the existing syntax independently of range checks: no leading plus, existing
underscore/DEL separator rules and terminal `L`/`l`. Arbitrary leading zeros
must not cause false overflow. Exact signed-long endpoints remain accepted;
the minimum is a supplied target sentinel, not a missing STRING input. Do not
route parsed values through the LONG or floating setters, whose source-null
rules apply to different input types.

Reactivated SOL ownership remains client implementation/corpus/tests, server
E2E/builds, and independent adversarial review. Root will independently check
the checked arithmetic and acceptance evidence. The skill's optimization gate
does not apply cleanly to this requested functional extension; its evidence
discipline is retained without optimization edits or performance claims. Shared
`/tmp` now has roughly 211 MB free; all new test JVMs must use repository-local
temporary directories. No unrelated cleanup, commits, or pushes are authorized.

### Implementation review and independent checks

The first candidate adds direct numeric writes and small private SHORT/LONG
parsers to the existing binding. Numeric parse errors share one conversion
boundary; no additional owner, cache, protocol or delivery state is introduced.
SHORT checks its strict syntax before the existing INT parser/range check.
LONG uses checked negative accumulation so both signed-long endpoints remain
representable without overflowing intermediate arithmetic.

Root compiled the candidate separately from Maven outputs and exercised the
public binding and serialized LONG wire against an independent regex/BigInteger
oracle. All 14,598 deterministic cases passed (2,921 accepted, 11,677 rejected;
seed 2132026), including boundary neighborhoods, known wrapped values, syntax
mutations, Unicode and 10,000 leading zeros. Candidate binding SHA-256:
`724834280cffa6b43d8b229513211c97c9df89d7f55390e4e841dc19511477d1`.
The same diagnostic had failed against the old 2.12 artifact with the expected
missing-conversion error. Final acceptance must rerun against the frozen new
artifact; this isolated compile does not replace Maven or server validation.

Independent SOL review and root both caught two draft test weaknesses before
acceptance: BOOLEAN text `"2"` is INVALID_VALUE, not an unsupported pair, and
the mutable-input test discarded its first row before observing its converted
value. The author was asked to correct those assertions and format new test
helpers as readable code. Tests must prove their named behavior.

The legacy NBSP rejection exposed a pre-existing server diagnostic bug, not a
new conversion contract: after numeric parsing fails for `"\u00a01"`, error
logging throws `LogError: Invalid UTF-8` and the response is INTERNAL_ERROR
instead of SCHEMA_MISMATCH. The earlier characterization log records this at
the numeric-error logging path. Keep this server behavior outside the patch;
tests must identify this specific observed response rather than accepting an
arbitrary failure status. Local conversion still rejects the malformed value.

Runtime checks exposed another load-bearing difference: the server's UTF-8
memory parsers accept both `f` and `d` suffixes for decimal FLOAT/DOUBLE text,
but the client character parsers accept only their own decimal suffix. The
initial corpus correctly expected server acceptance for FLOAT `"1d"` and DOUBLE
`"1f"`; the binding failed those cases. This was a production compatibility
defect, not a reason to weaken the corpus. Source equality with the server's
character parser had not established parity with its ingestion memory parser.

The fix remains binding-local: find an opposite numeric suffix before trailing
ASCII whitespace, require a preceding digit or decimal point, then pass a slice
of the original CharSequence to the target parser. It does not copy/rewrite
text, use an exception for a successful conversion, change shared parsers or
add state. The prefix parser validates the remaining grammar. Explicit cases
cover complete hex forms, suffix case, trailing dots, NUL/whitespace, malformed
double suffixes and suffixed NaN/Infinity. No new compatibility exception is
needed; this restores the agreed server behavior.

Root exercised this candidate binding and serialized floating values against
the actual server UTF-8 memory parsers over 16,628 deterministic inputs (2,806
accepted, 13,822 rejected; seed 2132027), with zero mismatches. This is a
public-API/parser differential diagnostic, not a real-server ingestion test or
an independent implementation of floating arithmetic. The separate JDK and
BigInteger corpus audit checks 429 cases: all 255 non-null integer acceptance/
value expectations, 111 accepted floating bit patterns and 6 explicit nulls;
the 57 rejected floating cases only receive structural marker checks there.
Real-server tests own the floating rejection-policy evidence.

The final candidate corpus has 429 cases (188 accepted, 241 locally invalid),
including the five explicit legacy wrapping exceptions. Client review is
accepted at binding SHA-256
`a54f9ad6a68ed1d180c5d7a63599be6dcb69750ca7903ee9ef5411ed58501215`
and corpus SHA-256
`e1b3728510afd915bd1cf589ff2c2900337614e331ff821d1014cbb6c825c9d1`.
Client focused aggregate passed 53 tests plus 2 packaging checks; canonical
passed 386 tests plus 2 packaging checks. The initial failing fixture/parser
runs remain distinct from the accepted logs. Server ingestion, recovery and
final artifact identity gates are still pending at this point.

Final source review strengthened the server tests without changing production:
replace the print-based characterization with fixed corpus assertions; require
exactly 236 legacy rejections after the five separately asserted wrap cases;
distinguish the known NBSP logging error from numeric parse/range failures;
check literal sentinel/default values and cross-suffix values through SF.
The encoded identity is compared to the actual DESCRIBE snapshot, not a
synthetic fixture constant. The final focused rerun after that identity check
passed all six tests; the broader server gate passed 312 tests with zero
failures/errors/skips. Three seeded repeats remain before final acceptance.

Root reran both generated diagnostics against the frozen installed artifact,
not the isolated candidate classes: all 31,226 public-binding/wire cases passed.
The standalone artifact SHA-256 is
`5c1d2a24e6147a066a9fbc2e28e4fdf8faaca1b6baece62af7d1769c02b71144`.
Exact diagnostic commands, scopes and limitations are recorded in
`core/target/schema-string-numeric-2.13/oracle-manifest.md`.

Reflection: reuse of an existing parser is the right starting point, but
character versus byte entry points can implement different grammar. Test the
actual ingestion path before claiming parity. The two necessary adaptations
stay local and explicit (checked LONG arithmetic and floating suffix handling),
with a strict SHORT grammar check. No conversion registry, compatibility mode,
alternate buffer owner, shared-parser refactor or send-time transformation was
needed. This slice reduces conversion gaps; it does not resolve D022's Sender
row-lifecycle, inference and activation obligations.

### Final validation and acceptance — 2026-09-11

All final gates passed with zero failures, errors or skips:

- Client focused aggregate: 53 tests plus 2 packaging checks.
- Client canonical: 386 tests plus 2 packaging checks.
- Server focused: 6 tests, rerun after the final schema-identity assertion.
- Server canonical: 312 tests.
- Server repeats: 51 tests each at seeds 1/2, 42/43 and 1234567/7654321.

The selectors extend the accepted 2.12 gates with
`QwpSchemaBindingStringNumericTest` on the client and
`QwpSchemaStringNumericE2ETest` on the server. Existing conversion, negotiation,
feedback, replay and legacy characterization coverage remains selected.
The shared 429-case corpus is byte-identical in both repositories: all 188
accepted cases have exact target wire and stored-value checks, all 241 invalid
cases fail locally, 236 also receive the specified legacy rejection, and the
five approved overflow differences have separate exact legacy stored-value
assertions. The Unicode logging failure is specifically asserted, not treated
as permission to accept arbitrary server errors.

The null matrix distinguishes present parsed INT/LONG sentinels and floating
NaN from explicit bitmap nulls, with and without another omitted row. BYTE and
SHORT missing values are asserted as exact SQL zero. The SF case checks
conversion before persistence and recovered INT 42, FLOAT `"0.1d"` bits
`0x3dcccccd`, and DOUBLE `"-0.0f"` bits `0x8000000000000000`. It does not replace
the existing generic replay byte-identity evidence.

Client commands/logs: `core/target/schema-string-numeric-2.13/run-manifest.md`,
with final `logs/focused-final.log`, `logs/canonical-final.log` and
`logs/install-final.log`. Server commands/logs: sibling
`questdb/core/target/schema-string-numeric.2.13-server/run-manifest.md`, with
final `logs/focused-final2.log`, `logs/canonical312.log` and all three
`logs/repeat-*.log`. Diagnostic characterization and superseded fixture/parser
runs remain separately recorded. They are not final acceptance evidence.

Root verified the successful log summaries, final Surefire seeds and absolute
project-local temp path, the installed standalone client on the server test
classpath, and unchanged artifact identity. There was no `-Plocal-client` or
client reinstall during server gates. Runtime: Corretto 25.0.4, Maven 3.9.11.

- Workspace/installed client JAR SHA-256:
  `5c1d2a24e6147a066a9fbc2e28e4fdf8faaca1b6baece62af7d1769c02b71144`.
- Client binding SHA-256:
  `a54f9ad6a68ed1d180c5d7a63599be6dcb69750ca7903ee9ef5411ed58501215`.
- Shared corpus SHA-256:
  `e1b3728510afd915bd1cf589ff2c2900337614e331ff821d1014cbb6c825c9d1`.
- Client test SHA-256:
  `a475aafdd1c9863d1f04722751a5af3d1296ecac81afdc20d2b9a1c1cb09eb55`.
- Server test SHA-256:
  `b5f5f04f118d032cc2da5cea6c7cdbf3de911ec87aa65e8b66703241cb3c3444`.
- Client HEAD: `981bdb02a471f3b290c89b8e78cbc422610e329e`.
- Server HEAD: `12a33d651e51e2682e7a448c8db5168fc72dfad3`.

Both SOL source reviews accepted the final server test identity; the independent
reviewer also accepted the final client binding, corpus and tests. Root's
generated diagnostics passed again against the final installed artifact.
Tracked production diff hashes in both repositories, shared buffer/writer/
Sender hashes and server schema-control hash are unchanged from 2.12. The only
new executable production behavior is in the existing untracked binding.
Tracked diff and explicit new-file whitespace checks are clean. No unrelated
files were removed, no shared parser or server production code was changed,
and no commits or pushes were made.

Root accepts iteration 2.13 as a production-quality conversion component.
The performance skill kept functional verification separate from unmeasured
performance claims; no optimization or hardware-bottleneck claim is made.
Ordinary Sender conversion, remaining conversion families, inference and
automatic row-boundary adoption remain deferred under D022. This is not an
activated or release-complete schema-aware Sender.

## D028 — Complete missing-conversion inventory — 2026-09-11

### Request and scope

User requested all missing conversions in the design, cross-checked against
server source. This is a documentation-only audit after accepted iteration
2.13, not another implementation iteration. Root owns the document changes;
three SOL agents independently inspected scalar dispatch, complex types and
the public client/binding surface. No production code or tests were changed,
and no builds, commits or pushes were requested.

### Decisions and findings

Design revision 28 adds a complete public-input-to-target inventory, including
missing identity setters, parameterized target representations and wire-only
DATE coverage. "Implemented" means the existing binding component, not ordinary
Sender activation. Null acceptance, ignored duplicates and no-ops cannot be
used to claim support for effective non-null values.

The inventory follows the actual wire cursor, dispatcher and value routine,
not general SQL casting or helper names. STRING input emits VARCHAR; no separate
TYPE_STRING exists. BYTE/SHORT/INT are genuine public input types, not LONG
aliases. SYMBOL is dictionary-backed. Typed decimal and geohash text overloads
parse locally before producing their respective wire types and are not the
same conversion path as stringColumn.

Missing work includes numeric/date/timestamp/text/decimal cross-pairs, remaining
string parsers, native setters for CHAR/IPv4/LONG256/BINARY, all decimal/geohash/
array bindings, and numeric/special-type text formatting. Source-specific null
rules remain explicit: approved numeric source-null exceptions do not silently
decide new families, and decimal cursors use bitmap-only null detection rather
than the fixed-width cursor's sentinel inference.

The source audit found five boundaries that must stay visible until explicitly
resolved: BINARY reaching text parsers; existing-array element/rank validation
gaps despite LONG_ARRAY auto-create rejection; non-ASCII CHAR text corruption
and UTF-16 surrogate representation; designated timestamp fixed-width fallback;
and decimal auto-create scale checks that rely on assertions. These are recorded
as source-proven paths requiring observable tests and a compatibility decision,
not as approved casts, approved behaviour changes or runtime-confirmed failures.

Corrected the earlier array coverage wording: the server copies elements; it
does not establish LONG/DOUBLE element conversion. Keep missing-table/column
inference, designated row completion and automatic row-boundary adoption apart
from the conversion matrix. Do not add a conversion framework, fallback,
send-time transformation or public DATE setter simply to fill out the table.

### Source verification and review

The design contains a linked source map with methods and snapshot line numbers.
Both repositories contain existing uncommitted feature work, so base commits
alone are not presented as the audited implementation:

- Client base: `981bdb02a471f3b290c89b8e78cbc422610e329e`.
- Server base: `12a33d651e51e2682e7a448c8db5168fc72dfad3`.
- Client binding SHA-256:
  `a54f9ad6a68ed1d180c5d7a63599be6dcb69750ca7903ee9ef5411ed58501215`.
- Server QwpWalAppender SHA-256:
  `9d61e060177bae1c10f507f60dc98443fe9ce350d9c0a18790935f58695e5d40`.
- Server WalColumnarRowAppender SHA-256:
  `61d80aa814f0674be3aa655f65e8b9570da98b261d6d25c7123bccfcc1cc71b0`.

Root cross-checked the entire target dispatcher, cursor factory, binding
entrypoints/target selection and the cited parser/formatter boundaries. Root
corrected initial audit assumptions about a nonexistent TYPE_STRING and the
actual UTF-8 sink implementation. Independent draft review corrected the binary
overload list and added decimal-specific sentinel coverage. The public-client
review accepted the corrected inventory without remaining findings; the complex
type review accepted the final corrections and found no missing family or
overbroad target pair. All linked source paths resolve. Tracked diff and explicit
new-document whitespace checks are clean. Client/server tracked production diff
hashes, the binding, ordinary Sender and both audited server appenders are
unchanged from the start of the audit.

No new runtime coverage is claimed. Iteration 2.13's recorded acceptance remains
the latest test evidence; this audit does not make missing conversions or the
ordinary Sender release-ready.

## D029 — BINARY target conversion (iteration 2.14) — 2026-09-11

Status: accepted as a conversion component; ordinary Sender activation remains deferred.

### Scope and rationale

After the conversion audit, the user asked to proceed incrementally. Root chose
BINARY target support: the three existing binary input forms (byte array,
DirectByteSlice, native pointer/length) and stringColumn into BINARY. This is a
bounded family with an existing buffer representation and no new parser. The
unresolved BINARY-to-number/date/etc. parser paths are explicitly excluded,
along with ordinary Sender activation and all other conversion families.

The existing ColumnBuffer supports BINARY and VARCHAR using the same offsets
and byte storage. Use addBinary for opaque input and addString for existing
UTF-8 encoding; select TYPE_BINARY before append. Add no second buffer owner,
temporary converted byte array, shared parser change or send-time conversion.
The binding remains non-owning and the caller retains row completion/rollback.

Effective null byte-array/slice arguments are INVALID_VALUE, not implicit nulls.
Native lengths must be 0..Integer.MAX_VALUE, with a nonzero pointer required
only for nonempty input. Reject out-of-range lengths before payload growth or
source-memory access. The documented caller obligation to provide readable native memory
remains; the client cannot validate arbitrary addresses. A zero pointer with
zero length is an empty value. Apply duplicate suppression before argument
validation, as already required for schema-mode setters.

String null and omission use the target null representation. Empty bytes stay
distinct from null. Root verified that current server MemoryCARW.putBin and
MemoryPARWImpl.putBin preserve zero length, despite stale public Sender Javadoc
saying empty becomes null. That existing documentation discrepancy is recorded,
not used as a reason to copy incorrect behaviour or change ordinary Sender.

### Validation and ownership plan

SOL client owner implements the binding, shared literal-byte corpus and public
component tests. SOL server owner independently checks the server contract and
adds real-server discovery/conversion/wire/SQL/recovery tests. A third SOL agent
reviews assumptions and then the implementation adversarially. Root owns the
design/journal, scope decisions, source cross-checks and final acceptance.

Tests must cover every input form, all byte values, UTF-8 edge cases and lone
surrogates, null versus empty, bitmap boundaries, immediate copying of mutable
or subsequently freed input, ignored invalid duplicates, typed local errors,
rollback retaining completed rows and removing failed-row-only definitions,
reset/clear, exact pinned schema identity and TYPE_BINARY, compression and SF
recovery. Shared vectors carry literal expected bytes; legacy comparisons must
check actual stored bytes. No reflection, private-state inspection or new
test-only production hooks. Oversized native-length tests must reject before
accessing deliberately unusable memory, never allocate multi-gigabyte payloads.

Maven execution and client installation are serialized. Client focused tests
precede independent review and the extended canonical gate; only the approved
standalone artifact is installed for server focused/canonical/seeded gates.
No server production, shared buffer/writer or ordinary Sender changes are
planned. Previous dirty work and unrelated server parquet files are preserved.

The performance skill was found under its installed package path after the
catalog path failed. Its measured bottleneck/optimization workflow does not
provide a baseline for this newly supported functional path. Root uses source
copy/allocation inspection and correctness validation here, makes no measured
performance claim, and does not optimize existing shared hot paths. Source
inspection confirms that the existing buffer directly encodes UTF-8 and copies
native input during the call; tests must verify the ownership consequences.

### Starting source identity

Base commits remain those recorded in D028. The starting binding SHA-256 is
`a54f9ad6a68ed1d180c5d7a63599be6dcb69750ca7903ee9ef5411ed58501215`.
Protected tracked production diffs remain:

- Client: `ab7c4ef0ee366553ec27bbdbe7cf3a517eff44c2fdba15070320fb8a786378af`.
- Server: `cef922f159d0b62fc72cdf330b3fa70f41024568e9de26c17cbaf808c4e422ce`.

QwpTableBuffer, QwpColumnWriter, ordinary Sender and the two server appenders
match their D027/D028 hashes. The latest accepted tests remain iteration 2.13
until the final results below are recorded.

### Adversarial findings and narrow scope adjustment

The independent corpus review caught a copied-data error: the draft
binary_all_256 vector had only 242 bytes, and both expected fields repeated the
mistake. The corrected vector is exactly 00..ff, with an independent length and
byte-by-byte progression assertion. Draft exact-target enforcement was also
corrected to reuse the binding's existing expected-target helper; BINARY input
must not silently enter VARCHAR merely because the storage layout is shared.

The requested partial-encoding test then found a real shared-buffer bug.
`focused-second.log` ran 7 tests with one failure: expected byte 1, observed
120 (the injected UTF-8 prefix `x`). On a pre-existing column, addString had
already appended bytes when the CharSequence threw, but had not incremented
size or published the terminal offset. cancelCurrentRow skipped the column
because its size still equaled the completed row count. An explicit truncateTo
at the same size would also be a no-op. Failed-row-only columns were removed
correctly; that case alone would have hidden the existing-column failure.

Root and both SOL reviewers confirmed that a binding-only repair would need a
new internal checkpoint API or temporary conversion buffer. Root instead
authorizes one narrow shared-buffer correction required to complete this slice:
make addString and both addBinary variable-width appends exception-atomic by
restoring their data and offset positions on RuntimeException/Error. Publish
value and row counts only after success. The guarded region includes encoding/
copying, checked-offset validation and offset publication/growth; observable
tests inject STRING prefix failures, including after data-buffer growth. Binary
copy/allocation and offset-buffer failures are source-reviewed, not fault-injected.
No new per-column state, callbacks, rollback scan, buffer owner or shared-memory
primitive changes are authorized. Successful encoding and null conventions
remain unchanged; exceptional ordinary Sender cleanup is intentionally repaired.

This replaces the original no-buffer-edit scope after evidence showed it would
leave the new feature unsafe. Keep the failing test, add VARCHAR/BINARY prefix
failures with exact exception identity, and add ordinary Sender real-server A/C
regression coverage. Do not manufacture OOM/native faults to claim recovery;
arbitrary invalid pointers and fatal process faults remain caller/environment
limits. Other column families' exceptional mutation paths are separate follow-up
work, not silently claimed fixed by this variable-width repair.

### Final validation and acceptance

All final gates passed with zero failures, errors or skips:

- Client focused aggregate, including the shared buffer suite: 109 unit tests
  plus 2 packaging checks.
- Client canonical: 394 unit tests plus 2 packaging checks.
- Server focused: 4 real-server tests.
- Server canonical: 325 tests, including the nine pre-existing binary Sender
  regressions in addition to the previous schema/compatibility/conversion gates.
- Server repeats: 55 tests each at seeds 1/2, 42/43 and 1234567/7654321.

The 18 shared cases (4 BINARY, 14 STRING) are byte-identical in both repositories.
Client tests inspect exact target wire bytes, 130-row bitmap/offset boundaries,
all three binary overloads, invalid inputs and duplicates, lifecycle guards,
completed-row retention and failed-row-only definition removal. Mutable inputs
are changed after their setters return, and native input is freed before
encoding; the encoded bytes still match the original values. RuntimeException
and Error prefix failures preserve the original exception object and completed
data, including when UTF-8 capacity grows before the failure.

Real-server tests obtain live DESCRIBE identities, assert TYPE_BINARY before
sending and compare every stored byte. Separate legacy senders for BINARY and
VARCHAR input preserve legacy inferred-layout rules while checking server
conversion parity. NULL, omission and present empty input remain distinct.
The ordinary Sender VARCHAR and schema-binding BINARY A/failing-B/C regressions
both store exactly A/C after the shared repair. The SF case checks target bytes
before persistence and exact recovered bytes after source memory has been freed.
It does not replace existing generic replay byte-identity evidence. BINARY has
no separate compression encoding; the test checks its unchanged payload in
Gorilla-enabled messages, not a binary compression algorithm.

Root also ran an independent JDK UTF-8/literal-byte oracle over the shared corpus:
all 18 vectors passed, including an independently checked complete 00..ff
sequence. The diagnostic uses no client or server converter. It is recorded in
`core/target/schema-binary-2.14/CorpusOracle.java`, SHA-256
`7e769a145326b333c4936875680fd5e27b0f6a443bfa09538e640aa163be740c`.

Client commands/logs: `core/target/schema-binary-2.14/run-manifest.md`, especially
`logs/focused-final-with-buffer.log`, `logs/canonical-final.log` and
`logs/install-final.log`. Server commands/logs: sibling
`questdb/core/target/schema-binary.2.14-server/run-manifest.md`, with
`logs/focused.log`, `logs/canonical.log` and all three `logs/repeat-*.log`.
Earlier fixture/compiler failures and the decisive pre-repair behavioural
failure remain separately recorded; they are not final acceptance results.

Final source and artifact SHA-256:

- Binding: `1b21f16f9312d06f6a3ba741426fd53f16477c0280a2bd01fd196afa1615f94d`.
- Shared buffer: `e6a9451e82058d29094b89472af387c6cb23e986a70a9e24fb32c35d79da7731`.
- Client test: `b5350a7d261611c8b73a343718f20e2ba6d0474ed6d2b21c591da41b46a2dd35`.
- Server test: `ca056571bd8225c217e2975386d18e8eb19e8773d2b6eac67088659ef0108427`.
- Shared corpus: `aba29365c5e148a5bd0eae5f95e8e68dbd441c9715741d1ab4a8d93f5c89813a`.
- Workspace and installed client JAR:
  `b9f658d20a0e00a56a6cf287a6d407a849e389d0aa4390890039605bd5fc7394`.

Root verified final log summaries, the standalone installed artifact on the
server test classpath, explicit final seeds and project-local JVM temp paths.
All runs were serialized; the client was not rebuilt/reinstalled during server
gates, and no local-client profile was used. Runtime: Corretto 25.0.4 and Maven
3.9.11. SOL source review accepted the final binding/buffer/tests/corpus hashes;
the documentation review's fault-coverage wording correction is incorporated.

The only new executable production changes in this iteration are BINARY target
binding and the authorized string/binary append-position repair. Ordinary Sender
source, column writer, off-heap primitive and server production remain unchanged;
ordinary Sender benefits only from the shared exceptional-cleanup repair.
No array/symbol cleanup rewrite, new buffer owner or measured performance claim
is included. Native copy/allocation fault recovery remains source-reviewed,
not dynamically fault-injected. The five server compatibility boundaries from
D028 remain unresolved and are not activated by this slice.

Root accepts iteration 2.14. Design revision 29 updates the contract, status and
conversion inventory. Remaining conversion families, inference, row-boundary
adoption and full Sender activation are still required. No unrelated files were
removed and no commits or pushes were made.

## D030 — Integrate public Sender early; keep completeness as a release gate

Date: 2026-09-11. Status: accepted for the bounded integration scope; unreleased.

The user requested integration E2Es, asked root to validate the assumptions,
then authorized implementation. Root and three SOL agents traced current
Sender construction, setters, rollback, batching, schema lookup, negotiation
and the real-server test infrastructure. This changes D022's sequencing, not
the product contract: all conversion families need not precede the first public
Sender E2E. An incomplete automatically activated client remains unreleased.
Unsupported conversions and missing-table/column inference may report explicit
development limitations; no raw-value fallback, feature switch or alternate
Sender is authorized. Existing legacy-mode behavior remains in scope.

The first driving test uses normal `Sender.fromConfig`, a pre-created UUID
table, valid A, invalid B after another field has been appended, valid C, and
explicit flush. B must fail locally and leave no partial row; SQL must contain
exactly A/C. A complementary public-Sender socket test checks the actual
DESCRIBE, FLAG_SCHEMA, UUID wire representation and pinned table identity.
The current real-server fixture does not capture Sender wire bytes, and SQL
alone cannot distinguish client conversion from the existing server cast.
An absent-confirmation scripted peer proves protocol fallback, not execution
against an older QuestDB release. No actual old-server run is claimed.

Validated implementation constraints and simplifications:

- Always request the extension through existing connection factories. The
  existing engine/factory requirement latches on first confirmed support,
  before any extended bytes; do not add a user option or delay that boundary
  until publication. A failed handshake is not legacy confirmation.
- Resolve before the first effective operation of a row, not at `table()` or
  flush. Empty buffer allocation is harmless. Use the existing I/O loop and
  readiness/lifetime signals; ASYNC must not guess legacy while offline.
- Keep producer-owned row pinning separate from the latest lookup cache.
  Replacement connections clear the latest cache, not buffered snapshots.
  A cached conversion rejection refreshes once; compare the relevant target
  and table incarnation, not metadata-version inequality alone, when deciding
  INVALID_VALUE versus SCHEMA_CHANGED. Cancel the whole partial row.
- Reuse owner-backed global-dictionary encoding with `beginSchemaMessage` and
  `addSchemaTable`; the component's single-table local-dictionary encoder is
  not a replacement for Sender batching. `copySplitMessage` already copies
  the schema flag and table body; validate it rather than introducing another
  split encoder. Dictionary-only and commit-only frames remain flag-clear.
- `flushPendingRows` publishes data. Schema adoption must retain completed
  old-snapshot rows without using a public flush as layout cleanup. Preserve
  row/byte accounting, symbol ownership, per-table ordering, reset/close,
  split preflight, deferred commits and store-and-forward behavior. Do not
  create a general queue framework or transform bytes at send time.
- `atNow()` is a useful first test condition, not a new restricted API
  contract. Both row-completion paths must avoid raw writes into schema-bound
  buffers; reuse the established timestamp conversion rules when integrating
  designated timestamps. No new converter family is justified merely to
  broaden this milestone.

Root owns design/journal and acceptance; SOL owns client implementation and
public socket tests, another SOL owns server E2Es, and an independent SOL
reviews lifecycle, compatibility and complexity. Builds are serialized per
repository, with an explicit installed-client artifact freeze before server
GREEN validation; the initial real-server RED uses the accepted 2.14 artifact.
Use project-local JVM temp directories because shared `/tmp` has only about
311 MiB free. No destructive cleanup or commit is authorized by this increment.

The performance skill is used for the performance-sensitive Sender path.
This is authorized functional integration, not a production optimization or
hardware-bottleneck claim. Its pre-optimization measurement gate is not claimed
to have passed; do not bundle speculative optimization into this work. Review
added allocation, locking and repeated scans separately from correctness, and
do not present source inspection as a throughput or latency measurement.

Validation evidence, final scope and review outcomes will be appended here
after the actual test runs. A first passing E2E alone is not iteration acceptance.

### First failing public-path tests

The client socket test failed before production changes: malformed UUID text
was accepted by ordinary Sender and no DESCRIBE was sent. This is the intended
missing integration, not a converter failure. Evidence:
`core/target/schema-sender-2.15/logs/focused-red.log` (one test, one failure).

The real-server test against the frozen 2.14 client also failed on the public
path: after a string UUID input bound legacy VARCHAR, a duplicate typed UUID
setter reported a local wire-type mismatch. Schema mode must instead keep the
target UUID binding and ignore the duplicate before conversion. Evidence:
sibling `questdb/core/target/schema-sender.2.15-server/logs/`
`red-installed-2.14-schema.log`. Superseded fixture compilation/configuration
errors are recorded by the server owner and are not counted as behavioral RED.

Root rejected a review suggestion to keep a successfully negotiated legacy
producer permanently in legacy mode after a supporting-server reconnect. That
would silently bypass the agreed one-way upgrade. Pending legacy blocks must
keep their old framing, while the next effective row adopts schema mode.
Uniform schema generations can share a schema frame; only mixed legacy/schema
blocks need separate frames. Any rare mixed-family path must size-check all
groups before its first publish and preserve deferred commit and symbol state.

### First real-server GREEN, before full integration acceptance

The first public socket test passed after negotiation/string routing was wired.
A deliberately intermediate standalone client artifact was then installed and
frozen for the real-server test, SHA-256
`8180dc92d99122013ce3cfcc2d5a4c84781e25b1d3aa58ad0871895a8830a0ed`.
`QwpSchemaSenderE2ETest` passed 1/1 against that artifact. It uses normal
`Sender.fromConfig`, pre-created schema, string/UUID inputs into the same UUID
target, target-first duplicate handling, local cancellation of invalid B,
same-table C without another `table()`, `atNow`, explicit flush/ACK and exact
A/C SQL. No manual binding/encoding occurs in the test's write path.

Server evidence: `core/target/schema-sender.2.15-server/logs/`
`intermediate-first-green-final.log`. The prior run reached its SQL assertion
but incorrectly nested a query memory check inside a live-server context;
the final query uses the fixture's normal outer memory-leak check. The installed
artifact hash was unchanged through both runs. Client work continued without
reinstallation during that gate; the server owner released the artifact only
after stopping Maven. This checkpoint does not validate unfinished setter,
refresh, generation, split or recovery paths and is not iteration acceptance.

### Rejection recovery clarification

An implemented input family refreshes once on cached INVALID_VALUE or
UNSUPPORTED_FEATURE, including a column that was absent from the cached schema
but has since been added. Compare table incarnation and the relevant column's
existence/type/parameters. An unchanged target preserves the original reason;
an unrelated version change does not become SCHEMA_CHANGED. An intrinsically
unimplemented input family fails directly. Authorization/availability failures
are not conversion retries. Freshness is per setter: a lookup by an earlier
setter in the row does not make a later setter's rejection freshly validated.
This sharpens the existing error contract without adding setter replay or
retaining original values. A sender-reused lookup-result holder may report
cache/fresh origin without allocating a result per row or using an exception
for an ordinary cache miss.

### Candidate 2 integration validation

The second installed candidate, SHA-256
`78cb193a394dbff4136c45118bc24113fd0698180f4598e943e4f8cb946064e7`,
passed all eight real-server `QwpSchemaSenderE2ETest` cases. These add exact
micro/nano designated timestamps, primitive and Instant inputs, failed timestamp
rollback, cold setter-free `atNow`, relevant versus unrelated DDL with buffered
A, repeated cached MISSING, denied lookup recovery, observable auto-flush,
reset discard and close-only publication. The test's typed UUID uses different
low/high limbs, so reversed argument routing cannot pass. Auto-flush is observed
through ACK 0 and SQL before any explicit flush; no explicit flush can mask it.
Cold `atNow` uses a separate fresh Sender. Timestamp tests also assert total
cardinality, not only filtered values. Reset currently covers the active
generation; do not claim its retired-generation case from this test alone.

Server log: `core/target/schema-sender.2.15-server/logs/candidate-2-eight.log`.
The installed JAR remained byte-identical during the run. Root independently
checked the log and both workspace/installed JAR hashes. Client focused
consolidation passed 74 tests (`logs/focused-current-3.log`); comprehensive
socket tests and canonical regressions are still pending. Exact candidate
source hashes and commands are in both repositories' 2.15 run manifests.
Root verified Maven 3.9.11, Corretto 25.0.4 and project-local JVM temp storage
against the running Maven and Surefire XML, not only the prior manifest.

The rare mixed legacy/schema batch uses one frame per table block and a
two-pass size-check/publish loop, reusing the existing encoder. Uniform schema
blocks retain combined/split batching. There is no new queue framework or
send-time transformation. The old split-path at-least-once rule remains:
partial publication failures retain all source rows and retries may duplicate
the published prefix. A review request to reset only published groups was
rejected because it would introduce new bookkeeping and a stronger guarantee.
Size rejection must still publish nothing; deferred commit debt must remain
visible to recovery and close.

### Candidate 2 broad regression and invariant review

The broader prior-325 plus new-eight server selection ran 333 tests: 10 failures
and 48 errors, not a passing regression suite. The new eight tests passed in
that run and in three seeded repeats. The separate protocol/discovery/identity/
feedback baseline passed 214 tests. Existing public-Sender tests now negotiate
schema mode: missing inference, unimplemented conversions and deliberately
different schema-mode semantics expose remaining release work. Some old
conversion fixtures also write through the existing `@TestOnly` buffer seam;
those are not evidence about the supported public row API. Preserve the failing
evidence and classify it; do not silently rewrite legacy expectations to claim
a clean release. The exact failures are recorded in the server run directory's
`logs/broad-failure-lines.txt`.

Root rejected a mock-only proposed fix that changed a target type while keeping
the same `(tableId, metadataVersion)`. The reviewer could identify no coherent
server path producing that response and retracted the finding. The pair is an
immutable encoding identity, relied on by both buffered rows and replay. Adding
target-change adoption for an unchanged identity would conceal a broken
protocol assumption. The narrow accommodation and impossible mock regression
were removed; real version-changing DDL remains covered.

Independent production review found no confirmed lifecycle or delivery defect
in candidate 2. Root continues to review observable test sensitivity: exact
wire types and asymmetric UUID limbs, both pinned identities across generations,
no manual cancellation masking automatic rollback, and observable reconnect
confirmation instead of timing assumptions. These checks precede final
iteration acceptance; they do not remove the release blockers above.

The independent follow-up classified all 58 failures: 38 missing conversions
(22 decimal-related), four missing-inference/lookup-versus-argument-validation
cases, seven documented schema-mode semantic differences, four obsolete
direct-buffer `@TestOnly` fixtures, and five old server-error/message assertions
now receiving local typed rejections. The named LONG/designated timestamp
failures belong to the direct-buffer fixtures, not the public timestamp setters.
This diagnostic selection is not an exhaustive count of missing input families.

Root also rejected waiting through every observed reconnect gap for a sender
that had already confirmed legacy mode. That would unnecessarily stop legacy
store-and-forward buffering during an outage. Only a never-confirmed sender
must wait to select its initial mode. Once support is confirmed it stays
mandatory; before that, confirmed legacy rows remain valid even offline. The
upgrade test must observe the installed supporting connection before asserting
the next row's mode, rather than use a server-start timestamp to change the
production contract.

The encoder now rejects table counts outside the wire header's unsigned-16-bit
range before writing a header. Sender also rejects more than 65,535 pending
nonempty blocks before publication, rather than truncating a combined header.
This is conservative: separately grouped frames could theoretically carry a
larger logical batch. Supporting that would require a further batching change;
it is not part of this increment and is unrelated to the million-entry schema
cache ceiling. Boundary tests cover zero, 65,535, negative and overflow counts.

### Final client gate and artifact freeze

The final focused selection passed 142 tests plus two packaging checks. The
canonical selection passed 410 tests plus two packaging checks, all with zero
failures/errors/skips. This extends the prior 394-test client selection with
15 public-Sender socket tests and the encoder header-range test; it is not a
claim that every repository test was run. Logs: client run directory
`logs/focused-final.log` and `logs/canonical-final.log`.

The 15 socket cases include exact target UUID bytes, asymmetric limbs, automatic
partial-row rollback, cached missing tables, no-op and duplicate behavior,
legacy VARCHAR bytes with no describe, both pinned identities across an
unrelated schema update, exact cap-split payloads, retired-generation reset,
startup waiting, actual negotiation deadline/retry and interrupt preservation.
The mixed upgrade sequence completes legacy A, starts legacy B while offline,
observes the installed supporting connection via server PING/client PONG,
finishes B unchanged, then buffers schema C. Flush emits exact legacy A/B
followed by schema C with the correct deferred/final commit flags. No sleep,
reflection or production testing bypass establishes the transition.

Several old client fixtures used an unconnected Sender with injected connection
state. They now connect to a real non-confirming socket peer through public
construction, retaining the original cap/autoflush/input assertions. Existing
unrelated testing seams were not removed and no new ones were added. The mock
server's additions are ordinary wire PING/PONG and negotiated schema replies.

The final standalone install succeeded with main JAR SHA-256
`b269286af96ba34d8b051686b70c515f880b04572e8dc537d2558960617b2531`;
the tests JAR is
`48e9665b4bc5b05f4134899c6e301d6e834a067152dfac88b2d4c4f527e786f8`.
Root independently verified workspace/installed main JAR equality, final
source/test hashes, the 410+2 result and Surefire's Corretto 25.0.4/project-local
temp settings. Exact commands and hashes are in
`core/target/schema-sender-2.15/run-manifest.md`. The artifact is frozen for final
server validation; local installation is not publication or release.

Separate source reviews found no confirmed production blocker. Root reviewed
test sensitivity independently of the test author. Candidate-2 executable
Sender behavior was retained; the later functional production delta is the
encoder count guard. First-use gating, generation replacement and mixed-family
batching reuse the existing I/O loop, table buffers and encoder. The performance
skill guided allocation/ownership scrutiny and avoidance of speculative
optimization, not measured throughput claims. Lookup/cache overhead has not
been benchmarked. The lookup deadline remains fixed at 1000 ms; the proposed
configuration key is not implemented in this slice.

### Final server gate, acceptance and remaining work

Against the frozen final standalone JAR, the eight public-Sender E2Es plus
214 protocol/discovery/identity/feedback/transport tests passed 222/222 with no
failures/errors/skips. Each of three public-Sender repeats also passed 8/8,
with seeds `1/2`, `42/43` and `1234567/7654321`. These are repeated executions
of the same eight tests, not 24 additional scenarios. Server evidence is in
`../questdb/core/target/schema-sender.2.15-server/run-manifest.md`,
`logs/final-222.log` and `logs/final-repeat-*.log` within that run directory.

The server E2E source hash is
`6e3c29d854e7986170224a9ef8e4cb80ea3e440d3e847ed5d2c8712775501a64`.
Before/after client artifact hashes match. Root independently checked the
222-test result, the final eight-test XML, its standalone installed-client
classpath and absolute project-local temp path. No `-Plocal-client` substitution
was used. Client HEAD remains `981bdb02a471f3b290c89b8e78cbc422610e329e`;
server HEAD remains `12a33d651e51e2682e7a448c8db5168fc72dfad3`, both with
preserved working-tree changes. Final diff whitespace checks passed in both
repositories. No commits or pushes were made.

Root accepts iteration 2.15 for its stated scope: ordinary Sender automatically
uses the existing schema machinery, fails unsupported writes explicitly, keeps
row/block ownership and delivery semantics, and has working public-path E2E
and exact-wire coverage. Separate SOL reviews found no unresolved blocker in
that scope. This acceptance does not turn the broad 333-test diagnostic into
a passing regression suite or authorize release of the incomplete client.

The remaining release work is still explicit:

- Implement the audited conversion families and missing-table/column inference.
  The broad failures are useful evidence of these gaps, not tests to disable.
- Separate old-wire compatibility expectations from approved schema-mode
  differences in the final regression matrix. Actual old-server distribution
  compatibility was not exercised by this increment's scripted peer.
- Extend the ordinary Sender E2E through file-backed SF/process restart; the
  existing component/engine recovery tests were retained, but the new public
  Sender tests do not claim that additional scenario.
- Finish proposed configuration wiring and validate performance before making
  latency or throughput claims. No new conversion framework, user opt-out or
  send-time transformation is implied by any of this work.

Reflection: the integration exposed useful contract and fixture errors sooner
than another converter-only increment would have. The most valuable
simplifications were keeping one connection owner, one buffer owner and the
existing encoder/split path, plus refusing mock-driven changes to immutable
schema identities or legacy outage behavior. Continue with bounded conversion
families exercised through this now-working public path; do not return to a
separate final integration phase.

## D031 — Public Sender creates and recovers its own backlog (iteration 2.16)

Date: 2026-09-11. Status: accepted for the bounded public-Sender process-restart
test scope; the complete feature remains unreleased.

After accepting 2.15, the user asked for next-step reasoning and approved the
recommendation to validate public-Sender restart before another conversion
family. Existing recovery tests construct frames and append them directly to
`CursorSendEngine`, then use Sender to recover. That proves the replay component,
not the complete owner-level path from public setters through file-backed
publication and a producer-process crash. Prioritization follows that risk,
not the number of broad-suite failures attributed to DECIMAL.

The bounded implementation is a real-server E2E, using the unchanged accepted
2.15 standalone client JAR:
`b269286af96ba34d8b051686b70c515f880b04572e8dc537d2558960617b2531`.
No converter, protocol, recovery API or production hook is planned. A confirmed
production defect must be reviewed before a narrowly necessary fix is made.

Acceptance sequence:

1. A child JVM creates normal `Sender.fromConfig` with `sf_dir` and `sender_id`.
   Public writes produce valid UUID/SYMBOL A, partially invalid B, then valid C.
   B fails locally and is not manually cancelled by the test.
2. A test-only network gate forwards WebSocket upgrade and schema discovery,
   but captures and withholds table-bearing QWP data. Wait for both the child's
   explicit post-`flushAndGetSequence` publication marker and the captured data.
   Verify SQL is empty before stopping the process.
3. Force-stop the child without calling Sender.close, require a bounded process
   exit, and close/join its old sockets and relay threads. Start a fresh Sender
   with the same public directory/identity configuration.
4. Drain to the real server, compare captured unmasked QWP data before/after
   restart, and assert exact A/C values, no B, symbol integrity and ACK progress.
   Reopen after acknowledged recovery to check that no rows replay again.
5. Extend the same fixture to A and C encoded under different schema identities,
   using the already-tested UUID-to-VARCHAR DDL/whole-row retry sequence.

Source-backed limits and simplifications:

- `flushAndGetSequence` returns after producer-owned `appendBlocking` publishes
  the mmap-backed frame; it does not wait for a server ACK or imply fsync.
  The current MEMORY durability policy survives a JVM crash through the OS page
  cache. This is a process-crash test, not a host crash or power-loss test.
  Reserved FLUSH/APPEND durability settings remain unsupported and unchanged.
- Exact A/C cardinality is valid because the gate proves zero initial delivery.
  This does not strengthen the general at-least-once retry contract. No dedup
  table should hide an accidental first delivery or duplicate replay here.
- Compare decoded QWP payloads, not WebSocket masks. Permit extra dictionary
  catch-up/control frames during recovery. Test the original table-bearing bytes
  and pinned identities; never reconstruct persisted files or inspect private
  ACK/segment bookkeeping.
- UUID plus SYMBOL exercises symbol replay integrity. It is not proof that a
  dictionary side file alone can reconstruct references after all introducing
  frames have been trimmed; that stronger scenario is outside the initial test.
- Reuse the real server fixture and public protocol constants. A small gate is
  justified because the existing transparent ACK tee cannot selectively pass
  discovery and withhold data. Do not build a general proxy/process framework.
  All barriers, process joins and socket/thread cleanup must be bounded.

Root owns architecture, source validation, design/journal and acceptance. SOL
owns the new server test and serialized server Maven runs; a separate SOL
performs adversarial source/test review. The client artifact stays frozen.
Root verified Maven 3.9.11, Corretto 25.0.4 and artifact hashes. Shared `/tmp`
still has about 311 MiB free: parent and child JVMs must use explicit project-local
temp directories under the 2.16 run directory. Preserve both dirty worktrees;
no commits, pushes or broad cleanup are authorized.

### First public process-restart checkpoint

The first one-generation test passed 1/1 against the frozen 2.15 artifact:
server run log `logs/focused-fourth.log`. A child JVM used ordinary Sender to
create file-backed UUID/SYMBOL A/C rows after rejecting partial B. The parent
observed public local publication and a withheld schema data frame, verified
SQL count zero, forcibly stopped the child with nonzero exit, and reopened the
same configuration through a new Sender. ACK progress and exact A/C SQL passed.
At this early checkpoint, exact replay-byte comparison, post-ACK reopen and
the two-generation extension remained pending; it was not full 2.16 acceptance.

The first three runs exposed fixture errors, not a production recovery defect:
an incorrect hard-coded control flag withheld DESCRIBE, the replacement schema
flag referenced the wrong constants class, and `auto_flush_rows=0` is invalid
configuration syntax. Logs `focused-initial.log`, `focused-second.log` and
`focused-third.log` retain these failures. The final fixture uses public
protocol constants and the already-working 2.15 auto-flush settings.

Independent review also required explicit child temp storage, one tracked
stdout reader instead of competing/dropping readers, controlled complete masked
WebSocket messages, bounded process exit and relay/reply-thread cleanup. These
are real subprocess/network observations, not injected Sender state. No
production change was required for the first passing checkpoint.

### Final implementation, review and acceptance

`QwpSchemaSenderRecoveryE2ETest` now contains two real-server cases. The first
recovers UUID/SYMBOL A/C after local `INVALID_VALUE` rejects partial B, then
reopens the acknowledged slot and verifies no rows replay again. The second
holds A while the server drops UUID `id` and adds VARCHAR `id`; B encounters
`SCHEMA_CHANGED` and is cancelled, then C uses the new schema. The public server
wire cursor verifies two one-row blocks with the same table identity, different
metadata versions and native UUID/VARCHAR types. Both cases require local
publication, captured table-bearing data, SQL count zero before force-stop,
nonzero child exit, acknowledged recovery, identical unmasked QWP payloads and
exact final A/C values. No reflection, private persistence inspection or
manually appended replay frames were introduced.

The independent SOL review and root review found fixture defects worth fixing
before acceptance:

- Joining the relay while holding its socket-registration monitor could stall
  cleanup. Shutdown marks/closes sockets under the monitor, then joins outside.
- An extra or malformed frame arriving after the initial capture assertion could
  set a relay failure that was never checked. Health is now checked after both
  relay threads terminate, so a rejected extra frame cannot hide a false pass.
- Exceptional cleanup must explicitly verify the forced child's termination;
  interrupted relay cleanup must fail, not silently skip its checks.

All were corrected without production changes. Final source SHA-256:
`9183f38aebccfac35bd2658aee2ad861d7dcec19adb8bff571ca3ea04356488d`.
Independent adversarial review accepted that source, and root independently
checked the assertions, final logs, artifact hash and whitespace checks.

Final evidence is under server
`core/target/schema-sender-recovery.2.16-server/`; `run-manifest.md` records
commands, runtime, artifact provenance and superseded development runs:

- `logs/final-230-rerun.log`: 230 tests, zero failures/errors/skips. This combines
  the accepted 2.15 server selection of 222 tests, the two new public recovery
  cases and six existing timestamp/text/string-numeric/floating/boolean/binary
  component replay methods.
- `logs/repeat-1-2-final.log`, `logs/repeat-42-43-final.log` and
  `logs/repeat-1234567-7654321-final.log`: both new cases passed each seed pair.
  Earlier green logs predate the final cleanup corrections and are checkpoints,
  not the final acceptance evidence.
- The workspace and installed standalone client JAR still have SHA-256
  `b269286af96ba34d8b051686b70c515f880b04572e8dc537d2558960617b2531`.
  The earlier 410 client tests and two packaging checks were not rerun in this
  test-only increment; they remain 2.15 evidence, not new 2.16 results.
- Maven 3.9.11 / Corretto 25.0.4; parent and child JVMs use project-local temp
  storage. Server Maven runs were serialized. Both worktrees pass
  `git diff --check`; no producer or Maven processes survived the final runs.
  No commits or pushes were made.

Reflection: the existing production ownership and replay paths survived the
real process boundary without another recovery layer. The added complexity is
confined to a bounded test-local child process and data gate. Keep this public
path as the integration baseline for future conversion families instead of
building another parallel Sender or send-time conversion path.

Acceptance does not erase D030's 58 classified broad-suite failures or prove
actual-old-server distribution compatibility. Conversion/inference coverage,
configuration wiring and measured performance remain release work. This test
also deliberately leaves power loss and dictionary-side-file-only recovery
after introducing frames are trimmed outside its claim.

## D032 — Resolve failures without losing the compatibility oracle (iteration 2.17)

Date: 2026-09-11. Status: first bounded legacy-comparison repair accepted;
the remaining 56 broad-suite failures are unresolved.

The user authorized exploring and resolving the failures after 2.16. Root
assigned SOL a frozen-client broad rerun, another SOL independent prioritization
and compatibility review, and a third SOL the next functional inference design.
Root owns the source cross-checks, scope, journal and acceptance. The installed
2.15 standalone client remains frozen at SHA-256
`b269286af96ba34d8b051686b70c515f880b04572e8dc537d2558960617b2531`.

### Reproduction and corrected classification

The original 333-test selection ran again. The first result was 12 failures
and 48 errors, but the apparent increase from 58 to 60 was not two new schema
defects. The invalid floating and string numeric corpus tests already send
unflagged legacy frames; their new INTERNAL_ERROR responses contain
`No space left [size=1048576]`. Root rejected the initial classification of
those two as schema/legacy semantic differences after reading their actual
write path and the server error immediately before each failed assertion.

Evidence: server `core/target/schema-failures.2.17-server/logs/baseline.log`
and `failure-summary.txt`. The working classification is:

| Group | Baseline count | Resolution rule |
| --- | ---: | --- |
| Missing conversions | 38 | Implement bounded families with source, wire and SQL parity checks |
| Missing inference / validation order | 4 | Restore confirmed-missing inference, preserving lookup failures and server policy |
| Approved schema/legacy differences | 7 | Keep distinct, explicit tests of both contracts |
| Old direct-buffer / wrong-frame fixtures | 4 | Use real legacy wire tests; do not pretend they are public Sender writes |
| Old error shape/message expectations | 5 | Assert local typed failure and row outcome, retaining server rejection coverage |
| Disk-space failures in raw invalid corpora | 2 | Correct runtime storage; do not loosen expected rejection status |

The first implementation group repairs the Boolean, timestamp, floating-numeric
and string-numeric legacy comparisons, including false comparisons that were
passing. Two SOL authors own disjoint pairs of server test files; the third
reviews all four and root independently checks load-bearing findings. Keep
the schema binding, rollback and public Sender recovery branches unchanged.
Use existing non-requesting WebSocket clients, source-native table buffers and
the existing unflagged encoder. No alternate Sender, schema-disable switch,
production hook, reflection or general test framework is authorized by this
group. Server Maven runs remain serialized.

### Runtime and review corrections

Maven `-Djava.io.tmpdir` alone produced a reassuring Surefire XML property but
did not establish the JVM's actual temporary-file root. Root inspected the
running JDK's `File.TempDirectory` and `TempFileHelper` bytecode: both use
`StaticProperty.javaIoTmpDir()`, which retains the startup value. Set the
absolute project-local directory in the fork's actual `argLine`, not only a
later system property. Shared `/tmp` has only about 311 MiB available; the
project filesystem has about 26 GiB. No unrelated temporary files are deleted.
On the corrected focused run, root verified both the actual Surefire Java
launch command and its open WAL/metadata file descriptors under the explicit
project-local `tmp/focused4-argline/junit.../dbRoot` directory, not merely XML.

This also corrects D031's overconfident parent-temp provenance claim: its
Surefire XML property did not prove the parent test JVM's actual temporary-file
root. Its child used an explicit JVM argument. The passing 2.16 test outcomes
remain evidence; the stronger storage-location claim does not.

Review findings must survive source checking. Root rejected a proposed broad
timestamp-corpus repair after verifying that `timestamp-units.tsv` contains
only 26 MICROS and 22 NANOS cases (all four invalid cases are MICROS). The
reviewer had conflated it with the separate broader `timestamp-inputs.tsv`
corpus and retracted the finding. No seven-unit normalization framework is
needed to repair that two-unit legacy-wire test. A separate renamed raw-wire
characterization must not claim to execute public Sender's `Instant` or
larger-unit normalization merely because it injects their known output values.

### Next functional group: confirmed-missing inference

After the comparison tests are reliable, prioritize the common auto-create path
over choosing DECIMAL merely because it accounts for 22 failures. The reviewed
minimal mechanism is inference within the existing non-owning binding and
caller-owned table buffer, not synthetic server schemas or a second registry.

- A MISSING response is authenticated absence, not permission to create a
  table: `QwpSchemaControl.snapshot` returns MISSING before table authorization.
  Server write-time authorization and auto-create policy remain authoritative.
  DENIED, UNAVAILABLE and TOO_LARGE must never become inferred schemas; keep
  their existing distinct failure/retry semantics.
- Preserve the coordinator's existing result filter: only KNOWN/MISSING reach
  the producer; other responses already throw typed errors before identity
  reuse. Root's concern that a non-known `(-1,-1)` response might reuse a missing
  binding was disproved by tracing `completeResponse` and `resolve`. Test that
  invariant with inferred rows, rather than adding a redundant Sender branch.
- Infer established native targets only for already-supported setter families.
  Preserve existing timestamp inference: NANOS selects nano precision; other
  primitive units and `Instant` select micros. For a known target, its actual
  precision still wins. This does not introduce new conversion families.
- Buffer column definitions pin inferred types and existing rollback removes
  failed-only definitions. A narrow inferred-column lookup must suppress a
  same-row duplicate before checking its setter type; the existing generic
  lookup checks type first. Do not globally change legacy duplicate semantics.
- Missing tables keep schema framing with unknown identity `(-1,-1)`; missing
  columns on known tables retain the known identity. ACK metadata starts a new
  generation through the existing adoption path, never relabeling buffered bytes.
- Infer an unnamed designated timestamp only for a missing table. A known
  table without one must continue to reject explicit `at(...)`; `atNow()`
  retains server-assigned time. Test all these cases through public Sender.

This is a source-reviewed next-step recommendation, not implemented inference.

### First repair: controlled broad-suite result

The final compiled four-class focused run passed 25/25 with the corrected
JVM-startup temp setting (`logs/focused4-acceptance-2.log`). The identical broad
333-test selection then reported 9 failures and 47 errors, zero skips
(`logs/broad-acceptance.log`). Its 56 failing method names are exactly the first
run's set minus these four:

- Boolean schema/legacy omission comparison: repaired legacy producer path.
- Timestamp normalization characterization: repaired raw legacy producer path
  and narrowed the method name/claim to what it actually executes.
- Invalid floating corpus and invalid string-numeric corpus: their original
  rejection assertions pass after correcting temporary storage; these were
  environment failures, not repaired production conversion defects.

Thus two of the original 58 contract/fixture failures are resolved, not four.
The remaining 56 consist of 38 missing conversions, four inference/order cases,
five remaining schema/legacy expectation differences, four old direct-buffer
fixtures and five error-shape assertions. The accepted floating/string and
other Boolean comparisons were also corrected even though they had been green:
they now observe real legacy source bytes instead of comparing local schema
conversion with itself.

Stronger raw-wire assertions required respecting the public cursor contract:
reading a FLOAT through `getDouble()` quiets a signaling NaN, so inspect the
original fixed-width payload after validating its count, width and absent null
bitmap. VARCHAR's null value accessor is an empty flyweight; test the structural
null flag separately, then compare all non-null bytes, including present empty
strings. Legacy BOOLEAN omission must use its actual non-nullable source layout
to preserve false, rather than introducing a null bitmap. Existing SQL values
and rejection statuses were retained. Development logs preserve the temporary
directory, cursor API/exception declaration and assertion mistakes; only the
final compiled source is eligible for the remaining regression gates.

### Final acceptance and next boundary

All final gates used the same four test sources and the unchanged installed
client JAR. Evidence lives under server
`core/target/schema-failures.2.17-server/`; `run-manifest.md` records exact
commands, runtime and superseded runs:

- `logs/focused4-acceptance-2.log`: all 25 affected tests passed.
- `logs/broad-acceptance.log`: the unchanged 333-test selection has 9 failures,
  47 errors and zero skips. The exact method-set comparison shows only the
  four removals described above and no newly failing methods.
- `logs/final-251.log`: 251 tests passed, zero failures/errors/skips. This
  retains the previous protocol, negotiation, feedback and public Sender/crash
  recovery selection, including text/binary replay, and runs all four repaired
  conversion-comparison classes in full.
- `logs/repeat-1-2.log`, `logs/repeat-42-43.log` and
  `logs/repeat-1234567-7654321.log`: all 25 affected tests passed each seed pair.
  These are repeated executions, not 75 additional scenarios.
- `logs/final-before.sha256` and `logs/final-after.sha256` record the frozen
  artifact and test-source identities. Final test source SHA-256 values:
  Boolean `d4041c6530bb0b980e9b96657b08e12fbad95da34bf223a1eb6b1cebd9da7a53`;
  timestamp `7b897ea71a5d710a3cdaa570631c76319439f4905741469f85af8255f778c1d8`;
  floating `fe7cc286490c9de1de49a7fe3247962cb387f8525dc74c4ae6e272f15f874aed`;
  string-numeric `68d9fc992b6c07242acb306acc7df1356610ce7b008a3544df0abd6147f0e269`.

The independent SOL reviewer accepted the final wire/null/NaN checks; root
independently verified those contracts, logs and source hashes. Client tests
were not rerun or repackaged in this server-test-only increment. No production
source, conversion policy, fixture opt-out or testing hook was added. Existing
dirty work was preserved; no commit or push was made.

Reflection: passing SQL is not an independent conversion oracle when both
writers now run the same client conversion. Inspect the emitted source bytes
and keep client conversion and server conversion as separate tests. Conversely,
an increased failure count is not proof of another feature regression: inspect
the server cause and actual runtime configuration. Preserve these corrections
before moving on to the source-reviewed auto-create inference implementation.
The complete feature is still unreleased; this iteration does not claim to
resolve all failures, prove old-distribution compatibility or measure performance.

## D033 — Confirmed-missing inference using the existing buffer (iteration 2.18)

Date: 2026-09-11. Status: accepted for the bounded inference scope below;
the remaining 54 broad-suite failures are unresolved.

Continue with D032's functional priority: permit automatic table and column
creation for setter families whose schema-mode conversion is already supported.
The user reiterated that simplicity is a requirement: read enough surrounding
code to justify each change, and avoid speculative defensive checks.

Root rechecked the binding, producer row-boundary adoption, coordinator result
filter and server schema discovery/write paths. The buffer already owns column
definitions and row rollback; inference should use that state. No synthetic
server schema, inferred-column registry, protocol change, opt-out or send-time
conversion is needed. MISSING permits inference, not creation authorization.
The existing server remains responsible for authorization and auto-create policy.

SOL owns the narrow client implementation and client tests; another SOL owns
public-Sender server tests; a third independently reviews contracts and unnecessary
complexity. Root owns integration, serialized builds, source checks and acceptance.
Preserve earlier dirty work and keep additional conversion families out of scope.
Required observations include source-native inferred bytes, known/unknown schema
identity, duplicate handling, failed-row rollback, timestamp precision, ACK-driven
adoption and rejection by server policy. No release or performance claim follows
from this increment alone.

### Source-review refinement: inferred type conflicts must refresh

The generic buffer rejects a later row's different wire type with an ordinary
`LineSenderException`. Reusing that error unchanged in inference would bypass
Sender's existing schema-rejection refresh: a cached MISSING result could keep
rejecting a string setter after another writer creates a compatible VARCHAR
column. Both root and the independent reviewer traced this concrete path.

Keep the generic legacy lookup unchanged. In the binding's missing-column path,
look up the existing buffer column, ignore an already-written value in the same
row, then report a conflicting inferred type as `UNSUPPORTED_FEATURE`. This
uses the existing one-refresh/whole-row-retry path. Do not catch arbitrary buffer
exceptions, parse their messages or add a second retry loop. The name-only lookup
can expose the existing column's type and size; no result enum, callback or new
state is needed. Test unchanged MISSING, newly created server targets and failed
refresh, with completed bytes retained in every case.

Root also rejected an impossible draft test fixture: current server SQL only
enables WAL on partitioned tables, which require a designated timestamp, and
QWP rejects writes to non-WAL tables (`SqlParser` and `QwpTudCache`). A real
known table without a designated timestamp can prove local `at(...)` rejection
and recovery to another writable table. It cannot prove successful QWP storage
via `atNow()`. Keep local row-completion behavior and server writability separate;
do not change production or weaken server checks to make the fixture work.

### Initial runtime and simplicity corrections

The first focused client run compiled and executed 140 tests: five failures,
zero errors. All five new wire assertions assumed an unknown identity was
encoded as discriminator 1 followed by literal `(-1,-1)`. The existing writer
uses discriminator 0 with no pair bytes; discriminator 1 introduces a known
pair. Correct the test readers, not the encoder. Static review missed this
and its claim of proven byte preservation was withdrawn pending a green run.

Root also removed a proposed duplicate-designated-timestamp guard and its two
nullable-result branches. Public `at()` already commits the row; repeating a
designated setter within one uncommitted row was only a component fixture
scenario, and the known-schema path did not support that new behavior anyway.
Keep across-row inferred precision conflicts typed and refreshable, without
inventing an additional within-row designated timestamp contract.

After those corrections, the focused client gate passed 140/140. The first
broader gate ran 414 tests and found four obsolete expectations in the text,
boolean, floating and string-numeric binding suites: each still required an
absent column to throw. The implemented families now correctly infer those
columns. Update only these expectations, retaining their unsupported-pair,
parameterized-type and designated-column rejection checks. Packaging checks
did not run after this failed gate; do not count them as passed.

The corrected canonical client gate passed 414 tests plus two packaging checks.
The standalone client was then installed and frozen at SHA-256
`102128206d124f8963b89dd6f6764da641086697272f0bb9d93493c55903330b`.
The first server run stopped at compilation because the new test imported
`LineSenderServerException` from the wrong package. Only that test import was
corrected; the failed log is retained and no server execution was counted.

Replay evidence is more specific than a generic assurance but not a new
inferred-Sender crash test: the existing, rerun
`QwpSchemaReplayNetworkTest.testOldPeerSeesNoBytesThenSupportingPeerReplaysExactMixedFrames`
persists manually encoded legacy and UNKNOWN-schema frames through
`CursorSendEngine`, reopens through public Sender, and compares both replayed
payloads byte-for-byte. The new inference tests separately prove exact emitted
UNKNOWN bytes and retention across a producer schema-generation replacement.
Existing public-Sender process-restart tests remain in the server gate; do not
describe these separate observations as one new inferred-Sender crash scenario.

The first compiled server run passed all six new inference cases; the migrated
existing test failed only because its SQL used the reserved word `column`
unquoted in ORDER BY. Quoting that identifier preserved the expected schema
and data. The corrected focused gate passed all 14 cases with the frozen client.
Those runtime results now establish actual ACK adoption, source-native creation,
timestamp-only micro/nano/Instant/atNow behavior and write-time policy checks.

One existing server diagnostic remains unchanged: disabled creation of a missing
table returns INTERNAL_ERROR (`failed to create table update details`) and then
uses the normal poison-frame terminal escalation. Create/add-column authorization
denial returns SECURITY_ERROR. The tests assert the current distinctions and no
table/column creation; this client increment does not reclassify server failures.

### Final acceptance and remaining work

The implemented families are BOOLEAN, LONG, FLOAT, DOUBLE, text as VARCHAR,
SYMBOL, UUID, BINARY and primitive/Instant timestamps. Known server targets
continue to select conversion; absent targets use existing buffer definitions.
No additional binding registry, producer/I/O state, protocol variant, opt-out
or conversion family was introduced. Relative to the saved 2.17 source, the
binding adds 88/removes 60 lines, the buffer adds a five-line package-private
name lookup, and Sender changes only the now-stale unbound-error comment.
Known-column writes do not use the added inferred-column lookup. This is a
source-level cost observation, not a throughput or allocation benchmark.

Evidence is recorded in client `core/target/schema-inference-2.18/run-manifest.md`
and server `core/target/schema-inference.2.18-server/run-manifest.md`:

- Client `logs/focused-2.log`: 140 tests passed.
- Client `logs/canonical-2.log`: 414 tests and two packaging checks passed.
- Client `logs/install-1.log`: standalone install succeeded; main JAR SHA-256
  `102128206d124f8963b89dd6f6764da641086697272f0bb9d93493c55903330b` stayed
  unchanged throughout all final server runs.
- Server `logs/focused-acceptance.log`: 14 tests passed.
- Server `logs/canonical.log`: 261 tests passed, preserving the prior 251-test
  gate and adding six inference cases plus four existing auto-create checks.
- Server `logs/broad.log`: the unchanged 333-test selection has 10 failures,
  44 errors and zero skips. Root independently compared exact method names:
  only `QwpSenderE2ETest.testAutoCreateBinaryColumn` and `testUuid` disappeared
  from the prior 56; no failing method was added.
- Server `logs/repeat-1-2.log`, `logs/repeat-42-43.log` and
  `logs/repeat-1234567-7654321.log`: all 16 integration/recovery cases passed
  each seed pair. These are repeated executions, not 48 different scenarios.

The final broad failures classify as 38 missing conversions, five approved
schema/legacy expectation differences, four old direct-buffer/frame fixtures
and seven diagnostic expectations. Two former inference/order cases now reach
their later assertions: binary argument validation expects the old message
capitalization, and the column-mismatch test expects old text after a flush
that does not await ACK. The latter now sees the required MISSING-to-KNOWN
refresh and `SCHEMA_CHANGED`. Do not change production semantics to satisfy
these stale messages. Classifications describe this selected suite, not an
exhaustive count of all release gaps.

Root verified the canonical server fork's actual startup temp argument and
open database descriptors under the project-local directory (PID 4123391),
not only Surefire XML. SOL independently reviewed the final production path,
wire tests and failure transitions. Source/test review mistakes were corrected
against actual protocol bytes and runtime results before acceptance. Existing
dirty work was preserved; no commit or push was made.

Reflection: the existing buffer already had the necessary ownership and state.
The useful additional behavior is confined to selecting an inferred type and
reporting a genuine conflict through the existing refresh path. Read both the
public API lifecycle and the actual wire format before adding guards or tests.
Next, repair stale diagnostics while preserving independent legacy rejection
coverage, then continue bounded conversion families. Unsupported inputs,
old-distribution compatibility and the proposed lookup-timeout configuration
remain release work; this increment does not claim complete feature readiness.

## D034 — Iteration 2.19: repair diagnostics without changing behavior

Recommendation: fix the seven stale diagnostic expectations before adding
another conversion family. This is server-test-only work. The accepted 2.18
client JAR stays frozen at SHA-256
`102128206d124f8963b89dd6f6764da641086697272f0bb9d93493c55903330b`.

The selected tests now encounter schema-directed setters, so old generic error
text and asynchronous error callbacks no longer describe the operation being
tested. This is not a reason to change production behavior or accept either
local or server errors interchangeably.

Source-backed decisions:

- Assert `LineSenderSchemaException` and its reason for local invalid values or
  unsupported conversions. Check useful table/column/type context without
  pinning incidental capitalization.
- Preserve independent legacy BINARY-to-CHAR/STRING/SYMBOL/VARCHAR rejection
  coverage. A nonrequesting WebSocket client sends an unflagged frame; parse
  and assert its TYPE_BINARY definition and exact opaque bytes before sending.
  Assert server SCHEMA_MISMATCH and zero rows, then separately test public
  Sender's local rejection and partial-row recovery. `QwpWalAppender` explicitly
  rejects TYPE_BINARY in all four target branches.
- Await the first row's ACK in the inferred-LONG mismatch test. In
  `CursorWebSocketSendLoop`, schema feedback is applied before the acknowledged
  sequence advances. This establishes the known LONG target before rejecting
  fractional 3.14; a valid 3.0 then checks recovery. A sleep or an assertion
  accepting multiple reasons would hide the actual lifecycle contract.
- Keep the UUID test's exact completed-A/failed-B/successful-C SQL result.
  Exercise recovery without manual `cancelRow`; the binary cases additionally
  continue without table reselection.

SOL owns the two narrowly edited server test files and serialized validation;
a separate SOL reviewer checks compatibility, rollback and test independence.
Root checked the setter/flush/ACK ordering and the server rejection branches.
No production code, general test framework, private-state hook or opt-out is
needed. No client rebuild/install is planned, and old server distributions are
not covered by these current-server legacy-frame tests.

Evidence limitation: the requested pre-edit copies of the two server files
were not captured by the authors. Do not reconstruct them and call them
snapshots. Review uses the observed original methods, existing worktree diff
and D033's recorded test evidence; the final source hashes and exact failure
method comparison will be recorded separately. Existing unrelated dirty work
must remain intact.

### Final acceptance

The iteration is accepted for the seven diagnostic tests. Both authors and an
independent SOL reviewer checked the source; root also checked exact wire,
setter rollback and ACK ordering. A stale comment claiming BINARY still passed
unchecked into VARCHAR was removed. Root requested continuation without table
reselection and an empty-flush assertion so the test observes recovery rather
than supplying cleanup itself.

Server evidence lives in `core/target/schema-diagnostics.2.19-server/`, including
the exact commands in `run-manifest.md` and preserved logs:

- `logs/focused.log`: seven affected tests passed.
- `logs/canonical.log`: the previous 261-test green selection plus seven
  diagnostic tests passed, 268 total; no skips.
- `logs/broad.log`: the unchanged 333-test selection ran with seven failures,
  40 errors and no skips. Root independently compared method names against
  D033: the six owned `QwpSenderE2ETest` diagnostics and
  `QwpWebSocketSenderReceiverTest.testColumnTypeMismatchThrowsClientSide` are
  exactly the seven removals; no method was added to the failing set.
- `logs/repeat-1-2.log`, `logs/repeat-42-43.log` and
  `logs/repeat-1234567-7654321.log`: all 23 selected diagnostic/integration/
  recovery tests passed each seed pair. These are repeated executions, not
  69 distinct scenarios.

The remaining 47 classify as 38 missing-conversion cases, five approved
schema/legacy expectation differences and four obsolete direct-buffer/frame
fixtures. This is the selected regression suite, not an exhaustive release
backlog. Source-null text conversion decisions, unsupported input families,
actual older-distribution compatibility and the proposed lookup-timeout
configuration remain separate work.

Final server source SHA-256 values:

- `QwpSenderE2ETest.java`:
  `4246989e6899c27035a37a3b7a586028a6d001f0704f6151880d3a6c8d132219`.
- `QwpWebSocketSenderReceiverTest.java`:
  `f167130d6770f3c1c2b8961ab29cd2a47cc3fb1253d34f6eb4150230d3cc5990`.

Before/after artifact and test-source hashes match. Root independently verified
the canonical fork's startup `java.io.tmpdir` and open database files under the
project-local directory (PID 4145918). No client rebuild/install or client test
rerun was needed; D033's 414 client tests plus two packaging checks remain
previous-iteration evidence, not new 2.19 executions. Diff checks pass; existing
dirty work was preserved, and no commit or push was made.

### Next-step reflection: source nulls before text output

Read-only planning identified a contract decision before LONG-to-STRING/VARCHAR/
SYMBOL. The server's `putFixedToStringColumn`, `putFixedToVarcharColumn` and
`putFixedToSymbolColumn` check `cursor.isNull()` before decimal formatting.
`QwpFixedWidthColumnCursor.advanceRow` recognizes LONG's sentinel only when
there is no null bitmap; a supplied `Long.MIN_VALUE` marked present by a bitmap
therefore becomes decimal text instead. D013's approved client exception is
limited to numeric targets and does not silently authorize text targets.
Correction from D035's first runtime test: the formatter emits literal text
`"null"`, not decimal minimum. The bitmap-dependent NULL-versus-text contrast
is real; the initially predicted text was wrong.

An initial SOL suggestion that UUID-to-text avoided this decision was rejected
on source review and retracted. Both public UUID paths always append the two
limbs; neither treats `(Long.MIN_VALUE, Long.MIN_VALUE)` as a no-op. The same
server cursor checks the UUID sentinel only without a bitmap, and both text
appenders otherwise format it as a UUID. It is not a decision-free alternative.

Recommended next step: characterize these supplied-sentinel text cases through
the public and legacy-wire paths, then explicitly agree that source nulls remain
null before formatting in schema mode. That would be simpler than reproducing
batch-dependent behavior, but it is a proposed compatibility exception, not an
implemented or newly approved contract. No new converter is added in 2.19.

## D035 — Iteration 2.20: characterize source nulls before text conversion

The user requested proceeding from D034's next step. Scope: establish runtime
evidence for the five disputed pairs, then recommend an explicit contract.
This is characterization, not conversion activation. No production code,
public API, negotiated mode or server acceptance rule is changed. The 2.18
client artifact remains frozen at SHA-256
`102128206d124f8963b89dd6f6764da641086697272f0bb9d93493c55903330b`.

Source review covers public Sender, the non-owning binding, native column
appends, server dispatch, fixed-width cursor null handling and the actual
formatters. LONG-to-STRING/VARCHAR/SYMBOL and UUID-to-STRING/VARCHAR are accepted
legacy QWP conversions. UUID-to-SYMBOL is not included or newly authorized.
The supplied sentinels are effective writes, not no-ops: LONG minimum is
appended as eight bytes, and the UUID pair as two eight-byte limbs. Only the
server cursor's interpretation depends on the presence of a bitmap.

SOL owns one new server test class and a separate SOL agent owns serialized
validation; an independent SOL reviewer checks the contract and assertions.
The new file's pre-edit state is explicitly absent. Existing production and
fixture files stay unchanged; no opt-out, private-state test hook or general
conversion/test framework is needed.

Implemented and validated test matrix:

- Legacy unflagged LONG frames to STRING, VARCHAR and SYMBOL: ordinary signed
  values and range boundaries, including MIN+1/MAX, plus supplied MIN without
  a bitmap; contrast a frame containing supplied MIN and an omitted companion
  row. Assert source type, original bytes, exact bitmap bits, SQL text and
  `is null`, not merely the cursor's interpretation.
- Legacy unflagged UUID frames to STRING and VARCHAR: an asymmetric ordinary
  UUID, either limb alone equal to MIN and both limbs equal to MIN; contrast
  the all-MIN pair without and with an omitted companion row. Assert raw
  limb order and canonical text explicitly rather than using the production
  formatter to compute expected values.
- Public schema-aware Sender for all five pairs: ordinary and sentinel native
  inputs remain unsupported in the current build. Assert typed local rejection,
  completed row A, rollback of partial B and successful text row C without
  manual cancellation or table reselection. This does not claim that the
  proposed converters or proposed null normalization are implemented.

Recommendation, now runtime-backed but pending explicit contract agreement:
when these five conversions are implemented, normalize the supplied source
sentinels to target SQL NULL before formatting, independently of surrounding
rows. Keep effective-write and duplicate rules; do not reinterpret them as
setter no-ops. All other LONG values use signed decimal text, and all other
UUID limb pairs use canonical lowercase UUID text. This is a deliberate
exception to bitmap-present legacy results, not a claim of exact parity.
Do not extend it to FLOAT/DOUBLE, IPv4, LONG256, dates, timestamps or other
targets by analogy. Public UUID setter documentation currently does not state
this sentinel rule; implementation must document the approved scope.

### First runtime findings and oracle corrections

The first focused run selected all three tests: UUID characterization passed,
LONG characterization failed on its first bitmap-present STRING target, and
the public recovery case reached an invalid query-assertion configuration.
The log is preserved at server
`core/target/schema-text-source-null.2.20-server/logs/focused.log`.

The LONG result was literal text `"null"` with `v is null = false`, not decimal
minimum. The supplied eight-byte MIN and row-0-present bitmap were exactly as
asserted. Root and SOL had traced the appender to `Numbers.append` but had not
read that overload's own sentinel handling. `Numbers.append(sink, long)` calls
the overload with `checkNaN=true`; its MIN branch writes `"null"`. The variant
with `checkNaN=false` would print decimal minimum, but is not called here.
Correct the expected literal and current design table; do not change production
or confuse the string `"null"` with SQL NULL. D034's predicted decimal text is
explicitly corrected above. UUID's formatter has no corresponding special
case and its source/byte/SQL expectations passed.

The second failure is fixture misuse, not ingestion: `returnsOnce()` rejects
`.timestamp()` configuration. Remove that unnecessary setting while retaining
the exact SQL row values, timestamp strings and ordering. Adding `expectSize()`
would not make that configuration supported. This illustrates why checking
the test helper's actual contract matters as much as checking the formatter.
All subsequent validation must use a newly frozen source hash and separate
logs; no broad or repeat gate ran against the initial failing draft.

### Final acceptance and next decision

The corrected characterization is accepted. Source and adversarial SOL review
found no remaining issue after the two corrections; root independently checked
the actual formatter overload, query helper, frozen source and run results.
Automatic flush triggers are pinned using the existing configuration so row A
remains buffered across B's error. Legacy sockets are owned directly by
try-with-resources, including handshake failure; no extra ownership flag is
needed in a helper.

Final new test SHA-256:
`47f9bf1ac588ed7c2b6ca74235a330a6f8c6e70adf8a90a6ba05ab9fd05bf816`
for server `QwpSchemaTextSourceNullE2ETest.java`.
Server evidence is under `core/target/schema-text-source-null.2.20-server/`:

- `logs/focused-acceptance.log`: three tests passed, covering all five pairs
  through loops; zero failures, errors or skips.
- `logs/canonical.log`: 271 tests passed, the previous 268 plus the three new
  characterization tests; zero failures, errors or skips.
- `logs/broad.log`: the original 333-test selection has seven failures,
  40 errors and no skips. Root independently compared exact method names:
  the same 47 as D034, with neither additions nor removals. The three new tests
  are deliberately outside this unchanged comparison selection.
- `logs/repeat-1-2.log`, `logs/repeat-42-43.log` and
  `logs/repeat-1234567-7654321.log`: all 26 selected characterization,
  diagnostic, integration and recovery tests passed at each seed pair.
  These are repetitions, not 78 distinct scenarios.

`run-manifest.md` records exact commands, the preserved initial red run and
initial source hash, and corrected before/after source/artifact hashes.
Root independently verified canonical fork PID 4168481's startup temp argument
and open database/WAL files beneath the project-local directory. The new test
source stayed frozen through all accepted runs; the two 2.19 fixture hashes and
workspace/installed client artifact remain unchanged. Client production code
was not changed, rebuilt or retested. Existing dirty work was preserved; diff
and whitespace checks pass. No commit or push was made.

The evidence establishes the current server's legacy-frame behavior and the
current schema Sender's rejection/rollback. It is not proof of an older server
distribution, implemented normalization, or improved conversion coverage.
Keep the two independent legacy characterization tests when implementing the
converters; migrate only the corresponding current-public-rejection cases to
the approved behavior. Those rejections are a baseline, not a permanent goal.

The next product decision is explicit: should these reserved LONG/UUID values
always become SQL NULL for the five text/SYMBOL target pairs, instead of
sometimes becoming literal text? Recommendation: yes, with legacy mode and
persisted legacy bytes unchanged. Document this compatibility exception before
implementing it; do not add batch-sensitive or send-time transformation.

## D036 — Plan the remaining 47 repairs — 2026-09-11

The user requested an execution plan for all remaining failures, superseding
the tentative next LONG-to-text implementation. The separate
[repair plan](schema-aware-sender-remaining-failures-plan.md) is the execution
ledger; the main design remains the product contract. No production or test
source was edited, and no Maven/test execution was performed for this plan.

### Evidence and sequencing

Root and SOL audited the exact D035 broad-failure list against complete affected
tests, public setters/bindings, server dispatch, and concrete parser/formatter
implementations. The baseline remains 333 tests with seven failures and 40
errors, not 47 distinct production defects. A mechanical comparison of the
plan's ledger with the preserved list found 47 entries, 47 unique methods,
no missing methods and no extras.

Recommended order, divided into 16 independently reviewable increments:

1. Nine test repairs: five approved schema/legacy expectation differences and
   four fixtures that mutate Sender-owned buffers. Restore independent raw
   legacy frames; do not weaken production behavior or add a schema opt-out.
2. Eight text conversions: LONG, UUID and FLOAT/DOUBLE input families.
3. Four ordinary timestamp input/output conversion cases.
4. Four CHAR, LONG256 and GEOHASH target cases.
5. Twenty-two decimal cases: native decimal first, then LONG, STRING,
   FLOAT/DOUBLE and finally decimal text output.

Every increment must retain a working public Sender real-server test, exact
wire assertions, independent legacy expectations, rollback/recovery coverage,
serialized frozen-artifact regression runs, seeded repeats and independent SOL
review. Count reductions are checkpoints, not permission to delete cases or
weaken assertions. A first fixed setter can expose a later stale assertion;
classify it against the approved contract before changing it.

The plan uses the frozen 2.20 JAR and run manifest as its starting evidence.
The previous 271-test green gate and three 26-test repeats are historical
accepted results, not new planning-turn runs. Future increments must grow that
gate and compare exact names in the unchanged 333-test diagnostic.

### Simplicity decisions and review corrections

- Schema metadata already carries the complete packed server column type.
  `QwpSchemaControl` writes it with an empty extension block; the client retains
  it. GEOHASH bits and decimal precision/scale can use this existing metadata.
  Do not invent new parameter fields, a second schema cache, or a new registry,
  and do not relax rejection of unknown extension blocks.
- Decimal conversion belongs at setter time. One review suggestion to preserve
  arbitrary input scale in schema-bound wire data and leave rescaling to the
  server was challenged and withdrawn: it contradicts early conversion
  validation. Convert non-null coefficients to target scale/precision and
  compatible wire storage, then reuse existing append storage. Decimal-to-text
  is different: it preserves source scale because that scale is visible text.
  Cover emitted all-null columns separately from null/no-op setters; do not
  infer decimal null behavior from fixed-width cursors.
- CHAR tests must distinguish malformed raw UTF-8 from malformed public Java
  text. The public UTF-16 input is replacement-encoded before the server sees
  it; a raw-byte parser result is not automatically the public API result.
- FLOAT and `Instant` overload coverage goes beyond the exact failing method
  names but belongs with its corresponding input family. These extra tests
  are not additional entries in the 47-method count.
- LONG/UUID text-null normalization remains an explicit contract prerequisite
  for implementation; this plan does not silently extend it to floating-point,
  timestamp or decimal inputs. Keep the independent 2.20 legacy characterization.

The final independent SOL plan review found no remaining substantive scope,
dependency or ledger issue after those corrections. Root checked the ledger
and the relevant packed-metadata and decimal append/wire paths. Reviews inform
the plan; they do not substitute for runtime acceptance of its future changes.

### Scope boundary

All proposed functional changes are QWP client encoding/binding changes. SQL
only creates test tables and checks stored values; no SQL parser, planner or
execution-engine change is proposed. No send-time transformation, raw-type
fallback, reflection or new test-only production API is authorized by this plan.

Zero remaining baseline failures will not establish full feature readiness.
Unrepresented conversion families, unsafe legacy-path compatibility decisions,
an actual older-server distribution, lookup-timeout configuration and broader
lifecycle/performance evidence remain visible release work in the main design.
No commit or push was made.

## D037 — Iteration 2.21: separate schema expectations from legacy frames

Status: accepted. The user approved proceeding with the repair
plan. This increment implements phase 1a only: five test repairs, plus one
public Sender counterpart to preserve integration coverage. No conversion or
other production behavior is changed.

### Scope and source-backed decisions

- The three Boolean conversion tests remain public Sender schema tests. They
  keep every supplied true/false value and omitted row, but now assert the
  target-native missing values and explicit nullness: BYTE/SHORT zero, nullable
  numeric and text targets null. Independent raw BOOLEAN comparisons remain
  in `QwpSchemaBooleanE2ETest`. One review initially recommended moving all
  five tests to raw frames; root rejected that recommendation for the Boolean
  cases, and the reviewer corrected it against the plan.
- The two floating legacy diagnostics now use nonrequesting WebSocket clients,
  standalone native FLOAT/DOUBLE buffers and unflagged frames. They prove
  no-bitmap NaN acceptance versus bitmap-present NaN rejection for both source
  types and all four integer targets. Both source widths retain the accepted
  predecessor, legacy clamp at positive 2^63, and rejected successor. Tests
  assert raw bits, column definitions, bitmap bits, row counts/exhaustion,
  precise server NACK context, stored values and empty rejected tables.
- Existing schema floating tests exercise bindings, not the public Sender
  boundary path. Converting the two diagnostics solely to raw frames would
  remove that integration coverage. The added
  `testSchemaFloatingNaNAndPositiveTwoToThe63Rules` checks public FLOAT NaN in
  a separately flushed block, DOUBLE NaN with an omitted companion row, and
  local `INVALID_VALUE` for both positive-2^63 overloads. Completed row A and
  valid row C survive two partial-row failures without manual cancellation or
  table reselection. Existing auto-flush controls keep A buffered during errors.
- Root corrected a draft cursor assertion: no-bitmap supplied NaN is a cursor
  null even though its raw value bytes exist; bitmap-present supplied NaN is
  not. Do not replace the byte assertion with the cursor's interpretation.
  Independent review also required the auxiliary label column's exact name
  and VARCHAR wire type, not only its text bytes. Both fixes preceded the
  first test run.

The old NaN-only Sender error helper is removed because these cases now assert
server wire rejection directly. Shared Sender terminal-policy helpers remain
unchanged. No schema opt-out, shared test framework, reflection, new TestOnly
API or send-time conversion was added. SQL changes are only test queries that
check values and nullness; no SQL implementation was touched.

### Frozen evidence and validation

Server evidence directory: `core/target/schema-expectation-repair.2.21-server/`.
`baseline/` preserves both pre-edit test files; `run-manifest.md` records exact
commands, hashes and outcomes. The unchanged client JAR is
`102128206d124f8963b89dd6f6764da641086697272f0bb9d93493c55903330b`.
Frozen changed test hashes:

- `QwpSenderE2ETest.java`:
  `9c48a484369d588114c291c8f6b8a2e939380ebb04c314523e00649b8ed9cab8`.
- `QwpWebSocketTypeConversionE2ETest.java`:
  `253dcc2a8149a05fa57913868a19051f571d5b365d81ec431bbe08161da1a134`.

Both independent SOL reviewers accepted the final source. Root independently
checked the diffs and current client/server behavior. The prior frozen 2.20
broad log supplies the before-fix failures; no new red baseline run is claimed.

- Focused: 25 tests passed, zero failures/errors/skips.
- Expanded green gate: 277 tests passed, zero failures/errors/skips.
- Original 333-test comparison: three failures and 39 errors, zero skips.
  The exact 42-method set is the previous 47 minus the five phase-1a repairs,
  with no new failing method. No case was removed or renamed.
- Three seeded repeats: 32 tests passed at each seed pair (`1/2`, `42/43`,
  `1234567/7654321`), zero failures/errors/skips. These are repeats, not 96
  distinct tests. The corresponding Surefire properties confirm each seed pair.

Root verified live canonical fork PID 5929's startup temp path and database
descriptors under the project-local directory. Surefire XML confirms the same
temp path and standalone installed client JAR. No client build or client test
run is claimed for this test-only increment. Both changed test sources and
the workspace/installed client JAR retained their frozen hashes through all
runs. Root and an independent SOL reviewer separately verified the exact
five-method removal with no additions. Diff and whitespace checks pass.
Existing unrelated work was preserved; no commit or push was made.

Next: phase 1b, the four direct-buffer legacy fixtures. The 38 missing-conversion
cases are unchanged. This test-only repair does not implement additional
conversions, approve new null rules, prove an older server distribution, or
make the feature release-ready.

## D038 — Iteration 2.22: isolate four legacy wire fixtures

Status: accepted. The user requested the next increment. Scope
is phase 1b only: the two GEOHASH-to-text and two named LONG-to-designated-
timestamp tests in `QwpWebSocketTypeConversionE2ETest`. Existing 2.21 repairs
and all original stored-value expectations are preserved.

### Decisions and source validation

The old fixtures obtained a Sender-owned buffer, appended directly to it and
then called public row completion. That bypassed the producer's binding and
column tracking while row completion could negotiate/install schema state.
Do not accommodate that test-only mixture in production. The repaired tests
own standalone native buffers and send ordinary unflagged frames through a
nonrequesting WebSocket connection. Public Sender conversion behavior is tested
separately; no opt-out or new production testing API is needed.

Root and SOL checked the concrete server paths, not just type names:

- Positive GEOHASH precision 5 selects binary text formatting, not base32.
  Preserve `[22, null, 31, null, 10]` and `[1, null, 0, null, 21]`, including
  the non-null zero and leading zero bits. Explicit original timestamps keep
  row order deterministic. The GEOHASH column is emitted last and checked as
  exact bytes: bitmap marker 1, bitmap `0x0a`, precision 5, three compact values.
  Cursor checks separately verify precision, row nullness/values and exhaustion.
- A named designated `TYPE_LONG` column is not timestamp-unit conversion input.
  `QwpWalAppender` applies neither micro-to-nano nor nano-to-micro scaling to
  this wire type; its designated fixed-width branch copies literal values.
  Tests pin both column definitions, absent bitmaps, 8-byte widths, two values,
  raw value/timestamp bytes and rowwise values, then exact micro/nano SQL output.
- The existing public Sender designated tests already cover both precisions,
  primitive/Instant input, partial-row rollback and cold `atNow()`. They remain
  in the focused/regression/repeat gates. Unlike phase 1a, no missing public
  counterpart required a new test method. Native public GEOHASH conversion
  remains unimplemented and must not be credited to these legacy fixtures.

Root found one review correction before execution: the draft ACK parser passed
`true` for `schemaNegotiated` despite using a nonrequesting connection. The
final default overload rejects unexpected schema feedback. Exact table names
and native LONG byte assertions were also added. Both independent SOL reviewers
accepted the refrozen source after these corrections. Helpers remain local to
the test class; class documentation now distinguishes public conversion tests
from raw legacy server tests. No reflection, shared framework, or production
change was introduced.

### Frozen evidence and validation

Server evidence directory: `core/target/schema-legacy-fixture-repair.2.22-server/`.
The pre-edit source is preserved in `baseline/`; `run-manifest.md` records exact
commands and results. The changed test source moved from
`253dcc2a8149a05fa57913868a19051f571d5b365d81ec431bbe08161da1a134` to
`726112e1bc4c88befd0f40df088ce5409485f59b170d707829a214debafa260c`.
The prior Sender test remains `9c48a484369d588114c291c8f6b8a2e939380ebb04c314523e00649b8ed9cab8`.
Workspace and installed client JAR remain
`102128206d124f8963b89dd6f6764da641086697272f0bb9d93493c55903330b`.

- Focused: 37 tests passed, zero failures/errors/skips.
- Expanded green gate: 281 tests passed, zero failures/errors/skips.
- Original 333-test comparison: zero failures, 38 errors, zero skips. Exactly
  the four phase-1b methods disappeared from the prior 42; no new failure.
- Three seeded repeats: 36 tests passed at each pair (`1/2`, `42/43`,
  `1234567/7654321`), zero failures/errors/skips. These are repetitions, not
  108 distinct scenarios. Each run's Surefire properties confirm the seed pair.

No client build/test run or new red baseline is claimed. The accepted 2.21
broad log supplies the original four failures against the preserved source.
Root observed canonical fork PID 22089's startup temp argument and checked its
completed Surefire path/artifact properties. Broad fork PID 24170's startup
argument and live database descriptors independently confirm project-local
storage. No shared `/tmp` database workaround or overlapping Maven run was used.

The changed test, previous Sender test and workspace/installed client JAR stayed
at their frozen hashes through all runs. Root and an independent SOL reviewer
separately confirmed the exact four-name removal. No method was renamed or
deleted. Diff/whitespace checks pass; unrelated changes were preserved. No
commit or push was made.

Phase 1 is complete: all nine expectation/fixture repairs are accepted. The
remaining 38 baseline cases require missing conversions; this does not finish
the complete feature-release checklist. Next is LONG-to-text, with the proposed
source-null rule recorded as an explicit contract before implementation.

## D039 — Iteration 2.23: approve LONG-to-text source nulls

Status: accepted on 2026-09-11; contract, implementation and validation complete
for phase 2a only.
The user's “makes sense” approves the preceding LONG-only recommendation:
in schema mode, `longColumn` writing to STRING, VARCHAR or SYMBOL converts
`Long.MIN_VALUE` to target SQL NULL, independently of other rows. This is an
effective first write, so subsequent duplicates remain ignored. Other LONG
values use canonical signed decimal text. Legacy encoding, conversions and
replay remain unchanged. UUID and floating-point text-null rules are not
approved by analogy.

Two SOL source checks confirmed that existing VARCHAR and dictionary-backed
SYMBOL append paths suffice. Both copy mutable formatter input before returning;
reuse of a formatter sink does not require changing dictionary ownership.
Missing-table/column inference must remain native LONG. Resolve target and
duplicate rules before handling the sentinel; unsupported target pairs must
remain unsupported even for a sentinel input.

Scope is phase 2a only: three baseline failures, with exact target wire,
public Sender, real-server stored-value and recovery tests. Keep the independent
2.20 legacy sentinel/bitmap characterization and all unsupported UUID checks.
No SQL, server production, send-time transformation or new conversion framework
is proposed. Test preparation was delegated to SOL. Production edits awaited the
pre-edit checks at the decision point; the completed work is recorded below.

### Implementation and review

The production change is confined to `QwpSchemaBinding`: permit the three text
targets in `longColumn`, retain target/duplicate validation before the existing
MIN null branch, format other values through `Numbers.append(..., false)`, then
use existing VARCHAR or SYMBOL append paths. One lazily allocated 20-character
sink belongs to the non-thread-safe binding. Numeric writes never format or
allocate that sink; new distinct symbols still require the existing owned
dictionary entry. No new cache, registry, writer, protocol or connection state.

The shared 18-vector corpus has literal expected UTF-8 and SQL values. Tests
cover MIN+1/MAX, signs/zero, MIN-first followed by an otherwise-invalid UUID
duplicate, omission bitmaps, reset, native LONG inference, and local/global
SYMBOL sequences `42 -> 43 -> 42` to detect retained mutable formatter input.
Public Sender socket tests pin the actual described identity and target bytes,
cancel partial B while preserving A/C, and retain separate SYMBOL/VARCHAR
encoding generations after an explicit setter-triggered refresh. The new
real-server test checks all pairs and SQL nullness. Its SF case seeds a local-
dictionary frame into the real log and replays it through a new Sender; it is
not described as public-Sender crash persistence. Existing public crash/global-
dictionary and two-generation recovery tests remain in the server gates.

Independent SOL source and adversarial reviews accepted this bounded change.
Root preserved these findings rather than adding production workarounds:

- A cancelled row can leave an unused global symbol entry. The dictionary is
  append-only; row rollback does not rewind it. A draft socket oracle incorrectly
  expected B's `7` entry to disappear. The corrected oracle requires `[7, MAX]`,
  MAX at ID 1, and only A/C rows. No dictionary-rollback mechanism was added.
- The first broad comparison found another stale LONG-to-STRING rejection in
  `QwpSchemaRowBufferE2ETest.testNullOmissionAndUnsupportedUsedColumns`. Change
  only its unused target declaration from STRING to BINARY: LONG-to-BINARY is
  genuinely unsupported, as confirmed by `QwpWalAppender`'s BINARY branch.
  The method name, cancellation, omission checks and exact SQL expectations
  remain unchanged. Add this method to the green/repeat gates. Do not substitute
  UUID-to-STRING, which the next planned increment intends to support.
- Compile-only fixture mistakes (client Unsafe API, response constant types,
  server helper visibility/checked exceptions) and a malformed MISSING-schema
  response are retained in development logs, not counted as behavioral red
  evidence. Source review did not replace compilation. Valid red baselines
  then showed five client conversion errors with one native-inference pass,
  and all three new server tests failing on the missing conversion.

The old mixed text-rejection test now retains only the four UUID ordinary/null-
sentinel cases; its independent legacy LONG/UUID byte/bitmap tests are unchanged.
The original three LONG-to-text methods retain their existing SQL expectations.

### Focused performance evidence

The hardware-first skill caused a pre-edit baseline check, not an optimization.
The validated study in `core/target/schema-long-text.2.23-client/perf/` is limited
to the existing warmed LONG-to-LONG binding/readback case. Six fixed-work
baseline runs preceded the production edit; twelve interleaved control/candidate
runs followed. Every run completes 500 million rows with the same checksum.
Basic and pipeline-counter median elapsed differences are +0.233% and +0.155%,
below observed variation; instructions/row are effectively unchanged. No
repeatable 5% regression was detected for this case. This is not proof of
sub-percent equality, new text-conversion speed, or end-to-end Sender throughput.
CPU affinity was fixed, but shared-host frequency/boost/thermals were not
controlled. Independent SOL review verified counts, identities and this scope.

Final server comparison and seeded acceptance results follow. The first broad
run is retained: 333 tests, one stale fixture
failure and 35 missing-conversion errors; it is not the accepted final baseline.

### Frozen evidence and acceptance

Client evidence: `core/target/schema-long-text.2.23-client/run-manifest.md`.
Server evidence: `core/target/schema-long-text.2.23-server/run-manifest.md`.
Both include exact commands and distinguish failed development runs from final
gates. No client install or benchmark overlapped server validation. Every test
fork received a pre-created project-local temporary directory through startup
`argLine`; root observed canonical PID 66407's command line and broad PID 68499's
command line/open database descriptors. The canonical descriptor inspection
missed the already-exited process and is not claimed as successful. Saved
Surefire reports confirm temp paths, standalone installed client dependency and
all three seed pairs.

- Client focused: 6 tests passed. Expanded client gate: 420 tests plus two
  packaging checks passed; zero failures/errors/skips.
- Server final focused: 16 tests passed. Expanded green gate: 288 tests passed;
  zero failures/errors/skips. Earlier 15/287 runs preceded the additional stale
  fixture repair and are retained separately.
- Original unchanged selection: 333 tests, zero failures, 35 errors, zero skips.
  Exactly `testLongToStringColumn`, `testLongToSymbolColumn` and
  `testLongToVarcharColumn` disappeared from 2.22's 38-name failure set; no added
  failure. Root and an independent SOL reviewer compared the sets separately.
- Final seeded repeats: 43 tests passed at each of `1/2`, `42/43` and
  `1234567/7654321`, zero failures/errors/skips. These are repetitions, not 129
  distinct scenarios. An earlier 42-test repeat is retained, not substituted
  for the expanded final selection.

Client binding moved from
`cf1415484f32067b9135e235d1a002607d1a99e5dd195c8c7d558aaae111aaca`
to `e24350a9522ceecb64267038410c914dd6b305299c146d5a9c9974c61cbac600`.
Workspace/installed main JAR moved from
`102128206d124f8963b89dd6f6764da641086697272f0bb9d93493c55903330b`
to `0d2d66ce80e7fb20fd3e7468b70ad2d49888b60cb0108694e30f0d713a8ecb49`.
Client/server corpus SHA-256 is
`0db1278342169f2291fa84027c7179a2097549fce1e1eec4aac60b1dab8988c7`.
Final source hashes, including the three changed server test classes, are in
the manifests. Buffer, Sender, original type-conversion/Sender fixtures and
server production remain unchanged through validation. Diff/whitespace checks
pass; unrelated working-tree changes were preserved. No commit or push.

Phase 2a is complete. Of the original 47 cases, 35 remain; this is not full
feature-release acceptance. Next is UUID-to-text, but its proposed both-limbs-
MIN null rule still needs a separate decision. Do not infer approval for UUID,
floating, timestamp or decimal null behavior from this LONG-only increment.

## D040 — Iteration 2.24: UUID-to-text with explicit source nulls

Status: accepted on 2026-09-11 for phase 2b only. The user's "proceed" explicitly
approved the separately proposed UUID rule: for STRING/VARCHAR targets, both limbs equal to
`Long.MIN_VALUE` become SQL NULL; every other pair produces canonical lowercase
UUID text. A single minimum-valued limb remains a value. The null is an
effective first write, so later duplicates are ignored. This deliberately
differs from legacy bitmap-present formatting of the both-MIN pair. Preserve
legacy ingestion/replay and native UUID-to-UUID bytes. UUID-to-SYMBOL remains
unsupported; no floating, timestamp or decimal policy is inferred.

Scope is phase 2b, two baseline cases. SOL was assigned client implementation/tests,
server E2Es and independent review. Tests must pin literal target bytes and SQL
nullness, limb order, sentinel boundaries, omission/duplicates, row rollback and
encoding snapshot retention. Keep the independent raw legacy characterization.
No server production, SQL implementation, send-time transformation or generic
conversion machinery is planned.

The hardware-first skill required a pre-edit gate for the changed value-writing
path. A focused existing native UUID-to-UUID baseline and interleaved comparison
were scheduled to check regression risk, not new text conversion speed or
full Sender throughput. The accepted 2.23 binding and JAR were archived under
`core/target/schema-uuid-text.2.24-client/baseline/` before edits. Their hashes are
`e24350a9522ceecb64267038410c914dd6b305299c146d5a9c9974c61cbac600`
and `0d2d66ce80e7fb20fd3e7468b70ad2d49888b60cb0108694e30f0d713a8ecb49`.
At this pre-edit checkpoint, the last accepted baseline had 35 errors.

Pre-edit source review found two LONG null-first tests whose UUID duplicate was
described as invalid. It becomes supported for STRING/VARCHAR in this increment.
Use genuinely unsupported BINARY-to-text/SYMBOL duplicates instead, retaining
the same null/first-write and rollback assertions. The SYMBOL-to-VARCHAR rebind
test still correctly uses UUID to trigger refresh: its pinned SYMBOL schema
rejects UUID, irrespective of the refreshed target. Keep it unchanged.

Behavioral red evidence uses the frozen 2.23 production source/JAR. Client
focused selection ran 11 tests with three missing UUID-to-text errors and eight
passes. Server selection ran four tests: all three new conversion E2Es failed
with `UNSUPPORTED_FEATURE`; the narrowed UUID-to-SYMBOL recovery test passed.
Source review caught malformed draft schema-fixture parameter/designated fields
before compilation and corrected them to the real wire protocol. Neither these
draft errors nor the later reset-oracle correction are production findings.
Raw logs are preserved under each repo's `core/target/schema-uuid-text.2.24-*/`.

### Implementation and client validation

Only `QwpSchemaBinding` changes in production: target-directed UUID dispatch,
a dedicated lazy 36-character sink and five fixed-width lowercase hex groups.
The native UUID branch returns before text-null normalization. Existing target
validation and duplicate handling run before conversion, and `addString` copies
the scratch contents synchronously. No change to LONG formatting, shared
Numbers, Buffer, Sender or server production. Independent SOL source and
adversarial reviews accepted this scope.

The expanded red client gate has 12 tests and four expected conversion errors.
The first candidate run exposed a test-oracle defect: all column definitions
precede all payloads, but the rollback reader expected them interleaved. Both
source reviewers confirmed the encoder contract; fix the reader only. The
corrected focused gate passes all 12 tests. The expanded client gate passes
426 tests plus two packaging checks, with zero failures/errors/skips. The new
public Sender test verifies typed UUID rows on both sides of a UUID-to-VARCHAR
schema change: the old block retains native limbs, the new block contains text.
Refresh is setter-triggered; no passive ACK-adoption claim is made.

Current candidate binding SHA-256 is
`9d49b8476e844aae15227448d103372c18ebfe620cbb2bd4c49e42528d6743ff`.
After successful install, workspace and installed main JAR both hash to
`c06908f19e82b95625c1f3b5661a06a0fe9ced10637defde7d4f845d817c989d`.
The shared 16-vector corpus hashes to
`3278914c60022786037fb09380b717bfde23f327f750b8e33c938e14f0eaba03`;
root independently checked signed limbs against literal UUID text and UTF-8 hex.
Server acceptance and the interleaved performance comparison were pending at
that checkpoint; their completed results follow.

### Regression checks and fixture corrections

The pre-edit hardware study was corrected before acceptance: retired-operation
counts alone did not justify a limiting-resource claim, major faults were not a
CPU error metric, and a fixed footprint was not measured saturation. The final
study records actual pipeline fractions, normalized instruction/cycle costs,
fresh cgroup throttle/OOM deltas and qualified kernel-error visibility. Root
reran the validator and reviewed the evidence before authorizing production.
Six baseline comparisons and one separate systemic run preceded edits; the
systemic run started after client red validation finished.

All twelve interleaved candidate/control measurements completed 500 million
native UUID rows with the same two-limb checksum. Basic instructions per row
changed by -0.186%; neither elapsed nor normalized instruction/cycle costs show
a repeatable 5% regression. The observed lower candidate elapsed/cycle costs
are not a speedup or mechanism claim: shared-host frequency/boost are uncontrolled.
This check covers native UUID-to-UUID only, not the new formatter, allocations,
public Sender or networking. Independent SOL review checked all twelve raw runs.
See `core/target/schema-uuid-text.2.24-client/perf/comparison.md`.

The first server focused run passed 20 of 21 tests, including all shared vectors,
source-null/legacy checks and text replay. The new rollback test expected an
omitted LONG column to print blank; `CursorPrinter` and the actual output show
`null`. Change only the two expected cells, retaining the same projection,
rows and rollback checks. The failed source and log are archived. These small
fixture defects reinforce that source review does not replace execution; none
justifies a production workaround.

### Frozen evidence and acceptance

Client manifest: `core/target/schema-uuid-text.2.24-client/run-manifest.md`.
Server manifest: `core/target/schema-uuid-text.2.24-server/run-manifest.md`.
Both preserve exact commands, identities and unsuccessful development runs.
No Maven runs or client installs overlapped the fixed-work measurements; no
client rebuild overlapped server validation. Saved reports verify startup
project-local temp directories and the installed client dependency. Root also
observed canonical Java PID 121872 and its open database files under the expected
temp path. The focused live-process snapshots missed the short-lived fork and
are not claimed as successful inspections.

- Client: final focused 12 passed; expanded 426 plus two packaging checks
  passed; zero failures/errors/skips. Install succeeded.
- Server: final focused 21 passed; expanded green gate 293 passed; zero
  failures/errors/skips.
- Original unchanged selection: 333 tests, zero failures, 33 errors, zero skips.
  Only `testUuidToStringColumn` and `testUuidToVarcharColumn` disappeared from
  2.23's 35-name list; no added failure. Root and an independent SOL reviewer
  separately checked both directions and the identical selection/fixture hashes.
- Repeats: 48 tests passed at each of `1/2`, `42/43`, and `1234567/7654321`.
  Saved UUID test reports confirm each seed pair and temp path. These are
  repetitions of 48 cases, not 144 distinct scenarios.

Final binding and main-JAR hashes remain `9d49b847...6743ff` and
`c06908f1...17c989d` (full hashes above and in the manifests). Final client UUID
test SHA is `c23aa77b7edeff165bcef36ed60bce72425b45507d60e935227cbd53588ce1c6`;
server UUID test SHA is
`4371b0e3aab331755c4101fffde5384ec2966d01f29f8ecba650ea2273dc17ce`.
All frozen production/test/corpus identities remained unchanged through final
gates. Independent SOL cross-reviews accepted client production/tests and server
tests. Raw legacy LONG/UUID characterization is unchanged; the new SF test is
explicitly component-seeded replay, with public crash/two-generation recovery
retained in existing regression tests. Diff/whitespace checks pass. Unrelated
working-tree changes were preserved. No commit or push.

Phase 2b is complete. Of the original 47 cases, 33 remain; the full conversion
inventory and release checklist are not complete. Next is phase 2c FLOAT/DOUBLE
text output, after separately characterizing and deciding its source-null rule.
Do not infer that policy from LONG or UUID approval.

## D041 — Iteration 2.25: floating-point text output

Status: accepted, 2026-09-11. The user requested that work continue one
increment at a time until the remaining conversions are fixed. Start with
phase 2c, then the timestamp, scalar-target and decimal increments in the plan.
The last accepted original selection still has 33 errors. Do not confuse that
finite test list with the wider source-audited conversion inventory or silently
implement the documented unsafe server boundaries.

Keep production changes sequential, with SOL implementation and independent
review, real-server tests, exact target bytes, unchanged legacy comparisons
and frozen client artifacts. No SQL-engine changes or general conversion
framework. Reuse the hardware-first regression checks for affected existing
paths; no throughput claim is implied.

The floating text-null rule remains a separate explicit decision. Source
inspection confirms the legacy cursor recognizes NaN as null only without a
bitmap; when a bitmap marks it present, the server formatter emits text `NaN`.
Root recommends deterministic target SQL NULL and has asked the user, without
treating the general continuation request as approval of that choice. SOL is
preparing raw real-server characterization while this decision is pending.

A second load-bearing assumption failed source inspection: current server
`Numbers.append(double)` delegates to Ryu, while the client retains the older
double formatter. Both expose the same method/default scale. Differential
checks must establish output compatibility before reusing client formatting;
do not change shared legacy formatting to fix this schema-only feature.

Accepted 2.24 binding, Numbers source and main JAR are archived in
`core/target/schema-floating-text.2.25-client/baseline/` before any production
edit. Binding/JAR hashes remain `9d49b847...6743ff` / `c06908f1...17c989d`.

The user subsequently explicitly selected SQL NULL for NaN. This approves all
six FLOAT/DOUBLE-to-STRING/VARCHAR/SYMBOL pairs, with effective-write/duplicate
semantics, no legacy change and no inference for other source families.
The new raw real-server characterization passed: one test covering 12
source/target/bitmap combinations, exact raw bits, null flags, stored text and
SQL nullness. Its source/report are archived under the server's
`core/target/schema-floating-text.2.25-server/` before schema-positive additions.

The formatter differential found 1,543 mismatches across 150,004 supplied values
(275 DOUBLE, 1,268 widened FLOAT). Examples include `1e23` and the smallest
DOUBLE subnormal. Reusing old client Numbers is therefore not a simplification
that preserves the contract. Prefer an isolated source-faithful schema formatter;
leave global Numbers and legacy text formatting untouched.

A parallel timestamp audit corrected a stale plan statement: the server first
converts nanoseconds to micros, but its text formatter emits milliseconds, not
microseconds. Root verified the call sites and UTC pattern and corrected phase
3c. This is a source-backed plan correction, not permission to change server
output. Timestamp sentinel/empty-text behavior still needs characterization.

The pre-edit performance check completed six fixed-work native DOUBLE-to-DOUBLE
runs with identical raw-bit checksums. Root inspected the source attribution,
resource checks and normalized counters and independently ran the study validator.
This is a narrow regression baseline, not floating-to-text throughput evidence.
Production edits were cleared only after this check and the client red tests:
six tests ran, five failed with the expected missing-conversion error; the
native-inference/unsupported-target guard passed. The server red run then had
eight tests, seven expected missing-conversion errors and a passing independent
legacy characterization. Both red XML sets are archived in the 2.25 evidence
directories. Independent SOL source review requested one test strengthening:
compare the SF frame identity with its actual describe reply, not just its types.

The candidate adds a schema-only formatter and an exact server Ryu port; only
package/class names and provenance differ in the latter. The binding adds lazy
scratch and the three text target arms. Its numeric branches, native inference,
target/duplicate ordering and public Sender code are unchanged. Main client
Numbers is byte-identical to the pre-edit source. Independent SOL review accepted
the frozen code and exact wire fixtures.

Root reproduced the public-binding/encoder differential: seeds 225202601 and
225202602 generate one million DOUBLE and one million FLOAT bit patterns.
The 999,505 non-NaN DOUBLE and 996,216 non-NaN widened FLOAT results, plus 46
structured cases, match the actual server formatter byte for byte. Random NaNs
are counted and skipped in this formatter comparison; the shared 48-vector
corpus and public tests separately assert their approved target-null behavior.
`core/target/schema-floating-text-audit/run.sh` records source identities and
the exact build/classpaths; `root-replay.log` records the independent replay.

The six focused client tests and packaging checks passed. The first 432-test
client regression run exposed a stale numeric-only fixture that still rejected
NaN-to-STRING/SYMBOL. Those two negative cases now use unsupported TIMESTAMP and
BINARY targets, preserving their guards and rollback checks. No production fix
was needed. The rerun passes all 432 tests and two packaging checks. The installed
and archived candidate JAR SHA-256 is
`44fdc2bf48f83db01eed089f915ab451c8490ec8d0aea6fbd638c070142dd43b`.
Interleaved native-path performance and server acceptance gates remain pending.

Read-only preparation for phase 3a confirms that LONG-to-ordinary timestamp needs
only target dispatch: both target units store the raw supplied count. For MIN,
the legacy no-bitmap null and bitmap-present raw value both store the same
SQL-null sentinel (`QwpWalAppender` ordinary TIMESTAMP branch and
`WalColumnarRowAppender.putIntegerToNumericColumn`). This requires no new
source-null policy exception. Keep designated-name rejection and native LONG
inference; do not route raw LONG through a timestamp unit converter. SOL may
prepare tests while 2.25 is validated, but no next-increment production edits
are permitted before this checkpoint is accepted.

The first candidate's performance result is not accepted as unchanged: all three
paired basic runs add about 25 instructions per native DOUBLE row (+4.62%),
with roughly 2–4% elapsed cost across the event groups. Although narrowly under
the predeclared 5% rejection line, root requested source attribution before
acceptance. SOL confirmed a C2 inlining cliff: the numeric helper grew from 322
to 420 bytecodes as its switch range expanded from 2–10 to 2–26. The baseline
inlines hot; the candidate is rejected as too large. This is a repeatable native
cost, not a text-throughput result or mere timing noise.

Authorize only a small separation of the cold text branch, preserving the
original numeric switch and all validation/null/duplicate ordering. Predict
restored hot inlining and removal of the extra per-row instruction work. Retain
the original candidate and all A/B artifacts; require a new frozen artifact,
complete functional gates and interleaved performance comparison. Do not tune
JVM settings, suppress the evidence or lower the acceptance bar.

The small split restores the numeric helper to 311 bytecodes; all numeric
arithmetic is unchanged. The first server candidate run reached 29 tests and
found one new expected-output mistake: an omitted UUID prints as an empty cell,
not numeric `null`. Correct only that fixture; the finite/NaN corpus, rebind,
SF and legacy checks passed. The earlier attempt failed test compilation in a
prepared phase-3a fixture (a string case label used as timestamp arithmetic).
That test-only mistake was corrected. Preparation of future increments must
not pollute the current test selection: archive future integration additions
outside the source tree until the preceding checkpoint is accepted. The live
integration class was restored to its exact 2.25 snapshot before the rerun.

Final acceptance: the corrected helper inlines hot at 311 bytecodes. Twelve
new paired measurements have identical fixed-work checksums, and the first
candidate's extra 25 native DOUBLE instructions per row is gone. Basic cycles
show no regression; pipeline-group elapsed remains 0.73–2.99% higher, qualified
as a limited shared-host/group result rather than a speedup or universal
unchanged-performance claim. Root independently checked the raw comparisons,
inlining evidence and study validator. Both candidate histories are retained.

The final workspace, installed and archived client JAR SHA-256 is
`1173fe3e8dd3fcd764c3513882e4461e608ff000e8d00d1dac6f91e8386f8b70`;
binding SHA is `bb4955a8c347f9b8c9fb6741f9a3b88e10cc8f471f5a8e78084dd5a31e278f1e`.
The final candidate passes 432 client tests plus two packaging checks; root
replayed the differential successfully after the split. Server gates pass
29 focused tests, 301 regressions and three 56-test repeats (seeds 1/2, 42/43,
1234567/7654321). Saved XML verifies startup tmp and seed properties. The exact
original 333-test selector has zero failures and 30 errors, removing only
DOUBLE-to-STRING/VARCHAR/SYMBOL from the previous 33-name list. No additions.
Root and independent SOL review verified the names, frozen source identities
and typed/null/lifecycle contracts. Server production, legacy Numbers, Buffer,
Sender and the original conversion-test source are unchanged in this increment.

Evidence and commands: client and server
`core/target/schema-floating-text.2.25-{client,server}/run-manifest.md`, saved
reports, both performance comparisons and the separate differential runner.
No commit/push. Phase 2 of the remaining-failures plan is complete; 30 baseline
cases and the separately tracked wider conversion inventory remain. Phase 3a
has reviewed test preparation only, including two corrected fixture mistakes;
its production change must receive its own red tests, measurements and gates.

## D042 — Iteration 2.26: LONG to ordinary timestamp

Status: accepted, 2026-09-11. Continue phase 3a after the accepted 2.25
checkpoint (30 baseline errors). This is a small extension of the existing
LONG setter, not timestamp parsing or formatting. A known ordinary micro/nano
timestamp column takes the raw LONG count in its own unit; there is no implicit
micro-to-nano multiplication. Supplied MIN remains an effective target-null
write. Source review confirms both legacy bitmap contexts store the same SQL
null, so this adds no source-null policy exception. Named designated timestamp
writes remain rejected, and missing schema still infers LONG.

The implementation brief permits only the target allowlist and the two existing
target-native addLong switch arms. No timestamp driver, protocol field, new
buffer representation or conversion framework is needed. Root and SOL read
the producer path, target-column guards, wire mapping and ordinary timestamp
branches in QwpWalAppender/WalColumnarRowAppender before this brief.

Prepared tests use a shared 12-vector corpus, exact target-unit bytes, null and
omission bitmaps, invalid duplicates, automatic A/B/C recovery and pinned native
LONG/timestamp generations. Real-server assertions inspect raw stored timestamps
and SQL nullness in both units; independent unflagged frames characterize MIN
with/without a bitmap. Preparation review caught and fixed two fixture mistakes:
MISSING replies carry no identity pair, and a textual corpus label is not an
epoch count. No production implementation has been authorized yet. The native
LONG baseline is being measured against the accepted 1173fe3e... JAR before
red tests and production changes; no concurrent Maven is permitted.

Root read and independently validated the fresh native LONG baseline study.
Client red: five tests, four expected unsupported-conversion errors; the
native-inference/designated guard passed. Server red: four tests, three expected
unsupported-conversion errors; independent legacy MIN characterization passed.
The authorized production patch adds only two timestamp target checks and two
labels sharing LONG's raw addLong arm (binding SHA 1f1a0396...).

The first focused client run found four wire-fixture failures. Both source
reviews had missed an existing timestamp encoding discriminator: the default
Gorilla-enabled stream writes raw-encoding byte 0 after the null bitmap even
for zero, one or two values. Root traced QwpColumnWriter and the existing
timestamp corpus test, then requested explicit assertions of that byte in all
new timestamp readers. Native LONG readers stay unchanged. Production did not
change. Record this as a review miss, not a converter bug: exact-wire fixture
reviews must trace the actual per-type writer and cursor, and never substitute
source review for the runtime red/green gate. The corrected fixtures are frozen
and under test; no generalized reader framework is introduced.

The corrected client run passes all five focused tests plus two packaging
checks, and all 437 canonical tests plus two packaging checks. Final workspace,
installed and archived JAR SHA is
`35f3eaa6c0b663be59df12cc422346756ee120fa60d4ebde15e0d80d7f98cf58`.
Twelve paired native LONG measurements completed identical 500-million-row
work and checksum. Instructions are effectively unchanged, with no repeated
elapsed/cycle regression at the predeclared threshold, major faults or cgroup
throttle/OOM deltas. Root independently validated the final study/comparison.
This establishes no detected LONG-to-LONG regression, not timestamp throughput.

Server focused (41) and canonical (305) gates pass. The unchanged original
333-test selector has zero failures and 29 errors; exact name comparison removes
only `testLongToTimestampColumn` from the previous 30. Both 1/2 and 42/43 seeded
68-test repeats pass. The final 1234567/7654321 repeat also passes all 68 tests.
Root checked selected XML seed/tmp properties, exact selector equality,
failure-name delta, installed/workspace/archive identities and both frozen
source manifests. Server appenders and original conversion-test source are
unchanged. Evidence and commands are in client/server
`core/target/schema-long-timestamp.2.26-{client,server}/run-manifest.md`.
No commit/push. Phase 3a is complete; 29 baseline errors and the wider inventory
remain, not a release-ready client.

Final independent SOL acceptance directly verifies all raw server summaries,
the exact one-name delta, selected repeat XML and final artifact/source hashes.
No remaining finding for this slice.

## D043 — STRING-to-timestamp: fixed parser, narrow timezone adapter

Status: iteration 2.27 accepted, 2026-09-11. Preparation and implementation
decisions follow chronologically; final acceptance evidence is at the end.
Root and two SOL audits traced the actual QWP STRING path
through `WalColumnarRowAppender.putStringToTimestampColumn` and
`MicrosFormatUtils.parseTimestamp`, then its generated lexical rules, calendar
computation, English locale tokens and timezone rules. Source and public-parser
probes are under `core/target/schema-string-timestamp-preparation/`.

Reject the initial false choice between a reduced ISO-only grammar and porting
the general compiler. Implement the eleven fixed formats in their real priority,
with exact relevant calendar helpers and a small zone adapter. The server accepts
hour 24 with modulo normalization even with nonzero minutes; fractional widths
are not a generic arbitrary-length ISO fraction. Preserve existing UTF-16-to-UTF-8
replacement followed by the server's ASCII-byte parsing view. Do not parse Java
Unicode input more broadly than the bytes the server receives.

The first zone proposal used JDK `ZoneRules.getOffset(Instant)` everywhere.
Root challenged its treatment of separately parsed years after microsecond
arithmetic wraps. The public-server probe disproves it:
`3509450-07-01 00:00:00.000Europe/Berlin` produces
`4929198542690304` micros on the server, while the naive JDK adapter gives
`4929202142690304`, a one-hour difference. Root independently replayed the probe.
The wrapped naive epoch is `4929205742690304`; server future rules still use
parsed year 3509450 rather than the wrapped instant's year.

Use public immutable JDK rule metadata and preserve the server's split: fixed
and historical offsets through the exact cutoff boundary; recurring transitions
after it, calculated with the separately parsed year and source-faithful
arithmetic. Retain rule order and UTC/STANDARD/WALL adjustments. This requires
only the relevant transition calculation, not its general timezone engine,
Unsafe access, per-year caches or `LocalDateTime` resolution. The English token
index must retain UTC-first insertion, first duplicate wins, longest-first
matching and the source's case behavior. The proposed hybrid still needs broad
differential validation; source review is not proof of complete equivalence.

Root also found that `Micros.yearMicros` is not uniformly wrapping arithmetic:
negative day counts whose product wraps positive return MIN. Near-MIN day-of-week
subtraction can itself wrap. Preserve both source helpers exactly and cover these
cases, rather than replacing them with Java Time's narrower calendar range.

Two-digit years use the same class-initialized reference-century calculation as
the server, not a per-value clock lookup. Named-zone meanings depend on JDK
locale/tzdb data. Different process initialization windows or runtime data can
differ; matching-runtime differential tests cannot prove cross-runtime equality.
Record this limitation explicitly. No new protocol fields, version negotiation,
reduced-year contract or SQL-engine change is proposed.

The 2.27 test preparation caught an incorrect provisional four-digit fraction
expectation before code activation: `.1234` is parsed as `.234000` seconds,
not `.123400`. The `SSz` pattern compiles as two `S` operations; the second is
greedy and overwrites the first millisecond value. This behavior was already
visible in the original public-parser probe and missed in its first summary.
The independent test author caught it when checking the proposed literal corpus.
Root verified the compiler/token explanation. Every provisional corpus row must
be checked against the actual compiled server parser before freezing; unsigned
fraction-width reasoning is insufficient. No production change has begun.

Preparation progress: root checked 45 deterministic vectors with the actual
compiled server parser after client UTF-8 normalization; zero differences.
Seven reference-century/timezone vectors are separate runtime-dependent cases.
The fixed-width number parser is reusable from client Numbers: signs and internal
underscores are allowed; it is not a digit-only parser. Greedy numeric fields
have different rules and need narrow source copies. Parsed non-null text can
produce raw MIN with a present wire value and SQL NULL; Java-null strings instead
write the null bitmap. Do not import the typed-LONG source-null policy here.

Root caught and corrected test-only preparation mistakes before activation:
a nano lifecycle expected 1 instead of 1000; a native VARCHAR fixture treated
its int32 offsets as int64 values; the standalone legacy encoder fixture
incorrectly assumed Sender's delta-dictionary prefix. The chosen encoder entry
point matters as much as its per-type writer. The two-character greedy year
`-1` also belongs with reference-century vectors, not fixed 1999 literals.

The independent hybrid-zone experiment uses public server calendar helpers to
isolate the adapter: all 604 zone IDs and 34,885 deterministic probes agree,
with checksum 6947487320943606609. The deliberately wrong all-Instant control
has 512 mismatches. Root read and independently replayed the diagnostic. This
does not prove the new client parser, locale tokens or copied calendar helpers;
their final binding/encoder differential remains required.

The fresh native STRING-to-VARCHAR baseline is measured and independently
validated before production edits: six identical-checksum 100-million-row runs,
12 copied bytes per row, fixed 4096-row resets, no new major faults or cgroup
throttle/OOM errors. The measurement is an existing-path regression reference,
not a timestamp conversion benchmark. Test-only activation and red gates follow;
production remains frozen at the accepted 2.26 binding/JAR.

Red gates complete on that frozen implementation. Client: five tests, one
failure and three errors, all due to missing STRING-to-timestamp conversion;
the native VARCHAR/inference/designated guard passes. Server: six tests, four
missing-conversion errors; independent legacy VARCHAR wire/SQL characterization
and public designated-timestamp rejection/recovery both pass. No fixture or
compile failure remains. Saved red XML/logs are under the 2.27 client/server
target directories. Authoring the narrowly scoped parser may now proceed.

The corpus grew to 110 literal cases, checked against the actual server parser
after client UTF-8 normalization (zero differences), plus seven runtime inputs
exercised with both units. Extreme micros are not arbitrary representable longs:
the only grammar reaching them has millisecond precision, so wrapping results
are multiples of eight. MAX-7 is reachable; MAX through MAX-6 are not. Tests use
source-proven reachable strings, saturation/wrap boundaries and exact nano
conversion limits rather than inventing impossible MAX fixtures.

The initial candidate passed the focused client tests but failed broader public
binding/encoder comparison. Root found the missing last fixed-SSS fallback:
`.+12Z`, `.1_2Z` and `.10000` are accepted through fixed integer parsing and
numeric-zone parsing. The fix adds that attempt after the existing greedy
forms, without changing their priority. Expanded ISO-week testing then found
1297-W01 and 1697-W01 differing by one day. The server retains the originally
parsed year's leap flag after adjusting the ISO-week year; the client had
recomputed it. Removing that recomputation preserves source behavior. Both
findings now have shared literal regressions for both timestamp units.

Validation also caught a diagnostic-fixture error: the scratch encoder reader
assumed Sender's dictionary prefix and a bitmap-length field. Standalone
encodeSchema has neither. Root rejected that run, checked the actual writer,
and required the corrected diagnostic to decode complete frames through EOF.
The failed log remains saved. An independent hour-24 review finding was withdrawn
after reading the prior normalization in the server's compute method; no
production change was made for it. Reviews are inputs, not acceptance evidence.

Final parser hash `8c8999302508f7758f564cab5df3b12ba1d3d527206faeda88f9e1d9b1a8399d`
and binding hash `71a53f5fe783551819f5277b76767fedfb506b90c85b932ac90fbd8bd33f4d78`
are frozen for final gates. The public binding/encoder differential passes
214,030 cases in each fresh en_US/tr_TR JVM, including all four-digit ISO-week
years, named-zone aliases, transitions, fraction/sign combinations and seeded
inputs. Checksums are 16401770755196548037 and 7304463666663708165 respectively.
Locale-dependent acceptance differs as it does on the server; each run compares
matched runtime data. Focused client validation passes five tests plus two
packaging checks. Canonical, installed-server and native-path performance gates
remain pending; this is not yet an accepted conversion checkpoint.

Final functional gates now pass 442 client tests plus two packaging checks,
47 focused server tests and 311 server regressions. The unchanged original
333-test selector reports zero failures and 28 errors; exact name comparison
removes only STRING-to-TIMESTAMP from the previous 29. Seeded recovery repeats
are still required before acceptance.

The final native STRING-to-VARCHAR performance comparison completes all twelve
100-million-row runs with identical byte/checksum verification. Instructions
are effectively unchanged (-0.01%); basic cycles and elapsed consistently rise
about 2–3%. This passes the predeclared 5% rejection threshold but must not be
called unchanged performance. Variable pipeline metrics do not identify a
hardware cause. No new major faults, throttle/OOM events or visible kernel
warnings occurred. Root read the measurements and independently validated the
study. This guard does not measure timestamp-parser or ingestion throughput.

Final acceptance: all three seeded repeats pass 74 tests each (1/2, 42/43,
1234567/7654321). Root verifies the selected XML seed and project-local tmp
properties after every run, the exact one-name diagnostic delta, mirrored
corpora, frozen source hashes and installed JAR. The final focused gate also
passes after strengthening public error-message context assertions. Accepted
checkpoint: 28 baseline errors remain; the wider conversion inventory and
release gates are still incomplete. Exact commands, failed diagnostics and
frozen identities are in the 2.27 client/server run manifests.

Final independent SOL audit confirms the same counts, one-name removal,
standalone installed JAR classpath, all three seed/tmp pairs and frozen hashes.
Both run manifests are updated from pending to accepted; no review finding
remains for this slice.

## D044 — Timestamp-to-text: approved checked normalization

Status: accepted in iteration 2.28, 2026-09-11. The initial
preparation and approval follow chronologically. Root and SOL source audits confirm both
text targets use the server's millisecond formatter; nano source counts divide
by 1000 toward zero first. Instant's legacy source precision is micros, even
when it contains a finer fractional part. Prefer one schema-only formatter
over a general format compiler or refactoring the now-tested parser.

Root rejected an initial null-policy finding based on the wrong cursor.
QwpTableBlockCursor always uses QwpTimestampColumnCursor for both timestamp
types, regardless of Gorilla mode. That cursor never infers null from MIN;
only the bitmap decides nullness. A standalone unflagged encoder/server-cursor
probe confirms present MICRO MIN becomes empty text, present NANO MIN becomes
1677-09-21T00:12:43.145Z, and bitmap omissions remain null. Preserve these
semantics; no new timestamp-to-text null-policy decision is needed. The probe
uses the public encoder's Gorilla-advertised, uncompressed-column fallback;
it does not claim a separately configured Gorilla-disabled Sender test.
Its literal output also distinguishes year -1 (`000-1`) from year zero/one
(`0001`); summaries of helpers are not a substitute for full formatter output.

D021 approved range checking for timestamp targets, not text targets. Legacy
larger-unit and Instant lowering can wrap into unrelated dates. Root recommends
checked conversion into the existing source precision before formatting, while
leaving legacy mode unchanged, and has requested this narrow user decision.
Do not activate the new converter or assume approval while it is pending.
Read-only preparation is under
core/target/schema-timestamp-text.2.28-client/preparation/; it is not an accepted
conversion or a real-server ingestion test.
Root independently replays the probe against the frozen installed 2.27 client
JAR: exit zero and byte-identical literal output. The pending decision remains
timestamp-to-text overflow, not timestamp null handling.

The user explicitly approved rejection of out-of-range timestamps written to
STRING/VARCHAR. Normalize primitive NANOS by division toward zero; convert
MICROS through DAYS with checked microsecond arithmetic. Convert Instant with
the existing final-value-safe helper (including its negative endpoint handling)
and existing microsecond rounding. Reject invalid units and null Instant as
before; ignore duplicates before conversion or argument validation. Preserve
the source timestamp MIN behavior described above. Legacy code and server/SQL
production code remain outside this increment.

Implementation starts with public client/server test preparation and a fresh
native-timestamp performance baseline. Production remains frozen at accepted
2.27 until the baseline gate and red tests are checked. Prefer a small separate
schema formatter and reuse timestampTextSink, whose producer-owned ASCII view
can be copied directly into the existing VARCHAR buffer. Do not add another
scratch field, a compiler, send-time transformation or a new replay path without
evidence that the existing structure is insufficient.

The pre-edit native MICRO baseline passes the hardware-study gate: six fixed
500-million-row runs have identical checksum 3807237185703477635. Basic medians
are 6.956 seconds, 76.790 cycles/row and 550.126 instructions/row. The comparison
will reject a repeatable 5% cycles/instructions increase; this is a native-path
regression guard, not a text-conversion throughput claim.

Root validates all 63 shared literals independently against checked BigInteger
normalization and the public server formatter (checksum 13924035717609678081).
The test-only red gate reports five client tests (one failure/four unsupported
conversion errors) and four server tests (one failure/two unsupported conversion
errors); the independent legacy ingestion method passes. Native generation
encoding succeeds before the expected text rebind error. Production is now
authorized for this increment.

Preparation review rejected test defects before implementation: an invalid UUID
setup value, a misplaced catch, and a scratch oracle/reader that incorrectly
treated empty timestamp text as NULL and invented VARCHAR framing fields.
Corrected tests and the oracle read the actual writer and cursor contracts;
failed diagnostics remain saved. Review summaries alone are not acceptance.

The production review caught an incorrect epoch-day constant (719528 rather
than the server's 719527) and a Java constant forward reference before the
candidate gate. Both were corrected. Target validation now precedes a simple
NANO-versus-micro conversion branch; there is no repeated text-target conversion
decision or new conversion framework. The formatter has one fixed presentation
and reuses the existing producer-owned UTF-8 sink.

Frozen candidate binding fb36c954 and formatter c99de83d pass root and independent
SOL source review. Client focused validation passes five tests plus two packaging
checks; canonical validation passes 447 plus two. Installed/workspace/archive JAR
SHA256 is bc65098cf79eb28baeeb63ab1617c6479e34ff75302f1fad750161f33139252a.
The public binding/encoder differential checks 160,202 conversions against the
independent server formatter and BigInteger range oracle: zero mismatches,
checksum 4937659523843806437. Server and performance gates remain pending.

Focused server validation now passes 53 tests, including the new real-Sender
corpus and independent legacy MIN/bitmap ingestion. The original conversion
tests and both server appenders retain their 2.27 hashes.

The native MICRO A/B guard passes all twelve fixed 500-million-row runs with
the baseline checksum. Basic paired cycles differ by +0.432%, -1.091%, -0.340%;
instructions are effectively equal. No repeatable regression is detected on
this path. This does not measure new timestamp-text or overall ingestion
throughput. Final server regression and repeat gates remain pending.

The final server regression gate passes 317 tests. The literal original 333-test
selector reports zero failures and 26 errors; name-by-name comparison removes
only testTimestampToStringColumn and testTimestampToVarcharColumn from the prior
28. No new failing method appears. Three seeded recovery repeats are running
before acceptance; the broader conversion inventory remains a release gate.

Final acceptance: all three seeded repeats pass 80 tests each (1/2, 42/43,
1234567/7654321). Root checks every run's raw result, archived seed/tmp evidence,
unchanged original failure selector and exact two-name removal. Both frozen
source manifests and installed/archive JAR remain consistent. The hardware
study independently validates. Accepted checkpoint: 26 original errors remain;
this is not full conversion or release acceptance. Exact commands and evidence
are in the 2.28 client/server run manifests.

Final independent SOL acceptance confirms all raw totals, exact failure-name
delta, corpus/source/JAR hashes and all 48 selected repeat XML files. Root also
parses those 48 reports independently: 16 classes and 80 passing tests per run,
with exact seeds, project-local tmp and installed client JAR. Both manifests
now mark phase 3c accepted; no review finding remains for this increment.

Next-slice preparation is read-only: phase 4a adds CHAR identity and STRING-to-CHAR,
not CHAR-to-text. Preserve first-character truncation rather than introducing
new one-character validation. Public UTF-16 replacement must match actual server
UTF-8 decoding: BMP first characters survive, supplementary-first becomes NUL,
and malformed surrogates become '?'. Reuse short storage; add the missing CHAR
wire-type mapping before activating the native setter. No new sink, formatter,
protocol parameters or conversion framework is needed. Implementation and
new tests are not yet started for this slice.

## D045 — CHAR identity and STRING-to-CHAR

Status: iteration 2.29 accepted, 2026-09-11. The user approved the next slice.
Root and SOL reread the server CHAR dispatch, first-character decoder, client
UTF-16 replacement, existing short storage and Sender rollback before edits.
Keep native CHAR as a two-byte code unit. STRING input takes the first BMP
character; trailing text is ignored. Empty, NUL and supplementary-first inputs
produce a present code unit zero. For malformed UTF-16, schema lowering produces
the same '?' as existing client UTF-8 replacement followed by server decoding;
schema mode writes the resulting CHAR directly, not replacement UTF-8 bytes.
Java null remains an effective bitmap-null write. CHAR's stored null sentinel
is zero, so present empty/NUL/supplementary-first inputs also read as SQL NULL.
Malformed raw UTF-8 yields zero on the legacy server path; it is not the same
case as malformed UTF-16 passed to the public string setter.

Reuse targetColumn's exact-type helper for the native setter, add the missing
CHAR wire mapping, and use a small first-character conversion without allocating
another sink. Do not add CHAR-to-text, SQL/server changes, protocol metadata,
send-time conversion, a new replay path or a conversion framework.

Public Sender tests provide the pre-edit red gate. A component test that calls
the new binding method is prepared outside the test source tree until that
method exists; do not add a temporary production stub merely to compile a red
test. A fresh native STRING-to-VARCHAR performance baseline precedes production
edits. Root owns serialized Maven and acceptance; SOL owns bounded test,
implementation, independent-review and performance tasks.

The pre-edit native string baseline passes the validated hardware study: six
100-million-row runs each verify 1.2 billion bytes and checksum
15640344951909530213. Basic medians are 3.340 seconds, 186.271 cycles/row and
916.699 instructions/row. The predeclared guard rejects repeatable 5% normalized
regressions; it makes no claim about new CHAR-conversion throughput.

Root independently validates all 23 corpus literals against the actual client
UTF-8 sink and public server first-character decoder (checksum
1837560376326485833). Native CHAR expectations come from input code units, not
the expected-result field. Client red reports two tests with one failure and
one missing-conversion error; server red reports four tests with three missing-
conversion errors and the independent legacy test passing. Production is now
authorized. Preparation review corrected circular test inputs, impossible
designated metadata, incorrect error reasons, raw frame/ACK assumptions and a
scratch reader's nonexistent CHAR encoding byte before candidate validation.

The first focused candidate gate reports five tests with one failure: native
CHAR did not refresh a cached schema on rejection. Initial review incorrectly
attributed this to the mock fixture. Root traced the public setter and found
the missing LineSenderSchemaException catch used by every previously activated
setter. Keep the original failing generation test and add that same existing
rollback-and-refresh handling; do not weaken the test or change cache freshness.
The initial source acceptance is superseded pending this correction and rerun.

The corrected Sender (4904b4ef) passes the original CHAR-triggered refresh test;
independent review confirms the standard catch is now present. Binding remains
09170988. Focused client validation passes five tests plus two packaging checks.
The frozen candidate JAR is
edb2a9dbae3ba64d0ab3caf07309d4755187eabbfe676ad7a1fc36aa43642c6a.
Root's public binding/encoder differential checks 65,549 STRING inputs against
the actual client UTF-8 sink and public server decoder: zero mismatches,
checksum 13914181573124632075. Performance and remaining regression gates are
still pending; this is not acceptance.

The existing native STRING-to-VARCHAR hardware guard passes all twelve fixed-
work A/B runs. Paired basic cycles change by +0.659%, +0.795%, +0.114%, with
instructions effectively unchanged and identical output checksums. No
repeatable regression reaches the declared 5% guard. This is not a CHAR
conversion throughput measurement; full functional validation remains pending.

Client canonical validation passes 452 tests plus two packaging checks. Install
is green; workspace, installed and archived candidate JAR hashes are identical.
Root reruns the hardware-study validator successfully. Server validation now
uses that exact installed artifact, with no overlapping client rebuild.

The first focused server candidate run reports 59 tests with one test-query
error: the inferred-table assertion used `ts`, but auto-creation names the
designated column `timestamp`. Both writes and ACKs succeeded, as did the two
original CHAR conversion methods. Correct only the test query/header; preserve
the failed run as evidence. No server or SQL production code changes.

With the inferred-column assertion corrected, all 59 focused server tests pass.
The final server fixture hash is db3cbf8c. The wider regression gate passes
323 tests. The original 333-test selector is unchanged; its diagnostic and
three seeded repeats are still pending.

The unchanged original 333-test diagnostic now reports zero failures and 24
errors. Root compares method names against 2.28: only testCharToCharColumn and
testStringToCharColumn are removed, with no additions. Three seeded repeats
are running before final acceptance.

Final root acceptance: all three seeded repeats pass 86 tests each (1/2, 42/43,
1234567/7654321). Root parses all 51 selected XML reports independently: 17
classes per run, exact seeds and project-local tmp, standalone installed client
JAR, no failures/errors/skips. Source/corpus/JAR hashes remain frozen. The
accepted checkpoint has 24 original errors remaining, not full release coverage.
Exact commands, raw logs and hashes are in the 2.29 client/server run manifests.

Read-only next-slice preparation confirms STRING-to-LONG256 can use the existing
32-byte append storage and four local longs; do not add a retained scratch
object or activate native LONG256. Root and SOL checked Numbers.extractLong256,
parseLong256, Long256FromCharSequenceDecoder and putStringToLong256Column:
lowercase 0x, 2–64 hex digits in complete pairs, right-to-left limbs, and rejection
of the four-MIN-limb literal. Java null/omission remains null. These are existing
server rules, not a new policy choice. No implementation has started for 4b.

Final independent SOL acceptance confirms all raw totals, the exact two-name
failure reduction, all 51 selected repeat reports, frozen source/JAR/corpus
hashes and revision 47's bounded acceptance claims. No review blocker remains.

## D046 — STRING-to-LONG256

Status: iteration 2.30 accepted, 2026-09-11. The user approved proceeding.
Root and SOL reread the server target dispatch, both extractLong256 overloads,
parseLong256, the four-word decoder and the concrete WAL appender. Preserve the
lowercase 0x prefix, complete byte pairs, 2–64 case-insensitive hex digits and
right-to-left limbs. Java null/omission remains null; a non-null literal with
four Long.MIN_VALUE limbs is INVALID_VALUE, as on the server. No policy change.

Add only the STRING target arm, LONG256 wire mapping and a narrow append helper
using four local longs and existing parseHexLong/addLong256. No retained scratch
object, native LONG256 setter activation, schema state, send-time conversion,
server production or SQL-engine change. Existing stringColumn already has the
typed rollback/refresh handling missing from CHAR's initial activation.

SOL prepares shared literal vectors, exact public-wire component tests, public
Sender rollback/generation tests and real-server/independent legacy E2Es. Root
owns serialized red, regression, install and seeded repeat gates. A fresh native
STRING-to-VARCHAR baseline and validated hardware study precede production
edits; it is a regression guard, not a LONG256 throughput claim.

Fresh pre-edit measurements pass the hardware study, independently validated
by root. Six 100-million-row runs verify 1.2 billion bytes and checksum
15640344951909530213 each. Basic median cycles/row 184.538 and instructions/row
916.792; no major faults or new throttle/OOM events. Baseline JAR edb2a9db and
binding 09170988 are archived before production changes.

Pre-red root review catches two test assumptions: parsing hex as LONG yields
INVALID_VALUE, not UNSUPPORTED_FEATURE; a cached VARCHAR accepts arbitrary
strings and cannot trigger rejection-driven refresh. The generation test will
instead retain a LONG256 block, switch to VARCHAR and use an invalid LONG256
string to trigger refresh through the same public setter. Keep the existing
cache policy; correct the test sequence, not production.

Client red confirms six tests with four missing-conversion errors and no
failures. Root independently checks the 23 shared literals against public
server parsing: 10 values, one Java null, 12 invalid, zero mismatches (checksum
12475823559777660557). Review corrected a scratch oracle that had mistaken the
NULL marker for literal invalid text; it now checks actual Java null and exact
bitmap-only encoding and surfaces unexpected parser failures.

The initial server red has two missing-conversion errors and one legacy fixture
failure. Correct the raw column count to include the unnamed timestamp, and
retain whole-byte padding in the expected hex text. These are test corrections;
the legacy oracle must pass before production edits. The first red log/XML is
retained separately.

The corrected server red reports three tests with two missing-conversion errors
and the independent legacy test passing, including rejection of odd-length and
four-MIN literals. Root authorizes the Binding-only production change after
both behavioral red gates, the corrected literal oracle and the validated
pre-edit performance study. Existing Sender lifecycle code remains unchanged.

Independent source review accepts the bounded parser logic. The first focused
build catches the existing error helper's boxed Integer parameter: new calls
must cast the short ColumnType constant to int, as neighboring helpers do.
Correct only those calls and rerun before freezing a candidate artifact.

The corrected Binding (a57ce9ae) passes six focused client tests plus two
packaging checks; independent source review confirms only the two casts changed.
Frozen candidate JAR:
37169edc56700c7e34ec357d0eb55d8b25886731faf8a65491f7d80fdd4a270a.
Root's public binding/encoder differential completes 40,055 cases with zero
mismatches against actual client UTF-8 encoding and public server parsing.
Performance A/B and broader client/server gates remain pending.

The native STRING-to-VARCHAR A/B guard passes all twelve fixed-work runs with
matching output checksums. Paired cycles change by +1.271%, +1.152%, +0.055%;
instructions change by about -0.01%. No repeatable change reaches the declared
5% regression threshold. This does not measure STRING-to-LONG256 throughput.
Broader functional validation now resumes with the frozen candidate.

Client canonical passes 458 tests plus two packaging checks, and install passes.
Workspace, installed and archived JAR hashes match 37169edc. Root independently
reruns the final hardware-study validator successfully. Real-server validation
now uses this exact installed artifact with no overlapping client rebuild.

All 63 focused server tests pass, including the original STRING-to-LONG256
method, shared value/null/invalid corpus, whole-row recovery and independent
legacy value/null/omission/rejection checks. The broader server gate is running.

The wider server regression gate passes 327 tests. The original 333-test
selector is unchanged and now running; final acceptance still requires its
exact failure-name comparison and three seeded recovery repeats.

The unchanged original 333-test diagnostic reports zero failures and 23 errors.
Root compares names against 2.29: only testStringToLong256Column is removed;
there are no additions. Three seeded recovery runs remain before acceptance.

Final root acceptance: all three seeded repeats pass 90 tests each (1/2, 42/43,
1234567/7654321). Root independently parses the 54 selected XML reports: 18
classes per run, exact seeds and project-local temporary storage, standalone
installed client JAR, zero failures/errors/skips. Frozen production/test/corpus
hashes remain unchanged. Client canonical is 458 plus two packaging checks;
server focused is 63, canonical 327, and the original diagnostic remains
333 tests with 23 errors. Revision 48 and the remaining-failures plan record
phase 4b acceptance, not release acceptance. Exact commands, raw logs, hashes
and the selected-report audit are in the 2.30 client/server run manifests.

Read-only preparation for phase 4c traces the server's string-to-geohash parser,
cursor and target precision checks against the existing client buffer/writer.
Precision is already packed in the full schema type; no protocol parameter is
needed. A null-only column still needs precision initialized without a value
append. Empty text takes the present all-ones path, whereas Java null uses the
bitmap; both read as SQL NULL. Keep this distinction in exact wire tests and
reuse the existing precision field. Native geohash setter activation remains
separate work. No phase 4c implementation has started.

Final independent SOL acceptance confirms all 54 selected repeat reports,
raw totals, exact seeds/tmp/installed-JAR provenance and frozen fixture/corpus
hashes. Revision 48, the plan and D046 consistently bound acceptance to phase
4b with 23 original errors remaining. Native LONG256 remains unimplemented;
performance evidence is only the native STRING-to-VARCHAR regression guard.
No review blocker remains.

## D047 — STRING-to-GEOHASH

Status: iteration 2.31 accepted, 2026-09-11. The user approved continuing.
Scope is phase 4c only: string conversion to existing GEOHASH targets using
their packed schema precision. Native geohash setters and decimals stay separate.
Root delegates client tests/implementation, server E2Es, and performance plus
adversarial review to SOL. Root owns serialized Maven/install gates and records
decisions. No server production or SQL-engine change is planned.

Revalidation corrects D046's read-only next-slice recommendation. Server VARCHAR
conversion maps empty text to the target's -1 null sentinel at every precision.
But native GEOHASH reads only ceil(bits/8) bytes: without a bitmap, a valid
all-ones value at byte-aligned precisions becomes null; with a bitmap, a packed
empty sentinel can zero-extend to a non-null value in larger target storage.
Root and SOL independently traced QwpGeoHashColumnCursor.advanceRow/readValue,
WalColumnarRowAppender.putStringToGeoHashColumn/putGeoHashColumn and the type's
storage-width calculation. Source presence cannot simply be copied to target
presence. The earlier claim that native present -1 always stores as null was
wrong. Add real-server characterization before production edits.

Recommended minimal representation: empty text uses addNull, and schema-directed
GEOHASH columns always emit the explicit null bitmap, even when all rows have
values. A private writer argument can distinguish the existing schema/legacy
encoding entrypoints without new retained state or scanning values. Cost is one
bit per row; legacy encoding stays unchanged. This is null framing, not a
send-time value conversion. Initialize precision through the existing column
field, factoring the current addGeoHash validation. Preserve full type flags
and existing timestamp handling; do not globally switch all dispatch to tagOf.

The hardware-first performance skill requires a fresh pre-edit regression
baseline for the shared string/buffer/writer path. SOL has completed six fixed
100-million-row native STRING-to-VARCHAR runs with matching 1.2-billion-byte
checksums; the validated study and behavioral red gates still precede production
authorization. This guard does not measure GEOHASH conversion throughput.

Root independently validates the pre-edit study and all 18 shared literal
vectors against actual client UTF-8 plus the current server's public parser:
13 values, two nulls, three invalid, zero mismatches. The initial client red
reports four missing-conversion errors and no failures. Review then corrects
test-only assumptions before the final red: at eight-bit precision `04`, not
`01`, truncates to 1; ordinary encode(table) has no symbol-delta prefix. Expand
permanent tests to all 60 precisions rather than only one 60-bit target.

Two boundary checks refine the brief without a new abstraction. First, the
existing append benchmark does not call the encoder; extend the pre-edit
measurement to cover the shared writer separately. Second, the low-level
schema encoder accepts manually constructed non-nullable buffers whose bitmap
address is zero. A forced bitmap must be limited to existing nullable storage;
schema bindings always use that storage. Keep manual sentinel-mode encoding
unchanged and test it through the public encoder, avoiding a new allocation or
a null-address copy.

The final corrected client red reports five missing-conversion errors, no
failures. Server red reports three tests with two missing-conversion errors;
the independent legacy VARCHAR and native GEOHASH characterization passes.
This confirms both packed-width traps at runtime before production changes.
Root also validates the extended two-card performance study: separate append
and encodeSchema baselines, three basic and three pipeline runs each. The
encoding runs verify checksum 114033733800999820 with zero major faults.

Root authorizes the narrow production change after these gates. The existing
ColumnBuffer.usesNullBitmap() getter expresses the nullable-storage condition
directly; reuse it rather than inspecting pointers or adding a field. Keep
the current full-type switches for all non-geohash targets. Review, focused
green, regressions, A/B, original failure-name delta and repeats remain ahead.

The first candidate passes five focused client tests plus two packaging checks,
and 7,210 public differential cases across all precisions (6,870 accepted, 340
rejected, zero mismatches). However, the full 463-test client gate finds three
real regressions: existing flagged STRING/BOOLEAN/LONG schema types are mistaken
for geohashes and produce an untyped zero-precision error. Source-only acceptance
is withdrawn. Preserve these existing tests; recognize a GEOHASH target only
when its precision is 1–60 and its full type exactly matches the canonical
packed type for that precision. Unknown flags/tags must keep the existing typed
UNSUPPORTED_FEATURE path. No generic exception wrapper or new schema state is
needed. Candidate 41722b7b and the failed full-suite evidence are archived as
initial, not accepted; A/B remains on hold until the corrected candidate.

The server recovery fixture also had an insufficient one-character zero at
eight-bit precision. Change it to `00`, assert the failed row's INVALID_VALUE,
and assert exact A/C/D values and nullness. The red-tested fixture was not copied
before this edit; its archive was reconstructed by reversing exactly those
three edits and verifying the original SHA-256 77ab62ac. Current fixture SHA is
6bd7f662. This is an exact reconstruction, not a pre-edit capture.

The corrected Binding (b85add96) recognizes only complete canonical GEOHASH
types. The original three regression tests are unchanged; new malformed-type
vectors add zero/61-bit, wrong-tag and unknown-flag coverage. Six focused client
tests plus two packaging checks pass. Independent SOL source review accepts
the correction. Frozen candidate JAR:
c75a826aeb85e9e5f3728d8bd4284c8cd62aab18b8cc3d3c9829e686cc4396c4.
Root reruns the 7,210-case public differential against current server classes:
6,870 accepted, 340 rejected, zero mismatches, checksum 11748646508706949247.
The full client gate is rerunning before install and A/B.

The corrected full client gate passes 464 tests plus two packaging checks.
Install passes, and workspace/archived/installed JARs all match c75a826a.
The two-path A/B window now runs exclusively, with no overlapping Maven work.

All 24 counterbalanced A/B runs pass the declared repeatable 5% guard, with
matching fixed-work checksums. The cost is small but measured: native string
append instructions increase about 0.437%, cycles 1.457–1.740%, elapsed
1.461–1.799%; schema encoding instructions increase about 0.472%, cycles
0.131–1.091%, elapsed 0.166–1.148%. Do not describe this as unchanged performance
or infer GEOHASH/ingestion throughput. Root independently validates the final
two-card study. No new throttle/OOM/major-fault or visible kernel-error evidence.

The final installed candidate passes all 67 focused server tests, including the
original STRING-to-GEOHASH case, all 60 target precisions, null-only following
batches, invalid row recovery and the independent legacy/native characterizations.
The 331-test server regression gate is running before the unchanged original
diagnostic and three seeded repeats. Frozen source/test/corpus hashes still match.

The wider server gate passes all 331 tests. The unchanged original 333-test
diagnostic is now running; acceptance still requires its exact failure-name
comparison and three seeded recovery repeats.

The unchanged original 333-test diagnostic reports zero failures and 22 errors.
Root compares raw failing method names against 2.30: only
testStringToGeoHashColumn is removed, with no additions. The remaining original
cases are all decimal conversions. Three seeded recovery repeats are running.

Read-only phase 5a preparation recommends all three typed decimal inputs as one
coherent increment, with exact target precision/scale and existing wire widths.
Root rejects a second binding-owned Decimal256 scratch: ColumnBuffer already
owns one that can normalize a copy, validate it and append atomically. Preserve
public decimal-null no-op behavior and caller input objects. Internal decimal
word order and wire order differ and need exact independent assertions.
The next slice must also preserve target scale across reset and failed-row
cleanup when an emitted column is later all-null through omission, while legacy
columns retain their first-input-scale-per-batch behavior. The remaining-failures
plan records these tests; no decimal implementation is included in 2.31.

Final root acceptance: all three seeded repeats pass 94 tests each (1/2, 42/43,
1234567/7654321). Root independently audits all 57 selected XML reports: 19
classes per run, exact seeds and project-local temporary storage, standalone
installed client JAR and zero failures/errors/skips. Production, server fixture
and corpus hashes remain frozen. A final whitespace-only cleanup in the new
client component test is followed by a fresh six-test plus two-packaging-check
run; the client JAR remains byte-identical. The earlier tested source hash is
preserved in validated-tests-before-formatting.sha256.

Revision 49 and the plan accept phase 4c, with 22 original decimal errors still
remaining. They do not imply complete conversion coverage or release readiness.
Exact commands, raw logs, hashes and selected-report audit are in the 2.31
client/server run manifests. No commit, server production or SQL-engine change.

Final independent SOL acceptance confirms all 57 selected repeat reports,
seeds/tmp/installed-artifact provenance, unchanged final JAR and bounded revision
49 claims. It reruns the hardware-study validator and verifies the measured
cost disclosure, native-setter exclusion, 22 remaining errors and final
whitespace-only test check. No review blocker remains.

## D048 — Native decimal-to-decimal conversion

Status: iteration 2.32 and phase 5a accepted, 2026-09-11. Revision 50 records
the bounded scope: Decimal64, Decimal128 and Decimal256 public inputs now reach
DECIMAL8/16/32/64/128/256 targets. These are 18 logical pairs over nine physical
source-width/target-width paths because the three small target types use
DECIMAL64 wire storage. Phases 5b–5e remain; this is neither complete conversion
coverage nor release readiness.

The implementation reuses the ColumnBuffer-owned Decimal256 scratch. It copies
the caller value, applies exact rescaling, checks declared target precision and
the selected physical storage width, then appends direct target-width limbs.
Precision loss, scale-up overflow, target-precision overflow and storage-width
overflow fail locally without mutating the caller. The target scale survives
buffer reset and failed-row cleanup. Exact component and public-wire assertions
cover high-to-low internal limbs versus low-to-high wire order, row rollback,
completed-row preservation and adoption of a later schema generation.

Public typed-decimal null/sentinel inputs retain their established no-op behavior.
Confirmed MISSING columns and legacy mode keep source-width inference and the
legacy first-input-scale-per-batch rule; schema-directed known targets do not.
Server characterization confirms decimal cursor nullness is bitmap-only: a set
bitmap bit is null, while raw sentinel limb patterns without a bitmap remain
supplied values entering conversion. This characterization does not activate
decimal text output or the LONG/STRING/FLOAT/DOUBLE decimal inputs.

The shared client/server corpus has 60 data rows: 45 accepted values and 15
invalid values, covering all 18 logical pairs. Both copies have SHA-256
`3819694736f54de4d2f2f396b09ff487667051c6045b34cc6d7579552551d11d`.
Client focused validation passes eight tests plus two packaging checks; canonical
validation passes 472 tests plus two packaging checks. Frozen candidate JAR
SHA-256: `1e814a2f82836b5c9ac04cf7b5674e56f1b939440df486ae6628ebb1b8eebb08`.

Server focused validation passes seven tests and the canonical gate passes 338.
The unchanged original 333-test diagnostic reports zero failures and 18 errors:
only the four intended decimal-to-decimal methods disappeared from the prior 22,
with no additions. All three seeded repeats pass 101 tests each for seeds 1/2,
42/43 and 1234567/7654321. Root audits 20 archived XML reports per repeat with
zero failures, errors or skips.

The legacy-path regression guard passes all 12 balanced A/B runs with matching
fixed work and checksums. Paired instructions change by +0.001% to +0.002%; paired
cycles range from -0.47% to +1.99%, below the declared repeatable 5% rejection
threshold. A separate candidate-only composite covers all nine physical width
pairs in six runs of 20 million rows each: 180 million conversions and 3.36 GB
verified per run. Its basic medians are 32.91 ns, 182.03 cycles and 1140.18
instructions per value. This has no semantically matched schema baseline and is
absolute-cost evidence only; it supports no network, ingestion or general-
throughput claim. One 200-million-row sizing run and an interrupted follow-on
are excluded from the accepted measurements.

Residual P3: a scale-up overflow has the correct `INVALID_VALUE` reason and row
rollback, but its detail currently says `decimal value cannot be rescaled
exactly` instead of identifying overflow. Keep this as diagnostic wording debt;
do not add another hot-path branch solely to refine the message.

Final acceptance is limited to phase 5a. The remaining 18 original diagnostic
errors are phase 5b LONG-to-decimal, 5c STRING-to-decimal, 5d floating-to-decimal
and 5e native-decimal-to-text. No commit, server production or SQL-engine change
is included in this documentation update.

## D049 — LONG-to-decimal conversion

Status: iteration 2.33 and phase 5b accepted, 2026-09-11. Revision 51 records
the bounded scope: public `longColumn` now reaches every
DECIMAL8/16/32/64/128/256 target in schema mode. Phase 5a remains the decimal
foundation; phases 5c–5e remain. This is neither complete conversion coverage
nor release readiness.

Root and the SOL contract reviewer read the server's LONG dispatch, decimal
rescale/range helpers, decimal cursor and physical target selection before the
change. The contract is scale-zero signed input followed by exact multiplication
for a positive target scale, declared-precision validation and physical-width
validation. Small target classes use DECIMAL64 wire storage. Duplicate
suppression precedes conversion. Confirmed MISSING keeps inferred LONG.

`Long.MIN_VALUE` needs an explicit schema-mode rule because legacy fixed-width
sentinel handling changes when another row creates a null bitmap. The accepted
rule is stable source null: for a known decimal target it marks the decimal null
bitmap and first locks the target scale. A raw-frame real-server test preserves
and documents the legacy difference: without a bitmap the sentinel is null;
with a bitmap it is supplied as a value and can either fit or fail by target
precision. No legacy behavior was changed.

The implementation adds no Sender, protocol or server production state. It
initializes the ColumnBuffer-owned Decimal256 scratch from LONG at scale zero,
then shares phase 5a's exact rescale, precision/storage check and direct-width
append tail. A separate null helper only locks target scale before `addNull()`.
The common numeric-target predicate was deliberately not broadened because that
would silently admit decimal targets for unrelated source families.

The shared client/server corpus has 39 rows: 19 values, 14 invalid values and
six source-null cases across all six target classes. Both copies have SHA-256
`d9a47432a14583f8675b0035ab10c3ef9f36e78c9f4aab6a8634958eb5aafcb1`.
An independent arithmetic oracle confirms every outcome, SQL value and unsigned
low-to-high wire limb. Component tests assert target type, bitmap, target scale,
limbs, duplicate behavior, omission, whole-row rollback and reset. Public
Sender E2Es assert exact stored values and A/failed-B/C recovery.

The first schema-generation fixture was wrong: its proposed new LONG value was
also valid under the old DECIMAL(18,4) schema, so no refresh was justified.
Root replaced it with a value invalid under DECIMAL(18,4) but valid under
DECIMAL(20,5). The corrected test proves that the sealed old DECIMAL64(scale 4)
block remains unchanged while the later block uses DECIMAL128(scale 5). This
was a test-only correction; no production response to the false assumption was
added. A first raw legacy fixture also omitted designated timestamps; adding
them made the frame valid without changing the behavior under test.

Red evidence uses the frozen phase-5a client JAR. The four original
LONG-to-decimal methods all reject the missing conversion; the new public
fixture's two acceptance tests fail while its independent raw legacy
characterization passes. Final client focused validation passes three tests
plus two packaging checks; canonical validation passes 475 tests plus two
packaging checks. Install passes, and workspace, archived and installed client
JARs all have SHA-256
`41d137a1c2388ed21f776e372771785377d91c15a887d94f04e82c97f505d403`.

Final server focused validation passes seven tests and the canonical gate
passes 345. The unchanged original 333-test diagnostic reports zero failures
and 14 errors: exactly the four intended LONG-to-decimal methods disappeared
from phase 5a's 18, with no additions. Three seeded repeats pass 108 tests each
for seeds 1/2, 42/43 and 1234567/7654321; their selected XML reports preserve
the exact seed, project-local temporary directory, installed client artifact
and zero failures/errors/skips. No server production or SQL-engine file changed
for this increment.

The performance check targets the changed common LONG binding path, not decimal
ingestion. A pre-edit fixed-work study and 12 balanced control/candidate runs
each process 500 million decoded known-schema LONG-to-LONG writes with identical
checksum `2715042977495835685`. Paired instructions change by +0.003% to +0.004%;
paired cycles change by -0.164% to +2.237%, below the declared repeatable 5%
guard. All measured runs have zero major faults and 100% counter coverage, with
no run-window cgroup throttle/OOM delta. The hardware-study validator passes.
This supports only that no material regression was detected in LONG-to-LONG;
it makes no decimal, network, ingestion or general-throughput claim.

Final acceptance leaves 14 original diagnostic errors: STRING-to-decimal,
FLOAT/DOUBLE-to-decimal and native-decimal-to-text. No commit or push is part of
this checkpoint.

## D050 — STRING-to-decimal conversion

Status: iteration 2.34 and phase 5c accepted, 2026-09-11. Revision 52 records
the bounded scope: public `stringColumn` now reaches every
DECIMAL8/16/32/64/128/256 target in schema mode. Phases 5d–5e remain; this is
neither complete conversion coverage nor release readiness.

The server parser and all six decimal target paths were read before integration.
For a known decimal target, the client parses with that target's precision and
scale, `strict=false` and `lossy=false`, preserving the server grammar's exponent
underscores, then emits the QWP decimal wire type for the target storage class;
DECIMAL8/16/32/64 share QWP's DECIMAL64 wire type. The shared client
parser now uses the current server's leading-zero and full-scale precision
calculation. This broad precision-metadata correction required updating 19 stale
assertions in 16 existing tests; the parsed values and scales are unchanged.

The stable schema-mode null rule is independent of target width: Java null and
exact optionally signed `NaN` or `Infinity`, after supported suffix processing,
emit target-scale bitmap SQL null. Malformed special-value prefixes and extreme
nonzero integer-boundary exponents fail locally as `INVALID_VALUE`. This
avoids the server's small-decimal special-null narrowing bug. The exact-special,
precision and exponent corrections are global shared-parser changes, not only
schema-mode behavior: direct `Decimal64`, `Decimal128` and `Decimal256` string
parsing now rejects malformed special prefixes and reports extreme nonzero
exponents as `NumericException`. Confirmed MISSING and legacy Sender paths remain
VARCHAR and use server-side conversion.

The implementation reuses the existing per-column Decimal256 scratch and the
phase-5a precision/storage/direct-width append tail. Exact special checking is
corrected centrally according to the parser's documented contract. No separate
schema parser, conversion registry, retained state, protocol, server production,
SQL or send-time transformation was added.

The shared client/server corpus has 55 rows: 19 values, 24 invalid inputs and
12 nulls. Both copies have SHA-256
`67739711b320b07fd4d5eb72ecd19e14b5999f6a07e756ed83a847d809577568`.
Client red validation first failed eight tests. Final focused validation passes
eight tests plus two packaging checks, all-decimal validation passes 463 tests,
and the canonical gate passes 941. Candidate, archived and installed JARs have
SHA-256
`30e593b27a6eb71c022e198bb5e331af973af643e1fd3eedde8555c8d2d2112c`.
A 3,601,944-case differential passes ordinary server parity and records 72
deliberate malformed-prefix corrections.

Server focused validation passes seven tests and the canonical gate passes 352.
The unchanged original 56-test conversion class reports ten errors: exactly the
four STRING-to-decimal methods disappeared from phase 5b's 14, with no additions.
Three seeded repeats pass 115 tests and 22 selected XML reports each.

The performance check targets the changed native STRING binding path, not
decimal ingestion. Twelve balanced fixed-work native STRING-to-VARCHAR runs have
identical checksum `15640344951909530213`. Paired basic instructions change by
-0.001% to -0.000% and paired cycles by -1.028% to -0.007%, with no repeatable
5% regression. This makes no decimal, network, ingestion or general-throughput
claim.

Final acceptance leaves ten original errors: FLOAT/DOUBLE-to-decimal and
native-decimal-to-text. No commit or push is part of this checkpoint.

## D051 — FLOAT/DOUBLE-to-decimal conversion

Status: iteration 2.35 and phase 5d accepted, 2026-09-12. Revision 53 records
the bounded scope: public `floatColumn` and `doubleColumn` now reach every
DECIMAL8/16/32/64/128/256 target in schema mode. Phase 5e remains; this is
neither complete conversion coverage nor release readiness.

Root and two SOL reviewers traced the real server route before integration.
`WalColumnarRowAppender` reads FLOAT as a Java float and widens it to double;
both source widths then call `Numbers.doubleToDecimal(value, precision, scale,
false)`. That path formats the shortest round-tripping decimal and rejects a
natural scale above the target instead of rounding or truncating. Target
precision overflow also rejects. All six server target loops map NaN and both
infinities to decimal null. SQL casts use a different lossy setting and were not
used as the oracle.

The client follows that contract directly. FLOAT widens through the existing
double-valued binding path. Finite values reuse the schema-only Ryu formatter,
the shared decimal parser at the target precision/scale, the existing per-column
Decimal256 scratch and phase 5a's direct target-width append tail. Small decimal
targets use DECIMAL64 wire storage while retaining their declared precision and
scale. Non-finite values use the target-scale decimal bitmap-null helper.
Duplicate suppression still precedes conversion, and failures remain
nonretryable `INVALID_VALUE` errors that permit whole-row rollback. Confirmed
MISSING and legacy mode keep native FLOAT/DOUBLE tags and server-side conversion.
No parser, state, conversion registry, protocol, server production, SQL or
send-time transformation was added.

The first correct implementation placed the decimal branch in the common
floating numeric helper. Functional tests passed, but the hardware guard found
a real native-path cost: the helper grew from 311 to 411 bytecodes, exceeded
HotSpot's hot inlining threshold and increased instructions by about 4.7% for
DOUBLE-to-DOUBLE and 3.8% for FLOAT-to-FLOAT. The implementation was simplified
before acceptance: numeric targets retain their old branch and bytecode shape,
while decimal dispatch lives at the top of the existing cold nonnumeric helper.
No production guard or abstraction was added.

The final fixed-work A/B study contains six counterbalanced control/candidate
pairs for each native path, 500 million writes per run. DOUBLE checksum is
`11101713426464862245`; median paired elapsed, cycles and instructions changes
are -1.36%, -1.04% and +0.0037%. FLOAT checksum is `3646854813611232293`;
medians are -0.95%, -0.83% and +0.0001%. All 24 runs have identical work and
checksums, zero major faults, complete counters and no cgroup throttle/OOM delta.
Both hardware-study validators pass. This supports only that no material native
binding regression was detected; it is not decimal, allocation, network,
ingestion or general-throughput evidence.

The server-generated client/server corpus has 60 rows: 24 FLOAT and 36 DOUBLE;
37 values, 17 invalid values and six nulls. It covers all twelve source/target-
class pairs, exact wire width, scale and limbs, no-rounding boundaries, widened
float `0.1`, signed zero, non-finite values, `Float.MIN_VALUE` scale 60/59,
underflow and extreme overflow. Both copies have SHA-256
`366a889719c6e7b688ff00e1bdfb95e273ab192508a1094dac5312c3d899611d`.
The generator calls the current server `Numbers.doubleToDecimal(..., false)`;
its output is byte-identical to both resources. An independent differential
checks 5,996,868 finite target/value combinations and matches acceptance, scale
and all four limbs.

Component tests consume every vector and assert schema flag, physical target
type, bitmap, target scale, limbs and frame length. A second test covers
duplicate-first-wins, non-finite-null-first, omission, reset and A/failed-B/C
rollback. The public socket test changes DECIMAL64(18,4) to DECIMAL128(20,5):
the old block stays pinned while a value invalid under the old snapshot causes
one refresh and succeeds under the new snapshot. Final focused client validation
passes 63 tests plus both packaging checks. The corrected canonical gate passes
944 tests across 46 classes. Candidate, archive and installed JAR SHA-256 is
`da44a24c6d100a0bff8a984f37c40409a0e3cc3f132f7ac9304a9e0b5d25c99d`.
The final binding source SHA-256 is
`2f8e23ba68fb415a8ff9ca7b5535c9f27d040a7b9d39541212baec0310efa1ea`.

Four real-server E2Es consume the same corpus through public Sender, assert exact
SQL/null results and local typed errors/recovery, preserve native inference for
a missing table, and independently send an unflagged raw legacy frame with and
without null bitmaps. They inspect native tags, raw bits and bitmap context before
the server performs its old conversion. A separate server rebind or store-and-
forward fixture was not added: the new client socket test already pins the
width/scale generation, while existing decimal and schema replay tests exercise
persisted direct decimal bytes. Adding another scenario would repeat machinery
without exposing a phase-specific behavior.

Server focused validation passes the four new E2Es and four recovered original
methods. The canonical gate passes 360 tests. The unchanged original 56-test
conversion diagnostic has zero failures and six errors: exactly the four phase-
5d methods disappeared from phase 5c's ten, with no additions. The remaining six
are the phase-5e native-decimal-to-STRING/VARCHAR methods. Three seeded repeats
pass 123 tests each for seeds 1/2, 42/43 and 1234567/7654321; each archived
selection has 23 XML reports with the requested seeds, project-local temporary
directory and zero failures/errors/skips. The installed JAR hash remained the
accepted candidate throughout server validation. No server production or SQL
engine file changed for this increment.

Two failed validation setups are retained but excluded. The first 60-table
server run used the nearly full shared `/tmp`; WAL mmap creation hit errno 28 on
the last two tables. The complete project-local-temp rerun passed all four tests.
The first client canonical reconstruction incorrectly read every stale XML file
in the phase-5c evidence directory instead of the 45 classes actually named in
its log. That exploratory run executed 1,278 tests and exposed four errors in
`CursorWebSocketSendLoopErrorLatchTest`: the test used
`Unsafe.allocateInstance`, skipped the real final schema-coordinator initializer
and then invoked private `recordFatal` by reflection. Adding a production null
guard would have served only this impossible object state. The obsolete 251-line
test was deleted after SOL review; seven public/real-loop replacement classes
pass 12 tests, and the expanded 79-class regression gate passes 1,269 tests.
The exact prior-log-plus-new canonical selector then passes 944. This cleanup is
not evidence for the conversion itself, but it preserves the design rule against
reflection fixtures and avoids defensive production code.

The review identified one sibling fixture with the same bare-loop pattern.
`CursorWebSocketSendLoopJvmErrorTest` left the engine null, so its private
`ioLoop` case entered a zero-backoff retry and the isolated run timed out after
60 seconds with millions of attempts. The legitimate public
`connectWithRetry` test remains; only the two reflective private-loop cases and
their fake-object plumbing were removed. The retained test passes. A normally
constructed real-loop test now covers one-attempt JVM-error escape, producer-
visible terminal wrapping and clean I/O-thread shutdown without reflection or a
test hook. No production change was made for this impossible object state.

Client HEAD remains `981bdb02a471f3b290c89b8e78cbc422610e329e`; server HEAD
remains `12a33d651e51e2682e7a448c8db5168fc72dfad3`, both with the recorded
dirty worktrees. Final acceptance leaves six original diagnostic errors, all in
native-decimal-to-text phase 5e. No commit or push is part of this checkpoint.

## D052 — Native-decimal-to-text contract and malformed-wire boundary

Status: phase 5e prepared, not yet accepted, 2026-09-12. Revision 54 records the
contract before production code changes. The remaining six original failures
are `Decimal64`, `Decimal128` and `Decimal256` into STRING and VARCHAR.

The server target dispatch promotes each native wire width to its reusable
Decimal256 value, applies the unsigned source wire scale and calls `toSink`.
The output is plain fixed-point text with valid source scales and trailing zeros
preserved. Schema mode will therefore promote a non-null public decimal into a
reused binding-owned Decimal256, apply the same unsigned one-byte scale, format
it into the reused text sink and append the existing `TYPE_VARCHAR`
representation for both text targets. MISSING and legacy mode remain native
decimal. Public null and `NULL_VALUE` inputs retain their existing no-op
semantics and do not claim a duplicate slot. No protocol, server, SQL,
buffer-state, conversion-registry or send-time change is justified.

The compatibility boundary is explicit. A raw non-bitmap DECIMAL64/128 sentinel
is currently formatted by the server as the physical minimum integer; the raw
DECIMAL256 sentinel tuple produces an empty non-null string. Public decimal null
sentinels remain setter no-ops. Public low-level constructors and `setScale`
mutators can, however, construct other coefficients and scales outside the
documented 18/38/76-digit domains. The first implementation incorrectly called
each narrow concrete formatter directly; a full-width Decimal128 value was
silently truncated because the server formats it only after Decimal256
promotion. That implementation is rejected. The corrected path mirrors the
server's promotion and unsigned one-byte scale instead of adding a new local
rejection policy. Hardening the Decimal API or server validation would be a
separate production decision.

The frozen phase-5d client artifact is
`da44a24c6d100a0bff8a984f37c40409a0e3cc3f132f7ac9304a9e0b5d25c99d`.
Against it, the unchanged real-server diagnostic fails exactly the six phase-5e
methods with unsupported conversion errors. One earlier setup failed before
tests because its exact project temporary directory did not exist; that log is
retained as invalid setup evidence and is excluded. Shared corpus, component,
public Sender and independent legacy-frame tests must be red against this frozen
artifact before the narrow binding branch is implemented.

## D053 — Promote native decimals before text formatting

Status: accepted in iteration 2.36, 2026-09-12.
Revision 55 supersedes D052's initial direct-formatter assumption.

Adversarial review found that public two-argument raw constructors and low-level
`setScale` mutators can expose values outside the narrow classes' documented
precision and scale domains. Calling `Decimal128.toSink` directly silently
changed the full physical value `2^127 - 1` into 38 nines, while the server first
promotes it and emits all 39 digits. Adding a new local rejection policy would
also change previously accepted old-server writes and is unnecessary.

The corrected binding lazily reuses one Decimal256 scratch, copies every native
decimal into it, applies the unsigned low QWP scale byte, then formats into the
existing reused text sink. This is the server's scalar algorithm. It reuses its
scratch and sink; after the sink reaches the required high-water capacity,
formatting needs no per-value object allocation. Public null sentinels remain
no-ops.

Parity is deliberately per setter value, not per legacy column block. A known
STRING/VARCHAR target accepts mixed native widths and independently preserves
each value's effective scale. Legacy inference must choose one native wire width
and scale for the whole block, so it rejects a later width and may rescale or
reject a later scale. Recreating that intermediate constraint after the target
is known to be text would add buffer state and make a value's conversion depend
on neighboring rows; revision 55 rejects that complexity. Tests must name and
cover this behavior difference directly rather than calling it whole-batch
legacy parity.

D052's proposed separate component fixture is also superseded. The public
socket test already drives the binding and asserts the exact bitmap, offsets,
bytes and end-of-frame boundary. Repeating that path through a lower-level
fixture would add test machinery without another observable contract.

The first candidate JAR with direct narrow formatting and SHA-256
`4af31d76224a3e811527114638240eabfe5939445f218c798c7f2fc3d1247b9e`
is rejected and retained only as superseded evidence. The accepted candidate
and installed JAR is
`e06759605ee3d1cf01c3277ba13531b0172e42138042c1f2f49c2db49e04b22d`;
only `QwpSchemaBinding.class` differs from the frozen phase-5d JAR.

The stored production/server differential passes 3,000,294 deterministic
comparisons across full physical limbs, arbitrary integer scales, explicit
scale boundaries and all three public null sentinels. The 24-row shared corpus
is byte-identical in both repositories. Client validation passes the 46-test
public integration class, including four phase-5e tests, the 950-test canonical
gate, 1,275-test broad gate and two packaged-JAR tests. All 80 selected broad
XML reports have zero failures, errors and skips.

Real-server validation passes all three Phase-5e E2Es, including mixed widths/
scales, the full physical Decimal128 value and wrapped scale 257. The unchanged
original conversion diagnostic passes all 56 methods, resolving the original
47 failures. The accumulated server gate passes 363 tests. Three seeded repeats
pass 126 tests each; all 24 selected reports per run contain the requested seeds
and project-local temporary path with zero failures, errors and skips. No server
production or SQL-engine change belongs to phase 5e.

The clean performance window contains six counterbalanced pairs for each native
identity width and 200 million setter calls per process. Instructions change by
+1.05% for Decimal64, +1.38% for Decimal128 and +0.71% for Decimal256; paired
median cycles change by -1.93%, +1.13% and -1.03%. Exact work and checksums
match, counters run at 100%, and no run has GC, major faults, throttling or OOM
changes. The result accepts the change against the 5% regression threshold but
makes no speed, decimal-to-text, network or ingestion-throughput claim. One
Maven-overlapped matrix and the withdrawn-candidate matrix are isolated and
excluded. Exact commands, hashes and logs are in the client and server 2.36 run
manifests. No commit or push is part of this checkpoint.

Final independent SOL acceptance found no P0, P1 or P2 blocker after checking
production ownership, compatibility wording, exact hashes, raw logs, XML
provenance and performance claim bounds. One P3 remains: older source-map line
numbers in the design have drifted, although their method names remain correct.
That navigation debt is outside phase 5e and does not change the contract.

## D054 — Post-2.36 simplification and next-iteration plan

Status: accepted planning decision, 2026-09-14. Revision 56 records the plan;
it does not claim release acceptance or add production code.

The schema-aware implementation is now committed as client `6fe3ce44` and
server `ffbb7125d2`. Server integration commit `818608c297` pins that exact
client revision. A Maven reactor run using the pinned local client passed the
public-Sender UUID conversion and rollback E2E against the server, so the next
work starts from an integrated baseline rather than another component-only
slice.

Compatibility becomes a permanent CI and release gate, not another feature
iteration. It must exercise released binaries for new-client/old-server and
old-client/new-server behavior, current new/new schema mode, and the one-way
upgrade failure against an old endpoint. This validates the promised behavior
without adding a compatibility adapter, downgrade encoder or send-time
transformation.

Timeout productization is removed from the roadmap. The policy is a fixed
30-second deadline with no new public configuration: initial negotiation and
its first describe share one budget, while later cache-miss describes and
refreshes each get their own budget. The timeout change is owned separately
from these conversion slices and is implemented by client commit `98be3b0b`;
it is not recreated or extended by iteration 2.37.

The next implementation areas are deliberately narrow: iteration 2.37 adds
BYTE/SHORT/INT to the six numeric targets with a small shared helper; iteration
group 2.38 adds fixed-width identity/text support as separate IPv4, LONG256 and
exact-precision GEOHASH slices. Each slice retains a real public-Sender E2E and
the existing wire, SQL, rollback, refresh, recovery, packaging and
affected-performance gates.

The source audit remains broader than the intended product. BINARY-to-parser
paths, LONG_ARRAY, non-ASCII CHAR-to-VARCHAR and malformed decimal metadata are
server-contract questions, not automatic client backlog. The client will not
reproduce parser accidents or validation gaps merely because current server
dispatch can reach them. The encoded 32-bit `ColumnType` remains the single
parameter representation, and no converter registry, schema patch protocol,
retry framework, second request channel or per-path timeout setting is planned.

Before iteration 2.37 code lands, tests must freeze `Integer.MIN_VALUE` behavior
with and without a companion null bitmap. The recommended schema-mode contract
is stable source NULL, independent of neighboring rows; this remains an explicit
contract checkpoint rather than an assumption copied from LONG.

## D055 — Small integer inputs use direct numeric target widths

Status: iteration 2.37 implemented and locally validated, 2026-09-14. Revision
57 is recorded in the client commit containing this decision; the companion
server commit pins that exact client revision.

BYTE, SHORT and INT setters now support BYTE, SHORT, INT, LONG, FLOAT and DOUBLE
targets. One private helper owns their shared numeric-target check, narrowing
range checks and direct append. The public Sender branches reuse the established
schema-rejection rollback and one-refresh path. Missing columns retain the
setter's native source type. Legacy mode and the established LONG implementation
are unchanged; no registry, converter graph, new buffer state or server
production change was added.

The source-null decision is now explicit. `Integer.MIN_VALUE` is a stable INT
source null in schema mode, independent of whether another row creates a null
bitmap. BYTE and SHORT have no public source-null sentinel. An unsupported
target still rejects the setter even when the INT value is the null sentinel;
null does not make an otherwise unsupported conversion available.

Both repositories consume the same 64-case boundary corpus, SHA-256
`93dd3987145766e6b9d7fff0b592e0a56b67577d83de4de623ed0e8231e3b96d`.
It covers identity, widening, valid and invalid narrowing, FLOAT/DOUBLE raw-bit
rounding and INT nulls. Client component tests assert exact target wire data and
bitmap behavior. Public scripted-Sender tests add partial-row rollback, one
refresh, snapshot pinning and missing-column inference. A public Sender restart
test retains an unacknowledged schema frame and proves byte-identical replay.
The real-server E2E creates all six numeric target tables and verifies the
accepted values through SQL while range failures remain local typed errors.

Focused client validation passes 91 tests: 35 binding, 47 public Sender and 9
network replay tests. The established client gate passes 957 tests, followed by
both packaged-JAR integration checks. The focused real-server E2E passes, and
the corrected accumulated server selector passes 379 tests with zero failures,
errors or skips.

One exploratory server run incorrectly selected every method in
`QwpSenderE2ETest` and `QwpWebSocketSenderReceiverTest`. It ran 631 tests and
reported 3 failures plus 65 errors in conversion families that remain explicitly
unsupported, chiefly arrays, geohashes and parser paths. That run is not a
regression result. A second invalid setup exhausted shared `/tmp`; its generated
512 MiB test directory and crash log were moved under the run-owned server
target directory without touching unrelated temporary files. The valid run uses
project-local temporary storage and only the intentionally supported legacy
methods from the established acceptance selector.

The affected fixed-work guard runs each identity source for two million warmup
and 50 million measured rows, five times on one CPU. All checksums and work
counts match and hardware counters run at 100%. Average retired instructions are
30.45 billion for BYTE, 30.76 billion for SHORT, 32.54 billion for INT and 35.14
billion for the established LONG reference. A separate 100-million-row INT run
records no GC after startup. These data reject an obvious helper-path regression;
they are not a before/after speed claim or network/ingestion benchmark because
the prior client had no corresponding schema-aware small-integer operation.

The fixed 30-second timeout remains outside the conversion roadmap. Iteration
2.37 is rebased on the user's timeout commit `98be3b0b` and does not duplicate
or extend that change.

## D056 — Released binaries form the standing compatibility gate

Status: implemented and locally validated, 2026-09-14. Design revision 58.

Compatibility is now a build gate rather than another conversion iteration.
The gate adds test and CI files only; it does not change client or server
production code, add a protocol adapter, or transform buffered data at send
time. It runs these three process combinations:

1. current client against QuestDB 10.0.1;
2. client 1.3.9 against the current server; and
3. current client against the current server.

QuestDB 10.0.1 and client 1.3.9 are the fixed last released baseline before the
schema extension. Their tagged sources contain no schema negotiation headers,
and QuestDB 10.0.1 itself pins client 1.3.9. The peeled source tags are server
`7a391566ac827e0d8c269b91f2415c16b0a32a84` and client
`3d035bdcab9d65b487aa16ffcee2fbd608e84d74`. The gate downloads the
published JARs into its own target directory and verifies SHA-256
`432d27433836d4430aaca4d701cb0dfac23f7b8281963a5fae97079c0436892d`
for the server and
`41c31bc25646cfe1f045615f53edcfa685a01182749d96c381ea75cd27e7a6a0`
for the client. The shared SLF4J API 2.0.17 dependency is pinned to
`7b751d952061954d5abfed7181c1f645d336091b679891591d63329c622eb832`.
These are deliberate baseline pins, not moving `latest` lookups.

One Java probe is compiled with `--release 11` against client 1.3.9's public
`Sender` API, then launched in isolated JVMs with exactly one selected client
JAR. It writes one buffered three-row block to a table whose `value` column is
FLOAT: `42`, `Long.MIN_VALUE`, and an omitted value. Explicit auto-flush limits
keep the rows in one block because the last omission creates the bitmap that
makes the old LONG sentinel behavior observable. Legacy mode stores `42.0`,
`-9.223372E18`, and SQL NULL. Schema mode stores `42.0`, SQL NULL, and SQL NULL.
Exact exported CSV distinguishes the modes; mere write success would not.

The runner starts each server with separate roots and ports, waits for HTTP
readiness, creates WAL tables through the public HTTP endpoint, executes the
probe, polls exported rows for WAL visibility, and retains per-run server logs.
Released artifacts live under `compat/target`, outside the shared Maven cache.
The default source is the canonical Maven Central endpoint; an internal mirror
can be supplied without bypassing checksum validation.

The same script runs after the existing distribution build in
`.github/workflows/pgwire_stable.yml` and in the Linux release build before
artifacts are copied. It reuses the current server JAR and, when the server pins
a client snapshot, the exact checked-out submodule JAR already built by the job.
When the server pins a released client, it uses that exact resolved dependency
from the local Maven cache. A cold local run, including roughly 40 MiB of
released artifact downloads, passed all three combinations in 5.03 seconds. A
focused client run also passed 60 negotiation, Sender-integration and replay
tests with zero failures, errors or skips. The corresponding focused server run
passed 15 schema discovery, numeric-conversion and identity tests with zero
failures, errors or skips using project-local temporary storage. An initial run
against the already-full shared `/tmp` failed with `No space left`; it is
environmental evidence, not a product regression.

The process matrix intentionally does not recreate the upgraded-sender state
transition with two server processes. Deterministic socket and replay tests
already prove the stronger exact contract: after schema confirmation, an old
endpoint receives no schema frame, durable and ordinary watermarks do not
advance, and retained bytes replay unchanged to a compatible endpoint. Keeping
that assertion there avoids a timing-sensitive failover rig while the process
gate proves released-binary interoperability and observable mode selection.

Three independent SOL reviews checked the contract, published artifacts and
CI harness. The first review found an unbounded server `wait`, a probe without
a kill grace period and wording that overstated the public-API level of the
existing watermark tests. The CI review found that Azure's custom Linux
condition replaced its default success gate, that Azure did not retain failure
logs, and that fixed ports plus iteration-counted polling left avoidable false
target and timeout risks. Root replaced those with bounded TERM-to-KILL
shutdown, a probe kill grace period, five-minute CI limits, a success-preserving
Azure condition and failure artifact, occupied-port rejection, real 30-second
deadlines, and accurate socket/replay wording. Released client pins now select
the exact Maven-cache artifact while snapshots select the checked-out submodule
build. A negative run substituting the old client for the current client failed
the schema assertion and cleaned up both servers. Final re-review reports no
blocker or remaining concrete finding.

## D057 — Textual decimal input reuses Decimal256 conversion

Status: iteration 2.38 implemented and locally validated, 2026-09-14. No commit
or parent-repository submodule update has been made.

The public `decimalColumn(CharSequence)` overload now participates in schema
mode. It remains a DECIMAL256 input, not an alias for `stringColumn`: the Sender
parses once into its existing Decimal256 scratch value, and the binding reuses
the native decimal target conversion and text-formatting tails. Known decimal
targets apply their declared precision and scale. STRING and VARCHAR receive
canonical fixed-point text. This adds no converter registry, retained per-row
object or server production change.

Target lookup, duplicate suppression and target support checks precede parsing.
An unsupported known target therefore reports `UNSUPPORTED_FEATURE` even for
malformed input, while a duplicate malformed value remains ignored. Java null
and empty input remain no-ops. Parsed `NaN` and infinities remain effective
values that write target NULL; this differs intentionally from typed decimal
null sentinels, which remain no-ops. The public Sender uses the established
partial-row rollback and one-refresh path for local schema errors.

A confirmed missing column infers DECIMAL256 at the first finite input's natural
scale. A preceding parsed special must not pin scale zero. If a block contains
only decimal nulls, `QwpTableBuffer` emits legal scale-zero wire metadata while
retaining its internal unset scale, so a later finite batch can still choose its
natural scale. This is an encoding-boundary normalization, not new scale state.

Tests use public behavior and real wire/data results, without reflection or
test-only production hooks. On JDK 25, 114 affected client tests and both
packaged-JAR checks pass with the fork's startup temp root under
`/mnt/pcie5/schema-aware-decimal-text-20260914/client/jdk25-verify`. They cover
all six decimal widths, STRING/VARCHAR formatting, missing inference, parsed
specials, null/empty omission, duplicate ordering, invalid-row rollback,
refresh and exact frames. The companion server selection passes 12 tests for
the new overload plus existing native/string decimal paths under
`/mnt/pcie5/schema-aware-decimal-text-20260914/server/regression`. After
replacing deterministic `returnsOnce()` assertions with the full query battery,
the three new real-server tests pass again under the `server/final3` directory.

Early server attempts inherited a full shared `/tmp`; server logs proved WAL
mmap failures with `No space left`. No product change followed from those runs.
After the test scratch location was moved to `/mnt/pcie5`, all subsequent test
databases and scratch data used that filesystem. It remains required for future
validation.

Two broader checks exposed existing branch constraints rather than decimal
failures. First, the Java 8 build stops in the committed, untouched
`QwpSchemaRyuDouble` because it calls Java 9+ `Math.multiplyHigh`; the new code
uses Java 8 syntax and APIs, but a real Java 8 build remains a release gate after
that baseline blocker is repaired. Second, the full client suite blocks in
offline-first tests because the current mandatory initial schema lookup waits
before accepting the first value. The exact recovery failure reproduces at
client baseline `1310b0dd`, and `CloseDrainTest` reaches the same wait on the
current tree. This is the unresolved product conflict between cold offline
buffering, mandatory local schema conversion and the prohibition on send-time
transformation. The simplest coherent contract is to require the first
effective write to establish schema mode; changing that contract needs a
separate decision and is not hidden by excluding individual tests.

No setter-specific SF replay test was added. This slice changes how the Sender
constructs an ordinary schema-pinned table buffer, not persistence or replay;
the public exact-frame tests cover the newly constructed bytes and the existing
schema-frame replay tests remain byte-oriented. Adding another replay scenario
would duplicate those two contracts without exercising new recovery logic.

An author and an independent SOL reviewer challenged the first draft. They
found and fixed missing-column dispatch through an unparameterized DECIMAL256
type and a special-null path that prematurely locked scale zero. Final review
found no implementation blocker, confirmed no apparent per-row heap allocation,
and corrected the affected-test count from an earlier unsubstantiated 218 to
the preserved 114-test reports.

## D058 — IPv4 uses target-native wire data

Status: iteration 2.39a implemented and locally validated, 2026-09-14. Design
revision 60. The parent-repository test and submodule update land separately.

Both public `ipv4Column` overloads now participate in schema mode. IPv4 targets
retain the four-byte packed value. STRING and VARCHAR targets receive canonical
dotted-quad text through the binding's existing reusable numeric sink. The
implementation adds no converter registry, address object, parser fork or
per-row string allocation. Legacy mode keeps its existing native IPv4 wire path.

Packed zero is the public IPv4 null sentinel and becomes an explicit bitmap
NULL for every target. A Java null text reference remains a no-op. The
case-insensitive `"null"` literal and exact `"0.0.0.0"` remain rejected, while
legacy dotted aliases such as `".0.0.0.0."` remain accepted and normalize to
NULL. Target selection and duplicate suppression precede parsing, so an invalid
duplicate stays ignored and an unsupported known target wins over malformed
text. Sender-level schema errors use the established partial-row rollback and
single-refresh lifecycle.

The implementation review found one pre-existing parser boundary: an address
made only of dots read past the input and leaked `StringIndexOutOfBoundsException`
instead of the public invalid-value error. One bounds check now converts that
case to `NumericException`; `"."` and `"...."` are fixed shared-corpus cases.
The permissive leading/trailing-dot grammar is otherwise unchanged.

The 16-row corpus covers packed boundaries, canonical and normalized text,
zero/null behavior and malformed input against IPv4, STRING and VARCHAR
targets. Component tests assert exact wire bytes and bitmaps, inference,
omission, duplicate ordering, unsupported/parameterized/designated targets and
rollback. Public scripted-socket tests assert the same target wire plus a schema
change from IPv4 to VARCHAR while the already-buffered block remains native
IPv4. A separate old-peer test proves the integer and text overloads still emit
one unflagged native IPv4 frame without a DESCRIBE. The real-server test sends
all three targets in one batch, verifies exact stored values, local invalid-row
recovery and missing-table inference. A compact store-and-forward test inspects
the persisted frame before replay and verifies the same rows after replay.

On JDK 25, 238 focused client tests pass with zero failures, errors or skips:
the schema binding, Sender integration, legacy Sender, QWP constants and numeric
parser suites.
Five focused server tests pass against the installed client snapshot: the three
new IPv4 tests plus existing auto-create and schema-aware text-target tests. All
validation used `/mnt/pcie5/schema-aware-ipv4-20260914`. An earlier run inherited
the full shared `/tmp`; logs showed all three WAL writers accepted eight rows,
then STRING and VARCHAR WAL apply suspended on `errno=28`. That run is discarded
as environmental evidence. Setting the forked JVM's `java.io.tmpdir` moved the
JUnit database roots to the project scratch filesystem and the same single-flush
test passed.

The final SOL review found no production correctness, compatibility, allocation
or complexity blocker. It corrected one design sentence that named the server's
IPv4 formatter as though it existed in the client, and requested the explicit
old-peer setter test above. Re-review found both corrections complete.

## D059 — LONG256 uses target-native wire data

Status: iteration 2.39b implemented and locally validated, 2026-09-14. Design
revision 61. The pre-iteration baseline is client `21168def` and server
`d7b94647ae`; neither repository is committed by this decision record.

The public `long256Column` setter now participates in schema mode. A LONG256
target receives the four input limbs directly in the existing 32-byte wire
layout. STRING and VARCHAR targets receive direct VARCHAR wire data in the
server's canonical lowercase `0x` form: the most-significant nonzero limb is
written first with even-byte leading width, and each lower limb is padded to 16
hex digits. The implementation keeps four primitive longs and reuses the
binding's numeric text sink and existing hex appenders. It adds no `BigInteger`,
LONG256 object, converter registry, retained source value, server production
change or per-row string allocation.

Four `Long.MIN_VALUE` limbs are the source null sentinel in schema mode and are
encoded as an explicit bitmap NULL for LONG256, STRING and VARCHAR targets. Any
partial match remains a value. This removes a legacy row-context dependency:
without a bitmap the server cursor recognizes the four-limb sentinel as null,
but with a bitmap it treats only marked rows as null, so the same unmarked
sentinel converted to text becomes an empty non-null string. Schema mode makes
the intended null explicit before target conversion. Legacy mode deliberately
keeps its native LONG256 wire data and existing server-side behavior. The public
Javadoc states this qualification instead of promising uniform legacy results.

Target selection and duplicate suppression happen before formatting. Known
unsupported, parameterized and designated targets keep typed rejection;
Sender applies the established partial-row rollback and one-refresh lifecycle.
A confirmed missing column still infers native LONG256. A later schema
generation changes only subsequent buffers: exact scripted-wire coverage proves
an already buffered LONG256 block stays native before a new VARCHAR block is
created. An old peer receives the original unflagged native LONG256 frame and no
DESCRIBE.

The shared eight-row corpus covers zero, the highest nonzero `l0`, `l1`, `l2`
and `l3` formatter branches, all-one limbs, a partial sentinel, the high sign
bit and the full sentinel null. The client and server resources are
byte-identical with SHA-256
`a634074af646eb961768599e73727a4a1ab0cd0916dc97a86a8ee4a027723275`.
Component tests assert exact target type, payload and bitmap bytes plus
duplicate, omission, rollback, inference and rejection order. Public socket
tests cover exact schema and legacy frames, rollback and generation rebind. The
real-server corpus uses one Sender and one flush across LONG256, STRING and
VARCHAR tables, with an omitted first value to force late column creation and
two explicit bitmap-null positions. Separate E2E tests cover A/error/C row
recovery, missing-table inference and inspection plus replay of the persisted
target-native frame. A raw legacy fixture pins the bitmap-dependent server
behavior instead of making the schema test silently replace it.

Final focused validation on JDK 25 passes 96 client tests and nine server tests
with zero failures, errors or skips. The client set is
`QwpSchemaBindingTest,QwpSchemaSenderIntegrationTest`. The server set is
`QwpSchemaLong256E2ETest,QwpSchemaTextSourceNullE2ETest` plus
`QwpSenderE2ETest.testSchemaModePreservesUuidAndLong256NullsForTextTargets`.
An expanded client regression covering the table buffer, encoder, Sender,
constants, prior STRING-to-LONG256 and UUID-to-text bindings, recovered-frame
analysis and schema replay passes 283 tests with zero failures, errors or skips.
All test roots were created under `/mnt/pcie5` by setting `java.io.tmpdir` at
test-JVM startup through `JAVA_TOOL_OPTIONS`.

An exploratory run of the full `QwpSenderE2ETest` is not green: 137 tests
produced three failures and 41 errors. The LONG256 test changed by this slice
passed. Observed errors include still-missing inventory entries such as
INT-to-IPv4, STRING-to-DATE and LONG_ARRAY, but this run was not fully
classified and is not presented as regression evidence. It confirms the
standing release status: the conversion contract remains incomplete even
though the bounded LONG256 slice is accepted.

The first server corpus run appeared to return an empty table. The server log
disproved a QWP rejection: LONG256 had applied all eight rows, while the next
STRING WAL apply failed with `errno=28` because shared `/tmp` was full. Passing
`-Djava.io.tmpdir` as an ordinary Maven property did not fix the environment;
`strace -f -yy -e trace=fallocate` showed that Surefire's JVM had cached
`/tmp/junit...` before that property was applied. Supplying it at JVM startup
moved the actual database files to `/mnt/pcie5`, where all three eight-row WAL
applications and queries passed before the final corpus branch was added. The
final nine-row version passes as part of the focused server gate. No production
or test-shape workaround follows from the environmental failure. In particular,
the final test restores the simpler and stronger one-Sender, one-flush shape.

Independent adversarial review found and closed two issues before acceptance:
the initial Javadoc overstated legacy null semantics, and the seven-row corpus
missed the separate `l1` formatting branch. Re-review found both resolved and no
remaining correctness, compatibility, allocation or complexity issue in this
slice. The wider conversion inventory remains open; the next bounded slice is
native GEOHASH identity and text output with exact target precision.

## D060 — GEOHASH uses exact native precision or fixed-width text

Status: iteration 2.39c implemented and locally validated, 2026-09-14. Design
revision 62. The pre-iteration baseline is client `0d9663b26f` and server
`de96ef5eb5`; neither repository is committed by this decision record.

Both public `geoHashColumn` overloads now participate in schema mode. A GEOHASH
target receives the masked packed value only when the target and source bit
precisions match exactly; a mismatch fails locally with `UNSUPPORTED_FEATURE`.
STRING and VARCHAR targets receive direct VARCHAR wire data containing exactly
one `0` or `1` per source bit. This matches the server cursor's reachable
positive-precision formatter rather than its unreachable character-formatting
branch. The implementation keeps one primitive long and its precision and
reuses the existing text sink. It adds no geohash object, lookup table,
converter registry, retained source value, server production change or per-row
string allocation.

The packed overload accepts precision 1..60 and ignores higher input bits. The
textual overload preserves the existing public setter contract: strict,
case-insensitive base32 of 1..12 characters and five bits per character; null,
empty, overlong and invalid values fail locally. Omission is the only NULL
operation. All packed bit patterns are therefore values, including `0xff` at
eight bits. Schema-directed GEOHASH columns use the bitmap, so those
byte-aligned all-one values cannot collide with the fixed-width cursor sentinel.
Confirmed missing columns still infer the source precision. Legacy mode keeps
the original unflagged native GEOHASH frame and performs no DESCRIBE.

The 15-row shared corpus covers precisions 1, 5, 7, 8, 15, 16, 20, 31, 32, 35
and 60, leading zeroes, all-one values and masking of higher bits. Client and
server copies are byte-identical with SHA-256
`8468be87f7061c58862b7bbe47290f508485c5cbc61a4373b2c3932e59a225e6`.
Component tests assert all 60 text widths, exact target wire bytes and bitmap
semantics, textual parsing, inference, duplicate suppression, rollback and
typed rejection of precision mismatch, unsupported, parameterized and
designated targets. Public socket tests cover exact schema frames, an existing
buffer pinned through a GEOHASH-to-VARCHAR rebind, and old-peer framing. Four
real-server tests cover missing-table inference, all shared text vectors,
exact native values including eight-bit `0xff`, A/error/C recovery, persisted
frame inspection and byte-preserving replay.

The old `testCoercionToGeoHashErrors` expected every unsupported value to reach
the server. That no longer tests the product contract once schema mode rejects
the setter. It now asserts typed local errors and a subsequent successful row;
server-rejection tests remain separate instead of weakening a shared helper to
accept either path. This is a test-contract update, not a server behavior
change.

On JDK 25, an expanded client regression passes 294 tests with zero failures,
errors or skips. It covers schema binding and Sender integration, table-buffer
and encoder behavior, legacy Sender framing, constants, earlier STRING-to-
GEOHASH, LONG256 and UUID conversions, recovered-frame analysis and replay.
The current client artifact installs successfully. The focused server gate
passes 15 tests: the four new GEOHASH tests, three STRING-to-GEOHASH tests, six
public Sender GEOHASH tests and two existing GEOHASH-to-text server tests. All
test JVM scratch roots are under `/mnt/pcie5`.

The full `QwpSenderE2ETest` remains a diagnostic rather than an acceptance
gate: 137 tests produce two failures and 38 errors, improved from three failures
and 41 errors at D059. GEOHASH is no longer among them. The remaining product
gaps begin with integer temporal/text/decimal conversions, INT-to-IPv4 and
DOUBLE arrays; several other errors are legacy tests that still expect a
delayed server rejection after the new client correctly fails at the setter.
The next slice should implement integer temporal target representation, not
mechanically rewrite all remaining error tests.

## D061 — Integer temporal targets keep raw target-unit counts

Status: iteration 2.40 implemented and locally validated, 2026-09-14. Design
revision 63. The pre-iteration baseline is client `8932076f24` and server
`67e2c94f7a`, whose submodule pins that exact client revision. Neither
repository is committed by this decision record.

BYTE, SHORT and INT setters now accept DATE, TIMESTAMP and TIMESTAMP_NS targets;
LONG also accepts DATE. This is target representation selection, not a unit
conversion. The client writes the integer unchanged as epoch milliseconds for
DATE, epoch microseconds for TIMESTAMP, or epoch nanoseconds for TIMESTAMP_NS.
`Integer.MIN_VALUE` and `Long.MIN_VALUE` retain their schema-mode source-null
meaning and become explicit bitmap NULLs. BYTE and SHORT have no source-null
sentinel.

The implementation was cross-checked against `QwpWalAppender` and
`WalColumnarRowAppender`. The server admits BYTE, SHORT, INT and LONG into DATE,
and routes integer input for DATE and both timestamp types through the same raw
64-bit append. It performs no scale multiplication or division. The client
therefore adds DATE to the existing target-wire switch, adds the three temporal
targets to the existing small-integer helper, and adds DATE to the established
LONG path. No converter registry, temporal object, unit layer, retained value,
per-row allocation or server production change was added. Numeric identity
targets still short-circuit at the existing `isNumericTarget` check, so the new
temporal comparisons are not evaluated on that hot path; this is source-level
reasoning, not a new performance claim.

The first component test draft incorrectly expected the timestamp raw-encoding
discriminator after a DATE column. Inspecting `QwpColumnWriter` showed that
DATE is a plain fixed-width value, while TIMESTAMP and TIMESTAMP_NS carry the
discriminator. The test was corrected; production code was unchanged. This
wire difference is why the implementation deliberately does not introduce a
shared temporal encoder.

Both repositories consume the same 24-case corpus, SHA-256
`0872eab8b83f608c2e12778e4e44512c87bb7cff7e693109aed7effacd42fd85`.
It covers BYTE and SHORT extrema for all three targets, INT null/negative/max
for all three targets, and LONG null/minimum-non-null/max for DATE. Client
component tests assert exact target wire types, bitmap state, timestamp
discriminators and raw values. Public scripted-Sender tests cover all four
setters, a failed partial row followed by a valid row, the one-refresh path,
parameterized DATE rejection, and a DATE block remaining pinned when the next
schema generation changes the target to TIMESTAMP_NS. The real-server test
writes the shared corpus through public Sender setters, waits for ACK, and
checks the stored raw values and nulls through SQL.

Focused client validation passes 106 tests, and the expanded regression passes
297 tests with zero failures, errors or skips. Packaged-JAR verification passes
46 selected unit tests and both packaging integration tests. The focused server
gate passes the new corpus plus the existing TIMESTAMP and TIMESTAMP_NS
aggregate tests: three tests total, all green. All test JVM scratch roots are
under `/mnt/pcie5`.

The released-binary matrix also passes all three combinations with the freshly
packaged client: current client to QuestDB 10.0.1 remains legacy, client 1.3.9
to the current server remains legacy, and current client to current server uses
schema mode. The run used
`/mnt/pcie5/qwp-integer-temporal-compat/run.FKXeM7`.

The full `QwpSenderE2ETest` remains diagnostic: 137 tests now produce two
failures and 37 errors, down from two failures and 38 errors at D060. The
existing TIMESTAMP aggregate is now green. The DATE aggregate still stops at
the separate STRING-to-DATE gap after its integer setters succeed, so it is not
claimed as this slice's acceptance test. Other remaining errors include integer
text/decimal targets, INT-to-IPv4, arrays and legacy tests whose expected
server-side rejection is now a local setter error.

No setter-specific store-and-forward scenario was added. This slice changes
only construction of ordinary schema-pinned bytes; persisted framing and replay
remain opaque to column type. Exact public wire tests prove the new DATE and
timestamp bytes before publication, while the existing schema-frame replay
suite remains green. Adding a second recovery harness would duplicate those
contracts without exercising changed recovery code.

## D062 — Small integers reuse the established text and symbol paths

Status: iteration 2.41 implemented and locally validated, 2026-09-14. Design
revision 64. The pre-iteration baseline is client `853c3292c1` and server
`85f24785d3`, whose submodule pins that exact client revision. Neither
repository is committed by this decision record.

BYTE, SHORT and INT setters now accept STRING, VARCHAR and SYMBOL targets in
schema mode. Each input widens losslessly to long and uses the existing reusable
numeric text sink. STRING and VARCHAR append direct VARCHAR wire data; SYMBOL
uses the existing global dictionary writer. `Integer.MIN_VALUE` retains its
source-null meaning and becomes an explicit target bitmap NULL. BYTE and SHORT
have no source-null sentinel. Missing columns still infer their native input
types, and a legacy peer still receives native BYTE, SHORT and INT wire columns
without a DESCRIBE.

The implementation was cross-checked against the current server's
`QwpWalAppender` dispatch and `WalColumnarRowAppender`. Fixed-width integer
cursors targeting STRING, VARCHAR or SYMBOL all call `Numbers.append` on
`cursor.getLong()`. The client therefore extends the existing small-integer
target switch and reuses `formatLong`, `addString` and `addSymbol`. It adds no
source-width formatter, converter object, retained value, per-row `String`,
protocol field or server production change. Numeric identity targets still
short-circuit at the first `isNumericTarget` check, so the additional text check
is not evaluated on that hot path. This is source-level reasoning, not a new
performance claim.

Client and server consume the same 30-case corpus, SHA-256
`78481d27bbeb70354f309372650311e269ee365087e9003218222c2b0580dcfe`.
For each target it covers BYTE and SHORT extrema plus INT null,
minimum-non-null, negative one, zero, one and maximum. Component tests assert
the exact target wire type, UTF-8 payload or symbol dictionary entry, offsets,
IDs and bitmap null. Public socket tests additionally cover a rolled-back symbol
dictionary entry, A/error/C row recovery, the standard one-refresh path, a
SYMBOL block pinned before a VARCHAR schema generation, parameter rejection and
old-peer native framing.

The real-server E2E uses the public `Sender` API and one flush across STRING,
VARCHAR and SYMBOL tables, waits for the acknowledged sequence, drains WAL and
asserts exact SQL values and nullness for all 30 vectors. A second E2E proves a
partially written row is discarded after an INT-to-UUID local error, completed
rows survive and the same symbol value is reused. The focused server gate also
runs the existing `testCoercionToSymbol`; all three tests pass.

Expanded client validation passes 305 tests with zero failures, errors or
skips. Packaged-JAR verification passes 47 selected unit tests and both
packaging integration tests. The released-binary matrix passes current client
to QuestDB 10.0.1 in legacy mode, client 1.3.9 to current server in legacy mode,
and current client to current server in schema mode. All scratch roots are
under `/mnt/pcie5`.

The full `QwpSenderE2ETest` remains diagnostic: 137 tests now produce two
failures and 36 errors, down from 37 errors at D061. The SYMBOL aggregate is now
green. The STRING and VARCHAR aggregates advance through the small-integer
inputs and stop at the separate CHAR-to-text gap; this slice does not claim
that unresolved contract. Other open implementation entries include
small-integer decimal targets, INT-to-IPv4, STRING-to-DATE and DOUBLE arrays.

No new store-and-forward test was added. The changed code only chooses an
already supported target-native VARCHAR or SYMBOL representation before the
frame is persisted. Exact public wire tests prove those bytes, while existing
schema-frame replay and the established long-to-text persistence coverage remain
green. A setter-specific replay test would repeat unchanged persistence code.

## D063 — Small integers share the exact integer-decimal tail

Status: iteration 2.42 implemented and locally validated, 2026-09-14. Design
revision 65. The pre-iteration baseline is client `3acdceadbd` and server
`9bfa31a2fc`, whose submodule pins that exact client revision. Neither
repository is committed by this decision record.

BYTE, SHORT and INT setters now accept DECIMAL8, DECIMAL16, DECIMAL32,
DECIMAL64, DECIMAL128 and DECIMAL256 targets in schema mode. Each non-null
source is an exact scale-zero integer. The binding passes its widened value to
one shared tail that applies the target's declared scale, checks its declared
precision and appends the matching target storage width. `Integer.MIN_VALUE`
retains its source-null meaning and becomes a scaled bitmap NULL. BYTE and SHORT
have no source-null sentinel. Missing columns still infer their native integer
types; legacy mode is unchanged.

The current server source supports the same rule. `QwpWalAppender` dispatches
BYTE, SHORT, INT and LONG cursors to the small, 64-bit, 128-bit or 256-bit
decimal target appenders. `WalColumnarRowAppender` reads the integer through
`getLong()`, assigns scale zero, performs exact rescaling, validates precision
and storage, and writes the target decimal. The client therefore extracts the
existing LONG decimal branch into `appendIntegerDecimal` and calls it from all
four integer sources. Numeric and temporal target branches retain their earlier
short-circuits; no conversion registry, retained decimal object, per-value
allocation, protocol field or server production change was added. This is
source-level hot-path reasoning, not a new performance claim.

Client and server consume the same 30-case corpus, SHA-256
`e3ffe93367c2a6e65d1eeac821b3ccd798d88a9a4a7e37dfaaf9548a89d17ddf`.
It covers every one of the 18 source/target-width pairs, INT null at every
width, positive and negative scale conversion, source extrema and precision
overflow. Component tests assert the exact target wire type, scale byte, bitmap
and raw 64-bit limbs. Public socket tests additionally cover parameter rejection,
precision failure rollback, A/error/C row recovery and a DECIMAL(3,0) block
remaining pinned before a DECIMAL(20,5) schema generation.

The real-server corpus test sends every valid vector through public Sender
setters, waits for the acknowledged sequence, drains WAL and asserts exact SQL
values and nulls. A second public-Sender test proves a partially written row is
discarded after an INT-to-UUID local error while completed decimal rows survive.
The focused server gate also runs the existing decimal aggregate and variant
tests: four tests total, all green.

Expanded client validation passes 323 tests with zero failures, errors or skips.
Packaged-JAR verification passes 48 selected unit tests and both packaging
integration tests. The released-binary matrix passes current client to QuestDB
10.0.1 in legacy mode, client 1.3.9 to current server in legacy mode, and current
client to current server in schema mode. The matrix run used
`/mnt/pcie5/qwp-small-integer-decimal-compat/run.0W5Ji1`; all other scratch roots
also remain under `/mnt/pcie5`.

The full `QwpSenderE2ETest` remains diagnostic: 137 tests now produce two
failures and 34 errors, down from 36 errors at D062. Both decimal aggregate
tests are now green. The open intentional implementation entries are
INT-to-IPv4, STRING-to-DATE and DOUBLE arrays. STRING/VARCHAR aggregates still
stop at the separately unresolved CHAR-to-text contract; other errors are
legacy tests whose expected server-side rejection is now an early local error.

No new store-and-forward test was added. The changed code selects target-native
decimal bytes before persistence and reuses the already exercised decimal
buffer representation. Exact public wire tests prove those bytes; the existing
schema-frame replay suite remains green. A small-integer-specific recovery test
would repeat unchanged persistence code.

## D064 — INT-to-IPv4 translates both source and target null sentinels

Status: iteration 2.43 implemented and locally validated, 2026-09-14. Design
revision 66. The pre-iteration baseline is client `1630c04775` and server
`50d487e8b5`, whose submodule pins that exact client revision. Neither
repository is committed by this decision record.

`intColumn` now accepts an IPv4 target in schema mode and emits target-native
`TYPE_IPv4`. `Integer.MIN_VALUE` is the INT source null sentinel and becomes an
explicit bitmap NULL. A supplied integer zero also becomes bitmap NULL because
zero is QuestDB's IPv4 null sentinel. Every other 32-bit pattern is appended
unchanged, including negative Java integers representing addresses whose first
octet is at least 128. BYTE, SHORT and LONG remain rejected for IPv4, and a
parameterized IPv4 target remains rejected.

This rule is cross-checked against the current server's `QwpWalAppender` IPv4
dispatch, `QwpFixedWidthColumnCursor` null detection and
`WalColumnarRowAppender.putIntToIPv4Column`. The legacy server path reads a
TYPE_INT cursor, translates its source-null row to IPv4 zero, and copies every
other integer verbatim. The client performs the equivalent conversion before
buffering, using the existing four-byte IPv4 column. The implementation adds
one allowed-target condition and one switch arm; it adds no parser, formatter,
address object, allocation, retained state, protocol field or server production
change. Other integer target arms are unchanged. This is source-level reasoning,
not a measured throughput claim.

The component test covers `Integer.MIN_VALUE`, zero, one, minimum-plus-one,
maximum, all-one bits and omission. It asserts the exact TYPE_IPv4 wire code,
bitmap and four stored values. Negative tests retain BYTE/SHORT rejection and
cover IPv4 parameter rejection. Public socket tests prove A/error/C rollback,
the standard one-refresh lifecycle, exact target wire data, and a DECIMAL(3,0)
block remaining pinned before a new IPv4 generation.

The real-server E2E uses public Sender calls for both null cases and six
non-null patterns from `0.0.0.1` through `255.255.255.255`, plus omission. It
waits for the ACK, drains WAL and checks exact rendered IPv4 values and marker
rows; a partial `failed-B` row is absent after an INT-to-UUID local error. The
focused gate also runs the existing INT-null IPv4 test: two tests total, both
green. Its stale comment was corrected because the current client now exercises
target-native schema encoding rather than the server's legacy TYPE_INT arm.

Focused client validation passes 116 tests. Expanded client validation passes
326 tests, all with zero failures, errors or skips. Packaged-JAR verification
passes 49 selected unit tests and both packaging integration tests. The
released-binary matrix passes all three old/new combinations with current JAR
SHA-256 `82140012a88ee02b4aff5e7554a722f9f8a55b1eb83fb3763bd027a9cbc1469d`;
the run used `/mnt/pcie5/qwp-int-ipv4-compat/run.CKpGSZ`.

The full `QwpSenderE2ETest` remains diagnostic: 137 tests now produce two
failures and 33 errors, down from 34 errors at D063. The INT-to-IPv4 test is now
green. The only intentional conversion implementations left are STRING-to-DATE
and DOUBLE arrays; the separately unresolved and legacy-expectation cases remain
outside this slice.

No new store-and-forward test was added. The changed code selects the existing
target-native IPv4 representation before persistence. Exact public wire tests
prove those bytes, while the unchanged schema-frame replay suite remains green.
An INT-specific recovery test would repeat the already covered opaque-frame
persistence contract.

## D065 — STRING-to-DATE mirrors the fixed server grammar before buffering

Status: iteration 2.44 implemented and locally validated, 2026-09-14. Design
revision 67. The pre-iteration baseline is client `19557e2f` and server
`5338ac0845`, whose submodule pins that exact client revision. Neither
repository is committed by this decision record.

`stringColumn` now accepts an ordinary DATE target in schema mode. It attempts
the same formats, in the same order, as the current server's
`DateFormatUtils.parseDate`: PostgreSQL date-time, date-only, date plus zone,
date-time with milliseconds plus zone, UTC date-time with exactly three
fractional digits, then a raw signed epoch-millisecond long. Successful values
are appended as target-native `TYPE_DATE`; `Long.MIN_VALUE` becomes an explicit
bitmap NULL. Missing columns still infer VARCHAR. A parameterized DATE and a
designated DATE remain rejected.

The implementation extends the existing fixed timestamp parser with DATE's
five parse attempts and millisecond calendar/timezone arithmetic. It reuses the
same immutable JDK-derived timezone metadata and performs no per-value date
object allocation. It does not add a general format compiler, converter
registry, retained parser state, protocol field or server production change.
One precise guard rejects negative parsed years that wrap beyond a dynamic
zone's recurring-rule cutoff. Current server source otherwise reaches
`ConcurrentIntHashMap` with a negative year key and throws unchecked
`IllegalArgumentException`; schema mode must report a typed invalid value
instead of accepting bytes the legacy conversion cannot produce. This is a
bounded compatibility guard, not a general defensive layer.

Client and server consume the same 53-case corpus, SHA-256
`048c9360eb48eeda7785c7a0accc72ef3f87361ea9219fc7c6bddafa92f2f3a9`.
It covers raw long forms and the null sentinel, every grammar, numeric and
named zones, daylight-saving gaps and overlaps, hour 24, greedy fractional
milliseconds, year zero and negative years, wrapping arithmetic, invalid
calendar/time fields, missing or unknown zones, trailing input and Unicode
digits. A deterministic 100,015-input differential matches the current server
through its normal return/error domain. The server's unchecked extreme-year
named-zone crash is separately pinned as a local typed rejection.

Component tests assert exact DATE wire type, bitmap and raw millisecond values
for the corpus, plus duplicate, omission, rollback, reset, inference and target
metadata errors. Public socket tests prove exact target wire and rollback, and
that a completed DATE block retains its schema snapshot before the next block
rebinds to VARCHAR. Real-server tests write every valid corpus value through
public Sender calls, wait for ACK, drain WAL and compare exact raw DATE values
and nullness; a second A/error/C test proves partial-row cancellation. The
existing successful DATE coercion test and its now-local public rejection test
also pass: four focused server tests total.

Expanded client acceptance passes 443 tests with zero failures, errors or
skips. Packaged-JAR verification passes the three DATE component tests and both
packaging integration tests. The released-binary matrix passes current client
to QuestDB 10.0.1 in legacy mode, client 1.3.9 to current server in legacy mode,
and current client to current server in schema mode. Current artifact hashes
are client `58836b2fb469c87e264ff2d5f1a7f17f8e6ff9ad5ace3f6f0e5014034b44022d`
and server `dec115136d83b45f4f390bee7b50337fb0da7ee93cd6ae6c0427c0794fe31ce9d`;
the run used `/mnt/pcie5/qwp-string-date-compat/run.OpLD7e`.

The full `QwpSenderE2ETest` remains diagnostic: its intentional conversion
error count falls from 33 to 32 because both DATE tests are now green. That run
also encountered two independent server `No space left` errors and produced
cascading assertion failures, so its unrelated failure count is not acceptance
evidence. The final intentional conversion implementation left is ranked
DOUBLE-array identity. BINARY parser paths, LONG_ARRAY, non-ASCII CHAR text and
malformed decimal metadata remain explicit server-contract decisions.

## D066 — DOUBLE arrays reuse the existing wire path and pin rank

Status: iteration 2.45 implemented and locally validated, 2026-09-14. Design
revision 68. The pre-iteration baseline is client `47a37a6f` and server
`c1f970220f`, whose submodule pins that exact client revision. Neither
repository is committed by this decision record.

All four public `doubleArray` representations now participate in schema mode.
The primitive overloads have ranks 1, 2 and 3. The N-dimensional `DoubleArray`
wrapper exposes its current rank through an O(1) accessor, including after
`reshape`. A known target must be a canonical strong DOUBLE array of exactly
that rank. A confirmed missing column infers DOUBLE_ARRAY and pins its first
effective rank in the existing column buffer across resets; a later different
rank fails locally rather than creating a mixed-rank block. Null references
remain no-ops, empty shapes remain values, and the existing duplicate-first,
row rollback and one-refresh rules are preserved. Duplicate suppression runs
before inspecting a wrapper's rank, so an invalid or closed ignored second
value cannot change first-value-wins behavior.

This is rank-checked identity, not an element conversion. The binding selects
the existing `TYPE_DOUBLE_ARRAY` column, and the established buffer and encoder
continue to own shape validation, element copying and wire output. No converter,
second shape model, per-row object, array extension parameter, send-time
transformation or server production change was added. The wrapper accessor
avoids a second traversal or temporary capture before validation. The only new
retained state is one integer on a confirmed-missing inferred column; known
schemas already carry rank in the encoded `ColumnType`.

No throughput claim is made. There was no successful schema-aware array path
to compare before this slice, while the legacy append and encode loops are
unchanged. The new successful path adds only constant-time schema/rank checks
before the same element copy, so a synthetic A/B number would not isolate a
real regression boundary.

The exclusion of LONG arrays is source-backed. The current server validates
DOUBLE_ARRAY batch rank but does not compare a cursor's element type with an
existing array target. `WalColumnarRowAppender.putArrayColumn` then constructs
storage from the cursor element type. Auto-creation rejects LONG_ARRAY, and SQL
does not currently create LONG[] columns. Accepting DOUBLE_ARRAY-to-LONG[] or
enabling `longArray` in schema mode would therefore reproduce a server
validation gap, not implement a supported cast. Synthetic schema tests pin the
local rejection because a real-server LONG[] fixture is not constructible.

Five component tests cover every public representation, ranks 1, 2, 3 and 32,
live wrapper reshaping, all null overloads, empty shapes, raw NaN payload bits,
signed zero, infinity, duplicates, ragged inputs, rollback, reset, missing-rank
pinning, wrong element type, weak rank, parameters and designated-column
rejection. Three focused public-socket tests cover exact target wire bytes,
A/error/C rollback, the standard one-refresh path, generation pinning and null
no-op before negotiation. The expanded client acceptance selection passes 494
tests with zero failures, errors or skips. Packaged-JAR verification passes the
five component tests and both packaging integration tests.

The focused real-server gate passes nine tests: two new tests cover ranks 1
through 4, all public representations, empty shapes, exact stored shapes, ACKs
and partial-row recovery; seven existing array/rejection tests now assert the
new early local errors. The complete 137-test `QwpSenderE2ETest` diagnostic has
113 passing tests, two assertion failures and 22 early schema errors. None is
an unimplemented intentional conversion: 17 are old aggregate rejection tests,
three are specialized tests that still expect a server-side error stage, two
are the explicit CHAR-to-text boundary, and the assertion failures are stale
empty/null-column-name message checks. With the test JVM started on
`/mnt/pcie5`, this run has no disk-space infrastructure failures.

The released-binary matrix passes current client to QuestDB 10.0.1 in legacy
mode, client 1.3.9 to current server in legacy mode, and current client to
current server in schema mode. Current artifact hashes are client
`6d3cb13f4058e1cf9a2af7b3459266a2beffb4a92e7f148880dc5fcf40b24292`
and server
`87df2cee84f1cad1c450831fc2ec6f995437cb5eda701486bf452a8c3a3da524`;
the run used `/mnt/pcie5/qwp-double-array-compat-final/run.pK2tjd`.

No array-specific store-and-forward test was added. Schema mode writes the
same self-contained DOUBLE_ARRAY bytes before persistence, and the 494-test
gate includes schema network replay, self-sufficient-frame and recovery replay
coverage. Repeating that unchanged persistence path with another value family
would not add a new contract assertion.

A full client-module diagnostic encountered the existing
`QuestDBServerRecoveryTest.testFacadeStartsWhileServerDownThenWritesAndReaderConnectsOnRecovery`
expectation: the first effective write now waits for mandatory schema
negotiation and times out while no server exists. The timed-out test leaves its
reconnecting sender active, so that diagnostic run was stopped. This is a
separate startup-contract fixture, not an array regression; the 494-test
acceptance set is green.

The intentional, server-supported public-setter conversion catalogue is now
complete. BINARY parser paths, LONG_ARRAY, non-ASCII CHAR text and malformed
decimal metadata remain explicit server-contract decisions, not unfinished
client converters.

## D067 — ASYNC cold start uses best-effort legacy encoding

Status: design accepted, implementation pending, 2026-09-15. Design revision
69. No production code or tests changed in this decision record.

The full client-module diagnostic exposed a real contract conflict rather
than an array failure. `lazy_connect=true` selects the existing asynchronous
initial-connect policy, whose public contract says producers can buffer while
the server is unavailable. Schema activation instead made the first effective
write wait up to 30 seconds for negotiation and throw `SCHEMA_UNAVAILABLE`.
Preserving both mandatory cold-cache schema lookup and non-blocking offline
writes would require retaining original values for later conversion. That
would reintroduce the rejected send-time transformation model.

Reuse the existing initial-connect choice instead. `OFF` makes one strict
foreground connection/negotiation attempt, while `SYNC` retries strictly within
its configured reconnect budget. Before any successful handshake, `ASYNC`
accepts a row immediately using the existing legacy conversion and flag-clear
framing, provided recovery has not already found schema-framed data. The first
effective operation pins the row's mode. A handshake completed during a partial
legacy row affects only the next row; buffered bytes are never relabelled,
rewritten or reconverted.

This is a narrow startup exception, not a permanent schema switch. A successful
old-server handshake keeps subsequent rows legacy, but does not prohibit a
later one-way upgrade when another connection confirms schema support. Once
support is confirmed, cache misses and later outages retain the strict
30-second lookup/error contract and never fall back. A recovered extended
backlog also carries the sticky schema requirement before the fresh sender has
connected, so it overrides `ASYNC` fallback and waits for a supporting server.

Source review confirms the simple wire path. The server rejects a schema flag
without negotiation but accepts flag-clear legacy frames on a negotiated
connection. The sender already pins a partial row and separates legacy and
schema-bound buffers at a row boundary. The cursor engine already records when
recovered frames require schema support. Implementation therefore needs no new
protocol field, raw-row store, deferred converter, persisted side file or
schema-specific configuration option.

The availability cost is explicit. Pre-handshake rows use the whole legacy
conversion contract, not merely weaker validation. Handshake timing can change
successful stored values for cases such as sub-microsecond timestamps,
overflow and source-null normalization. A bad legacy row can also be accepted
locally and later receive a terminal NACK that stops the persisted queue; no
schema reply can repair its bytes. API documentation and release notes must
call this legacy conversion before confirmation and explain existing terminal
recovery, rather than promise deterministic schema-mode results.

Required acceptance covers offline `ASYNC` startup followed by old and new
servers, handshake before the first row and during a partial row, old-to-new
upgrade, exact legacy/schema value differences, mixed replay across restart,
recovered schema-required backlog while offline, terminal rejection of a bad
startup row and a post-upgrade cache miss. Replace the stale facade recovery
expectation that a blocking first write returns before the test starts its
server; retain its observable construction, reconnect, write and read recovery
coverage under the new contract.

## D068 — Integrate ASYNC fallback with one existing sticky latch

Status: implemented and validated in the working tree, 2026-09-15. Design
revision 70. Not yet committed.

Further source tracing reduced D067 to one producer-side decision in
`QwpWebSocketSender.bindingForEffectiveWrite()`. Preserve the existing partial-
row return first and call `ensureConnected()` as today. Before the current
bounded negotiation/lookup path, return the existing legacy binding result
when `initialConnectMode == ASYNC && !cursorEngine.requiresSchema()`, clearing
the per-call schema-freshness marker as on other legacy/in-progress paths.

Do not consult `hasEverConnected()`. A fresh asynchronous sender and a sender
successfully connected to an old server both need legacy behavior, so
distinguishing them adds no decision value. The engine requirement already
becomes sticky when a supporting handshake succeeds and is restored directly
from recovered schema-framed data. It is volatile and published before the I/O
loop exposes the supporting connection, so it is also the correct race and
recovery authority. If it flips immediately after the producer reads false,
the current row is deliberately legacy and the existing row-boundary adoption
makes the next row schema-aware.

No new state, helper abstraction, cache probe, schema mode, configuration key,
protocol field, raw-value owner, encoder path, persisted format, coordinator or
server change is justified. Existing retired-buffer and mixed-flush code owns
legacy-to-schema ordering, while the server already accepts flag-clear data on
a negotiated connection.

The essential new coverage is narrow. Replace the asynchronous first-write
blocking test with one public test that buffers legacy data offline, connects
to a supporting server and verifies exact legacy then schema representations;
use a nanosecond `Instant` so the semantic difference is observable. Retarget
the existing timeout and interruption tests to a confirmed schema connection
whose describe response is withheld. Add one public recovered-schema-backlog
test proving that `ASYNC` still waits or fails instead of appending legacy data.
Reuse the existing facade late-server recovery, partial-row old-to-new upgrade,
mixed restart replay, real-server negotiated-legacy and terminal-NACK tests;
do not duplicate those fixtures. Final acceptance requires the complete client
suite and the standing released-binary compatibility matrix.

The implementation matches the brief. `bindingForEffectiveWrite()` keeps its
in-progress-row path first, starts the existing I/O loop, then returns the
legacy binding for `ASYNC` only while the cursor engine does not require
schema. No production state, helper abstraction, coordinator path, encoder,
store-and-forward format, protocol field or server code changed. The public
`InitialConnectMode.ASYNC` documentation now states the legacy-before-support,
sticky-upgrade and recovered-backlog behavior.

One fixture assumption was disproved during implementation: its `ts` column was
marked as the designated timestamp, so it could not expose the ordinary
timestamp representation difference. The fixture now reports no designated
column for that test. A nanosecond `Instant` then proves the actual contract:
the offline row contains legacy microseconds and the post-handshake row contains
schema-mode nanoseconds.

Four focused new or changed tests pass. The full
`QwpSchemaSenderIntegrationTest` passes 72 tests and
`InitialConnectAsyncTest` passes seven. The complete client module passes 3,720
tests with zero failures or errors and seven pre-existing skips. The current
server's five-test `QwpSchemaIdentityE2ETest` passes, including mixed
legacy/schema replay and legacy data on a negotiated connection.

The released-binary matrix also passes: current client to QuestDB 10.0.1 uses
legacy conversion, client 1.3.9 to the current server remains legacy, and the
current client to current server enables schema mode. The packaged artifacts
used client SHA-256
`74bbc706b69a53b89b84ce738865321b639d06b92200e1e1fa497e85c36e2e40`
and server SHA-256
`38d597d80b12bb3c5ea13aa490b23df83061fb7299394b7c36a2c8227860627d`.
The completed full-client, build, server and compatibility runs used scratch
under `/mnt/pcie5`; `git diff --check` passes.

## D069 — Restore CHAR-to-text compatibility with valid UTF-8

Status: implemented and locally validated in the working tree, 2026-09-15.
Design revision 71. Not yet committed.

The complete server E2E classes exposed a conversion omitted by the earlier
hand-picked gates. On a known STRING or VARCHAR column, `charColumn` requested
only a CHAR target and therefore threw `UNSUPPORTED_FEATURE`. The existing
server accepts CHAR for both text targets, so mandatory schema mode turned a
previously valid public call into a local failure. The design had documented
the pair as an unresolved boundary, but that classification was not acceptable
for an always-on feature.

Resolve the actual target in the existing `charColumn` path. Keep exact CHAR
storage unchanged. For STRING and VARCHAR, append the character through one
reused single-character sink into the existing target-native VARCHAR buffer.
This adds no conversion framework, protocol field, sender state, replay path,
server change or per-row allocation.

Do not reproduce the server formatter's non-ASCII VARCHAR truncation. A valid
non-surrogate Java CHAR is encoded as valid UTF-8 for both text targets. A lone
UTF-16 surrogate cannot be represented as a standalone Unicode character, so
the setter reports `INVALID_VALUE` instead of silently replacing or corrupting
it. Native CHAR targets continue to preserve every 16-bit code unit.

The focused component test passes four tests and checks exact target wire bytes
for ASCII and non-ASCII input plus surrogate rejection. The complete client
module passes 3,721 tests with no failures or errors and seven skips. After
installing that client artifact, the complete 138-test real-server E2E class
passes both existing regressions (`testCoercionToString` and
`testCoercionToVarchar`) and a new non-ASCII SQL assertion. Its remaining two
failures and 20 errors are the previously catalogued DATE/GEOHASH wire issues
and stale legacy rejection expectations, down from two failures and 22 errors
before this change. All test scratch owned by this work is under `/mnt/pcie5`.

## D070 — Align complete server E2Es with mandatory schema validation

Status: accepted, 2026-09-15. The associated QuestDB changes are test-only.

The complete server test classes confirmed that the remaining failures were
stale test contracts and unstable test-only buffer handles, not missing client
conversions. Twenty-two Sender cases expected a server rejection for invalid
input that a schema-aware client must now reject before encoding. Their shared
helper now accepts only typed, non-retryable local schema errors and verifies
row recovery. A separate strict helper retains the server-NACK contract for
server-owned failures. No helper may accept either outcome interchangeably.

The deferred-commit case now states the actual ownership rule: a local schema
error cancels only the current partial row. It cannot ask the server to roll
back already completed deferred rows because the invalid row never reached the
server. Empty column names, STRING input for DOUBLE, and timestamp overflow
likewise assert local structured errors and subsequent valid writes.

DATE and GEOHASH integration fixtures exposed a separate test bug. They held a
test-only column object across rows, but schema installation can replace the
sender's active table buffer at the row boundary. Public integration tests now
write through the public row API. The six deliberately low-level tests keep
their raw coverage but reacquire the active column on every row. This adds no
production state, compatibility path, defensive check or abstraction.

The two real-server asynchronous in-flight/fragmentation tests no longer try to
force a type mismatch through a schema-aware public API. They now send a valid
public row to a non-WAL table: local validation succeeds and the server returns
the server-owned terminal NACK whose propagation those tests exercise. The
immediate type-mismatch test was removed because that state is unreachable after
discovery and its useful NACK behavior duplicates the existing non-WAL test.
Deterministic client tests continue to cover broader asynchronous NACK and
terminal-error delivery.

Validation passes the complete 138-test `QwpSenderE2ETest`, the complete
117-test `QwpWebSocketSenderReceiverTest` with one existing skip, and all six
`QwpSenderLowLevelTest` cases. The combined schema-aware server gate passes 480
tests with zero failures or errors and one existing skip. The complete client
module passes 3,721 tests with zero failures or errors and seven existing skips.
The two asynchronous server-rejection cases pass three times each, including
their fragmented-transport variant. Final server scratch is under
`/mnt/pcie5/qwp-schema-aware-tests/final-repeat`; the final complete client run
uses `/mnt/pcie5/qwp-schema-aware-tests/client-final`, with no `/tmp` paths in
its 295 current Surefire reports. No client or server production source changed
in this decision.

### Legacy-to-schema adoption moves to the batch boundary

The 2026-09-17 complexity review ranked the duplicated mixed-family flush first.
Rather than fold it into the split path, this decision removes its cause. Row
boundary adoption was the only source of mixed legacy/schema batches. A batch
now has one wire contract, selected by its first committed row
(`isPendingBatchLegacy`); `bindingForEffectiveWrite()` keeps returning the
legacy contract while that batch is pending, and the first row after the flush
adopts schema mode. The upgrade stays one-way and is delayed by at most one
batch. `flushPendingRowsMixed()`, `mixedFramesFit()`, `encodeSingleTableFrame()`
and `isMixedSchemaBatch()` are deleted; the encoder is unchanged. This
supersedes the earlier "next effective row adopts schema mode" rule. The
trade-off: rows written after the handshake but inside the pending legacy batch
keep legacy conversion (for example microsecond `Instant` truncation). Which
rows preceded the handshake was already a timing race.

The rewrite exposed an existing defect, reproduced on unmodified source: legacy
row, flush, supporting reconnect, then a row on the same table threw
`IllegalStateException: schema binding requires an empty, unbound table buffer`
from the column setter. `installSchemaBinding()` bound in place whenever the
buffer had no rows, but a flushed legacy buffer keeps its column layout. It now
binds in place only a buffer with no columns and replaces the buffer otherwise.
Both rewritten upgrade tests cover that path.

### FLAG_SCHEMA moves from 0x02 to 0x40

Merging server `master` on 2026-09-18 surfaced a wire-bit collision that git
merged cleanly: server PR #7531 (browser negotiation) had already claimed `0x02`
for `FLAG_DURABLE_ACK_POLL`, a zero-table control frame. The ingress dispatch
read a poll frame as a schema-flagged frame on a connection that had not
negotiated schema and rejected it, failing seven durable-ack poll tests in
`QwpIngressAckLeapfrogTest` and `QwpIngressUpgradeProcessorResumeRecvTest`.
The poll bit is already on `master`, while `FLAG_SCHEMA` is unreleased, so
`FLAG_SCHEMA` moves to the free bit `0x40` in both the server and the client
`QwpConstants`. This supersedes the `FLAG_SCHEMA=0x02` value recorded in design
revision 13. The trade-off: store-and-forward frames persisted by an earlier
build of this branch carry `0x02` and no longer replay as schema frames.
