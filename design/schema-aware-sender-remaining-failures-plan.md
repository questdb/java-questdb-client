# Plan: resolve the remaining 47 schema-aware Sender test failures

Status: all five phases accepted through iteration 2.36, 2026-09-12. The user
requested sequential completion of the remaining conversions. All original 47
failures now pass.
Follow the [main design](schema-aware-sender.md);
decision history and evidence belong in the [journal](schema-aware-sender-journal.md).

## Recommendation and scope

Fix the nine stale expectations/fixtures first, then add conversions in small
end-to-end increments: text output, timestamps, small scalar targets, decimals.
Do not treat 47 failing methods as 47 production bugs, or implement a generic
conversion framework to make them disappear.

All functional changes are in QWP client encoding/binding. No SQL parser,
planner or execution-engine change is proposed. SQL in these tests creates
tables and verifies stored results.

| Phase | Baseline failures addressed | Count | Baseline failures left after the phase |
| --- | --- | ---: | ---: |
| 1 | Existing semantics and invalid test fixtures | 9 | 38 |
| 2 | LONG, UUID and floating-point text output | 8 | 30 |
| 3 | Ordinary timestamp input/output conversions | 4 | 26 |
| 4 | CHAR, LONG256 and GEOHASH targets | 4 | 22 |
| 5 | Decimal conversions | 22 | 0 |

These are coverage checkpoints, not instructions to force the counts down.
A method is resolved only when its intended contract is tested and passes.
The substeps below are separate reviewable increments, not one large phase PR.

Current checkpoint: phases 1–4 and phases 5a–5e are accepted. Phase 5a covers
all 18 logical native-decimal pairs; phase 5b covers LONG into all six decimal
target classes; phase 5c covers STRING into all six; phase 5d covers FLOAT and
DOUBLE into all six; phase 5e covers all native decimal widths into STRING and
VARCHAR. The original 56-test conversion class is green. Exact-current client
validation passes the 46-test public integration class, including four phase-5e
tests, plus two packaging checks, 950 canonical tests and an expanded 1,275-test
regression gate. Server validation passes three Phase-5e E2Es, all 56 original
conversion tests, 363 canonical tests
and three 126-test seeded repeats with 24 selected XML reports each.
Legacy mode, row recovery and pinned encodings are preserved.
D041 records the NaN-null approval and formatter work; D042 records the small
raw timestamp extension, corrected wire fixtures and native LONG measurements.
D043 records the fixed timestamp parser, its source-quirk regressions, broad
matched-runtime differential and the measured 2–3% native string-write cost.
D044 records checked timestamp-to-text normalization, exact server formatting,
63 shared vectors and 160,202 public differential checks. Its native timestamp
performance guard detects no repeatable regression.
D045 records CHAR's first-character conversion, exact wire-presence versus
SQL-null distinction, 23 shared vectors, 65,549 public differential checks and
the missing standard schema-refresh catch caught by the new setter's own test.
D046 records STRING-to-LONG256's four-local-limb parser, 23 shared vectors,
40,055 public differential checks, exact rollback/generation wire tests and
independent legacy acceptance/rejection. The native string-write regression
guard passes; it is not a LONG256 throughput measurement.
D047 records all 60 GEOHASH precisions, exact schema bitmap framing, empty/null
normalization, 7,210 public differential checks and canonical packed-type
validation. The two-path performance guard passes with measured small costs:
about 1.46–1.80% append time and 0.17–1.15% encoding time in native string checks.
D048 records phase 5a's exact rescale/precision/storage checks, target-scale
retention, caller immutability, MISSING/legacy behavior, row rollback/schema
generation and bitmap-only decimal null characterization. Its 60-row shared
corpus has 45 values and 15 invalid cases across all 18 logical pairs (SHA-256
`3819694736f54de4d2f2f396b09ff487667051c6045b34cc6d7579552551d11d`).
The frozen candidate JAR SHA-256 is
`1e814a2f82836b5c9ac04cf7b5674e56f1b939440df486ae6628ebb1b8eebb08`.
The three seeded 101-test repeats have 20 archived XML reports each.
D049 records phase 5b's scale-zero LONG conversion, stable schema-mode
`Long.MIN_VALUE` null, target-scale locking, direct target-width wire output,
rollback and schema-generation coverage. Its shared corpus has 39 rows: 19
values, 14 invalid values and six null cases across every decimal target class
(SHA-256
`d9a47432a14583f8675b0035ab10c3ef9f36e78c9f4aab6a8634958eb5aafcb1`).
The frozen candidate JAR SHA-256 is
`41d137a1c2388ed21f776e372771785377d91c15a887d94f04e82c97f505d403`.
The 12-run native LONG-to-LONG guard detects no repeatable 5% cycles or
instructions regression; it is not decimal-throughput evidence.
D050 records phase 5c's target-precision/scale parser, direct target wire output,
stable bitmap-null rules and deliberate correction of three unsafe server
behaviors. Its 55-row shared corpus has 19 values, 24 invalid values and 12
nulls (SHA-256
`67739711b320b07fd4d5eb72ecd19e14b5999f6a07e756ed83a847d809577568`).
The installed candidate JAR SHA-256 is
`30e593b27a6eb71c022e198bb5e331af973af643e1fd3eedde8555c8d2d2112c`.
The 3,601,944-case differential passes ordinary server parity and records 72
deliberate malformed-prefix corrections. The 12-run native STRING-to-VARCHAR
guard detects no repeatable 5% cycles or instructions regression; it is not
decimal, network or ingestion throughput evidence.
D051 records phase 5d's exact FLOAT widening, shortest round-tripping decimal,
lossless target-scale check, non-finite bitmap nulls and direct target-width
output. Its shared corpus has 60 rows: 37 values, 17 invalid values and six nulls
(SHA-256
`366a889719c6e7b688ff00e1bdfb95e273ab192508a1094dac5312c3d899611d`).
The 5,996,868-case differential matches the server's acceptance, target scale and
limbs. The installed candidate JAR SHA-256 is
`da44a24c6d100a0bff8a984f37c40409a0e3cc3f132f7ac9304a9e0b5d25c99d`.
Performance validation caught and removed a native hot-inlining regression by
keeping decimal conversion in the existing cold nonnumeric helper. The final
12-run guards detect no material native FLOAT/DOUBLE regression; they do not
measure decimal conversion, network or ingestion throughput.

D053 records phase 5e's corrected Decimal256 promotion, unsigned scale-byte
semantics, full-physical public values and intentional per-value mixed-width/
scale behavior for known text targets. Its 24-row shared corpus has 18 values
and six public null cases across all six source/target pairs (SHA-256
`4fd43e6d1c08fc76cee6505e6ef06a250aa30281bd686bb0115ba6c710065b5f`).
The 3,000,294-case differential matches server scalar formatting over full
physical limbs and arbitrary integer scales. The accepted and installed JAR is
`e06759605ee3d1cf01c3277ba13531b0172e42138042c1f2f49c2db49e04b22d`.
Six counterbalanced pairs for each native identity width show instruction
changes of +1.05%, +1.38% and +0.71%, with no repeatable 5% regression. This is
an identity-path guard, not a decimal-to-text, network or ingestion benchmark.

Baseline: iteration 2.20, client HEAD
`981bdb02a471f3b290c89b8e78cbc422610e329e`, server HEAD
`12a33d651e51e2682e7a448c8db5168fc72dfad3`, with the recorded dirty working
trees. Frozen client JAR SHA-256:
`102128206d124f8963b89dd6f6764da641086697272f0bb9d93493c55903330b`.

Server evidence is in
`core/target/schema-text-source-null.2.20-server/run-manifest.md`,
`logs/broad.log` and `logs/broad-failure-names.txt`: the fixed 333-test
selection has seven failures and 40 errors, exactly 47 methods. The separate
271-test green gate and three 26-test seeded repeats passed. The inventory at
the end assigns every failing method exactly once.

## Phase 1: restore honest tests — 9 failures

### 1a. Separate existing schema and legacy behavior — 5

Accepted in iteration 2.21. The following describes the completed repair.

The three boolean tests already exercise working client conversions but expect
legacy omissions to become false/zero. Update the schema expectations to the
target's missing value: null for nullable targets, zero for BYTE/SHORT. Assert
nullness as well as printed values. Keep the existing independent unflagged
legacy comparison in `QwpSchemaBooleanE2ETest`.

The two floating tests deliberately describe legacy behavior: supplied NaN can
depend on the bitmap, and positive 2^63 can clamp to LONG maximum. Their current
public Sender calls instead apply the already-approved schema rules. Send the
legacy cases as actual unflagged FLOAT/DOUBLE frames, preserving both the
accepted values and server rejection checks. Keep explicit schema tests for
NaN normalization and local rejection at 2^63 in
`QwpSchemaFloatingNumericE2ETest`; do not restore legacy behavior in production.

### 1b. Remove direct mutation of Sender-owned buffers — 4

Accepted in iteration 2.22. The following describes the completed repair.

The two geohash-to-text and two designated-LONG timestamp tests obtain a buffer
through `getTableBuffer()` and manually add source columns. Subsequent public
`at()`/`atNow()` installs schema bindings and uses extended framing, so these
are no longer valid fixtures.

Rebuild them with a standalone native buffer, ordinary unflagged encoder and
nonrequesting WebSocket client. Assert source types, geohash precision, bitmap,
values, ACK and exact stored rows. Keep named LONG designated-timestamp wire
semantics separate from public `at()`/unit conversion. Do not relax Sender
ownership or add a schema opt-out.

The public native geohash converter remains unimplemented after this repair.
Keep that fact visible; a passing legacy wire test does not implement it.

## Phase 2: text output — 8 failures, three increments

### 2a. LONG to STRING/VARCHAR/SYMBOL — 3

Accepted in iteration 2.23. The following describes the implemented contract.

Approved on 2026-09-11 and recorded in the main contract before code:
supplied `Long.MIN_VALUE` becomes a target null, as an effective write rather
than a no-op. Ordinary values use canonical signed decimal text. The legacy
bitmap-present special value is the four-character text `"null"`, not decimal
minimum; preserve the 2.20 raw characterization.

Use the existing target VARCHAR representation for STRING/VARCHAR and existing
dictionary-backed SYMBOL representation for SYMBOL. Reuse verified formatters
and append paths, with no raw-LONG fallback. Cover MIN+1/MAX, zero, negatives,
omission, explicit sentinel, repeated symbols and duplicate-first behavior.

### 2b. UUID to STRING/VARCHAR — 2

Accepted in iteration 2.24, following separate approval on 2026-09-11. Only the pair
with both limbs equal to MIN becomes target SQL NULL, as an effective write.
A single minimum limb remains a real value. Preserve `(lo, hi)` input order and
canonical lowercase formatting. Do not add UUID-to-SYMBOL or change native UUID
bytes, inference or legacy behavior. The verified checkpoint is 35 to 33, with
only the two UUID-to-text baseline failures removed and no additions.

The obsolete public UUID-to-text rejection cases in
`QwpSchemaTextSourceNullE2ETest` now exercise unsupported UUID-to-SYMBOL instead;
its independent legacy-byte tests are unchanged. Shared literal vectors,
null-first ignored duplicates, omission/reset, public row recovery, native/text
encoding generations and component-seeded text replay are covered.

### 2c. FLOAT/DOUBLE to STRING/VARCHAR/SYMBOL — 3 baseline methods

Accepted in iteration 2.25. The following describes the implemented contract.

Cover the floating input family coherently, including the FLOAT overload
although these three failing methods happen to use DOUBLE. Match the server's
actual formatter, including FLOAT widening before formatting. Check signed zero,
finite rounding boundaries, infinities and NaN with/without an omitted row.

Separately approved on 2026-09-11: supplied FLOAT/DOUBLE NaN becomes SQL NULL
for all three text/SYMBOL targets, as an effective first write. Legacy raw
characterization confirms bitmap-present NaN becomes literal `NaN` instead.
Do not substitute Java formatting or add a per-row policy switch. Differential
tests found that the older client formatter differs from current server Ryu;
keep source-faithful formatting isolated from legacy paths.

## Phase 3: ordinary timestamp conversions — 4 failures

### 3a. LONG to timestamp — 1

Accepted in iteration 2.26. The following describes the implemented contract.

Reuse target timestamp encoding. LONG denotes a raw count in the target unit,
not a value whose unit is inferred or converted from micros. Test both micro-
and nanosecond targets, negative/zero/boundary values and source-null behavior.
This must not enable named writes to the designated timestamp.

### 3b. STRING to timestamp — 1

Accepted in iteration 2.27. The following describes the implemented contract.

Port/reuse the server's actual timestamp grammar and value checks, not an SQL
cast or an assumed ISO-only parser. The server uses its microsecond parser
and checked multiplication by 1000 for a nano target. Test accepted/rejected
grammar, overflow, fractional precision, null/omission and failed-row recovery.

Use a fixed parser for the eleven actual formats, preserving their priority,
not the server's general compiler. Copy the relevant calendar helpers exactly:
`yearMicros` has asymmetric negative-overflow saturation, while surrounding
arithmetic and timezone subtraction can wrap. The narrow zone adapter must
retain locale-token ordering and use the separately parsed year for future
rules, even after epoch arithmetic wraps. Plain JDK instant lookup differs on
a reproduced extreme-year input (D043). Use public JDK metadata, not reflection
or a general timezone engine. Same-runtime differential tests must precede
acceptance; runtime locale/tzdb data and the class-initialized reference century
remain independently sourced by client and server. Do not quietly narrow the
grammar or add protocol metadata to this increment.

### 3c. Timestamp to STRING/VARCHAR — 2

Accepted in iteration 2.28. The following describes the tested contract.

Cover both primitive timestamp units and `Instant`, not only the MICROS calls
in the failing tests. The extra overloads are coherent-family acceptance
coverage, not additional failures in the 47-method count.
The server formats text to millisecond precision; nanoseconds first
divide by 1000 with truncation toward zero to obtain micros, then
`MicrosFormatUtils.appendDateTime` uses the millisecond UTC pattern. Characterize
negative sub-microsecond values, extremes and source-null behavior. Existing
timestamp-target range/precision exceptions do not silently extend to text.
The timestamp cursor uses bitmap nulls only: present MICRO MIN formats as empty
text, while present NANO MIN formats as a finite historical timestamp. Preserve
these semantics; the LONG/NaN text-null approvals do not apply by analogy.
Separately approved by the user: checked conversion into the legacy source
precision for larger units and Instant, rejecting out-of-range values before
append. Keep legacy mode unchanged. D044 records this text-target overflow
decision; it is not an implicit extension of D021.

## Phase 4: small scalar targets — 4 failures

### 4a. CHAR identity and STRING to CHAR — 2

Accepted in iteration 2.29. The following describes the completed slice.

Use target-native CHAR encoding and the existing native CHAR setter. For text
input, reproduce QWP's first decodable BMP-character behavior. Empty input,
malformed raw UTF-8 and a supplementary first code point produce NUL. Test
public malformed UTF-16 separately: `stringColumn` applies replacement during
UTF-8 encoding, so assert the actual replacement bytes and resulting CHAR,
not an assumed NUL. Test more than the three ASCII values in the old test.

Do not bundle CHAR-to-text: its non-ASCII VARCHAR behavior is a separate,
documented server compatibility issue.

### 4b. STRING to LONG256 — 1

Accepted in iteration 2.30. The following describes the completed slice.

The target LONG256 representation and server-equivalent parser/limb order use
existing value storage. Tests cover accepted syntax, invalid/overflow input,
null, leading zeros, boundaries, exact wire bytes and failed-row recovery.

Read-only source verification after 2.29 confirms a lowercase `0x` prefix and
2–64 hex digits in complete byte pairs; hex digits themselves are case-insensitive.
Reject signs, whitespace, odd digit counts and overlong input. Parse right-to-left
into four local longs and append with existing addLong256; no retained scratch
object is needed. Java null/omission is null, but the literal whose four limbs
are all Long.MIN_VALUE must be rejected, matching the server parser/appender.

Native LONG256 setter activation remains separate, unimplemented work.

### 4c. STRING to GEOHASH — 1

Accepted in iteration 2.31. The following describes the completed slice.

Bind target bits from the schema and encode the parsed value with that precision.
Match the server's base32 parsing, truncation, empty/null and consumed-character
rules, including its 12-character cap. Test multiple precisions and invalid or
insufficient input, not just GEOHASH(4c).

Use the precision already packed in the server's column type. The current
schema response sends that full type code and an empty extension block;
decimal precision/scale is likewise already encoded in its type code. Do not
add new protocol parameter fields or stop rejecting unknown extension blocks
just to enable these types. Keep precision pinned with the existing schema
snapshot. Native geohash input/text-output activation remains separately tracked;
the phase-1 raw fixture repair does not supply it.

Read-only preparation after 2.30 identifies one storage detail to address:
`addGeoHash` currently sets precision only on a value write, but a null-only
column must also carry the schema's precision. Reuse the existing precision
field and keep the adjustment narrow.

Source revalidation for 2.31 corrects the initial empty-value proposal. Native
GEOHASH without a bitmap treats a packed all-ones value as null, losing valid
maximum values at byte-aligned precisions. With a bitmap, packing empty text
as all ones can instead produce a non-null value when packed width is smaller
than storage width. Preserve the server's VARCHAR conversion result: normalize
empty text to bitmap null, and always include the bitmap for schema-directed
GEOHASH columns, including all-present batches. This costs one bit per row and
needs no value scan or new retained state. Force only columns already using
nullable storage; low-level non-nullable schema buffers and legacy encoding
remain unchanged. Runtime characterization and exact wire tests establish both
cases: source-wire presence does not imply target-wire presence. Recognize only
the complete canonical packed type for a valid precision, preserving rejection
of malformed and future-flagged types.

## Phase 5: decimals — 22 failures, five increments

Keep decimals last because target precision/scale, source scale, wire width and
null semantics are shared dependencies. Reuse existing decimal arithmetic and
8/16/32-byte column storage only after checking their behavior against the server.

| Increment | Baseline methods | Required behavior |
| --- | ---: | --- |
| 5a. Native decimal to decimal (accepted in 2.32) | 4 | Target precision/scale and wire representation; all source widths; exact rescale, narrowing and overflow |
| 5b. LONG to decimal (accepted in 2.33) | 4 | Checked scaling and target precision/storage limits; explicit source-null rule |
| 5c. STRING to decimal (accepted in 2.34) | 4 | Server-equivalent grammar and target-scale parsing; precision-loss/overflow failures |
| 5d. FLOAT/DOUBLE to decimal (accepted in 2.35) | 4 | Actual server rounding/conversion behavior, non-finite values, underflow and overflow |
| 5e. Native decimal to text (accepted in 2.36) | 6 | All three input widths to STRING/VARCHAR, preserving per-value scale and server scalar formatting |

5a is accepted in iteration 2.32 and produces a working real-Sender E2E before
the later input families are added. Small decimal targets use DECIMAL64 wire
storage while enforcing the narrower target precision and scale. Convert
every input width/scale to the target precision, scale and compatible storage
width at setter time. The encoded decimal scale must therefore equal the
target scale; the original input scale is only conversion input. Assert that
wire scale and exact converted limbs in cross-width E2Es. Do not merely relabel
source limbs or leave rescaling/precision validation to the server. Reuse
existing append storage by supplying already-converted values, rather than
adopting its legacy first-input-scale rule for schema bindings.
Cover emitted all-null columns separately from null setters that omit the
column entirely; neither has a non-null coefficient to rescale.

Read-only preparation after phase 4c finds an existing per-column Decimal256
scratch value. Reuse it for copy/rescale/validation before append rather than
adding another binding-owned scratch object. Preserve caller-owned input values.
Keep internal high-to-low decimal words separate from the writer's low-to-high
wire order. Also preserve the schema's target scale across reset and failed-row
cleanup: a following batch may emit that retained column entirely as nulls,
without invoking a decimal setter. Legacy columns must keep their current
first-input-scale-per-batch behavior. Prove both paths before choosing the
smallest metadata-lifetime change; do not share a new blanket reset policy.

The accepted implementation covers all 18 logical pairs with nine physical
source-width/target-width paths. It copies each caller value into the existing
per-column Decimal256 scratch, performs exact rescale plus target-precision and
storage-width checks, then appends the direct target wire width. Target scale
survives reset and failed-row cleanup. Caller objects are unchanged; a failed
partial row rolls back without damaging completed rows or schema-generation
boundaries. Confirmed MISSING and legacy paths retain source-width inference,
public null/no-op behavior and the legacy first-input-scale rule.

Decimal cursors use bitmap-only nullness, not the fixed-width sentinel rule.
Characterize raw sentinel limbs with and without a bitmap and preserve public
typed-decimal null/no-op behavior. Check representable source scales and
representative malformed scale bytes; if an existing server validation gap is
found, report it separately rather than copying unsafe acceptance into the
client or quietly broadening this plan into a server rewrite.

That characterization is complete for 5a: explicit bitmap nulls are null, while
raw sentinel limbs without a null bitmap remain supplied conversion input at the
server boundary. At the 2.32 checkpoint this did not activate decimal-to-text
or the later input families.

5b is accepted in iteration 2.33. For a known decimal target, LONG starts as an
exact scale-zero integer in the existing per-column Decimal256 scratch, then
uses 5a's rescale, precision/storage validation and target-width append tail.
No conversion registry, second scratch value or Sender/protocol state was added.
Small decimal targets still use DECIMAL64 wire storage while enforcing their
declared precision. MISSING still infers LONG.

The schema-mode source-null rule is now explicit: `Long.MIN_VALUE` becomes a
decimal bitmap null and locks the target scale before append. Its meaning is
therefore independent of other rows' omissions. Legacy encoding and server
conversion are unchanged and retain the existing bitmap-dependent result,
which a raw-frame real-server test characterizes separately. Duplicate
suppression happens before validation, and an invalid partial row is rolled
back while completed rows remain. A schema-generation test proves that a
sealed DECIMAL64(scale 4) block retains its bytes when a refresh selects
DECIMAL128(scale 5) for the later block.

The 39-row shared client/server corpus covers all six target classes and has
19 accepted values, 14 local rejections and six source-null cases. Component
tests assert exact type, bitmap, scale and low-to-high limbs; public Sender E2Es
assert exact stored values and recovery. The original four server conversion
methods fail against the frozen 5a client and pass against 5b. After acceptance,
the unchanged original diagnostic has 14 errors, all assigned to 5c–5e.

The legacy-path performance guard passes 12 balanced A/B runs: instructions
change by +0.001% to +0.002% and paired cycles by -0.47% to +1.99%. A separate
candidate-only nine-physical-pair characterization has six runs of 20 million
rows each (180 million conversions and 3.36 GB verified per run), with basic
medians of 32.91 ns, 182.03 cycles and 1140.18 instructions per value. It has no
matched schema baseline and makes no network, ingestion or general-throughput
claim. One 200-million-row sizing run and an interrupted follow-on are excluded.

5c is accepted in iteration 2.34. For a known decimal target, STRING is parsed
at the target precision and scale with `strict=false` and `lossy=false`, including
the server grammar's exponent underscores. The existing Decimal256 scratch then
goes through phase 5a's precision/storage checks and direct target-width append
tail. The shared parser uses the current server's leading-zero and full-scale
precision rule; a schema-only parser fork was not added. Small decimal targets
continue to use DECIMAL64 wire storage while enforcing their declared precision
and scale.

Java null and exact optionally signed `NaN` or `Infinity`, after the parser's
supported suffix processing, become target-scale bitmap SQL nulls for all six
target classes. Malformed special-value prefixes and extreme nonzero integer-
boundary exponents fail locally as `INVALID_VALUE`. Schema mode also avoids the
server's small-decimal special-value narrowing bug by emitting bitmap nulls.
The exact-special and exponent checks live in the shared parser, so direct
`Decimal64`, `Decimal128` and `Decimal256` string parsing changes globally: it
rejects malformed special prefixes and reports extreme nonzero exponents as
`NumericException`. MISSING and legacy Sender paths remain VARCHAR and retain
server-side conversion.

The implementation adds no state, framework, protocol, server production, SQL
or send-time transformation. The 55-row shared corpus contains 19 values, 24
invalid inputs and 12 nulls and has SHA-256
`67739711b320b07fd4d5eb72ecd19e14b5999f6a07e756ed83a847d809577568`.
Client red validation failed eight tests before implementation. Final focused
validation passes eight tests plus two packaging checks, all-decimal validation
passes 463 tests, and the canonical gate passes 941. The candidate and installed
JAR SHA-256 is
`30e593b27a6eb71c022e198bb5e331af973af643e1fd3eedde8555c8d2d2112c`.
The 3,601,944-case differential passes ordinary server parity and identifies 72
intentional malformed-prefix corrections. Synchronizing shared-parser precision
metadata corrected 19 stale assertions in 16 existing tests without changing
their parsed values or scales.

Server focused validation passes seven tests and the canonical gate passes 352.
The original 56-test conversion class has ten errors: only the four
STRING-to-decimal methods disappeared, with no additions. Three seeded repeats pass 115 tests and
22 archived XML reports each. The 12 balanced fixed-work native
STRING-to-VARCHAR runs share checksum `15640344951909530213`; paired basic
instructions change by -0.001% to -0.000% and cycles by -1.028% to -0.007%,
with no repeatable 5% regression. This makes no decimal, network or ingestion
throughput claim.

5d is accepted in iteration 2.35. FLOAT is widened exactly to DOUBLE, the existing
schema Ryu formatter emits the shortest round-tripping decimal, and the existing
decimal parser applies the target precision and scale with `lossy=false`.
Natural-scale and precision overflow therefore reject instead of rounding or
truncating. NaN and both infinities become target-scale bitmap nulls, matching all
six server decimal paths. This is intentionally not the SQL cast contract.

The implementation reuses the phase-5a Decimal256 scratch, validation and direct
width append tail. It adds no new parser, state or conversion framework. The
first branch placement increased native FLOAT/DOUBLE instructions by about
3.8–4.7% after pushing a common method over the hot inlining threshold. Moving
decimal dispatch into the existing cold nonnumeric helper restores the common
method's prior bytecode shape. Six counterbalanced pairs per native path then
show median instruction changes of +0.0001% for FLOAT and +0.0037% for DOUBLE.

The 60-row shared corpus covers every source/target-class pair, exact width,
scale and limbs, no-rounding cases, non-finite nulls, signed zero, underflow and
overflow. Client and public Sender tests cover duplicate-first-wins, omission,
invalid-row rollback and a DECIMAL64-to-DECIMAL128 schema generation change.
Four real-server E2Es consume the same corpus and independently preserve raw
unflagged legacy FLOAT/DOUBLE bytes. The unchanged 56-test diagnostic now leaves
only the six 5e errors.

5e is deliberately binding-only. For a known STRING or VARCHAR target, promote
the supplied value into one lazily reused binding-owned Decimal256, reduce its
scale to the unsigned low QWP byte, format that promoted value into the existing
reused text sink, and append `TYPE_VARCHAR`. This mirrors the server's scalar
conversion and avoids the narrow formatters' precision limit for full physical
Decimal64/128 values. Preserve valid source scales, trailing zeros and plain
notation. Public Java null and decimal `NULL_VALUE` remain setter no-ops; they
do not claim first-value-wins. MISSING and legacy mode keep native decimal tags
and the original source scale. Do not add a formatter abstraction, conversion
registry or table-buffer state.

Scalar parity is not legacy batch emulation. Once schema binding establishes a
text target, every setter value is converted independently, so mixed native
decimal widths and scales in one target column are supported. Legacy inference
locks one native width and scale for a column block and may reject or rescale a
later value. Recreating that irrelevant intermediate constraint would add state
and make text conversion depend on neighboring rows, contrary to the schema-
directed design. Cover this intentional behavior difference explicitly.

The shared corpus must cover all six source/target pairs, each physical width,
positive, negative and zero coefficients, maximum precision/scale, leading and
trailing fractional zeros, and both public null forms. Public socket tests assert
the exact `TYPE_VARCHAR` bitmap, offsets and bytes while covering null-then-value,
duplicate-first-wins before conversion, A/failed-B/C rollback, omission, scratch
reuse and a DECIMAL64-to-VARCHAR schema-generation change. A second component
test would repeat the same binding path without adding an observable contract.
Real-server tests run the same valid corpus and an independent unflagged native-
decimal frame so the old server formatter remains covered.

Public low-level constructors and `setScale` mutators can expose coefficients
or scales outside the documented 18/38/76-digit domains. Phase 5e does not add
a local rejection policy: Decimal256 promotion preserves the physical value and
the unsigned low scale byte matches what the server receives. Public sentinel
values remain setter no-ops, so non-bitmap sentinel formatting stays confined
to independent raw-frame characterization. Any Decimal API or server
rejection/normalization is separate work.

## Acceptance required for every increment

1. Read the entire affected test, its public setter path, target dispatch and
   concrete parser/formatter overloads. Confirm the failure from the frozen
   baseline; save pre-edit copies of every file to be changed. Record any
   deliberate compatibility exception in the main design before code.
2. Implement only the selected family in the existing binding/buffer ownership.
   No second schema cache, conversion registry, new connection state, opt-out,
   send-time transformation, or source-type fallback. Avoid unrelated guards
   and hot-path refactors; check shared-path allocation/throughput regressions
   with comparable fixed work when those paths change. Do not claim a speedup
   from source inspection or test timings.
3. Add a public-wire test proving direct target types and parameters. Add a
   separate component test only when it covers a distinct observable contract.
   Use shared, identical conversion vectors for client/server tests where
   applicable. A server SQL result alone cannot prove client conversion.
4. Keep a real public-Sender E2E passing in that same increment. Cover completed
   row A, partial invalid row B, valid row C; automatic rollback without
   cancellation/reselection; first-value-wins; null vs omission; boundary values;
   typed local errors; schema change/rebinding and replay where affected.
   Pin auto-flush when asserting buffered-row preservation and await the
   published ACK when assertions depend on feedback.
5. Preserve independent legacy native-wire acceptance/rejection tests and
   byte-identical replay. A test helper must not accept either a local or a
   server error interchangeably. SQL is only setup/result verification.
6. Run client component/public-Sender regressions for production changes, then
   build/install one standalone client artifact. Freeze its hash before server
   validation; no overlapping client install and server tests, no
   `-Plocal-client`. Use an absolute project-local temp directory in the
   Surefire JVM startup `argLine`, not merely a later system property.
7. Extend the current 271-test green server gate with the increment's tests and
   recovered baseline cases. Rerun the unchanged 333-test diagnostic and compare
   exact failing methods, not only totals. Run three seeded repeats of affected
   integration/recovery cases; preserve failing development logs.
8. Independent SOL source/adversarial review plus architect acceptance. Update
   this ledger, the main conversion inventory and the journal with source/JAR
   hashes, commands, actual results and any next blocker.

If a resolved first error reveals a later stale assertion, classify that
assertion against the approved contract. Do not stop at the first green setter,
delete the test, loosen its expected error, or silently count a renamed/removed
method as fixed. Readiness requires every baseline case's behavior to remain
represented.

## Definition of done and remaining release work

Done for this plan: all 47 baseline cases are represented and pass, the original
333 selection is green (or has an explicitly recorded one-to-one test rename),
the accumulated green gate/client regressions pass, and independent legacy
checks retain their intended behavior. No unresolved new regression is accepted.

This is not the complete feature-release checklist. The design still inventories
unrepresented BYTE/SHORT/INT families, DATE targets, IPv4, arrays, native geohash/
LONG256 paths and other pairs, plus compatibility decisions for unsafe server
paths. Actual older-server-distribution testing, lookup-timeout configuration,
full lifecycle/compatibility validation and representative performance evidence
also remain release work. Do not advertise full schema conversion coverage just
because these 47 methods turn green.

## Exact 47-method ledger

Source files:
[conversion E2Es](../../questdb/core/src/test/java/io/questdb/test/cutlass/qwp/e2e/QwpWebSocketTypeConversionE2ETest.java),
[Sender E2Es](../../questdb/core/src/test/java/io/questdb/test/cutlass/qwp/e2e/QwpSenderE2ETest.java).
Group IDs match the increments above. All 47 entries are accepted. No case has
been removed or renamed in this original selection.

### 1a. Existing schema/legacy expectations — 5 accepted

- `QwpWebSocketTypeConversionE2ETest.testBooleanToNumericColumns`
- `QwpWebSocketTypeConversionE2ETest.testBooleanToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testBooleanToVarcharColumn`
- `QwpSenderE2ETest.testFloatingPositiveTwoToThe63ClampsToLongMax`
- `QwpSenderE2ETest.testFloatingToIntegerNaNDependsOnBlockNullBitmap`

### 1b. Direct-buffer legacy fixtures — 4 accepted

- `QwpWebSocketTypeConversionE2ETest.testGeoHashToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testGeoHashToVarcharColumn`
- `QwpWebSocketTypeConversionE2ETest.testLongToDesignatedTimestampMicroColumn`
- `QwpWebSocketTypeConversionE2ETest.testLongToDesignatedTimestampNanoColumn`

### 2a. LONG to text/SYMBOL — 3 accepted

- `QwpWebSocketTypeConversionE2ETest.testLongToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testLongToVarcharColumn`
- `QwpWebSocketTypeConversionE2ETest.testLongToSymbolColumn`

### 2b. UUID to text — 2 accepted

- `QwpWebSocketTypeConversionE2ETest.testUuidToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testUuidToVarcharColumn`

### 2c. Floating point to text/SYMBOL — 3 accepted

- `QwpWebSocketTypeConversionE2ETest.testDoubleToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testDoubleToVarcharColumn`
- `QwpWebSocketTypeConversionE2ETest.testDoubleToSymbolColumn`

### 3a. LONG to ordinary timestamps — 1 accepted

- `QwpWebSocketTypeConversionE2ETest.testLongToTimestampColumn`

### 3b. STRING to ordinary timestamps — 1 accepted

- `QwpWebSocketTypeConversionE2ETest.testStringToTimestampColumn`

### 3c. Timestamp to text — 2 accepted

- `QwpWebSocketTypeConversionE2ETest.testTimestampToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testTimestampToVarcharColumn`

### 4a. CHAR target — 2 accepted

- `QwpWebSocketTypeConversionE2ETest.testCharToCharColumn`
- `QwpWebSocketTypeConversionE2ETest.testStringToCharColumn`

### 4b. STRING to LONG256 — 1 accepted

- `QwpWebSocketTypeConversionE2ETest.testStringToLong256Column`

### 4c. STRING to GEOHASH — 1 accepted

- `QwpWebSocketTypeConversionE2ETest.testStringToGeoHashColumn`

### 5a. Native decimal to decimal — 4 accepted

- `QwpWebSocketTypeConversionE2ETest.testDecimalToDecimal64Column`
- `QwpWebSocketTypeConversionE2ETest.testDecimalToDecimal128Column`
- `QwpWebSocketTypeConversionE2ETest.testDecimalToDecimal256Column`
- `QwpWebSocketTypeConversionE2ETest.testDecimalToSmallDecimalColumn`

Residual P3: a scale-up overflow has the correct `INVALID_VALUE` reason and row
rollback, but its detail currently says `decimal value cannot be rescaled
exactly` instead of identifying overflow. Keep this as diagnostic wording debt;
do not add another hot-path branch solely to refine the message.

### 5b. LONG to decimal — 4 accepted

- `QwpWebSocketTypeConversionE2ETest.testLongToDecimal64Column`
- `QwpWebSocketTypeConversionE2ETest.testLongToDecimal128Column`
- `QwpWebSocketTypeConversionE2ETest.testLongToDecimal256Column`
- `QwpWebSocketTypeConversionE2ETest.testLongToSmallDecimalColumn`

### 5c. STRING to decimal — 4 accepted

- `QwpWebSocketTypeConversionE2ETest.testStringToDecimal64Column`
- `QwpWebSocketTypeConversionE2ETest.testStringToDecimal128Column`
- `QwpWebSocketTypeConversionE2ETest.testStringToDecimal256Column`
- `QwpWebSocketTypeConversionE2ETest.testStringToSmallDecimalColumn`

### 5d. Floating point to decimal — 4 accepted

- `QwpWebSocketTypeConversionE2ETest.testDoubleToDecimal64Column`
- `QwpWebSocketTypeConversionE2ETest.testDoubleToDecimal128Column`
- `QwpWebSocketTypeConversionE2ETest.testDoubleToDecimal256Column`
- `QwpWebSocketTypeConversionE2ETest.testDoubleToSmallDecimalColumn`

### 5e. Native decimal to text — 6 accepted

- `QwpWebSocketTypeConversionE2ETest.testDecimalToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testDecimalToVarcharColumn`
- `QwpWebSocketTypeConversionE2ETest.testDecimal128ToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testDecimal128ToVarcharColumn`
- `QwpWebSocketTypeConversionE2ETest.testDecimal256ToStringColumn`
- `QwpWebSocketTypeConversionE2ETest.testDecimal256ToVarcharColumn`
