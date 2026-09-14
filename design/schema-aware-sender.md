# Schema-aware sender: schema-directed encoding

Status: implemented on the development branch, revision 60. Scope: QWP v1 over
WebSocket, with an automatically negotiated schema extension, legacy-server
compatibility and companion server changes. The committed baseline contains the
protocol, Sender integration and conversions through iteration 2.38. Iteration
2.39a (IPv4 identity and text targets) is locally validated. A released-binary
compatibility gate is implemented and locally green. The remaining public-setter
conversion contract is not complete.

## Goal and contract

On supporting servers, convert values to the server column's type before
buffering them. Conversion failures are reported by the setter and cancel only
the partial row. Completed rows stay buffered. Against
old servers, retain the existing inferred-type behavior and server-side
conversions; those writes do not gain early schema-mismatch detection.

For a server UUID column, `stringColumn("id", validUuid)` parses locally and
appends a binary UUID; `stringColumn("id", "invalid")` throws
`LineSenderException`. `uuidColumn("id", lo, hi)` writes directly to the same
UUID buffer. The setter describes the input type; the server schema determines
the buffered representation.

This replaces revision 4's compatibility checks. Knowing that STRING-to-UUID
is supported cannot validate a particular string. The client now performs the
conversion, rather than merely checking an acceptance mask.

Schema mode has three parts:

1. Obtain the server schema synchronously on first use or a cache miss.
2. Bind columns to that schema and convert every effective value before append.
3. Pin each table block to its encoding snapshot. ACKs/NACKs provide newer
   schemas for subsequent blocks, without changing already converted data.

The guarantee is local conversion validity against the pinned schema, not
eventual server acceptance. Concurrent DDL, auto-creation races and other
server-side failures can still reject a batch.

## Implementation status

Current checkpoint: iteration 2.39a adds both `ipv4Column` overloads for IPv4,
STRING and VARCHAR targets. Its pre-iteration baseline is client `edf6f346` and
server `fea4d4a6c0`; the IPv4 slice is the current locally validated increment.
The wider conversion inventory below is not complete; this is not release acceptance.

The standing compatibility gate now runs three real process combinations:
current client against QuestDB 10.0.1, client 1.3.9 against the current server,
and current client against the current server. It proves legacy versus schema
encoding from stored values, uses checksum-pinned released artifacts, and runs
in both the stable-client CI workflow and the release pipeline. Exact no-send,
watermark and replay behavior after a confirmed sender reaches an old endpoint
remains covered by deterministic socket and replay tests; it does not need a
second multi-process failover harness. D056 records the gate and local evidence.

Schema discovery, pinned identity framing, safe replay and ACK/NACK feedback
are implemented and tested. Local UUID and LONG-to-numeric conversion now uses
a non-owning binding over a caller-owned `QwpTableBuffer`; the caller retains
row completion, rollback and buffer lifetime.

First-use lookup and forced refresh now run through the existing cursor I/O
loop and share a bounded latest-schema cache with ACK/NACK feedback. Tests
cover lookup failure/lifecycle handling and a real-server lookup, schema change,
ACK-driven cache update and same-buffer rebind, including fragmented responses.

Primitive timestamp units from NANOS through DAYS and `Instant` conversion into
non-designated timestamp columns are implemented and tested against real-server
ingestion. Coverage includes range boundaries, target precision, nulls, rollback,
compressed encoding, recovered delivery and deliberate legacy differences.
STRING-to-timestamp parsing is accepted in iteration 2.27. Designated timestamp row completion is
included in the Sender integration described below.

Text and symbol inputs into STRING, VARCHAR and SYMBOL targets are implemented
and tested with 90 shared cases, exact wire/stored-value checks, legacy value
parity and local-dictionary SF recovery. Client tests also cover Sender-owned
global symbol encoding.

Boolean inputs and strict text-to-boolean parsing are implemented and tested
against real-server acceptance/rejection, target-native omissions, packed nulls
and recovered delivery.

FLOAT/DOUBLE input conversion into the six numeric targets is implemented and
tested: NaN means missing and out-of-range integer conversions fail locally.
Coverage includes raw-bit rounding boundaries, legacy differences, rollback
and recovered delivery. D025/D026 record the decisions and verification.

BYTE/SHORT/INT input conversion into the six numeric targets is implemented in
iteration 2.37. One narrow helper performs target selection, range checks and
direct target-width append; it does not change the established LONG path.
Shared component, public-Sender, recovered-frame and real-server tests cover 64
boundary vectors, exact wire/SQL values, rollback, refresh, inference and the
stable INT null contract. D055 records the local acceptance evidence.

STRING input conversion into the six numeric targets is implemented and tested
with 429 shared cases, exact wire/stored values, legacy acceptance/rejection,
null/sentinel contexts, rollback and recovered delivery. Parsing preserves
target-specific syntax, including floating suffixes; overflowing LONG text
fails locally under the approved D027 compatibility exception.

BINARY targets now accept all three binary input forms and STRING input encoded
as UTF-8, using native BINARY wire representation before persistence. Component and
real-server tests cover exact bytes, source ownership, null versus empty,
duplicate suppression, local errors and rollback. A shared variable-width
append repair also preserves completed rows if string encoding throws midway;
successful legacy writes retain their encoding. D029 records the accepted slice,
independent reviews and complete verification evidence.

The textual decimal overload now behaves as a DECIMAL256 input in schema mode.
Known decimal targets use their declared precision and scale; STRING/VARCHAR
targets receive canonical fixed-point text. Parsed `NaN` and infinities write
target NULL, while Java `null` and empty input remain no-ops. A missing column
infers DECIMAL256 at the first finite value's natural scale; a null-only block
uses wire scale zero without locking later batches to that scale. The Sender
uses its existing rollback and one-refresh lifecycle, with no new retained
converter state or per-value allocation.

Both IPv4 overloads now use the server schema in schema mode. An IPv4 target
keeps the four-byte value; STRING and VARCHAR receive canonical dotted-quad
text through the existing reusable numeric sink. Packed zero is written as a
bitmap NULL for every target. A Java null text reference remains a no-op. Exact
`"null"` (case-insensitive) and `"0.0.0.0"` strings remain rejected, while
legacy dotted aliases such as `".0.0.0.0."` remain accepted and normalize to
NULL. Malformed input fails locally with `INVALID_VALUE`, including strings
made only of dots.

Iteration 2.15 is accepted as a bounded, unreleased Sender integration.
Public setters select the negotiated mode, use server-directed conversions,
cancel failed partial rows and retain completed rows under their original
schema. Both designated timestamp units and cold `atNow()` are integrated.
Eight real-server tests cover these operations, relevant/unrelated DDL,
schema lookups, auto-flush, reset and close. Fifteen public-Sender socket
tests separately cover exact wire encoding, legacy fallback, one-way upgrade,
startup timeout/interruption, pinned generations and split frames.
The broader server regression selection still has 38 classified failures
after the 2.22 expectation/fixture repairs;
this is not a release-ready client. Compatibility with a non-confirming socket
peer is tested; an actual older QuestDB distribution was not run in this slice.
The fixed 30-second schema deadline is settled policy and is not part of the
remaining conversion plan. Complete conversion
coverage gates release, not the first integrated test. Until that
coverage is complete, this is an unreleased development increment: unsupported
schema-mode inputs fail explicitly rather than falling back to server conversion.
The [conversion backlog](#conversion-backlog-server-source-audit) inventories
every public input family against the current server, including missing identity
setters, unsupported pairs and unresolved compatibility boundaries.
The [implementation journal](schema-aware-sender-journal.md)
records decisions, iteration scope and verification evidence.

Iteration 2.16 is accepted: two real-server tests create file-backed backlog
through public Sender setters, withhold data from the server and forcibly stop
the producer JVM. A fresh Sender replays byte-identical QWP payloads and stores
exact A/C rows after rejecting partial B. Coverage includes UUID/SYMBOL values,
two pinned UUID-to-VARCHAR schema generations, and reopening after ACK without
replaying acknowledged rows. The 230-test server gate and three seeded repeats
passed against the unchanged 2.15 client artifact. This increment changes only
tests and documentation; it proves neither power-loss durability nor a stronger
delivery guarantee, and does not close the release gaps above. D031 records
the evidence and review findings.

Iteration 2.17's first repair group is accepted: legacy conversion comparisons
now send unflagged source-typed QWP frames instead of accidentally using schema
conversion on both sides. Original expected values are preserved, with explicit
wire assertions for source types, values and nulls. All 25 tests in the four
affected suites, the 251-test regression selection and three seeded repeats
passed. The original 333-test selection remains red with 56 failures/errors,
down from 58 known contract/fixture failures. Two additional disk-space failures
were resolved through the test JVM's startup configuration, not changed
expectations. No production changes or Sender opt-out were introduced. D032
records the evidence and the next functional priority: missing-table/column
inference through the existing binding and buffer ownership.

Iteration 2.18 is accepted for confirmed-missing inference in already-supported
setter families. Missing tables and columns use native inferred types in the
existing buffer; server-provided types still govern known columns. No separate
schema registry or new stored state was added. Same-row duplicates are ignored
before type checking; later-row inferred type conflicts use the existing
schema-refresh path. New metadata never relabels completed rows. Server creation
policy and authorization remain authoritative.
The client gate passed 414 tests plus two packaging checks; the server gate
passed 261 tests, with three seeded repeats of 16 integration/recovery tests.
The original 333-test selection has 54 failures/errors, down from 56, with no
new failing methods. D033 records the implementation, review corrections and
remaining diagnostic/conversion work. This does not complete unsupported input
families or make the client release-ready.

Iteration 2.19 is accepted as a test-only diagnostic repair. Seven tests now
assert typed local errors and observable recovery. Independent legacy frames
still verify server rejection of BINARY-to-text/SYMBOL. The mismatch test waits
for ACK feedback before asserting the conversion error. No production behavior
or client artifact changed. The 268-test regression gate and three repeats of
23 integration/recovery cases passed. The unchanged 333-test selection has
47 failures/errors: exactly the seven repaired methods disappeared, with no
new failing methods. The remaining set is 38 missing-conversion cases, five
approved schema/legacy expectation differences and four obsolete fixtures.
D034 records evidence and a pending source-null decision for future text
conversions; this iteration does not approve another compatibility exception.

Iteration 2.20 is accepted for source-null characterization, not conversion
implementation. Three real-server tests prove bitmap-dependent LONG/UUID text
results and current public Sender rejection/recovery across all five pairs.
The bitmap-present LONG result is non-null text `"null"`, correcting the earlier
decimal-minimum prediction. The 271-test regression gate and three repeats of
26 cases passed; the original 333-test selection retains the exact same 47
failing methods. Production code and the client artifact are unchanged.
D035 records the evidence. The [text-null rules](#source-null-rules-for-text-output)
were proposals at that checkpoint; D039 subsequently approves LONG only.

The [remaining-failures plan](schema-aware-sender-remaining-failures-plan.md)
assigns all 47 failing methods to small, independently tested increments:
nine expectation/fixture repairs, then 38 conversion cases. It preserves
independent legacy tests and requires public Sender E2E coverage in every
increment. D036 records the source audit and sequencing; this is a plan,
not another accepted implementation iteration or the full release checklist.

Iteration 2.21 accepts phase 1a of that plan. Three public Sender Boolean tests
now assert target-native omissions; two floating-point legacy diagnostics send
independent unflagged frames. A new public Sender test preserves NaN and
positive-2^63 boundary/recovery coverage. All 25 focused tests, the expanded
277-test green gate and three repeats of 32 cases passed. The original 333-test
selection has 42 failing methods: exactly the five repaired cases disappeared,
with no new failure. No production code or client artifact changed. D037 records
the evidence. Four direct-buffer fixtures and all 38 missing-conversion cases
remained after that increment.

Iteration 2.22 accepts phase 1b: the four direct-buffer fixtures now send
standalone unflagged legacy frames, preserving their original stored values.
They assert GeoHash precision/bitmap/packed bytes, literal named LONG designated
timestamps in both units, and legacy ACK framing. Public `at()` coverage remains
separate. All 37 focused tests, the 281-test green gate and three repeats of
36 cases passed. The original 333-test selection has 38 failing methods:
exactly the four repaired cases disappeared, with no additions. Phase 1's nine
test repairs are complete; no production code or client artifact changed.
Native public GeoHash conversion and all other missing conversions remain
backlog items. D038 records that checkpoint, before the LONG text-null decision.

Iteration 2.23 accepts LONG-to-STRING/VARCHAR/SYMBOL in ordinary Sender. One
binding-local formatter uses existing target buffers; no server production or
send-time change is needed. `Long.MIN_VALUE` is an effective target-null write
in schema mode, with legacy behavior unchanged. Exact wire, shared vectors,
SQL, rollback, schema rebind and replay checks pass: 420 client tests plus two
packaging checks, 16 focused server tests, 288 server regressions and three
43-test seeded repeats. The original 333-test selection now has 35 errors:
exactly the three LONG-to-text cases disappeared, with no new failing method.
D039 records evidence and two stale rejection-oracle migrations.

Iteration 2.24 accepts UUID-to-STRING/VARCHAR with the separately approved
both-limbs-MIN target-null rule. Every other pair, including either single MIN
limb, becomes canonical lowercase text. One lazy binding-local formatter feeds
the existing VARCHAR buffer; native UUID, legacy behavior and UUID-to-SYMBOL
rejection remain unchanged. Validation passes 426 client tests plus two packaging
checks, 21 focused server tests, 293 server regressions and three 48-test seeded
repeats. The original 333-test selection now has 33 errors: exactly the two
UUID-to-text cases disappeared, with no additions. Native UUID performance
checks found no material regression; they do not measure text throughput. D040
records evidence and fixture corrections. Floating-point text-null rules were
still undecided at that checkpoint.

Iteration 2.25 accepts FLOAT/DOUBLE-to-STRING/VARCHAR/SYMBOL with NaN as an
effective SQL NULL. Finite values, signed zero and infinities match the server;
FLOAT widens before formatting. A schema-only Ryu port preserves legacy Numbers.
Validation passes 432 client tests plus two packaging checks, 29 focused server
tests, 301 server regressions and three 56-test seeded repeats. The original
333-test selection has 30 errors: only the three DOUBLE-to-text cases disappeared.
Independent differential checks cover approximately two million non-NaN inputs.
Performance checks caught an inlining regression; separating text dispatch
removed the extra native DOUBLE instruction cost. D041 records the evidence,
remaining measurement limits and fixture corrections. Phase 2 is complete.

Iteration 2.26 accepts LONG-to-ordinary-TIMESTAMP/TIMESTAMP_NS as raw counts in
the target unit. The existing LONG setter needs only two allowed targets and
two switch labels sharing its raw append. MIN remains an effective SQL NULL;
independent legacy frames confirm both bitmap contexts also store timestamp
NULL. Native LONG inference and named designated-timestamp rejection are
unchanged. Validation passes 437 client tests plus two packaging checks,
41 focused server tests, 305 regressions and three 68-test seeded repeats.
The original 333-test diagnostic has 29 errors: only LONG-to-TIMESTAMP was
removed from the prior 30. Native LONG measurements detect no regression;
they do not measure timestamp throughput. D042 records the evidence and a
corrected exact-wire fixture error.

Iteration 2.27 accepts STRING-to-ordinary-TIMESTAMP/TIMESTAMP_NS. A fixed parser
preserves the server's eleven formats, UTF-8 replacement/ASCII-byte input view,
calendar arithmetic and timezone rules; nano targets check the multiplication
after microsecond parsing. Java-null input writes a bitmap null, while non-null
text that parses to MIN remains a present raw timestamp. No general compiler,
SQL-engine change, legacy change or new replay path is introduced.
Validation passes 442 client tests plus two packaging checks, 47 focused server
tests, 311 regressions and three 74-test seeded repeats. The original diagnostic
has 28 errors: only STRING-to-TIMESTAMP was removed from the prior 29.
The shared corpus has 110 literal cases; 214,030 public binding/encoder checks
per locale agree with the server in fresh en_US/tr_TR processes. Reference
century, locale and tzdb data still come from each process's runtime; this does
not guarantee agreement across different runtime data. Native STRING-to-VARCHAR
measurements show a consistent roughly 2–3% cycle/time cost, below the declared
5% guard, with effectively unchanged instructions. D043 records the findings,
corrections and measured trade-off.

Iteration 2.28 accepts timestamp-to-STRING/VARCHAR. These writes reject
out-of-range values locally with INVALID_VALUE. NANOS are divided by 1000 toward
zero; MICROS through DAYS and Instant are converted to micros with range-correct
checked arithmetic before millisecond formatting. Instant keeps the existing
microsecond rounding. Preserve present MICRO MIN as empty text and NANO MIN as
finite timestamp text; this is not the LONG/NaN null policy. Old-server/legacy
behavior remains unchanged. One fixed formatter reuses the existing text sink;
no protocol, replay or SQL-engine change is needed. Validation passes 447 client
tests plus two packaging checks, 53 focused server tests, 317 regressions and
three 80-test seeded repeats. All 160,202 public binding/encoder comparisons
match the server formatter and checked-range oracle. The original 333-test
diagnostic now has 26 errors, removing only its two timestamp-to-text cases.
No repeatable native timestamp-write regression is detected by the performance
guard; this does not measure formatter throughput. D044 records the evidence.

Iteration 2.29 accepts native CHAR and STRING-to-CHAR using existing two-byte
storage. Native values retain all 16 bits. Text takes the first BMP character,
ignoring the remainder; empty/NUL/supplementary-first text becomes code unit
zero, while malformed UTF-16 becomes '?', matching existing client replacement
and server decoding. Java null is bitmap-null; present zero remains wire-present
but reads as SQL NULL. CHAR-to-text remains separate missing work. Validation
passes 452 client tests plus two packaging checks, 59 focused server tests,
323 regressions and three 86-test seeded repeats. All 65,549 public differential
checks match the server. The unchanged original 333-test diagnostic has 24
errors, removing only its two CHAR cases. The native string-write performance
guard passes. D045 records the corrected refresh catch and full evidence.

Iteration 2.30 accepts STRING-to-LONG256 using four local longs and existing
32-byte storage. Match the server's lowercase `0x` prefix, 2–64 hex digits in
complete byte pairs and right-to-left limb order. Java null/omission is null;
the literal with four Long.MIN_VALUE limbs is INVALID_VALUE. Parse completely
before append. Native `long256Column` remains unsupported, and missing string
columns still infer VARCHAR. Validation passes 458 client tests plus two
packaging checks, 63 focused server tests, 327 regressions and three 90-test
seeded repeats. All 40,055 public binding/encoder differential checks match
the server oracle. The unchanged original 333-test diagnostic has 23 errors,
removing only STRING-to-LONG256. The native STRING-to-VARCHAR regression guard
passes; this does not measure LONG256 throughput. D046 records the evidence.

Iteration 2.31 accepts STRING-to-GEOHASH using the existing base32 parser and
precision field. Parse at most 12 characters, require enough bits and truncate
to the target precision. Empty text and Java null become bitmap null. Always
emit the bitmap for schema-directed nullable GEOHASH columns, even with no null
rows: otherwise valid byte-aligned maximum values are mistaken for nulls.
Legacy and low-level non-nullable encoding remain unchanged. Only canonical
packed GEOHASH types are recognized; unknown flags retain typed rejection.
Native GEOHASH setters remain unsupported and missing strings still infer
VARCHAR. Validation passes 464 client tests plus two packaging checks, 67
focused server tests, 331 regressions and three 94-test seeded repeats. All
7,210 public differential cases match. The original 333-test diagnostic has
22 errors, removing only STRING-to-GEOHASH. Scoped native STRING-to-VARCHAR
checks measure 1.46–1.80% more append time and 0.17–1.15% more encoding time,
below the declared regression threshold; this is not GEOHASH throughput
evidence. D047 records the null-framing correction, flagged-type regression
and independent reviews.

Iteration 2.32 accepts all three native decimal setters into every
DECIMAL8/16/32/64/128/256 target. This is 18 logical pairs represented by nine
physical source/target wire-width pairs: small decimal targets use DECIMAL64
wire storage while retaining their own precision and scale. Conversion copies
the caller's value, rescales exactly, checks target precision and physical
storage width, and appends target-width limbs with the target scale retained
across reset. Public null setters remain no-ops; confirmed MISSING and legacy
paths keep source-width inference and legacy scale behavior. Tests cover caller
immutability, bitmap-only decimal nullness, exact wire/SQL results, failed-row
rollback and schema-generation transitions. Validation passes 472 client tests
plus two packaging checks, seven focused and 338 canonical server tests, and
three 101-test seeded repeats. The unchanged 333-test diagnostic has 18 errors:
only its four intended phase-5a methods disappeared. D048 records the 60-row
shared corpus and the bounded performance evidence. The 12-run legacy A/B guard
passes with +0.001% to +0.002% instructions and -0.47% to +1.99% cycles. Six
candidate-only nine-physical-pair runs measure median absolute cost of 32.91 ns,
182.03 cycles and 1140.18 instructions per value; they make no network, ingestion
or general-throughput claim. Phases 5b–5e remain; this does not establish complete
conversion coverage or release readiness. One low-priority diagnostic issue is
deferred: scale-up overflow has the correct `INVALID_VALUE` reason and rollback,
but its detail says the value cannot be rescaled exactly instead of saying that
it overflowed. Do not add another conversion branch solely to refine that text.

Iteration 2.33 accepts LONG into every DECIMAL8/16/32/64/128/256 target. The
client treats the integer as a scale-zero value, rescales it exactly to the
target scale, checks declared precision and physical storage width, and writes
the direct target decimal wire type. `Long.MIN_VALUE` is a stable schema-mode
source null: it emits a decimal bitmap null and locks the target scale, regardless
of whether another row omits the column. Confirmed MISSING and legacy paths are
unchanged; the latter retains its existing bitmap-dependent sentinel behavior.
The implementation reuses the decimal column's existing scratch value and the
phase-5a validation/append tail; it adds no schema state, conversion registry or
send-time transformation. Validation passes 475 client tests plus two packaging
checks, seven focused and 345 canonical server tests, and three 108-test seeded
repeats. The unchanged 333-test diagnostic has 14 errors: exactly the four
LONG-to-decimal methods disappeared, with no additions. D049 records the 39-row
shared corpus, schema-generation test, compatibility characterization and
bounded performance evidence. The 12-run LONG-to-LONG A/B guard detects no
repeatable 5% cycles/instructions regression; this is not decimal, network or
ingestion throughput evidence. Phases 5c–5e remain, so release coverage is not
complete.

Iteration 2.34 accepts STRING into every DECIMAL8/16/32/64/128/256 target. The
client parses at the target precision and scale with the server's `strict=false`,
`lossy=false` rules, including exponent underscores, then writes the QWP decimal
wire type for the target storage class through the existing decimal append tail;
DECIMAL8/16/32/64 targets share QWP's DECIMAL64 wire type. The shared
parser now uses the current server's leading-zero and full-scale precision
calculation. Exact, optionally signed `NaN` and `Infinity`, after supported
suffix processing, and Java null become target-scale bitmap SQL nulls.
Malformed special-value prefixes and extreme nonzero integer-boundary exponents
fail locally.
These rules deliberately do not copy three server defects: accepting
special-value prefix garbage as null, narrowing a special null to numeric zero
for small decimals, and throwing an array-bounds exception for extreme exponents.

Confirmed MISSING columns and legacy mode remain VARCHAR and retain server-side
conversion. The implementation reuses the per-column Decimal256 scratch and the
phase-5a append tail; it adds no state, framework, protocol, server production,
SQL or send-time transformation. The 55-row shared corpus contains 19 values,
24 invalid inputs and 12 nulls; both copies have SHA-256
`67739711b320b07fd4d5eb72ecd19e14b5999f6a07e756ed83a847d809577568`.
Client red validation first failed eight tests; final focused validation passes
eight tests plus two packaging checks, all-decimal validation passes 463 tests,
and the canonical gate passes 941. The installed candidate JAR has SHA-256
`30e593b27a6eb71c022e198bb5e331af973af643e1fd3eedde8555c8d2d2112c`.
A 3,601,944-case differential passes ordinary server parity and records 72
intentional malformed-prefix corrections. Updating the shared parser's precision
metadata required correcting 19 stale assertions in 16 existing tests; their
values and scales did not change.

The parser correction is global, not limited to schema mode: direct
`Decimal64`, `Decimal128` and `Decimal256` string parsing now uses the corrected
precision metadata, rejects malformed special-value prefixes and reports
extreme nonzero exponents as `NumericException`. This deliberate compatibility
change removes unsafe or undocumented behavior for legacy and standalone parser
callers as well as enabling schema-directed conversion.

Server validation passes seven focused and 352 canonical tests. The unchanged
original 56-test conversion class now has ten errors: exactly the four
STRING-to-decimal
methods disappeared, with no additions. Three seeded repeats pass 115 tests and
22 archived XML reports each. A 12-run balanced fixed-work native
STRING-to-VARCHAR guard uses checksum `15640344951909530213`; paired basic
instructions change by -0.001% to -0.000% and cycles by -1.028% to -0.007%, with
no repeatable 5% regression. This is not decimal, network or ingestion throughput
evidence. D050 records the bounded acceptance. Phases 5d–5e remain, so release
coverage is not complete.

Iteration 2.35 accepts FLOAT and DOUBLE into every
DECIMAL8/16/32/64/128/256 target. FLOAT is widened exactly to DOUBLE, then the
existing schema formatter produces the shortest round-tripping decimal and the
existing decimal parser enforces the target precision and scale without rounding
or truncation. Finite values whose natural scale or precision does not fit fail
locally with `INVALID_VALUE`; NaN and both infinities become target-scale bitmap
SQL nulls. Confirmed MISSING and legacy mode retain native FLOAT/DOUBLE bytes and
server-side conversion.

The implementation adds only a decimal branch to the existing cold nonnumeric
floating helper and reuses the per-column Decimal256 scratch plus the phase-5a
target-width append tail. It adds no state, parser, framework, protocol, server
production, SQL or send-time transformation. Moving the branch out of the common
numeric helper was necessary: the first implementation pushed that method past
HotSpot's hot inlining threshold and increased native FLOAT/DOUBLE instructions
by about 3.8–4.7%. After the split, six counterbalanced pairs per native path show
median instruction changes of +0.0001% for FLOAT and +0.0037% for DOUBLE; median
cycle changes are -0.83% and -1.04%. This is a native binding regression guard,
not floating-to-decimal or ingestion throughput evidence.

The shared client/server corpus has 60 rows: 37 values, 17 invalid values and six
nulls, with SHA-256
`366a889719c6e7b688ff00e1bdfb95e273ab192508a1094dac5312c3d899611d`.
A 5,996,868-case differential matches the server's accepted values, target scale
and all four limbs. Client focused validation passes 63 tests plus two packaging
checks and the canonical gate passes 944; an expanded 1,269-test regression gate
also passes. The candidate and installed JAR SHA-256 is
`da44a24c6d100a0bff8a984f37c40409a0e3cc3f132f7ac9304a9e0b5d25c99d`.
Server validation passes the eight-test focused gate and 360-test canonical
gate. At that checkpoint, the unchanged original 56-test conversion diagnostic
had six errors, all assigned to phase 5e, with no additions. Three seeded repeats
passed 123 tests and 23 selected XML reports each. D051 records the test-fixture
cleanup and bounded phase-5d acceptance; the phase-5e acceptance below
supersedes its remaining-failure count.

Iteration 2.36 accepts native Decimal64, Decimal128 and Decimal256 values into
STRING and VARCHAR. The binding promotes each non-null value into one lazily
reused Decimal256 scratch, applies the unsigned QWP scale byte, formats into the
existing reused text sink and appends `TYPE_VARCHAR`. This mirrors the server's
per-value scalar conversion, including full physical Decimal64/128 coefficients,
without a new validation policy, conversion registry, protocol state, server
change, SQL change or send-time transformation. Public null sentinels remain
setter no-ops.

Schema mode intentionally formats each value independently, so mixed native
widths and scales can share a known text target. Legacy inference still fixes
one native width and scale for a whole column block and may reject or rescale a
later value. Preserving that irrelevant intermediate restriction after the text
target is known would add state and make conversion depend on neighboring rows.

The 24-row shared corpus covers all six source/target pairs and both public null
forms. A second public boundary test covers the full physical Decimal128 value
`2^127 - 1` and unsigned scale-byte wrapping. The production/server differential
passes 3,000,294 full-physical and arbitrary-scale comparisons. Exact-current
client gates pass the 46-test public integration class, including four phase-5e
tests, plus 950 canonical tests, 1,275 broad tests and two packaged-JAR tests.
The candidate and installed JAR SHA-256 is
`e06759605ee3d1cf01c3277ba13531b0172e42138042c1f2f49c2db49e04b22d`;
only `QwpSchemaBinding.class` differs from the Phase-5d JAR.

The unchanged server conversion diagnostic now passes all 56 methods, resolving
the original 47 failures. The accumulated server gate passes 363 tests; three
seeded repeats pass 126 tests each. The identity-path performance guard finds a
0.71–1.38% instruction increase and cycle medians from -1.93% to +1.13%, below
the 5% rejection threshold. It makes no speed, network or ingestion-throughput
claim. D053 and the iteration-2.36 run manifests record exact evidence.

## Automatic negotiation and compatibility

The new client works with old servers and automatically enables schema-directed
encoding when the server supports it. There is no configuration switch or
alternate write API to disable it on a supporting server. The earlier
`schema_encoding` and `schema_validation` options are removed from the proposal.

Keep the base protocol at QWP v1: the existing magic, version byte and version
negotiation do not change. Negotiate the schema extension during WebSocket
upgrade using these proposed headers:

```
Client request:  X-QWP-Request-Schema: true
Server response: X-QWP-Schema: enabled
```

The new client always requests the extension. A successful upgrade without the
response header selects legacy behavior, unless this sender has previously
confirmed schema support. Explicit confirmation enables schema requests,
schema-directed encoding and extended responses. An unrecognized response
header is a negotiation error, not evidence of an old server; neither are
connection failures, timeouts or authorization failures.

The upgrade is one-way: once a sender confirms schema support, it assumes all
subsequent connections support the extension too. Require confirmation on
reconnect and background replay connections; never switch back to legacy mode.
Moving an upgraded sender to an old server is unsupported. Retain pending data
and report the capability mismatch under the existing reconnect policy. There
is no send-time transformation, metadata stripping or downgrade encoder.

| Client | Server | Behavior |
|---|---|---|
| Existing client, no schema request | New server | Existing QWP v1 framing and behavior |
| New client, not yet upgraded | Old server | Existing inferred-type QWP v1 behavior |
| New client, always requests schema | Supporting server | QWP v1 with the schema extension; one-way upgrade |
| New client, already upgraded | Old server | Unsupported endpoint; retain pending data, no downgrade |

The server enables the extension only when requested and confirmed. Connections
without negotiation receive no schema frames or extended ACKs/NACKs. Existing
clients need no changes. Clients may be upgraded before servers. Once schema
support has been confirmed, failover destinations must also support it; mixed
old/new endpoints are not supported for that sender after the upgrade.

An existing legacy row finishes in legacy mode. The producer adopts schema
mode at the next actual row boundary, seals completed legacy rows and replaces
the inferred layout before binding new rows. Pending legacy blocks retain
their original types and flag-clear framing, even if encoded or sent later.

The extension defines one stable conversion contract, backed by shared test
vectors, rather than a separate conversion-version handshake or a dependency on
a particular server release. A schema alone does not specify parsing or
rounding rules. In schema mode, unknown target types fail explicitly when used,
without blocking writes to unrelated supported columns or falling back to
raw-value encoding. Schema lookup failures never disable schema mode.

## Availability

In schema mode, required lookups use a fixed 30-second deadline. On the first
write, mode selection and its immediately following describe share one
end-to-end budget; a later cache-miss describe or forced refresh starts its own
30-second budget. This is not a public configuration surface.
Timeout, unavailable metadata or request saturation throws a lookup error
before accepting the value. It cancels any partial row but does not permanently
halt the sender. Do not add automatic retries or separate timeout settings for
the three lookup paths.

Known schemas can be used offline; unfamiliar tables require a live lookup.
Reconnect invalidates the lookup cache for future row starts, but does not reset
an upgraded sender to legacy mode, invalidate an already pinned row or rewrite
buffered and persisted blocks. Legacy mode retains existing offline buffering.

Asynchronous construction may remain, but the first write waits for the initial
successful negotiation to select the mode within a bounded deadline. An offline
or unreachable server is not assumed to be old. In schema mode, required lookup
must also complete before accepting the value; in legacy mode, no lookup is
needed. This changes cold-start offline behavior, not the minimum server version.

## Frame layout and replay compatibility

Negotiation authorizes the extension; a persisted frame flag identifies the
actual data layout. Use `FLAG_SCHEMA = 0x02` without changing the QWP version byte:

- Flag set: each table header includes its pinned schema identity, or an
  explicit unknown identity. The server uses the extended table-header parser.
- Flag clear: table headers have the existing layout and no schema identities.
  These are legacy-mode writes or legacy SF frames, accepted even on a
  schema-negotiated connection.
- Flag set without successful negotiation: an extension-aware server rejects
  the frame before processing table data. The client must never send it to an
  endpoint that did not confirm support.

In a flagged frame, each table header starts with the existing name-length
varint and UTF-8 name, followed by `identityKind:u8`. Kind 0 means unknown and
has no body. Kind 1 carries `tableId:i32 LE` and `metadataVersion:i64 LE`, both
non-negative. The existing row-count and column-count varints and column
definitions follow. Reject reserved kinds, truncated identities and negative
known fields. Flag-clear headers retain their exact old layout.

`FLAG_SCHEMA` cannot be combined with `FLAG_CONTROL`, and schema-flagged frames
must contain at least one table. Table-less commit/dictionary traffic stays
flag-clear. The parser still uses inline column definitions to interpret the
values; schema identities never replace them or enforce a version precondition.

Every frame containing schema-mode table blocks sets the flag, including writes
that use auto-creation. Legacy blocks keep flag-clear framing. Each frame uses
one table-header layout; keep legacy and schema-mode blocks in separate frames
during the one-way transition, preserving order and transaction boundaries.
Table-less dictionary/control traffic retains its own framing rules.

Persist the flag with the schema identities and data bytes. Replay preserves
all of them, allowing legacy and extended data frames to coexist in sequence on
one negotiated QWP v1 connection. Do not infer the table-header layout solely
from connection state or relabel old frames with new flags or identities.

Repeat negotiation on every reconnect, including background replay connections.
Before upgrade, legacy SF can drain to either an old or a supporting server.
After upgrade, require support even when the remaining frames happen to be
legacy. Recovery of pending extended SF data also requires support: establish
that requirement before replaying any part of that backlog, including a legacy
prefix. A fresh sender with only legacy SF negotiates normally. An incompatible
endpoint cannot drain a schema-required backlog: retain pending bytes and find
or wait for a compatible endpoint under the reconnect policy. Do not advance
watermarks or alter stored frames.

ACK/NACK extension framing is selected by the negotiated connection, independent
of the data frame's flag. A negotiated connection may receive schema feedback
for legacy blocks too; their absent identities are treated as unknown. Ordinary
and durable-ACK delivery semantics remain unchanged.

Keeping QWP at v1 does not make extended SF data readable by an older client.
Before downgrading the client binary, stop new writes and drain all pending
extended data, including background/orphan slots, with a supporting client and
server. Completion means acknowledgment under the configured ordinary or
durable-ACK policy, not merely a successful send or an attempted close.
Downgrading with pending extended data is unsupported. If draining fails,
retain the data and continue recovery with a supporting client; do not rewrite
frames, strip metadata, delete pending logs or advance watermarks to permit the
downgrade. This is an operational prerequisite, not a claim that older binaries
can detect and enforce it.

## Schema lookup

Each schema carries `(tableId, metadataVersion)`. The server already versions
table metadata; `tableId` distinguishes drop/recreate. The pair is an identity,
not a global ordering between table incarnations.

The logical control exchange is:

```
DESCRIBE: requestId, tableName
SCHEMA:   requestId, tableName, result, schema?
schema:   tableId, metadataVersion, designatedIndex, columns[]
column:   name, serverType, typeParams
```

The schema includes full target types and parameters: decimal precision/scale,
timestamp units, array element type/rank and geohash bits.
`designatedIndex` identifies the target of `at(ts)`; -1 means none.
Names follow the server's comparison rules. There is no per-column
`acceptMask`: the extension's conversion contract and target type determine
the client converter.

Results distinguish a known schema, a confirmed missing table and unavailable
metadata. Missing tables may follow auto-creation; unavailable metadata must
never be represented as a missing table. Permission denial has a distinct
machine-readable reason and must not trigger auto-creation.

An account authorized to write a table must be able to obtain the ingestion
schema needed for that write without gaining SELECT access to its data. Apply
the table-write authorization to both DESCRIBE and schema feedback in ACKs/NACKs;
metadata being available is not sufficient permission to return it. Return only
the ingestion schema, not table data or unrelated privileged metadata. Retain
the existing authorization rules for creating missing tables and adding columns.
Verify write-only accounts and denial/revocation behavior as part of the server
lookup implementation. Cached metadata never substitutes for server-side write
authorization.

The producer waits while the I/O thread services the connection and lookup.
Describes do not flush rows, consume data-frame sequence numbers or affect
ACK watermarks.

Direct SCHEMA replies retain the full QWP control-frame header. Route that
envelope separately from ordinary and durable ACKs, then validate it strictly;
do not guess the response type by trying multiple decoders. The request ID
identifies the requested table; the current direct reply does not repeat its
name. Named ACK/NACK feedback reuses only the inner schema payload.

## Column binding and conversion

This section describes schema mode. Legacy mode keeps existing binding,
conversion and missing-value behavior unchanged.

The sender maintains a sparse layout containing only columns being written.
For each known column, its binding contains the target type and parameters,
the chosen QWP wire representation, and converters for the supplied input
types. Schema resolution and converter selection can be cached; conversion
and value-dependent checks happen on every effective write.

Server types and wire types are not necessarily one-to-one. For example, a
small server DECIMAL may use a DECIMAL64 wire representation. The client must
still enforce the server column's precision and scale before buffering.

For text and symbol inputs, STRING and VARCHAR targets both use the existing
VARCHAR wire representation; a SYMBOL target uses symbol encoding. Reuse the
existing UTF-8 serialization rules: preserve case, whitespace, embedded NUL and
valid Unicode, and replace each unpaired UTF-16 surrogate with literal `?`.
Null and empty text remain distinct. Symbol dictionary IDs are representation,
not value identity: distinct source strings may encode to the same text after
surrogate replacement. No new dictionary owner or send-time type conversion is
needed for these text-to-text pairs.

For a BINARY target, binary setters copy opaque bytes without interpretation.
`stringColumn` writes the same UTF-8 bytes it would send through the legacy
VARCHAR representation, including NUL and the surrogate-replacement rule above;
this is not hex or base64 parsing. All three binary input forms (byte array,
DirectByteSlice and native pointer/length) select TYPE_BINARY. Effective null
byte-array/slice arguments report `INVALID_VALUE`; string null and omission
produce target NULL. Empty input remains a present zero-length BINARY value,
distinct from NULL on the current server.

Native binary lengths must be 0..Integer.MAX_VALUE, with a nonzero pointer for
nonempty input. Reject invalid lengths before reading memory. The caller must
provide readable memory for the duration of the setter; the binding retains
neither the source address nor the slice. A zero pointer with zero length is
valid empty input. First-value-wins precedes these checks. Binary input into
other target types is not implemented, including the unresolved parser paths
listed below.

String and binary byte appends preserve their previous data and offset positions
if a RuntimeException or Error occurs before publication. The same exception is
rethrown; the caller still cancels the partial row and rolls back its new column
definitions. This repairs exceptional cleanup in the shared buffer, not just
schema mode, without changing successful wire bytes. It does not claim recovery
from invalid native addresses or fatal process faults.

Boolean inputs use the target representation: BOOLEAN values, numeric `0`/`1`
for BYTE/SHORT/INT/LONG/FLOAT/DOUBLE, and lowercase `false`/`true` text for
STRING/VARCHAR. BOOLEAN-to-SYMBOL is not a supported server conversion.
Text-to-BOOLEAN accepts only `0`, `1` and ASCII-case-insensitive `true`/`false`.
Do not trim whitespace, apply Unicode case folding or silently treat an invalid
token as false. Invalid text reports `INVALID_VALUE` before append.
Explicit null and omission follow the target rule below: BOOLEAN stores false,
BYTE/SHORT store zero, and nullable numeric/text targets store NULL. This does
not make unsupported input/target pairs valid.

Unlike the current inferred layout, a known column is not bound to the first
setter's input type. Different setters may feed the same target buffer across
rows, provided their conversions are supported. Existing name validation and
setter no-op rules remain. In schema mode, first-value-wins is applied before
conversion even when the duplicate uses a different setter; ignored values
are not converted. Legacy mode ignores same-type duplicates but throws on
cross-setter type mismatches.

Omitted ordinary columns and explicit nulls use the target column's missing-value
representation: SQL NULL where supported, otherwise the target's native
missing-value/default representation. Do not synthesize a source-type default
and then convert it. A setter that treats null as a no-op still does not bind or
write a value; any resulting omission follows the target rule. Inferred columns
use their inferred target type until a server-backed binding replaces it.

This intentionally changes some existing results. After a boolean setter binds
the old client layout, an omitted value sent to a STRING column currently becomes
the text "false". With a STRING target binding, omission instead becomes NULL;
an explicitly supplied false still converts to "false". Missing-value behavior
must not depend on which input setter was used in an earlier row. The designated
timestamp remains the documented exception: `atNow()` uses server-assigned time.
Document this behavior change and test both negotiated modes and old clients.

For LONG input to a numeric target in schema mode, `Long.MIN_VALUE` means null
before conversion, regardless of other rows in the block. Append the target's
missing-value representation: zero for BYTE/SHORT, NULL for INT/LONG/FLOAT/DOUBLE.
Apply duplicate suppression and validate the supported input/target pair first;
this rule does not make an unsupported conversion such as LONG-to-UUID valid.

For LONG input to STRING, VARCHAR or SYMBOL, apply the same source sentinel
decision: `Long.MIN_VALUE` writes target SQL NULL before formatting, independently
of other rows. Other values become canonical signed decimal text. This approved
schema-mode rule deliberately differs from legacy bitmap-present MIN, which
can become the literal text "null". An explicit MIN is an effective first write,
not a no-op; an ignored duplicate cannot replace it.

This deliberately differs from legacy QWP's block-dependent sentinel handling.
With a null bitmap present, a supplied LONG minimum can be rejected by a narrow
integer target or stored as a finite FLOAT/DOUBLE; without the bitmap, it is
treated as null. Schema mode does not reproduce that inconsistency. Legacy
writes and existing legacy SF retain their behavior. No end-of-batch or
send-time conversion is introduced to imitate it.

Conversion parity with QWP ingestion for supplied values is a release
requirement, except for the deliberate missing-value and LONG source-null rules
above and the LONG text range, floating-point and timestamp rules below.
Coverage includes:

- String parsing into UUID, numbers, timestamps, decimals and other supported
  targets; conversions to text must preserve the server's formatting rules.
- Numeric range, fractional-value, precision-loss, rounding, null-sentinel
  and special floating-point behavior.
- Timestamp unit conversion and overflow; decimal precision and rescaling.
- Each non-null array's supported element type and rank, including wrapper and
  Java overloads. Rank is not fixed by today's client layout. Do not promise
  element conversion: the server copies array elements, and its exposed
  `LONG_ARRAY` path has unresolved validation gaps described in the backlog.
- Geohash precision: native GEOHASH input requires an exact bit-count match, whereas
  string parsing follows the server's geohash conversion rules.

Do not substitute a convenient library parser or Java cast without proving
equivalent behavior. Use shared conformance vectors across server and clients,
checking acceptance, rejection and resulting stored values. An implementation
with missing converters must report that limitation, not claim a schema
mismatch or silently pass the value through to the server.

For STRING input to LONG in schema mode, reject text outside the mathematical
signed-long range before append. This is an approved compatibility exception:
legacy ingestion accepts some overflowing decimal strings and stores wrapped
values, for example `"21000000000000000000"` becomes `2553255926290448384`.
Legacy parsing and ingestion retain that behavior. Preserve the server's other
LONG text grammar; do not use this range fix to broaden or tighten syntax.
An accepted string representing LONG minimum remains a supplied target
sentinel that reads SQL NULL, not a LONG setter's source-null conversion.

For FLOAT/DOUBLE input to supported numeric targets, NaN means source null,
independent of other rows. Apply duplicate suppression and validate the supported
input/target pair first, then append the target's missing-value representation.
NaN payload and sign bits are not retained as values. Integer targets require a
whole value within the target's mathematical range; fractions, infinities and
out-of-range values report `INVALID_VALUE` before append. The LONG upper bound
is exclusive positive `2^63`, not a floating-point approximation of
`Long.MAX_VALUE`. Exact negative `2^63` and INT minimum remain representable
values that read as the target's SQL-null sentinel, matching server behavior.

These are approved schema-mode exceptions: legacy NaN-to-integer writes use
the target's missing value without a null bitmap but fail when another row's
omission causes a bitmap; legacy positive `2^63` to LONG silently stores
`Long.MAX_VALUE`. Legacy ingestion and replay keep those behaviors. FLOAT/DOUBLE
targets still follow server widening/narrowing, including signed zero,
infinities, subnormal rounding and finite DOUBLE overflow to FLOAT infinity.
Do not add batch-dependent or send-time conversion.

For typed microsecond/nanosecond inputs into ordinary timestamp columns,
nanoseconds-to-microseconds division truncates toward zero, including negative
values. Microseconds-to-nanoseconds multiplication rejects values outside the
server's representable range before append. Matching units preserve the bits.
The primitive timestamp input does not inherit the LONG-input null rule:
`Long.MIN_VALUE` overflows when scaling micros to nanos, becomes a finite value
when dividing nanos to micros, and reads as SQL NULL when stored unchanged.
Explicit nulls and omissions still use the target's null representation.
Test these cases both with and without another omitted row in the block.

For primitive timestamp inputs, support NANOS, MICROS, MILLIS, SECONDS, MINUTES,
HOURS and DAYS. Convert directly to the target unit with checked arithmetic;
values outside its range report `INVALID_VALUE` before append. Other units
report `UNSUPPORTED_FEATURE`, and a null unit reports `INVALID_VALUE`.

For `Instant`, preserve all nanoseconds in a TIMESTAMP_NS target. For TIMESTAMP,
retain the existing formula `epochSeconds * 1000000 + nanoFraction / 1000`;
this differs from primitive nanos-to-micros truncation for negative sub-micro
instants. Check the final combined value's range: an intermediate multiplication
must not reject an otherwise representable negative instant. A null `Instant`
reports `INVALID_VALUE`, not an implicit timestamp null. Duplicate suppression
precedes argument validation and conversion for both overloads.

These are intentional schema-mode changes: legacy QWP scales larger units with
unchecked arithmetic and converts `Instant` to micros even for a nano target.
Legacy mode keeps that behavior. Shared vectors must test the new contract and
explicit legacy comparisons must document the changed overflow and precision.

`at(ts)` converts to the pinned designated timestamp's unit before append and
row commit, including when the timestamp column reference is cached. A known
schema without a designated timestamp rejects explicit `at(ts)`.
`atNow()` resolves a schema if no earlier effective setter did so, then keeps
server-assigned timestamp behavior.

Confirmed missing tables or columns retain client-side inference and existing
auto-create policy. Inferred bindings are distinct from server-backed bindings;
the client cannot promise conversion validity against a target that does not
yet exist. Successful writes return the resulting schema.

Inference uses the existing buffer's column definitions, not fabricated server
metadata. The first effective supported setter establishes the native type:
BOOLEAN, LONG, FLOAT, DOUBLE, VARCHAR for text, SYMBOL, UUID, BINARY, CHAR, or a
timestamp. Primitive NANOS infers TIMESTAMP_NS; other supported units and
`Instant` infer TIMESTAMP. Only a missing table may infer the unnamed designated
timestamp. A conflicting type in a later row is a schema rejection and gets
the same one-refresh opportunity as a known-column rejection; a duplicate
within the current row is ignored. Failed-only definitions are removed by the
existing row rollback. Unsupported setter families remain unsupported even
when the table or column is absent.

## Conversion backlog (server-source audit)

Cross-checked on 2026-09-11 after iteration 2.13, with implemented coverage
refreshed through iteration 2.38 on 2026-09-14. The accepted baseline is client
`1310b0dd` and server `8ad0b92baf`, whose submodule pins that exact client
revision. The original audit bases were client
`981bdb02a471f3b290c89b8e78cbc422610e329e` and server
`12a33d651e51e2682e7a448c8db5168fc72dfad3`. The matrix is a source audit;
runtime evidence for implemented slices is recorded separately in the journal.
Read the actual QWP cursor selection, target dispatch and
value routines together; neither a shared switch branch nor an SQL cast proves
that QWP accepts a pair.

### Complete input-to-target inventory

The table covers effective, non-null values into existing **ordinary** columns.
"Implemented" records `QwpSchemaBinding` coverage; Sender integration is
described separately below.
An accepted null, ignored duplicate or no-op does not establish support for a
non-null conversion. Known compatibility exceptions above still apply.

- **Numeric** means BYTE, SHORT, INT, LONG, FLOAT and DOUBLE, separately.
- **Text** means STRING and VARCHAR, separately; SYMBOL is always listed apart.
- **Timestamps** means TIMESTAMP (microseconds) and TIMESTAMP_NS (nanoseconds),
  separately (`TIMESTAMP_NANO` in server code).
- **Decimals** means every DECIMAL8/16/32/64/128/256 target, with its declared
  precision and scale. **Geohash** means every valid 1..60-bit target.
- Input names refer to public setters. `stringColumn` emits `TYPE_VARCHAR`;
  there is no separate `TYPE_STRING` wire code. `symbol` emits dictionary-backed
  `TYPE_SYMBOL`, not text-parser input. BYTE/SHORT/INT setters emit their own
  wire types; they are not aliases for `longColumn`.

| Public input (source wire type) | Implemented targets | Missing server-accepted targets |
| --- | --- | --- |
| `boolColumn` (BOOLEAN) | BOOLEAN, numeric, text | None |
| `byteColumn`, `shortColumn` (BYTE, SHORT) | Numeric | DATE, timestamps, text, SYMBOL, decimals |
| `intColumn` (INT) | Numeric | DATE, timestamps, text, SYMBOL, decimals, IPv4 |
| `longColumn` (LONG) | Numeric, text, SYMBOL, timestamps, decimals | DATE |
| `floatColumn`, `doubleColumn` (FLOAT, DOUBLE) | Numeric, text, SYMBOL, decimals | None |
| `stringColumn` (VARCHAR) | BOOLEAN, numeric, text, SYMBOL, UUID, BINARY, timestamps, CHAR, LONG256, geohash, decimals | DATE |
| `symbol` (SYMBOL) | Text, SYMBOL | None |
| `timestampColumn(long, unit)` (TIMESTAMP or TIMESTAMP_NANOS) | Timestamps, text | None |
| `timestampColumn(Instant)` (currently TIMESTAMP micros in legacy Sender) | Timestamps, preserving nanos for the schema-mode nano target; text | None |
| `uuidColumn` (UUID) | UUID, text | None |
| `charColumn` (CHAR) | CHAR | Text; non-ASCII text output needs the decision below |
| `ipv4Column(int)` and `ipv4Column(CharSequence)` (IPv4; text parsed locally) | IPv4, text | None |
| `long256Column` (LONG256) | None | LONG256, text |
| `binaryColumn` (byte array, `DirectByteSlice`, native pointer/length; BINARY) | BINARY | Parser-reachable targets remain deferred; see boundaries below |
| `decimalColumn(Decimal64/128/256)` (DECIMAL64/128/256) | All decimals from each source width, text | None |
| `decimalColumn(CharSequence)` (locally parsed DECIMAL256) | All decimals, text | None; this is not the `stringColumn` parser path |
| `geoHashColumn(long, bits)` and `geoHashColumn(CharSequence)` (GEOHASH with source bits) | None | Geohash with exactly matching bit precision, text |
| `doubleArray` (all Java ranks and `DoubleArray`; DOUBLE_ARRAY) | None | DOUBLE array of the target rank; identity/shape validation, not element casting |
| `longArray` (all Java ranks and `LongArray`; LONG_ARRAY) | None | No supported conversion contract established; see the existing-server gap below |

DATE also exists as a wire input, but there is no public WebSocket `dateColumn`
setter. The server accepts DATE-to-DATE and DATE-to-text only. Neither has a
binding entrypoint today; record this wire-only coverage without adding a new
public API just to mirror the protocol. DATE **targets** from existing integer
and string setters are real missing work in the table.

The binding also lacks target representation selection for DATE and arrays. It
rejects extension parameters.
Adding a parser alone therefore does not complete these pairs: select the
target wire representation and pin its parameters before append. Preserve the
existing non-owning binding and buffer ownership.

### Rules the missing conversions must cover

These are server-source requirements to test, not permission to silently add
new casts or copy the unresolved behaviours below.

- **Integer sources:** retain distinct BYTE/SHORT/INT/LONG input contracts.
  Numeric narrowing checks range. DATE takes raw epoch milliseconds; ordinary
  timestamps take raw counts in the target's unit, with no implicit unit
  conversion. INT-to-IPv4 has its own null translation; it is not LONG-to-IPv4.
  Integer-to-text/SYMBOL uses `Numbers.append(long)`.
- **Floating output:** text/SYMBOL formatting uses
  `Numbers.append(cursor.getDouble())`, including widening FLOAT first. Do not
  substitute `Float.toString`. Test signed zero, infinities, NaN and rounding.
  The current server uses Ryu while the client still has the older formatter;
  matching method names and default scale do not establish identical text.
  Iteration 2.25 implements an isolated server-equivalent Ryu formatter;
  the shared legacy formatter is unchanged.
- **Text parsing:** DATE uses `DateFormatUtils.parseDate`; timestamps use
  `MicrosFormatUtils.parseTimestamp`, then checked multiplication by 1000 for
  a nano target. Do not assume a separate nanosecond text parser. LONG256 uses
  `Numbers.parseLong256`. VARCHAR-to-BINARY preserves the existing UTF-8 byte
  payload, not hex/base64 decoding. CHAR takes the first decodable BMP character
  and ignores the remainder; empty, malformed raw UTF-8 or supplementary-first
  input produces NUL. Public malformed UTF-16 instead becomes '?', matching
  existing client replacement before server decoding. Java null uses bitmap
  null; present NUL is distinct on the wire but also reads as SQL NULL (D045).
- **Timestamp output:** both text targets use
  `MicrosFormatUtils.appendDateTime`; nano wire values divide by 1000, truncating
  toward zero, before formatting. Cover each primitive unit and `Instant`
  separately. D044 separately approves checked normalization into legacy
  source precision for text targets, without changing legacy mode.
- **Other text output:** UUID uses `Numbers.appendUuid`, LONG256 uses
  `Numbers.appendLong256`, DATE uses `DateFormatUtils.appendDateTime`, and
  decimal output preserves source scale. The server formats IPv4 through
  `Numbers.intToIPv4Sink`; iteration 2.39a uses the client's private reusable
  `formatIPv4` sink. CHAR uses the server's `putAscii` path; see the non-ASCII
  limitation below.
- **Decimals:** all three decimal wire widths reach every decimal target.
  Decimal input rescaling must be exact: discarded nonzero digits cause
  precision loss; scale-up/precision/storage overflow fails. Integer input is
  scaled to the target; text is parsed at target precision and scale.
  FLOAT/DOUBLE uses the server's `Numbers.doubleToDecimal(..., false)` path;
  non-finite input becomes decimal null, and finite conversion overflow fails.
  Do not replace its rounding with an assumed BigDecimal policy. Small server
  decimals still need a supported wire width, such as DECIMAL64, while enforcing
  the smaller target's precision/scale. All typed and text overloads need tests.
  Iteration 2.32 implements and validates the three typed overloads into all six
  decimal targets. Iteration 2.33 adds LONG into all six targets, treating
  `Long.MIN_VALUE` as a target-scale bitmap null in schema mode. Iteration 2.34
  adds STRING into all six targets at the target precision and scale, preserving
  exponent underscores and applying the stable special-value null rules above.
  Iteration 2.35 adds FLOAT/DOUBLE into all six targets through the server's
  lossless shortest-decimal path, with non-finite values encoded as decimal
  nulls. Phase 5e converts non-null `Decimal64`, `Decimal128` and `Decimal256`
  values targeting STRING or VARCHAR to plain fixed-point text before append.
  Like the server, it first promotes the physical value to a reusable
  `Decimal256`, applies the unsigned one-byte QWP scale, and then formats it.
  This preserves valid source scales and trailing zeros, handles the full
  physical Decimal64/128 range without the narrower formatters' precision
  limit, emits `TYPE_VARCHAR` for both targets, and keeps public decimal null
  sentinels as setter no-ops.
  MISSING and legacy mode retain their native decimal wire type and source scale.
  Other integer widths remain.

  Parity here is per setter value after schema binding, not a replay of the
  legacy native-decimal column buffer. For a known text target, each row is
  formatted independently and mixed Decimal64/128/256 inputs and scales are
  accepted. Legacy inference instead fixes one native decimal width and scale
  for the whole column block, rejecting a later width and rescaling or rejecting
  later scales. This is intentional: once the target is known to be text,
  preserving an irrelevant intermediate native-decimal layout would add state
  and make valid per-value conversion depend on neighboring rows.
- **IPv4:** the packed zero sentinel becomes a bitmap NULL for IPv4 and text
  targets, so its result cannot depend on whether another row creates a bitmap.
  Text is parsed locally after target selection and duplicate suppression.
  Preserve the legacy dotted grammar: case-insensitive `"null"` and exact
  `"0.0.0.0"` are rejected, but dotted aliases that parse to zero normalize to
  NULL. Both STRING and VARCHAR receive target-native VARCHAR wire data; no
  server-side second conversion is required.
- **Geohashes:** wire precision must equal target bits; no native GEOHASH narrowing is
  accepted. `stringColumn` uses `GeoHashes.fromStringTruncatingNl`: empty means
  null, parse at most the first 12 base32 characters, require enough bits and
  truncate to target precision. Invalid consumed characters fail; characters
  beyond the 12-character limit are ignored. The public geohash text setter
  first produces a precision-bearing GEOHASH and must not be confused with
  `stringColumn`. Geohash-to-text emits bit strings for valid positive wire
  precision, not the helper's unreachable negative-precision character branch.
- **Arrays:** DOUBLE_ARRAY copies 8-byte elements; rank must match the known
  target, with per-row lengths permitted to vary. The cursor validates rank
  1..32, nonnegative dimensions, checked element count and exact payload size.
  Test each Java overload and wrapper, omitted/null/empty arrays, mixed ranks,
  ragged inputs and capacity failures. No implicit LONG/DOUBLE element cast or
  rank conversion is established by the server.

Every new pair also needs local rejection/rollback, exact target-wire and
stored-value checks, recovery coverage, and explicit legacy comparisons using
shared vectors. In particular, the fixed-width server cursor treats a null
bitmap as authoritative and otherwise recognizes type-specific sentinels.
BYTE/SHORT/CHAR have no source sentinel; INT minimum, LONG minimum, floating
NaN, IPv4 zero and UUID/LONG256 sentinel limbs can behave differently when a
companion omitted row introduces a bitmap. Test both contexts. The approved
LONG/FLOAT/DOUBLE source-null rules currently specify numeric targets; extending
those rules to missing target families or new input types needs an explicit
contract, not an assumption that every legacy cast has stable null behaviour.

Decimal cursors are different again: only bitmap bits mark null; raw decimal
sentinel limbs still enter server conversion as supplied values without a bitmap.
Iteration 2.32 characterizes that distinction while preserving public typed-
decimal sentinel/null no-ops. Same/different widths, exact rescaling, explicit
nulls and raw sentinel payloads are covered for decimal targets. For phase 5e,
valid raw native-decimal frames remain a legacy server-formatting check. Public
low-level constructors and `setScale` mutators can also expose coefficients or
scales outside the documented 18/38/76-digit domains. Schema mode does not add
a second validation policy for those values: it uses the same Decimal256
promotion and unsigned one-byte scale that the server sees on the wire. Public
decimal null sentinels remain setter no-ops, so the server's non-bitmap sentinel
behavior is only a raw-frame boundary.

### Server boundaries requiring explicit decisions

These paths are not silently omitted from the inventory and are not newly
approved compatibility exceptions. Characterize them with public tests and
decide the contract before activating the affected Sender paths; copying an
unsafe behaviour merely because it is reachable is not the recommendation.

1. **BINARY reaches text parsers.** BINARY and VARCHAR share
   `QwpStringColumnCursor`. The BOOLEAN, numeric, DATE, timestamp, decimal,
   geohash, UUID and LONG256 target branches do not exclude BINARY, so suitable
   binary bytes reach those parsers. All those pairs are missing in the binding.
   In contrast, CHAR/STRING/VARCHAR/SYMBOL explicitly reject BINARY. Decide
   whether binary-to-parser pairs are supported compatibility or a server gap
   to close; do not advertise them as normal binary conversions without tests.
2. **LONG_ARRAY is not a supported array conversion.** Auto-creation explicitly
   rejects it, despite public client setters and a decoding cursor. For an
   existing array column, the append branch accepts either array cursor, but
   the rank check runs only for DOUBLE_ARRAY and does not compare element type.
   Thus the existing-target route is not proof of correct LONG_ARRAY ingestion
   or cross-element conversion. Resolve the server validation/compatibility
   gap rather than inventing client-side element casts to cover it.
3. **CHAR-to-text is not uniformly Unicode-safe.** CHAR-to-STRING writes the
   UTF-16 code unit; CHAR-to-VARCHAR calls `Utf8StringSink.putAscii(char)`, which
   truncates it to one byte. Non-ASCII values can therefore produce corrupted
   VARCHAR bytes. A surrogate code unit in STRING also cannot be preserved by
   the binding's normal VARCHAR wire encoding. Keep these value-level limits
   visible; correct UTF-8 output would require a deliberate compatibility
   decision, not an undocumented change disguised as parity.
4. **Designated timestamp fallback is not a conversion catalogue.** Its special
   branch can pass a fixed-width cursor to timestamp storage without the
   ordinary-column source-type allowlist. Do not derive new public casts from
   bit reinterpretation. The client must implement `at`/`atNow` using the pinned
   designated unit and the row-completion contract above.
5. **Malformed decimal wire metadata is outside the client contract.** SQL declarations enforce
   precision 1..76 and scale 0..precision. The decimal cursor stores a signed
   Java scale byte; auto-creation interprets it unsigned and passes it into type
   construction whose range checks are assertions. Existing-target conversion
   also trusts the unsigned scale. Non-bitmap DECIMAL64/128 sentinel limbs are
   formatted as physical minimum integers, while the DECIMAL256 sentinel tuple
   formats as an empty non-null string. These are server validation gaps, not
   useful schema semantics. Phase 5e nevertheless accepts the physical values
   reachable through public low-level decimal constructors and mutators. It
   mirrors the current server conversion by promoting to Decimal256 and
   interpreting the scale as an unsigned QWP byte; it does not claim that wire
   scales 77..255 are valid SQL decimal metadata. Public null sentinels remain
   no-ops. Rejecting malformed metadata on the server or hardening the Decimal
   API is a separate production change.

Outside these explicitly listed boundaries, unlisted pairs are not missing
features. In particular, no server conversion supports BOOLEAN-to-SYMBOL,
numeric-to-BOOLEAN, FLOAT/DOUBLE-to-DATE/timestamp, timestamp-to-numeric/DATE/
SYMBOL, DATE-to-numeric/timestamp, or UUID/LONG256/CHAR/IPv4/geohash/decimal-to-
SYMBOL. SYMBOL does not feed scalar parsers. Neither string-to-IPv4 nor
IPv4-to-integer is accepted: `ipv4Column(text)` parses locally and INT-to-IPv4
is a distinct migration path. Arrays do not convert to/from scalar values;
decimal and geohash inputs do not convert to ordinary numeric targets.

### Source-null rules for text output

Approved on 2026-09-11 for LONG-to-STRING/VARCHAR/SYMBOL only: supplied
`Long.MIN_VALUE` becomes target SQL NULL before formatting, regardless of
surrounding rows. This is an effective write, not a setter no-op;
first-value-wins still applies. Ordinary LONG values use canonical signed
decimal text. Implemented and validated in iteration 2.23 (journal D039).

Separately approved on 2026-09-11 for UUID-to-STRING/VARCHAR: only the pair with
both limbs `Long.MIN_VALUE` becomes target SQL NULL, as an effective first write.
Every other pair, including either single minimum-valued limb, produces canonical
lowercase UUID text. Implemented and validated in iteration 2.24 (D040).
Native UUID-to-UUID bytes and UUID-to-SYMBOL rejection remain unchanged.

Separately approved on 2026-09-11 for FLOAT/DOUBLE-to-STRING/VARCHAR/SYMBOL:
every supplied NaN becomes target SQL NULL as an effective first write,
independent of bitmap context. Validate target support and duplicates first.
Finite values, signed zero and infinities use server-equivalent text; FLOAT
widens to DOUBLE before formatting. Legacy behavior remains unchanged.
Implemented and validated in iteration 2.25 (D041).

Real-server characterization confirms the contrast below for all eleven pairs.
D035 records LONG/UUID evidence; D041 adds the six floating-point pairs:

| Supplied value | Legacy, no bitmap | Legacy, bitmap marks value present | Schema-mode text rule |
| --- | --- | --- | --- |
| LONG `Long.MIN_VALUE` | NULL | Text `"null"` (not SQL NULL) | NULL (approved) |
| UUID with both limbs `Long.MIN_VALUE` | NULL | `80000000-0000-0000-8000-000000000000` | NULL (approved) |
| FLOAT/DOUBLE NaN | NULL | Text `NaN` (not SQL NULL) | NULL (approved) |

Other LONG values retain signed decimal formatting; other UUID limb pairs
retain canonical lowercase UUID formatting, including a pair with just one
minimum limb. Legacy-mode bytes and replay remain unchanged. Implementation
must document this deliberate departure from bitmap-present legacy results.
Do not extend the rule to other input families or targets by analogy.

### Integration work is separate from conversion coverage

The table does not include missing-table/column inference or Sender lifecycle
integration. Iteration 2.15 implements first-effective-write schema resolution,
designated timestamp row completion and row-boundary adoption. Iteration 2.18
adds inference for the already-supported setter families, not the missing
families in the conversion inventory. Preserve each public overload's null/no-op
and duplicate rules:
for example decimal/array null and IPv4 text null can be no-ops, binary null
rejects, and a null `Instant` is not a missing timestamp. No fallback to sending
unconverted values, new conversion framework or send-time transformation is
implied by this inventory.

### Source map

Line numbers identify this audited working-tree snapshot; method names are the
durable lookup points.

- [Client Sender][conversion-sender]: public overloads and wire choice,
  lines 1118–3017. [Binding][conversion-binding]: implemented setters,
  lines 108–403; target selection/parameter rejection, lines 473–545.
  [Client buffer][conversion-buffer]: string/binary exception-atomic append,
  lines 710–763, 1211–1230 and `restoreStringAppendPositions` (1668).
- [Server cursor selection][conversion-cursors]: `initializeColumnCursor`,
  lines 469–562; [fixed-width cursor][conversion-fixed-cursor]: `advanceRow`
  and `isCurrentValueSentinelNull`, lines 67–87 and 301–313.
- [Server conversion dispatch and values][conversion-dispatch]: decimal target
  entrypoints (367–394), `putDecimalToDecimalColumn` (2087–2109), and decimal
  rescaling/range/write helpers (2019–2085, 2433–2522).
- The same [server value-conversion source][conversion-values] implements `putIntegerToNumericColumn`
  (1137), integer/float text and symbol writers (790–1041),
  `putStringToNumericColumn` (1399), `putStringToTimestampColumn` (1542),
  `putStringToLong256Column` (1377), `putCharColumn` (334),
  `putBinaryColumn` (207), `putTimestampToStringColumn` (1851) and
  `formatFixedOtherValue` (1917).
- The same [value-conversion source][conversion-values] implements decimal
  text output (398–516), integer/float input (684–902), string input (1275,
  2282–2429), rescaling/range checks (2019–2279, 2433–2522), geohash input/
  output (1046–1133, 1299–1373), and array copying (`putArrayColumn`, 156).
- [Decimal cursor][conversion-decimal-cursor], lines 73–91 and 196–281;
  [array cursor][conversion-array-cursor], lines 179–310;
  [geohash cursor][conversion-geo-cursor], lines 122–188;
  [GeoHashes][conversion-geohashes], `fromStringTruncatingNl` (238);
  [decimal type construction][conversion-column-type], `getDecimalType` (320);
  [SQL decimal validation][conversion-sql-parser], lines 269–297.
- [UTF-8 decoding][conversion-utf8], `utf8CharDecode` (1354–1401);
  [UTF-8 sink][conversion-utf8-sink], `putAscii(char)` (128–130).

[conversion-sender]: ../core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketSender.java
[conversion-binding]: ../core/src/main/java/io/questdb/client/cutlass/qwp/protocol/QwpSchemaBinding.java
[conversion-buffer]: ../core/src/main/java/io/questdb/client/cutlass/qwp/protocol/QwpTableBuffer.java
[conversion-cursors]: ../../questdb/core/src/main/java/io/questdb/cutlass/qwp/protocol/QwpTableBlockCursor.java
[conversion-fixed-cursor]: ../../questdb/core/src/main/java/io/questdb/cutlass/qwp/protocol/QwpFixedWidthColumnCursor.java
[conversion-dispatch]: ../../questdb/core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java
[conversion-values]: ../../questdb/core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java
[conversion-decimal-cursor]: ../../questdb/core/src/main/java/io/questdb/cutlass/qwp/protocol/QwpDecimalColumnCursor.java
[conversion-array-cursor]: ../../questdb/core/src/main/java/io/questdb/cutlass/qwp/protocol/QwpArrayColumnCursor.java
[conversion-geo-cursor]: ../../questdb/core/src/main/java/io/questdb/cutlass/qwp/protocol/QwpGeoHashColumnCursor.java
[conversion-geohashes]: ../../questdb/core/src/main/java/io/questdb/cairo/GeoHashes.java
[conversion-column-type]: ../../questdb/core/src/main/java/io/questdb/cairo/ColumnType.java
[conversion-sql-parser]: ../../questdb/core/src/main/java/io/questdb/griffin/SqlParser.java
[conversion-utf8]: ../../questdb/core/src/main/java/io/questdb/std/str/Utf8s.java
[conversion-utf8-sink]: ../../questdb/core/src/main/java/io/questdb/std/str/Utf8StringSink.java

## Local errors and stale schemas

Conversions run inside the producer's row-rollback guard. A failed conversion
or lookup cancels the partial row and removes bindings introduced only by that
row; it does not discard completed rows or halt the sender. Diagnostics identify
the table, column, target type, input type and conversion failure.

Expose these failures through a schema-specific `LineSenderException` subtype
with stable machine-readable reasons. Callers must not parse error messages to
decide recovery. The proposed reason codes and actions are:

| Reason | Caller action |
|---|---|
| `INVALID_VALUE` | Correct or reject the input; reconstruct the whole row if writing it again. Retrying the same input against the same target will fail. |
| `SCHEMA_CHANGED` | Reconstruct and retry the whole row using the refreshed schema. |
| `SCHEMA_UNAVAILABLE` | Retry the whole row when metadata can be obtained, for example after a timeout or temporary outage. |
| `ACCESS_DENIED` | Resolve authorization; unchanged credentials/permissions do not make an immediate retry useful. |
| `UNSUPPORTED_FEATURE` | Use a supporting endpoint/client for an extension required after upgrade or by pending extended SF, or for an unsupported target type. Do not downgrade or fall back to raw encoding. |

An old server's absent confirmation before upgrade is normal legacy negotiation,
not `UNSUPPORTED_FEATURE`.

For failures raised during row construction, the partial row has already been
cancelled. Retrying `flush()` cannot recover it, although a flush may still
publish earlier completed rows. Do not mark these errors as flush-retryable via
the existing `isRetryable()` convention; the schema reason determines whole-row
recovery. Existing transport/flush failures and terminal server NACKs keep their
own delivery and recovery semantics. Test the reason, row outcome and allowed
next public API operation, not only the diagnostic text.

A cached schema must not reject indefinitely after DDL makes an input valid.
Before reporting a conversion rejection against cached metadata, describe once.
If this operation already obtained a fresh schema, do not look up again.
Freshness belongs to the failing setter, not the whole row: a describe performed
by an earlier setter does not suppress refresh for a later failing setter.

- If the relevant target is unchanged, preserve the original rejection reason:
  `INVALID_VALUE` for an invalid value or `UNSUPPORTED_FEATURE` for an unsupported
  conversion. A changed metadata version alone is not `SCHEMA_CHANGED`.
- If the target or table incarnation changed, cancel the row and report
  `SCHEMA_CHANGED`. Prepare the fresh snapshot for the next whole-row retry.
- If refresh fails, cancel the partial row and report its actual reason:
  temporary unavailability, access denial or unsupported functionality.

Do not retry only the failing setter using the fresh schema: earlier values
in the same row have already been converted under the old one. The sender
does not retain original inputs for automatic whole-row reconversion.
There is at most one refresh per failing operation, not a DDL retry loop.
An implemented input family rejected against a cached missing column or target
type also gets this refresh, so a newly added or changed server column can be
discovered. An intrinsically unimplemented input family fails directly; looking
up another schema cannot implement that setter. Lookup and authorization errors
are not conversion-retry triggers.

## Pinned blocks and schema transitions

Keep two concepts separate:

- The latest server snapshot, learned through describes or ACKs/NACKs.
- The encoding snapshot pinned to a row and its table block.

All rows in a schema-mode table block use one encoding snapshot. Its identity
describes the schema used to resolve server-backed bindings, with explicit
inferred bindings for any missing columns. It is not replaced by the latest
cache identity when the block is sent. Wire column definitions still describe
the actual bytes; the identity does not replace those definitions.

For example, an ACK for an earlier batch may arrive while another block is
being built:

```
Server v10: count INT  -> current block: v10, INT, four-byte values
Server v11: count LONG -> next block:    v11, LONG, eight-byte values
```

Publishing v11 cannot relabel existing four-byte values as LONG or start
appending eight-byte values to their column buffer.

The I/O thread only publishes snapshots. The producer finishes an in-progress
row under its pinned snapshot, or cancels it on error. At the first effective
write of each schema-mode row, including timestamp-only rows, it resolves the
latest cached schema or performs a lookup on a miss. If the identity changed,
it seals completed rows under the old snapshot and installs a new layout before
accepting the new row's first value. This boundary is detected from actual row
state, including rows started without another `table()` call. Repeating
`table()` is not itself a safe transition point.

For the first implementation, adopting a different identity starts a new
block. Reusing unchanged allocations is allowed, but not relabeling existing
rows. Avoiding splits for provably irrelevant updates is an optional later
optimization, not a reason to mix snapshots.

Sealing freezes a block; it need not wait for a network send or ACK. Keep sealed
blocks ordered and included in normal pending-row/byte accounting, flush limits
and backpressure. Do not introduce an unbounded queue. Successive generations
of one table can use separate frames; preserve existing multi-table transaction
and deferred-commit boundaries.

The current `QwpTableBuffer.reset()` preserves column definitions. Transition
therefore needs an explicit producer-owned layout replacement after old rows
have been retained for encoding or encoded. Invalidate column-definition and
designated-timestamp caches with that replacement. Never clear data still
owned by a pending block.

## Schema feedback through writes

Each table block in an extended frame carries its name and pinned
`(tableId, metadataVersion)`, or an explicit unknown identity for a missing
table. Multi-table writes carry a separate identity per block. The name remains
the write target, including after drop/recreate; the pair is not a
compare-and-swap precondition or a schema lock.

The server processes the actual wire values using normal schema resolution and
conversion checks, then compares identities, including changes from auto-create
or added columns. It remains authoritative and may convert the already
converted values again if its schema changed.

| Outcome | Response |
|---|---|
| Identity matches | Ordinary ACK or NACK |
| Identity differs or is unknown; write succeeds | ACK plus current identity and full schema |
| Identity differs or is unknown; write fails and metadata is available and authorized | NACK plus current identity, full schema and error |

A version difference alone does not reject data. An old INT block can still
succeed against a new LONG column. Returned metadata must be a coherent
snapshot: identity and column definitions describe the same schema. Updates
are keyed by table name.

ACKs are cumulative. Accumulate and coalesce pending schema updates until an
eligible response carries them; preserve them across socket backpressure,
deferred frames and commit boundaries. A metadata update never makes otherwise
unacknowledged data safe to acknowledge.

Order describes and write-response metadata consistently per table name. A
delayed ACK must not overwrite a newer describe, including after drop/recreate.
Do not order different table IDs numerically. Ignore responses from old
connections and cancelled requests.

For feedback, reserve the top two bits of the response status byte as a mode;
the low six bits retain the existing ACK/NACK status:

| Mode bits | Payload after the status byte |
|---|---|
| `0x00` | Existing response body, unchanged; no feedback |
| `0x80` | Existing response body, then the complete schema update list |
| `0xC0` | Existing response body only; invalidate all latest-schema cache entries |
| `0x40` | Reserved; reject |

Both feedback modes require successful schema negotiation. Ordinary cumulative
ACKs and NACKs may carry feedback; durable ACKs retain their existing format
and never carry mode bits. No-feedback responses remain byte-identical even on
negotiated connections. Base response statuses must fit in six bits.

The update list is `count:u16 LE`, followed by that many entries:
`nameLength:u16 LE`, UTF-8 table name, `schemaPayloadLength:u32 LE`, schema
payload. Count must be positive; names must be valid and unique. Each payload
reuses the SCHEMA control payload without its QWP frame header:
`kind=2:u8`, `requestId=0:i64 LE`, result and any known-schema body. Zero is
reserved for write feedback; DESCRIBE replies still require a positive request
ID. Share schema payload validation, including safe skipping of unknown type
parameters. Column names must satisfy the server's column-name rules and be
unique under its case-insensitive comparison. Validate these once in the shared
schema decoder, before constructing a schema response. Validate the whole
response, exact lengths and trailing-byte exhaustion before publishing metadata
or processing its ACK/NACK outcome.

Bound the complete `UPDATES` binary response payload to 1 MiB and its WebSocket
header plus payload to the configured response-buffer capacity. If the complete
update set cannot fit, send `INVALIDATE_ALL` instead; never truncate the list,
paginate it or send extra cumulative ACKs to drain metadata. This fallback adds
no bytes to the base ACK/NACK, including at an exact buffer-capacity boundary.
`NONE` and `INVALIDATE_ALL` retain existing base-response size limits: do not
impose the new 1 MiB metadata limit on an otherwise valid large base ACK. If a coherent,
authorized complete update set cannot be returned, invalidation also provides
a bounded fallback without exposing metadata.

Invalidation affects only the latest-schema cache used for future row starts.
The next use describes the table normally; an individually oversized schema
can then report the existing lookup limit. Pinned snapshots, partial rows,
buffered/persisted bytes and ordinary/durable ACK semantics remain unchanged.

On the server, retain bounded pending table identities/names rather than stale
encoded snapshots. The current WebSocket path bounds these by its configured
table-writer ceiling, plus at most one failing table name in a rejected group.
Recheck that dependency if table-writer eviction or deferred-group ownership
changes; the response-size limit alone is not a bound on queued names.
Sample coherent metadata and recheck authorization when
serializing each response, in the same serialized response path as DESCRIBE.
Keep committed-prefix ACK feedback separate from feedback for a rejected frame
or deferred group. A NACK may describe authorized tables encountered in that
rejected work; it must not absorb a pending earlier ACK's feedback. Partial
sends retain their exact bytes and feedback ownership until completion. Old
peers must never receive unnegotiated extensions, including during SF replay.

## Cache and ownership

Allow up to 1,000,000 latest-schema cache entries per connection, allocated on
demand. No TTL, polling, subscriptions or unsolicited DDL push is required.
Eviction makes the next row's schema resolution a lookup.

The I/O thread publishes immutable entries in a thread-safe cache; all row,
binding and layout changes are producer-owned. Pinned snapshots may outlive
cache eviction or reconnect. Share immutable metadata where possible and
release it when no longer needed; the entry ceiling is not a total-memory
bound for schemas, converters and pending buffers.

Use one outstanding lookup because the producer owns row construction; no
general request queue is needed. Its deadline starts at the producer call and
includes waiting for the I/O thread, connection and response. Never reuse a
request ID; correlate completion with the connection that sent it. Timeout,
shutdown and connection loss release waiters. Ignore cancelled or unmatched
replies entirely, including their metadata.

Validate the fixed control header and request ID before parsing a direct
reply's schema. An identifiable stale reply is ignored even if its schema body
is malformed. Invalid headers that cannot safely identify a reply still fail
the connection. Parse outside the coordinator monitor; if parsing fails,
recheck the existing request identity and deadline before accepting the error
as a current connection failure. Successful parsing retains the final check
before publication. Existing timeout and connection-loss paths remain
responsible for cancellation; response validation must not pre-complete a
request before recycling its connection.

Cache known schemas and confirmed missing-table results, not denial or
unavailability. A forced refresh removes the previous entry before lookup;
failure does not restore it. Independently received new feedback remains usable.
Existing entries remain usable during an outage;
a later cache miss may wait for reconnection within its own deadline. Installing
a replacement connection clears the latest cache, without changing pinned
snapshots or cancelling a newer request that has not yet been sent.

A lazy insertion-order cache under the coordinator's monitor is sufficient for
this slice. Normalize names using the server's character comparison rules.
Apply complete validated replies in receive order: server-side metadata
sampling and serialized response sends provide the ordering guarantee above.
Do not add a client wire-sequence barrier: an ACK for older data can legitimately
contain metadata sampled after a newer DESCRIBE.

## Delivery and remaining limits

Flush, auto-flush and close encode or publish blocks in their pinned format
without schema lookups or value reconversion. SF persists the original encoded
bytes, frame flag and any pinned identity; replay requires neither original
inputs nor a fresh describe. Extension confirmation is required after upgrade
or when recovering extended SF, but legacy-only recovery before upgrade works
with old servers. No send-time transformation is needed in either mode. A stale
identity can produce repeated schema feedback but is not itself a reason to
reject a replay. Replaying legacy data preserves it; it does not retroactively
provide local conversion validation for those rows.

Schema feedback is independent of ACK/NACK delivery semantics. A NACK carrying
a new schema still rejects the batch: `SCHEMA_MISMATCH` remains TERMINAL under
[NACK policy v2](qwp-nack-policy-v2.md). Metadata cannot repair persisted bytes,
authorize automatic retry after a terminal error, discard data or advance
ordinary or durable-ACK watermarks. There is no `discard()` API in this feature.

Concurrent DDL can reject data that was locally valid. It can also make the
server apply another conversion to already converted values. Original inputs
may have lost precision or formatting during the first conversion, so replay
is not equivalent to converting those original inputs against the new schema.
The sender provides neither schema locking nor transparent reconversion after
DDL. Server checks and NACK recovery remain necessary.

## Simplification opportunities

These guide integration; the status section records what is implemented.
They do not change the compatibility contract.

1. **One owner of row data.** Reuse the schema conversion logic with Sender's
   existing table buffers. Do not add a parallel buffer lifecycle with separate
   row counts, rollback, flush accounting or close handling. Preserve the
   distinction between the latest schema and each pending block's pinned
   snapshot; reusing storage must never overwrite or relabel pending rows.
2. **One owner of the connection.** Route schema lookups through the existing
   I/O loop using a small, bounded request handoff. Reuse its connection,
   reconnect and shutdown lifecycle instead of adding another socket owner or
   a general-purpose request framework. Keep request correlation, deadlines
   and cancellation explicit. The producer still waits for required metadata
   before accepting a value and owns conversion; the I/O loop handles the
   exchange.
3. **One schema representation.** Share immutable metadata and schema payload
   encoding/validation between DESCRIBE and ACK/NACK feedback, while retaining
   their distinct message framing and delivery rules. Reuse authorization and
   size checks. Keep full snapshots; avoid schema patches, subscriptions or an
   additional conversion-version handshake. Treat the encoded 32-bit
   `ColumnType` as the canonical source of decimal precision/scale, geohash bits
   and array element/rank. Keep the existing parameter bytes reserved; do not
   add a second per-type parameter model until the encoded type cannot represent
   a required contract.
4. **A short view of remaining work.** Keep a concise implementation-status
   checklist near the beginning of the design, separate from the intended
   contract. Distinguish tested components from ordinary Sender activation.
   Keep decision history and detailed test evidence in the journal so readers
   do not need to reconstruct current status from past iterations. Link to
   that evidence rather than maintaining duplicate test logs in the design.
   Retain method and file names in the source map, but remove brittle source
   line numbers when the document is next compacted.
5. **Reuse inferred column definitions.** A confirmed missing target needs no
   second schema registry. Let the existing buffer pin the first effective
   native type and own rollback. Reuse the existing typed-rejection refresh
   when a later row conflicts. Add a guard only for a reachable contract case;
   `at()` already finishes a row, so another within-row designated-timestamp
   policy is unnecessary. D033 applies this simplification.
6. **Keep local and server rejection tests distinct.** Use public Sender to
   verify early errors and row recovery; use explicit legacy frames when the
   assertion concerns server conversion or rejection. Do not make a shared
   error helper accept either path: that can turn a missing server test into
   an apparent pass. Await the relevant ACK when a test depends on feedback,
   rather than adding sleeps or accepting multiple error reasons.
7. **Stable negative tests.** For general rejection/rollback checks, use a pair
   the server actually rejects, such as LONG-to-BINARY. A temporarily missing
   converter such as LONG-to-STRING makes that test obsolete when support is
   added. Keep explicit pending-conversion characterization separately. D039
   applies this rule without changing the existing SQL or rollback assertions.
8. **Keep common write paths small.** Isolate an infrequent conversion when
   measurements justify it, without adding a conversion framework. In D041,
   moving text formatting out of the numeric switch restores native-path
   inlining. Shared helper names are not proof of formatter compatibility:
   compare actual output before reusing or changing legacy utilities.
9. **Port only the timestamp behavior QWP uses.** The STRING-to-timestamp
   converter needs eleven fixed formats, not a general format compiler. Reuse
   public JDK timezone metadata, but preserve the server's token ordering,
   calendar edge cases and parsed-year recurring-rule calculation. A plain JDK
   instant lookup was disproved by an extreme-year input. D043 records the
   counterexample and the accepted narrow adapter. Differential and E2E tests
   caught a missing fixed-fraction fallback and a retained-leap calendar quirk
   before acceptance. No new protocol metadata or parser framework was needed.
   Timestamp output likewise uses one fixed formatter and the existing text
   sink; it does not need a general calendar-formatting framework (D044).
10. **Reuse the full setter lifecycle.** Activating a conversion includes the
    existing typed-error rollback and schema-refresh handling, not just the
    binding call. Test refresh through the newly activated setter itself.
    D045's CHAR generation test caught a missing catch; adding the standard
    catch fixed it without a new abstraction or changed cache policy.
11. **Reuse one decimal conversion tail.** Initialize the existing per-column
    scratch value from each source, then share exact rescaling, precision/storage
    checks and direct target-width append. LONG therefore needs no separate
    decimal encoder, registry or retained state. Its source-null case only locks
    the known target scale before marking the bitmap. D049 applies this rule.
12. **Keep one decimal text parser and append tail.** Sync the shared decimal
    parser's precision calculation with the current server instead of creating a
    schema-only fork, then route the parsed Decimal256 scratch through the same
    phase-5a append tail. Enforce exact special-value matching centrally according
    to the parser's documented contract, rather than adding target-width branches.
    D050 applies this rule.
13. **Implement intentional conversions, not every reachable cast.** The server
    audit is evidence, not a requirement to reproduce parser accidents or
    validation gaps. Keep BINARY-to-parser conversions, LONG_ARRAY, non-ASCII
    CHAR-to-VARCHAR and malformed decimal metadata out until their server
    contracts are deliberately resolved. Matching a server bug is not
    compatibility.
14. **Keep the schema deadline fixed.** Initial negotiation and its first
    describe share one 30-second end-to-end budget; later cache-miss describes
    and forced refreshes each get their own 30-second budget. Do not add public
    configuration, retries, another socket, a request queue or separate timeout
    policy per path.
15. **Prefer narrow family helpers to a conversion framework.** A shared helper
    for BYTE/SHORT/INT numeric targets is justified because those setters have
    the same range-check and append mechanics. Do not introduce a converter
    registry or graph. Do not refactor the established LONG hot path without
    measurements showing that the change is useful and safe.
16. **Make compatibility a standing gate, not a feature iteration.** Run the
    real old/new client-server binary matrix in CI and before release. Pin the
    last pre-extension releases and verify their checksums; do not use moving
    `latest` versions. Reuse one public-API probe compiled against the old client
    and launch each client in an isolated JVM. Keep exact upgraded-sender
    no-send, watermark and replay assertions in the faster deterministic socket
    and replay tests instead of adding a flaky process failover rig. Each numbered
    conversion iteration still keeps a focused current-server E2E test green;
    it does not reimplement negotiation or compatibility.
17. **Plan by conversion mechanism.** Group target paths that share a wire
    representation, conversion rule and rollback behavior. Do not create one
    iteration per matrix cell, and do not combine unrelated server-boundary
    decisions merely because they appear in the same source audit.
18. **Treat textual decimal as an existing source type.** Parse into the
    Sender-owned Decimal256 scratch value, then reuse the native decimal append
    and text-formatting tails. Keep parsed specials separate only because they
    mean an effective target NULL while typed decimal null sentinels are no-ops.
    Normalize an unset null-only scale at the encoding boundary; do not add a
    second scale state, parser or converter registry.
19. **Keep IPv4 as one primitive path.** Reuse the native four-byte column for
    IPv4 targets and the existing reusable numeric sink for canonical text.
    Normalize zero to the target bitmap before append. Do not add an address
    object, parser fork, converter registry or per-row string allocation.

These opportunities do not justify a per-setter opt-out, raw-value fallback or
send-time transformation. Partial activation is permitted on the unreleased
development branch to exercise the real public Sender path early. It is not a
release-ready client: missing conversion families still reject some
previously working writes. Preserve legacy-mode behavior and complete the
schema-mode contract before release.

## Iterative implementation and testing

Establish a working real-client/real-server E2E test first, then grow the
implementation and its tests together. Do not wait for negotiation,
compatibility and all converters to be implemented before exercising the
complete client-to-server path. Reuse the existing test infrastructure.

### Plan after iteration 2.39a

The permanent compatibility gate is implemented. It launches released and
current artifacts as separate processes and checks these observable contracts:

- Current client against QuestDB 10.0.1 stores legacy LONG-to-FLOAT conversion.
- Client 1.3.9 against the current server stores the same legacy result.
- Current client against the current server stores the schema-mode result.

The discriminator is one three-row block written to a FLOAT column: `42`,
`Long.MIN_VALUE`, then a row omitting the value. Legacy mode stores the minimum
long as a float; schema mode treats it as a source null. The old client probe is
compiled against its public API and reused unchanged with each client JAR, so
the gate tests binary compatibility as well as process interoperability. The
released JARs and their SHA-256 hashes are fixed in `versions.env`; current JARs
come from the build under test when the server pins a client snapshot. For a
released client dependency, the gate uses that exact resolved artifact from the
local Maven cache. The same script runs in stable-client CI and before release
artifacts are copied.

The existing deterministic socket and replay tests remain the authority for exact
negotiation traffic and for a confirmed sender later reaching an old endpoint:
it sends no buffered schema frame, advances no watermark and preserves exact
bytes for replay. Splitting that state transition across two server processes
would add timing and orchestration without improving the contract assertion.
The gate adds no protocol mode, adapter, send-time transformation or production
code.

The next implementation slice is:

1. **Iteration group 2.39 — fixed-width identity and text targets.** IPv4 to
   IPv4/STRING/VARCHAR is implemented in 2.39a. Next, land LONG256 to
   LONG256/STRING/VARCHAR, then GEOHASH to the exact same-precision
   GEOHASH/STRING/VARCHAR as separate reviewable slices. Each slice gets its own
   public-Sender real-server E2E, exact target wire checks, null/omission
   decisions, rollback, refresh and SF replay. Preserve geohash bit precision
   and the existing public overload semantics; do not use this group to approve
   unrelated parser paths.

After these slices, reassess the remaining inventory in this order: integer to
DATE/timestamps; integer to text/SYMBOL/decimals and INT-to-IPv4;
then DOUBLE arrays after rank metadata is proven end to end.
BINARY-to-parser conversions, LONG_ARRAY, non-ASCII
CHAR-to-VARCHAR and malformed decimal metadata remain server-contract decisions,
not client implementation backlog.

Every slice keeps the existing observable-contract rules below: public API,
exact wire, SQL result, partial-row rollback and relevant recovery behavior.
Run canonical and packaged-client gates, the server gate against its pinned
client submodule, and affected fixed-work performance checks before acceptance.
Update the journal and exact submodule pin with every accepted slice.

### Original milestones

The original milestones below describe how the present architecture was built;
they remain useful as constraints for later work:

1. **E2E baseline.** Write one typed value through the public `Sender` API to
   a real server and verify it through SQL. Keep this path working throughout
   subsequent changes.
2. **Schema lookup and binding.** Make that write use the server-supplied
   schema. Add matching-type, missing-table and lookup-failure cases alongside
   the implementation. This slice includes the minimal confirmation gate needed
   to use schema mode; milestone 3 covers the broader compatibility matrix.
   Land discovery controls/codecs and their real-server protocol tests before
   activating the high-level Sender. Until activation, it does not request the
   extension and keeps existing behavior. Do not enable schema mode merely to
   test negotiation while silently retaining server-side conversions.
   Producer-owned conversion components may also land before Sender activation:
   exercise real discovery, local conversion/rollback, existing typed QWP bytes
   and real-server SQL together. This is conversion-component E2E, not activated
   schema mode. Its first increment need not transmit schema identities or
   change persisted framing. Introduce the extended frame flag together with
   its required negotiation and recovery checks, not as a prerequisite for
   testing a local converter. The first public-Sender integration test need not
   wait for all conversion families. Start with a known table, implemented inputs
   and explicit flush; then extend that same path through row completion,
   metadata refresh, schema changes and delivery/recovery. A narrow passing test
   does not establish those other paths. No public setter may silently bypass
   schema conversion on a supporting connection, even during development.
3. **Negotiation and compatibility.** Add automatic legacy-server compatibility,
   mandatory schema mode on supporting servers and the one-way upgrade. Cover
   unchanged legacy-client behavior, pending legacy rows and SF during upgrade,
   and an old endpoint after upgrade. Test real handshake and framing behavior,
   not internal capability fields.
4. **Conversions, one family at a time.** Start with STRING-to-UUID, then add
   the other families in separate increments. Each gets valid, invalid,
   boundary and null cases, SQL verification and partial-row rollback tests.
5. **Schema evolution.** Add ACK feedback, pinned-block transitions, changes
   during row construction and whole-row retry. Test the observable sequence
   and resulting data for each behavior as it is introduced.
6. **Recovery scenarios, throughout development.** Extend reconnect,
   legacy/extended SF replay, interrupted-delivery and process-restart coverage
   whenever the corresponding behavior changes. In particular, recovery tests
   start with the first persisted-framing change, not after the other milestones.

These are milestones, not equally sized iterations or isolated layers. Split
them into small, independently reviewable changes, each production-quality for
its implemented scope. Every iteration includes extensive tests for its new
behavior and failure paths, runs the relevant existing regression suites, and
ends with the real E2E path passing. There is no later "add E2E" or "harden it"
phase. State unfinished scope explicitly; do not release the new client
or advertise the complete extension while its contract is incomplete.

Tests assert observable contracts: public API results and exceptions, wire
messages, SQL data and documented delivery behavior. No reflection, private
state inspection, test-only production hooks or visibility changes solely for
tests. Unit tests of genuine production component contracts are appropriate;
implementation-specific call counts and internal object layouts are not.

Use complementary public-Sender tests for integration: a real QuestDB fixture
checks local errors and exact stored rows; a scripted socket peer checks the
negotiated requests, target wire types and pinned identities. SQL alone cannot
prove local conversion because the legacy server can perform the same cast.
A peer omitting schema confirmation proves protocol fallback, not compatibility
with an actual older QuestDB distribution; label that distinction explicitly.

Use real servers for conversion and ingestion semantics. Scripted protocol
peers and TCP relays may inject malformed responses, hold ACKs, fragment traffic
or disconnect at controlled points; they do not replace real-server E2E tests.
Coordinate concurrent tests through observable events with bounded waits, not
arbitrary sleeps. Seeded fuzzing supplements deterministic cases and must leave
reproducible failures with an independent expected-data model.

A representative conversion test buffers valid row A, starts row B and supplies
an invalid UUID string to `stringColumn()`, then writes valid row C and flushes.
Assert that the setter throws and SQL contains A and C, with no part of B.
Observe encoded UUID values on the wire where needed to distinguish local
conversion from server-side parsing. Extend the scenario with SF and controlled
recovery as those paths change; do not substitute an assertion that an internal
rollback method ran.

Process-restart coverage must also create its backlog through public Sender
setters, not only by appending constructed frames to the replay engine. Wait
for local publication and captured data, prove zero server delivery, then force
the producer JVM to stop without closing Sender. A fresh Sender using the same
directory and identity must replay identical QWP payloads and produce exact
rows, including when those payloads contain different pinned schema versions.
Reopening after ACK must not replay acknowledged rows. This tests process death,
not power loss; zero initial delivery is what permits exact row-count assertions
without claiming general exactly-once delivery.

## Acceptance checks

- New clients always request the extension. Before upgrade, an old server's
  absent confirmation selects existing inferred-type behavior, with unchanged
  wire framing, server conversions, missing-value semantics and SF delivery.
  Supporting servers enable schema mode automatically; no option disables it.
- Failed handshakes, timeouts and invalid confirmation are not treated as old
  servers. Async startup waits for mode selection and cannot bypass a required
  first-use lookup.
- Existing clients against new servers see byte-compatible QWP v1 behavior,
  with no unsolicited schema messages or extended responses. The base protocol
  version remains 1 for both legacy and extended traffic.
- The persisted flag selects table-header parsing. Exercise mixed legacy and
  extended SF replay on a negotiated connection, and reject flagged frames on
  unnegotiated connections before processing data.
- Upgrade from an old to a supporting server finishes any partial legacy row
  unchanged and adopts schema mode at the next row boundary, including without
  another `table()` call. Pending legacy blocks and SF remain unchanged and
  ordered; new blocks use the extended layout. No send-time conversion occurs.
- Reconnect and background replay re-negotiate support. After upgrade, an old
  endpoint receives no pending data and causes neither downgrade nor watermark
  advancement. The requirement survives reconnect even with a legacy-only
  backlog; recovery detects pending extended SF before sending a legacy prefix.
  Legacy-only recovery by a fresh sender remains compatible with old servers.
- The downgrade procedure drains every pending extended slot with a supporting
  client under the configured ACK policy. Interrupted or timed-out draining
  leaves recoverable data and does not qualify as safe to downgrade.
- Write-only credentials can discover the schema and ingest without SELECT
  access. Denied/revoked credentials receive a classified failure and no
  unauthorized schema in either DESCRIBE or write-response feedback.
- In schema mode, first use waits for schema. Valid UUID strings and typed UUID
  inputs produce the same target representation; malformed strings fail before
  row commit/SF.
- Shared conversion vectors cover every input/target pair in the contract, edge
  values, nulls, text formatting, timestamp units, decimal rescaling, array
  ranks on later rows and geohash precision. Verify final server values too.
- Different input setters can feed one known target column. Inferred missing
  columns, duplicate/no-op and name behavior remain covered.
- In schema mode, omitted values and explicit nulls follow target missing-value
  semantics, independent of earlier input setter types. Cover boolean-to-STRING omission
  becoming NULL, explicit false remaining "false", non-nullable targets and
  unchanged results for legacy clients and new clients in legacy mode.
- For LONG input to each numeric target, test `Long.MIN_VALUE` with and without
  another omitted value in the same block. Schema mode always emits target
  missing/null; legacy mode retains its existing context-dependent behavior.
  Duplicate suppression and unsupported-pair rejection still precede normalization.
- For LONG-to-STRING/VARCHAR/SYMBOL, verify canonical decimal bytes, MIN target
  NULL versus omission, and MIN-first duplicate suppression. Preserve independent
  legacy bitmap-present MIN-to-literal-"null" tests. Distinct/repeated symbols
  must retain their own dictionary values when the formatter sink is reused.
- Timestamp overloads test every supported unit's range boundaries, negative
  fractions and `Instant` precision at both target resolutions. Include valid
  negative instants whose intermediate seconds multiplication would overflow,
  duplicate suppression, local failure/rollback and unchanged legacy behavior.
- A stale rejection refreshes once. A changed target cancels the whole row;
  retry uses the new snapshot. No setter splices new rules into a partial row.
- Local errors expose the expected reason and recovery action. An invalid row
  stays cancelled after flush; whole-row retries after schema refresh or a
  recovered lookup can succeed without losing earlier completed rows. Access
  denial and unsupported functionality are not mislabeled as transient lookup
  failures or flush-retryable errors.
- ACK metadata arriving mid-row leaves that row and completed rows unchanged.
  The next adopted snapshot gets a separate block with matching wire types
  and identity; exercise INT-to-LONG and cached designated timestamps.
- Transitions work without another `table()` call, preserve pending accounting,
  frame order, transaction boundaries, backpressure and completed rows on error.
- Auto-create, multi-table writes, cumulative ACKs, deferred commits, delayed
  sends and describe/ACK interleaving preserve schema feedback and watermarks.
- Lookup timeout, offline cache misses, eviction, reconnect and shutdown neither
  bypass required resolution nor invalidate pinned data or strand callers.
- Unsupported target types fail explicitly when used; unrelated columns with
  unknown types do not block supported writes. The million-entry cache grows
  on demand and obsolete pinned metadata is released.
- Flush, close, byte-identical SF replay and terminal-error handling retain their
  data semantics. Concurrent DDL can still NACK; metadata never repairs or
  silently acknowledges rejected data.
