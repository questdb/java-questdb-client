# QWP Client Review — branch `vi_egress`

Consolidated findings from 4 parallel review passes (concurrency, decoder, bind/protocol, idioms/API).
File paths are relative to `core/src/main/java/io/questdb/client/` unless noted.

---

## Tier 1 — Fix before merge (data corruption, memory safety, resource leaks)

### Decoder memory safety — `cutlass/qwp/client/QwpResultBatchDecoder.java`

- **`parseSymbolColumn` non-delta path (~L766–778):** no bounds on `dictSize`; no `entryLen < 0` guard; sign-extension of `entryLen` in `(long) entryLen << 32` corrupts the packed offset. A hostile/buggy server frame writes past the buffer end.
- **`parseDeltaSymbolDict` (L685–686):** `entryLen` checked `>= 0` but cast to `int` before `ensureConnDictHeapCapacity`; values > `Integer.MAX_VALUE` wrap negative.
- **`varint` accepts bit-63-set values.** Two callers cast to `int` without a non-negative check: `dictSize` and `precisionBits` (GEOHASH). Also no upper bound on geohash precision (should be 1–60).
- **Unbounded connection-scoped dict:** `connDictSize` / `connDictHeapPos` have no cap. Long-lived connection can grow native heap to int-overflow.
- **`handleResultBatch` decode-failure path (`QwpEgressIoThread.java` L548–558):** returns the buffer to `freeBuffers` after a partial decode; layout may retain dangling pointers into freed decoder scratch. Close/discard the buffer on decode failure instead of pooling it.

### Bind encoder — `cutlass/qwp/client/QwpBindValues.java`, `QwpBatchBuffer.java`

- **`checkScale` uses `Decimals.MAX_SCALE` (76) for all widths.** DECIMAL64 (max 18) and DECIMAL128 (max 38) silently encode out-of-range scales.
- **NULL path for DECIMAL64/128/256 and GEOHASH emits `type | null-flag | 0x01` only**, skipping the scale / precision-bits bytes. If the server reads them unconditionally for the type, every subsequent bind in the batch is mis-framed.
- **`setGeohash` does not mask `value` to `precisionBits`** — `value >>> (b*8)` can leak high bits into the top byte when `precisionBits` isn't byte-aligned.
- **`QwpBatchBuffer.ensureCapacity` (L95–101)** loops doubling `newCap *= 2`. With `scratchCapacity == 0` the loop is infinite; with `required > Integer.MAX_VALUE/2` it overflows silently.
- **`std/Zstd.java` has no `static { Os.init(); }`** — first native call throws `UnsatisfiedLinkError` unless another `Os.init()` ran first.

### Concurrency — `cutlass/qwp/client/QwpQueryClient.java`, `QwpEgressIoThread.java`

- **`QwpQueryClient.close()` is not idempotent and not thread-safe.** Overlapping calls race on `shutdown`/`join`/`closePool`/`bindValues.close()` — the latter double-frees native memory. Gate with `AtomicBoolean closed` CAS.
- **`releaseBuffer` races `closePool`:** reads `closed == false`, then `closePool` runs `freeBuffers.clear()`, then the offer lands in the drained pool and is leaked.
- **Generation-listener orphan pattern uses a single shared `AtomicReference terminalFailure` across generations.** A late callback from an orphaned I/O thread can poison the *new* connection. Give each generation its own AtomicReference.
- **`pendingRelease` handshake in `handleResultBatch`:** `freeBuffers.offer` precedes `pendingRelease.offer` → a two-batch window can consume the previous batch's token, leaving the current batch parked.
- **`connectToEndpoint` leaks the open WebSocket** if `receiveServerInfoSync` / `probeZstdAvailable` / `ioThreadHandle.start()` throw between upgrade and the outer catch.

---

## Tier 2 — Fix before API freeze (hard to undo later)

### API shape — `QwpQueryClient.java`, `QwpBindValues.java`, `std/Long256*.java`, `std/Uuid.java`

- **Builder mixed into the runtime object** (unlike `Sender`/`LineSenderBuilder`). 7 `withXxx` setters return `this`, 8 return `void`. Extract `QwpQueryClientBuilder` and match `Sender.builder(...)`, or at minimum align return types.
- **`QwpQueryClient.fromConfig()` returns the client, not a builder** — diverges from `Sender.builder(String)`.
- **`Long256` interface and `Long256Impl` have no `toString`/`equals`/`hashCode`/`isNull`.** `Uuid` has no `toString`/`isNull`/`equals`, no `final`. Very painful to add after release.
- **`isConnected()` returns true after a latched terminal failure** — retry loops spin forever emitting `STATUS_INTERNAL_ERROR`. Either check `terminalFailure.get() == null`, or rename to `isOpen()` and add `isHealthy()`.
- **`close()` silently leaks I/O thread + pool + socket when the 5s join times out;** `wasLastCloseTimedOut()` is the only signal. Consider a `shutdownNow()` that closes the socket to break the loop.
- **`QwpColumnBatchHandler.onError` has an empty default** — users who forget to override get silent server errors. Make abstract or rethrow as `RuntimeException`.
- **`withCompression`/`withTarget` take magic strings** — should be enums (`QwpCompression`, `QwpTarget`).
- **Demote to package-private before shipping:** `QueryEvent`, `QwpBatchBuffer`, `QwpColumnLayout`, `QwpEgressIoThread`, `NativeBufferWriter`. Nothing outside the package uses them.
- **Missing bind setters:** no `setDecimal64(Decimal64)` overload (asymmetric with 128/256); no `setIPv4`/`setBinary`/`setSymbol`/array setters.

### Wire-type coverage — `cutlass/qwp/protocol/QwpConstants.java`, `ColumnView.java`

- **`TYPE_IPv4` missing from `getFixedTypeSize`** → `ColumnView.bytesPerValue()` is `-1` for IPv4; anyone walking via `valuesAddr()` reads garbage.
- **`TYPE_BINARY` / `TYPE_IPv4` missing from `getTypeName`** → error messages print `UNKNOWN(23)`.
- **`TYPE_IPv4` naming** (camelCase) breaks the `TYPE_*` UPPER_SNAKE convention — rename to `TYPE_IPV4`.

---

## Tier 3 — Performance cleanups

- **`parseNullSection` re-issues `Unsafe.getByte` per row** instead of per 8-row span (7/8 of native loads wasted). Hoist with `if ((i & 7) == 0) bm = ...`.
- **Byte-by-byte copy loops** in `getBinary`/`getString`/`readColumnName`/`getGeohashValue` — replace with `Unsafe.copyMemory` or `getLong + mask`.
- **Non-ASCII `setVarchar`** allocates `String.toString()` + `getBytes(UTF_8)` — breaks allocation-free steady state. Use `Utf8s`/`NativeBufferWriter` streaming encode.
- **`parseNullSection` drops pool-owned `nonNullIdx` when column has no nulls** — workloads alternating null/no-null per batch churn the array. Use a boolean sentinel instead.
- **SPSC queue has no `@Contended`/padding** — producer `head` and consumer `tail` share a cache line.
- **`ArrayList<Endpoint>` in `QwpQueryClient`** — use `ObjList` per project idiom.
- **`ensureOwnedEntriesAddr` / `ensureTimestampDecodeAddr` / `ensureConnDict*` capacity doublings** are int-multiplies and can overflow silently at ≥ 2^30.

---

## Tier 4 — Structural smells

- **`QwpColumnBatch` — 755 lines, 40+ accessors**, plus 30+ methods duplicated across `ColumnView` and `RowView`. Centralize the read path.
- **`QwpQueryClient.fromConfig` — 244 lines**; **`QwpResultBatchDecoder.decodePayload` — 205 lines.** Split.
- **`WebSocketResponse` holds both ingress-phase-1 fields and the egress `STATUS_INTERNAL_ERROR` constant** used from 11 egress sites. Extract egress statuses to their own class.
- **Naming: `QwpEgressIoThread` is a misnomer** — "egress" is server-side terminology, but this thread is the client-side read loop. Rename `QwpReadIoThread` / `QwpQueryIoThread` for consistency with `QwpUdpSender` / `QwpWebSocketSender`.
- **Three ad-hoc pooling patterns** (`borrowLayout`, `eventPool`, `columnViews`) where `ObjectPool<Mutable>` is the project idiom.
- **`QwpDecodeException` vs `QwpRoleMismatchException`** — one checked, one unchecked; pick one (QuestDB idiom is unchecked).
- **Examples under `examples/src/main/java/com/example/query/`** swallow errors to `System.err` and return normally. `ExecStatementExample` has no `DROP TABLE IF EXISTS` setup — a crashed prior run leaves a dirty DB.
- **Endianness assumption** (LE host) is implicit across all `Unsafe.getInt/getLong` of wire data — add a bootstrap check or at minimum document.

---

## Server-side refactor checklist

The server contains tests against the client shape. Expect to touch server tests when you apply:

1. **Builder extraction** (`QwpQueryClientBuilder` + `fromConfig` moves to the builder).
2. **Return-type unification** on all `withXxx` setters.
3. **Demoting `QueryEvent`/`QwpBatchBuffer`/`QwpColumnLayout`/`QwpEgressIoThread`/`NativeBufferWriter` to package-private.** Any test reaching into these needs to move into the sibling test package.
4. **`QwpColumnBatchHandler.onError` becoming abstract** — every test handler must supply an impl.
5. **Enum introduction for compression/target** — any server test using magic strings breaks.
6. **`TYPE_IPv4` → `TYPE_IPV4` rename** — constant references in server tests.
7. **`WebSocketResponse` status-code split** (new `QwpStatus` class) — server tests asserting on `WebSocketResponse.STATUS_*` need to re-import.
8. **`QwpEgressIoThread` → `QwpReadIoThread`** rename (if adopted).
9. **Bind encoding NULL-with-scale fix** — server's bind parser must match whatever the new encoding is; align both ends in the same change.
