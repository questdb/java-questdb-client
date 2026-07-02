# Cursor SF — server error API: implementation plan

Branch: `vi_sf` (continues off the cursor SF work).
Spec: `design/qwp-cursor-error-api.md` (decisions 1–14 locked).
Depends on: `qwp-cursor-durability.md` (the SF substrate this builds on).

> **Historical note (Invariant B):** step statuses below reflect the state at
> the time of writing. The reconnect-budget-exhaustion terminal mentioned in
> step 4 was later removed — the background reconnect loop now retries
> indefinitely. See `qwp-client-startup-failover-behavior.md`.

## Shipped on `vi_sf`

| Step | Status | Notes |
|---|---|---|
| 1. Public types | ✅ | `SenderError`, `SenderErrorHandler`, `LineSenderServerException` (all in `io.questdb.client`); 11 unit tests in `SenderErrorTest`. |
| 2. Typed terminal-error stash | ✅ | Sibling `volatile SenderError lastTerminalServerError` on `CursorWebSocketSendLoop`; `recordFatal(Throwable, SenderError)` overload; `getLastTerminalServerError()` on the loop, `getLastTerminalError()` on `QwpWebSocketSender`. |
| 3. Wire-byte classification + DROP/HALT branches | ✅ | `classify()`, `defaultPolicyFor()`, `handleServerRejection()` in `CursorWebSocketSendLoop`; HALT routes through typed `LineSenderServerException`, DROP advances `engine.acknowledge` and continues. 12 tests in `CursorWebSocketSendLoopErrorClassificationTest`. |
| 4. WS close-frame routing | ✅ | `isTerminalCloseCode()` splits PROTOCOL_ERROR/UNSUPPORTED_DATA/INVALID_PAYLOAD_DATA/POLICY_VIOLATION/MESSAGE_TOO_BIG/MANDATORY_EXTENSION as terminal `PROTOCOL_VIOLATION`; reconnect-eligible codes preserve existing `fail()` retry. Auth-terminal upgrade and reconnect-budget exhaustion now stash typed `SenderError` payloads. |
| 5. Bounded inbox + dispatcher daemon | ✅ | `SenderErrorDispatcher` (lazy-start daemon, bounded `ArrayBlockingQueue`, idempotent close, drained handler exceptions). 11 tests in `SenderErrorDispatcherTest`. |
| 6. Default error handler | ✅ | `DefaultSenderErrorHandler.INSTANCE` — ERROR for HALT, WARN for DROP, full structured payload in the log line. |
| 7. Builder + connect-string knobs | ✅ (partial) | Builder: `errorHandler(SenderErrorHandler)`, `errorInboxCapacity(int)` — both gated to WebSocket. Connect string: `error_inbox_capacity=N`. **Per-category policy override (`errorPolicy(Category, Policy)`, `errorPolicyResolver(...)`, `on_*_error` keys) deferred — see § Deferred follow-ups.** 9 tests in `SenderBuilderErrorApiTest`. |
| 8. New `Sender` API | ✅ (partial) | `flushAndGetSequence(): long`, `getLastTerminalError()`, `getTotalServerErrors()`, `getDroppedErrorNotifications()`, `getTotalErrorNotificationsDelivered()`. **`resumeAfterHalt()` deferred** — the I/O loop is one-shot today; restart primitive is non-trivial. Workaround: close + rebuild the sender. |
| 9. End-to-end per-category integration tests | ⏭️ deferred | Lands in the `questdb` repo (`TestWebSocketServer` doesn't parse QWP wire format, so it cannot be scripted to emit category-specific frames in this repo without significant fixture work). |
| 10. `tableName` wiring | ✅ | Best-effort: populates `tableName` from `response.tableNames` when single-table; null otherwise. Today the response parser does not populate `tableNames` on error frames (only on STATUS_OK), so `tableName` is null on error frames until both client parser and server are extended. The wiring is forward-compatible. |
| 11. Docs | this doc | Spec + this implementation log. README/javadoc updates pending. |

Test totals on `vi_sf`: 154 non-mmap tests pass on linux x86_64. (`Files.mmap0` UnsatisfiedLinkError on linux — pre-existing, repo only ships macOS-aarch64 native lib. The mmap-dependent tests will run green on macOS / when the linux native lib is added.)

## Deferred follow-ups (not blocking)

1. **Per-category policy override** (`errorPolicy(Category, Policy)` + `errorPolicyResolver(...)`). Spec § "User overrides — one knob, two grains" describes the resolver composition (programmatic resolver > per-category map > global default). Today every category uses `defaultPolicyFor` baked into the loop. The most-asked variant — strict-mode `on_server_error=halt` — needs the connect-string parser side too. Moderate-sized addition; fits in a focused commit.
2. **`resumeAfterHalt()` escape hatch.** The cursor I/O loop today is one-shot (`running` is volatile boolean, no restart primitive). To resume, the loop needs: clear `lastError` / `lastTerminalServerError`, reopen the wire client via the reconnect factory, restart the thread. Today's workaround: close + rebuild the sender; SF data on disk survives. Document that.
3. **End-to-end integration tests in the `questdb` repo.** Use a real `ServerMain` to drive each `STATUS_*` byte against this client, asserting category, policy, FSN span, callback delivery, and producer-thread typed throw.
4. **Server-side gaps tracked in the spec § "Server-side follow-ups"**: split `0x06`/`0x09` for retry semantics, add retryable bit, per-table attribution. Each unblocks a corresponding client follow-up — e.g. retryable bit unblocks `RETRY_TRANSIENT` policy and full strict-ETL semantics.
5. **README + public Javadoc.** Document the new connect-string keys, builder methods, and accessor surface. The spec is locked but user-facing docs aren't yet.

## Context

The cursor SF send loop today (`CursorWebSocketSendLoop.ResponseHandler.onBinaryMessage`, line 712 onward) classifies inbound frames as `STATUS_OK` (advance ackedFsn) vs everything-else (always terminal via `recordFatal`). The "everything-else" branch is what we're refining: classify by status byte → category, resolve policy, surface to user via callback (async) and / or typed exception (next API call).

Wire codes already exist (`WebSocketResponse.java:74-83`, `WebSocketResponse.getStatusName()`). Nothing new on the wire.

## Discrete deliverables

### 1. Public API surfaces (do first, in isolation)
New types in `core/src/main/java/io/questdb/client/`:
- `SenderError.java` — immutable, public. Fields per spec § "SenderError". Include `Category` and `Policy` as nested public enums.
- `SenderErrorHandler.java` — `@FunctionalInterface` with `void onError(SenderError)`.
- `LineSenderServerException.java` — `extends LineSenderException`. Single field `SenderError serverError`; `getServerError()` accessor; `getMessage()` synthesizes from category + FSN span + serverMessage.

These are leaf types — write them and their unit tests first; nothing else depends on internals.

### 2. Typed terminal-error stash on the I/O loop
**Note:** the `connectionGeneration` field described in `qwp-cursor-durability.md` is an idealization — it didn't ship. The actual code already has the producer-side latch infrastructure:
- `CursorWebSocketSendLoop.lastError` (`volatile Throwable`, line 122) — terminal error, set by `recordFatal(...)`.
- `QwpWebSocketSender.connectionError` (`AtomicReference<LineSenderException>`, line 119) — connection-level latch.
- `QwpWebSocketSender.checkConnectionError()` (line 1417) polls both on every public API entry.

So the cache-line / `@Contended` extraction is unnecessary — the volatile that the producer thread already reads on every API call is the latch we need. What's left:

- Add `private volatile SenderError lastTerminalServerError` on `CursorWebSocketSendLoop`, sibling to `lastError`. Null in steady state.
- Overload `recordFatal(Throwable t)` → `recordFatal(Throwable t, SenderError serverError)`. Existing callers (wire-level failures) call the original signature with implicit `null`. Server-rejection callers (deliverable #3) pass the `SenderError`. Idempotent — only the first failure wins.
- Add `public SenderError getLastTerminalServerError()` accessor on the loop.
- Add `public SenderError getLastTerminalError()` on `QwpWebSocketSender`, delegating to the loop (with the standard `cursorSendLoop == null ? null` guard used by other accessors).

That's the whole change for #2. The producer-thread typed throw lands automatically once #3 starts stuffing `LineSenderServerException` (which extends `LineSenderException`) into `lastError` — `checkError()` already throws whatever `lastError` is; user code can `instanceof LineSenderServerException` to unpack the typed payload.

### 3. Error frame classification (`CursorWebSocketSendLoop.ResponseHandler.onBinaryMessage`)
Replace the current `else` branch (lines ~734-751) with classification:
```java
SenderError.Category category = classify(response.getStatus());   // wire byte → enum
SenderError.Policy policy = policyResolver.resolve(category);     // user override > per-cat > default
String tableName = response.getTableEntryCount() == 1
        ? response.getTableName(0)
        : null;
long fromFsn = fsnAtZero + Math.max(0, response.getSequence());   // single-frame span today
long toFsn = fromFsn;
SenderError err = new SenderError(category, policy, response.getStatus(),
        response.getErrorMessage(), response.getSequence(),
        fromFsn, toFsn, tableName, System.nanoTime());
totalServerErrors.incrementAndGet();
lastTerminalError = (policy == HALT) ? err : lastTerminalError;

if (policy == HALT) {
    signal.terminalError = err;     // memory-ordered write before inbox offer
    errorInbox.offer(err);           // non-blocking; drop+count if full
    recordFatal(new LineSenderServerException(err));   // breaks the loop; existing path
} else { // DROP_AND_CONTINUE
    errorInbox.offer(err);
    engine.acknowledge(fromFsn);    // advance past the rejected span
    totalAcks.incrementAndGet();    // for parity with success path counters
}
```
- Keep the success path untouched.
- Verify `WebSocketResponse` already exposes the error message after parsing a non-OK status (the `errorMessage` field is read by `getErrorMessage()` — confirm parser populates it on the error path).
- `STATUS_DURABLE_ACK` (0x02) handling stays as-is; it is not an error.

Helper:
```java
private static SenderError.Category classify(byte status) {
    switch (status) {
        case STATUS_SCHEMA_MISMATCH: return Category.SCHEMA_MISMATCH;
        case STATUS_PARSE_ERROR:     return Category.PARSE_ERROR;
        case STATUS_INTERNAL_ERROR:  return Category.INTERNAL_ERROR;
        case STATUS_SECURITY_ERROR:  return Category.SECURITY_ERROR;
        case STATUS_WRITE_ERROR:     return Category.WRITE_ERROR;
        default: return Category.UNKNOWN;
    }
}
```

### 4. WS close-frame routing
`ResponseHandler.onClose(int code, String reason)` (line 708) currently builds a `LineSenderException` directly and calls `fail(...)` → reconnect. Two cases:
- **Reconnect-eligible close** (server idle close, network blip): keep existing behavior — `fail(...)` enters reconnect loop.
- **Terminal close** (PROTOCOL_ERROR 1002, UNSUPPORTED_DATA 1003, MESSAGE_TOO_BIG 1009, policy violation 1008, custom server reason that asserts terminal): build a `SenderError(category=PROTOCOL_VIOLATION, status=-1, seq=-1, message="ws-close[<code>]: " + reason, fsn=ackedFsn+1..publishedFsn, tableName=null, policy=HALT)`, write `signal.terminalError`, inbox, then `recordFatal`.

Decision boundary between the two: the existing reconnect logic already differentiates terminal codes (see auth-terminal handling in commit `8828038`). Mirror that taxonomy here — anything currently treated as terminal becomes a `PROTOCOL_VIOLATION` with the same FSN span.

### 5. Bounded inbox + dispatcher daemon
- Implement as `ArrayBlockingQueue<SenderError>` for v1 (single producer = I/O thread; single consumer = dispatcher; capacity from builder). Project idiom prefers `QwpSpscQueue` — use it if a generic version exists, else `ArrayBlockingQueue` is fine for the off-hot-path side channel.
- Dispatcher thread: lazy-start on first `inbox.offer` success. Daemon, named `qwp-error-dispatcher-<senderId>`. Loop: `take()` → `try { handler.onError(err); } catch (Throwable t) { LOG.error(...); }`. Stops when `engine.close()` interrupts it; drains remaining queue entries on stop with a short deadline (~100ms) before giving up.
- Overflow handling on `offer`: returns false; I/O thread bumps `droppedErrorNotifications` and continues. Never block.

### 6. Default error handler
```java
class DefaultErrorHandler implements SenderErrorHandler {
    public void onError(SenderError e) {
        LogRecord r = (e.appliedPolicy == HALT) ? LOG.error() : LOG.advisory();
        r.$("server error: ").$(e.category)
         .$(" status=0x").$hex(e.serverStatusByte)
         .$(" fsn=[").$(e.fromFsn).$(',').$(e.toFsn).$(']')
         .$(" table=").$(e.tableName != null ? e.tableName : "(multi)")
         .$(" msg=").$(e.serverMessage)
         .$();
    }
}
```
Wire as the default if the user does not call `errorHandler(...)` on the builder. Match the project's logging idioms (use `LogFactory.getLog`, etc).

### 7. Builder + connect-string knobs
- `LineSenderBuilder.errorHandler(SenderErrorHandler)`, `errorPolicy(Category, Policy)`, `errorPolicyResolver(...)`, `errorInboxCapacity(int)`.
- Connect-string parser additions in `Sender.fromConfig` / `LineSenderBuilder.fromConfig`:
  - `on_server_error` (auto/halt/drop)
  - `on_schema_error`, `on_parse_error`, `on_internal_error`, `on_security_error`, `on_write_error` (halt/drop)
  - `error_inbox_capacity` (int)
- Internal `PolicyResolver`: composes user resolver (highest) → per-category map → global → per-spec defaults. Single method `Policy resolve(Category)`.

### 8. New public API methods on `Sender` / `QwpWebSocketSender`
- `Sender.flushAndGetSequence(): long` — returns `engine.publishedFsn()` after the publish, before returning. The existing `flush()` keeps `void` return — call the new method internally or have `flush()` discard the return.
- `Sender.resumeAfterHalt()` — only meaningful on QWP WS sender; default impl on `Sender` interface throws `UnsupportedOperationException("only WS senders support resumeAfterHalt")`. Implementation:
  ```java
  signal.terminalError = null;
  loop.requestReconnect();   // existing primitive used by reconnect path
  LOG.warn("resumeAfterHalt: clearing terminal error and restarting I/O loop");
  ```
- WS-only accessors on `QwpWebSocketSender`: `getTotalServerErrors()`, `getDroppedErrorNotifications()`, `getLastTerminalError()`. Match the existing accessor style (see § "Counter accessors" in `qwp-cursor-durability.md`).

### 9. Tests (mirror existing `io.questdb.client.test.cutlass.qwp.client.**` layout)

Per category:
- `ServerErrorSchemaMismatchTest` — `TestWebSocketServer` is augmented to send a `STATUS_SCHEMA_MISMATCH` frame; assert callback fires, FSN span correct, ackedFsn advances (DROP), `flush()` does NOT throw, error counter increments.
- `ServerErrorParseErrorTest` — same with `STATUS_PARSE_ERROR`; assert HALT, terminal latched, next `flush()` throws `LineSenderServerException` with correct `getServerError()`.
- `ServerErrorInternalErrorTest`, `ServerErrorSecurityErrorTest`, `ServerErrorWriteErrorTest` — similar.
- `ServerErrorUnknownStatusTest` — server sends 0xFF; assert `Category.UNKNOWN` + HALT.
- `ServerErrorWsCloseTest` — server sends WS close 1002; assert `Category.PROTOCOL_VIOLATION`, FSN span = unacked window.

Behavioral:
- `ErrorPolicyOverrideTest` — connect string `on_schema_error=halt` flips SCHEMA_MISMATCH default; assert HALT.
- `ErrorPolicyResolverTest` — programmatic resolver returns DROP for everything; assert no terminal latch even on PARSE_ERROR.
- `ErrorInboxOverflowTest` — slow handler + flood of errors; assert `droppedErrorNotifications > 0`, no I/O thread stall.
- `ResumeAfterHaltTest` — induce HALT, call `resumeAfterHalt()`, send fresh batch, assert it lands.
- `FlushAndGetSequenceTest` — assert returned FSN matches the FSN span surfaced in a synthesized rejection.

Hot-path:
- `ErrorPathHotPathBenchmark` (JMH, sibling of `QwpIngressLatencyBenchmark`) — measure per-batch publish latency with no errors before/after the change. Target: zero measurable regression.

Concurrency:
- `ErrorRaceTest` — fire HALT and a producer `flush()` simultaneously, repeat 10k times, assert: producer always sees the latch, never observes "callback fired but flush passed" or vice versa.

### 10. Wire `SenderError.tableName` from existing response state
`WebSocketResponse` already carries `tableNames` (list, see line 224 area). When the response has exactly 1 entry, we have a single-table batch; pass it as `tableName`. Multi-entry → null per spec. Verify the parser populates `tableNames` even on error frames (it might only populate on `STATUS_OK` today — if so, that's a server-side gap and `tableName` will always be null on the error path until both sides extend it).

### 11. README / public-API docs
- Connect-string reference table needs the new keys.
- New `LineSenderBuilder` setters documented.
- Worked example in javadoc of `SenderErrorHandler`: dead-letter to file from an error callback.

## Order of work

Recommended sequence (each step compiles + tests pass independently):

1. Public types (#1) — pure leaves, no risk.
2. ProducerSignal refactor (#2) — internal, behavior-preserving.
3. Default handler + dispatcher + inbox (#5, #6) — wire as plumbing; not yet hooked.
4. Classification + DROP/HALT branches in `ResponseHandler.onBinaryMessage` (#3) — flips behavior.
5. WS close routing (#4).
6. Builder + connect-string knobs (#7).
7. Public methods on `Sender` (#8).
8. Tests (#9), per category as you implement.
9. `tableName` wiring (#10) — last, depends on parser audit.
10. Docs (#11).

## How to run things

```bash
# QWP-only suite (fast, ~30s)
mvn -pl core test -Dtest='io.questdb.client.test.cutlass.qwp.client.**'

# Single test
mvn -pl core test -Dtest=ServerErrorSchemaMismatchTest

# Full core suite (run before merge)
mvn -pl core test

# Hot-path benchmark
mvn -pl core test -Dtest=ErrorPathHotPathBenchmark
```

## Files to know

Existing:
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/WebSocketResponse.java` — status-byte constants, error frame parser (`readFrom`, `getStatusName`, `getErrorMessage`, `getSequence`).
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/CursorWebSocketSendLoop.java` — I/O thread, ResponseHandler at line 706, current terminal-on-error path at line 734.
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketSender.java` — the Sender impl. Holds `connectionGeneration`, `flushPendingRows` is the producer entry point.
- `core/src/main/java/io/questdb/client/Sender.java` — top-level interface + `LineSenderBuilder` + connect-string parser.
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/CursorSendEngine.java` — `engine.acknowledge(fsn)` is the trim hook used by DROP path.

New (per #1):
- `core/src/main/java/io/questdb/client/SenderError.java`
- `core/src/main/java/io/questdb/client/SenderErrorHandler.java`
- `core/src/main/java/io/questdb/client/LineSenderServerException.java`

## Notes on the testing environment

`TestWebSocketServer` (in-process, hand-rolled) does NOT parse QWP wire format — it sees opaque binary frames. To test server error frames we need to extend it with a small "responder" hook: `setNextResponse(byte status, long seq, String msg)` that builds a synthetic error frame and sends it on the next inbound batch. Match the binary layout from `WebSocketResponse.readFrom` (line 256 onward). One such helper covers all category tests.

## Open
None. Ready to implement step 1.
