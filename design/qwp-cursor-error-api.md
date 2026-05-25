# QWP cursor SF — server error API spec

Status: **draft v1**, follow-on to `qwp-cursor-durability.md`. Targets branch `vi_sf`.

## Goals
- **Surface server-side rejections** (schema mismatch, parse, security, write, internal) to user code without compromising the async `flush()` contract.
- **Match the wire**: client categories align 1:1 with the stable status bytes already shipped by the server (`WebSocketResponse` + `QwpProcessorState` mapping). No client-side category the wire can't actually distinguish.
- **Zero hot-path cost** in the no-error case. One volatile load per batch boundary, no allocations, no locks.
- **Two surfacing paths**: builder-registered `errorHandler` for async dead-lettering, typed exception on next API call for connect-string-only users. Both deliver the same `SenderError` payload.
- **Loud defaults** — silence is forbidden. The default handler logs ERROR for HALT and WARN for DROP, with category + FSN span + table.

## Non-goals (this spec)
- Retryable / transient distinction. Server does not ship a retryable bit today; everything potentially transient is folded into `STATUS_INTERNAL_ERROR (0x06)` / `STATUS_WRITE_ERROR (0x09)`. The `RETRY_TRANSIENT` policy is reserved but not implemented; revisit when the server splits codes.
- Per-table attribution in multi-table batches. Server NACKs the whole batch atomically; `tableName` is best-effort and may be null.
- Per-row attribution (which row in the batch was bad). Out of scope until the wire format grows a row index field.

## Wire anchor (server-side, already shipped)
Server error frame layout (binary, **not** a WS close frame):
```
1 byte  status
8 byte  messageSequence (LE) — server's per-frame counter, mirrored back
2 byte  message length    (LE)
≤1024 byte UTF-8 message
```
Source: `QwpWebSocketUpgradeProcessor.java:895-956` (server repo).

Stable status bytes (`WebSocketResponse.java:74-83`, mirrored from server `QwpConstants.java:174-190`):

| Code | Constant | Server triggers |
|---|---|---|
| 0x00 | `STATUS_OK` | accepted |
| 0x02 | `STATUS_DURABLE_ACK` | post-fsync ack (per-table) |
| 0x03 | `STATUS_SCHEMA_MISMATCH` | `QwpParseException.SCHEMA_MISMATCH` |
| 0x05 | `STATUS_PARSE_ERROR` | other `QwpParseException` |
| 0x06 | `STATUS_INTERNAL_ERROR` | `CairoException.isCritical()` + catch-all `Throwable` |
| 0x08 | `STATUS_SECURITY_ERROR` | `CairoException.isAuthorizationError()` |
| 0x09 | `STATUS_WRITE_ERROR` | non-critical Cairo errors / table not accepting writes |

WS-level violations (fragmented binary, text frame, oversized payload, malformed header) come as **WebSocket close frames** with codes PROTOCOL_ERROR / UNSUPPORTED_DATA / MESSAGE_TOO_BIG, not QWP error frames. These need to be funnelled into the same surface.

## Client `Category` enum

```java
public enum Category {
    SCHEMA_MISMATCH,    // 0x03
    PARSE_ERROR,        // 0x05  — QWP-level malformed payload (likely client bug)
    INTERNAL_ERROR,     // 0x06  — catch-all server fault; bundles resource/transient
    SECURITY_ERROR,     // 0x08  — auth / ACL
    WRITE_ERROR,        // 0x09  — table not accepting writes; bundles rate-limit-style
    PROTOCOL_VIOLATION, // n/a   — WS-level close frame
    UNKNOWN             // forward-compat for any new server status byte
}
```

Forward-compat: unknown bytes map to `UNKNOWN`, the raw byte is preserved on `SenderError.serverStatusByte` for debugging.

## `Policy` enum

```java
public enum Policy {
    DROP_AND_CONTINUE, // ackedFsn advances past the bad span; loop keeps draining
    HALT               // terminalError latched; next producer API call throws
}
```

`RETRY_TRANSIENT` is **not** implemented — the wire has no retryable bit to drive it. The enum is binary today; expand later.

## Default category → policy

| Category | Default | Reasoning |
|---|---|---|
| SCHEMA_MISMATCH | DROP_AND_CONTINUE | Replay reproduces the same rejection; halting blocks unrelated tables on the same connection. |
| PARSE_ERROR | HALT | Almost certainly a client bug (we sent malformed bytes). Halt preserves the on-disk frames for postmortem. |
| INTERNAL_ERROR | HALT | Catch-all server fault; conservatively halt — could be transient, could be poison. Without a retryable bit we cannot tell. |
| SECURITY_ERROR | HALT | Misconfig; loud failure wanted. |
| WRITE_ERROR | DROP_AND_CONTINUE | "Non-critical Cairo errors / table not accepting writes" — per-batch in character. Halting blocks other tables. **Debatable; revisit once server splits 0x09 into transient vs permanent.** |
| PROTOCOL_VIOLATION | HALT (forced) | Connection is gone — no choice. |
| UNKNOWN | HALT | Never silently drop something we don't understand. |

User overrides via builder (`errorPolicy(Category, Policy)` or full `errorPolicyResolver`) and via connect-string knobs (see below).

## `SenderError` (public, immutable)

```java
/**
 * @param appliedPolicy  what the loop actually did
 * @param serverStatusByte  raw byte (0x03/0x05/...); -1 for PROTOCOL_VIOLATION
 * @param serverMessage  ≤1024 UTF-8 from frame, or WS close reason
 * @param messageSequence  server's per-frame seq (mirrors what server logs); -1 for PROTOCOL_VIOLATION
 * @param fromFsn  client-side FSN span — load-bearing for correlation
 * @param toFsn  inclusive
 * @param tableName  best-effort; null if multi-table batch
 * @param detectedAtNanos  System.nanoTime() at I/O thread receipt */
public record SenderError(Category category, Policy appliedPolicy, int serverStatusByte, String serverMessage,
                          long messageSequence, long fromFsn, long toFsn, String tableName, long detectedAtNanos) {
    // accessors only; no mutation
}
```

**Load-bearing fields**: `[fromFsn, toFsn]` and `appliedPolicy`. The FSN span is what the user joins to their producer-side log to identify the rejected data. `appliedPolicy` tells the user whether the data was dropped (must dead-letter) or halted (will be re-throw on next call) or — when retry lands — observed only.

`messageSequence` is preserved for cross-team debugging (server-side ops think in `messageSequence`).

## Mechanism — surfacing paths

### Path 1: async callback
- Builder-time `errorHandler(SenderErrorHandler)`. Default impl: ERROR log for HALT, WARN log for DROP, both with `category`, `[fromFsn, toFsn]`, `tableName`, `serverMessage`. Bumps a counter.
- I/O thread, on rejection frame, builds `SenderError` and `errorInbox.offer(err)` on a bounded SPSC queue.
- Bounded inbox: default cap 256. Overflow → drop the notification, bump `droppedErrorNotifications` counter, never block the I/O thread.
- Dispatcher daemon thread (`QwpSender-error-dispatcher-<id>`, lazy-start on first error) does `take()` + invokes user handler; catches `Throwable` so a buggy handler can't poison the dispatcher.

### Path 2: producer-side typed throw
- Single volatile field on the existing producer-signal object (the one that already holds `connectionGeneration`):
  ```java
  @Contended
  final class ProducerSignal {
      volatile long connectionGeneration;   // existing
      volatile SenderError terminalError;   // new
  }
  ```
- I/O thread, on a HALT-policy error (or PROTOCOL_VIOLATION, or UNKNOWN), writes `signal.terminalError = err` **before** `errorInbox.offer(err)`. Ordering matters: producer must see the latch no later than the dispatcher delivers, otherwise a `flush()` post-callback could still pass.
- Producer: `flushPendingRows` reads `signal.terminalError` once at batch entry (same cache line as `connectionGeneration` — single load-acquire). If non-null, throws `LineSenderServerException` carrying the `SenderError`.

### Producer hot path
- Per `at()` / `column*()`: zero change.
- Per batch boundary (`flush()` or implicit batch publish): one volatile load that piggybacks on the existing `connectionGeneration` read. Same cache line. In steady state the line stays in producer L1; the I/O thread does not write to it on the ACK path.

### I/O thread allocation
- Per ACK (common case): zero change.
- Per rejection: one `SenderError`, one queue node. NACK rate is bounded by batch rate, not row rate, and is rare in steady state. Pooling not justified.

## WS close frames

WS-level violations from `WebSocketCloseCode`-style paths (PROTOCOL_ERROR, UNSUPPORTED_DATA, MESSAGE_TOO_BIG, generic close-with-reason) surface as a `SenderError` with:
- `category = PROTOCOL_VIOLATION`
- `serverStatusByte = -1`
- `messageSequence = -1`
- `serverMessage = "ws-close[<code>]: <reason>"` or whatever `onClose(code, reason)` was given
- `appliedPolicy = HALT` (always — the connection is gone)
- FSN span = `[engine.ackedFsn() + 1, engine.publishedFsn()]` (the unacked window at close time)

This routes the existing `ResponseHandler.onClose` through the new sink instead of just calling `fail(...)`.

## Configuration knobs (connect string)

| Key | Default | Values | Notes |
|---|---|---|---|
| `on_server_error` | `auto` | `auto` \| `halt` \| `drop` | global default; `auto` uses per-category table |
| `on_schema_error` | `drop` | `halt` \| `drop` | overrides global for SCHEMA_MISMATCH |
| `on_parse_error` | `halt` | `halt` \| `drop` | |
| `on_internal_error` | `halt` | `halt` \| `drop` | |
| `on_security_error` | `halt` | `halt` \| `drop` | |
| `on_write_error` | `drop` | `halt` \| `drop` | |
| `error_inbox_capacity` | `256` | int ≥ 16 | bounded SPSC capacity |

PROTOCOL_VIOLATION and UNKNOWN are not user-configurable — both forced HALT.

Per-category knob takes precedence over `on_server_error` if both are set.

## Builder additions (`LineSenderBuilder`)

```java
.errorHandler(SenderErrorHandler)              // default: log ERROR/WARN + counter
.errorPolicy(Category, Policy)                 // overrides for one category
.errorPolicyResolver(SenderError -> Policy)    // full programmatic control; takes precedence
.errorInboxCapacity(int)
```

## Public API surface

- `SenderError` — public, final, immutable, in `io.questdb.client` package.
- `SenderError.Category`, `SenderError.Policy` — public enums on `SenderError`.
- `SenderErrorHandler` — `@FunctionalInterface` with `void onError(SenderError)`.
- `LineSenderServerException extends LineSenderException` — `getServerError(): SenderError` accessor.
- `Sender.flushAndGetSequence(): long` — returns FSN published; existing `flush()` kept verbatim. The returned FSN is the user's correlation handle for matching against `SenderError.fromFsn`.
- `Sender.resumeAfterHalt()` — opt-in escape hatch: clears `terminalError`, restarts I/O loop reconnect, logs WARN. No auto-resume.
- WS-only counter accessors on `QwpWebSocketSender`:
  - `getTotalServerErrors(): long`
  - `getDroppedErrorNotifications(): long`
  - `getLastTerminalError(): SenderError` (snapshot; null if none).

## Interaction with existing reconnect / ack paths

- `CursorWebSocketSendLoop.ResponseHandler.onBinaryMessage` (line 712 onward, current branch): currently routes any non-`STATUS_OK` to `recordFatal(...)`, always terminal. New behavior: classify by status byte → category, resolve policy, build `SenderError`, then either:
  - `DROP_AND_CONTINUE`: call `engine.acknowledge(fsnAtZero + wireSeq)` to advance past the bad span (the server already rejected it; we're not going to land it), inbox the error, continue.
  - `HALT`: write `terminalError`, inbox the error, then call `recordFatal(...)` to break the loop. The `LineSenderException` raised by `recordFatal` carries the `SenderError` via `LineSenderServerException`.
- `STATUS_DURABLE_ACK` (0x02) is unchanged — it's an upload-confirmation, not an error, and the existing handler already keeps it separate.
- Reconnect budget exhaustion remains terminal (existing behavior). Surfaces as a synthesized `SenderError` with `category = PROTOCOL_VIOLATION` and FSN span = unacked window at giveup time.
- Auth-terminal on reconnect (existing) is preserved as `category = SECURITY_ERROR` for consistency.

## DROP_AND_CONTINUE: what about the disk?

When the loop drops a rejected batch, the on-disk segment for that FSN range becomes garbage from the server's perspective — but the bytes are still there. Trim happens via the existing `engine.acknowledge(...)` → `SegmentManager.trim` path. Calling `acknowledge` with the rejected wireSeq advances `ackedFsn` past the bad batch, which trims it from disk on the next maintenance pass.

This means the dropped bytes are **lost forever** from the sender's perspective. The user must dead-letter via `errorHandler` if they want a record. This is by design: SF preserves data until the server acks; once the server has explicitly rejected, the data is no longer the sender's responsibility.

## Decisions locked
1. ✅ 6 wire-aligned categories + `PROTOCOL_VIOLATION` + `UNKNOWN`. No abstracted-up category not distinguishable on the wire.
2. ✅ Two policies only: `DROP_AND_CONTINUE`, `HALT`. `RETRY_TRANSIENT` reserved for post-server-split.
3. ✅ Defaults per the table above. WRITE_ERROR is DROP (debatable; revisit when server splits).
4. ✅ `SenderError` is public API, immutable, carries both `messageSequence` and `[fromFsn, toFsn]`.
5. ✅ Multi-table batches: `tableName` may be null; user correlates via FSN span.
6. ✅ WS close frames surface as `PROTOCOL_VIOLATION` with `serverStatusByte = -1`, `messageSequence = -1`, always HALT.
7. ✅ Connect string carries policy knobs + inbox capacity. Callbacks require builder. Typed exception covers connect-string-only users.
8. ✅ Producer hot path: zero allocations, one volatile load per batch (piggybacks `connectionGeneration` cache line).
9. ✅ I/O thread never invokes user code. Bounded inbox + lazy-start dispatcher daemon. Inbox overflow drops + counts.
10. ✅ Default handler is loud (ERROR for HALT, WARN for DROP). Silence forbidden.
11. ✅ Counters and `getLastTerminalError()` accessor for ops visibility.
12. ✅ `resumeAfterHalt()` is opt-in escape hatch; never auto-resume.
13. ✅ `DROP_AND_CONTINUE` advances `ackedFsn` past the rejected span; data is dropped from disk via existing trim path.
14. ✅ `flush()` signature unchanged. New `flushAndGetSequence()` returns FSN for user-side correlation.

## Server-side follow-ups (track separately, not blocking client work)
1. Split `0x06` and `0x09` to add explicit `RESOURCE_EXHAUSTED`, `RATE_LIMITED`, `TRANSIENT` codes — unblocks `RETRY_TRANSIENT` client policy.
2. Or: add an explicit retryable bit (1 reserved byte in the error frame) — alternative to (1).
3. Per-table attribution in multi-table batch errors — extend the error frame with an optional table index (`-1` = batch-level).
4. Document whether rejected `messageSequence` values count toward the server's dedup window or are excluded.

## Open
None. Ready to implement.
