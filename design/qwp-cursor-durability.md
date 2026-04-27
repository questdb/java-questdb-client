# QWP WebSocket sender — durability & reconnect spec

Status: **draft v3**, working notes for the cursor SF refactor on `vi_sf`.

## Goals
- **Reduce data loss.** SF mode preserves every batch the producer has handed to the engine until the server has ACK'd it, surviving JVM crashes, process restarts, and transient network outages.
- Memory mode (`ws::addr=...;` no `sf_dir`) is reliable enough for typical use under transient network blips.
- SF mode (`ws::...;sf_dir=...`) survives process restarts and JVM crashes; disk does not grow under steady-state traffic (only ACK'd data is trimmed).
- Failure surfaces are loud and distinguishable: "server slow" ≠ "server unreachable" ≠ "data refused".

## Modes
| | Memory | SF |
|---|---|---|
| Storage | malloc'd ring | mmap'd files under sender's slot dir |
| Cap | `sf_max_total_bytes` (default 128 MiB) | `sf_max_total_bytes` (default 10 GiB) |
| Cap-full behavior | Producer's `flush()`/`at()` blocks up to `sf_append_deadline_millis`, then throws | Same |
| Survives JVM exit | No | Yes (recovered on next startup; orphans optionally drained by another sender) |
| Reconnect retries | Yes | Yes |

## flush() contract
- Encodes accumulated rows into the cursor engine.
- Returns when data is **published into the engine** (in-RAM for memory mode, on-disk for SF). **Never** waits for server ACK — ACKs are asynchronous and not every flush correlates to one.
- The I/O loop drains in the background and retries on reconnect until either ACK or the cap forces backpressure → hard error to the producer.

## close() contract
- One knob: `close_flush_timeout_millis`.
  - **Default `5000`**: close() blocks waiting for `engine.ackedFsn() >= engine.publishedFsn()` (server ACK'd everything published) for up to 5 s, then logs WARN and proceeds with stop.
  - **`0` or `-1`**: close() does not flush at all — fast exit. Pending data is lost (memory mode) or recovered by next sender (SF mode).
  - Any other positive value: that timeout in millis.

## Reconnect policy (both modes)
- I/O loop catches any wire error (send fail, recv fail, server close, ACK timeout). Logs WARN and enters reconnect.
- Backoff: exponential with jitter. Reuse `LineSenderBuilder.maxBackoffMillis` (initial 100 ms, cap as configured).
- **Budget: `reconnect_max_duration_millis`** — per-outage time cap (resets on each successful reconnect). Once total elapsed time since the first failure of *this* outage exceeds the cap, the I/O loop gives up.
  - **Default 300_000 ms (5 min).** Long enough to ride out most server restarts and brief outages where the cause needs investigation; short enough that a permanently-gone server surfaces within minutes.
- **Auth failure on reconnect (401, 403, non-101 upgrade reject) is terminal** — don't burn the retry budget on errors that won't fix themselves.
- On successful reconnect: I/O loop restarts `nextWireSeq=0`, sets `fsnAtZero = engine.ackedFsn() + 1`, walks segments forward from there, and replays. Producer thread is signaled (volatile counter bump) so the next encoded batch carries full schema definitions instead of refs.
- On budget exhaustion: connection error recorded → next user-thread API call throws.

### Initial connect
- **Default: terminal.** Initial-connect failures (DNS, refused, bad auth, version mismatch) usually mean misconfig; throw immediately so the user sees the error, not a 5-minute hang.
- **Opt-in: `initial_connect_retry=true`** uses the same backoff + `reconnect_max_duration_millis` cap as reconnect. Useful for "publisher comes up before server" scenarios (k8s ordering, dev environments).

### Logging cadence
- WARN at first failure of an outage: `"disconnected from <addr>, reconnecting"`.
- WARN throttled to once per `BACKPRESSURE_LOG_THROTTLE_NANOS` (5 s) during the retry storm — not one per backoff sleep, otherwise a 5-min outage at 100 ms backoff = 3000 lines.
- INFO on each successful reconnect: `"reconnected to <addr> after <Xms>, <Y> attempts"`.
- ERROR on budget exhaustion: `"giving up reconnecting to <addr> after <Xs>, <Y> attempts"`.

## Backpressure semantics
- Engine cap full → `appendBlocking` spins for `sf_append_deadline_millis` (default 30 s) → throws.
- Error message must distinguish:
  - `"backpressured for Xms — wire path is not draining (server slow?)"` (engine published, but server hasn't ACKed)
  - `"backpressured for Xms — Y reconnect attempts in progress (server unreachable since Z)"` (the I/O loop is in retry-backoff)

## Schema state on reconnect
- Single volatile counter, single writer (I/O thread), shared across two roles:
  ```java
  private volatile long connectionGeneration;  // bumped by I/O loop on every successful reconnect AND on initial recovery from disk
  ```
- Producer's `flushPendingRows` does:
  ```java
  int retries = 0;
  while (true) {
      long genBefore = connectionGeneration;
      if (genBefore != lastSeenGeneration) {
          resetSchemaStateForNewConnection();
          lastSeenGeneration = genBefore;
      }
      encoder.beginMessage(...); /* encode all tables */
      int messageSize = encoder.finishMessage();
      if (connectionGeneration == genBefore) break;   // common case
      if (++retries >= MAX_SCHEMA_RACE_RETRIES /* =10 */) throw new LineSenderException("schema-reset race exceeded retry limit");
      // gen advanced mid-encode → bytes are poisoned, discard + loop.
      // Table buffers are NOT reset until after this loop, so source rows are intact.
  }
  ```
- **On initial open with on-disk recovery** (SF mode, non-empty slot): `connectionGeneration` starts at 1, not 0. Recovered FSNs were never seen by *this* server connection, so the first batch must publish full schemas.

## Slot directory model

**`sf_dir` is a parent (group root)**, not a slot. The actual slot is `<sf_dir>/<sender_id>/`.

### Identity
- **`sender_id` defaults to `"default"`.** Single-sender users get zero-config: their slot is `<sf_dir>/default/`.
- **Multi-sender users must set `sender_id` explicitly.** Two senders trying to use the default name will collide on the lock — surfaced loudly as `"sf slot already in use by PID X"`.
- The slot dir holds segments + `.lock` (advisory exclusive `FileChannel.tryLock`).
- Lock released on `engine.close()` or OS-level process exit (kernel releases `fcntl`/`LockFileEx` locks automatically on crash).

### Foreground sender
- Locks `<sf_dir>/<sender_id>/.lock`.
- Recovers segments via `SegmentRing.openExisting`. Recovery is per-slot, in baseSeq order — preserves publishing order trivially.
- Seeds `SegmentManager.fileGeneration` to `max(existing sf-<gen>.sfa hex) + 1` to avoid filename collisions with recovered files.

### Background drainers (orphan adoption)
- **Opt-in: `drain_orphans=true`** (default false).
- At foreground sender startup, scan `<sf_dir>/*/` for sibling slots that are (a) unlocked and (b) contain unacked segments.
- For each orphan, spawn a background drainer:
  - Locks the orphan's `.lock`
  - Opens its own `WebSocketClient` (separate connection from the foreground sender)
  - Recovers segments, drains them in baseSeq order
  - Releases lock and exits when the slot is fully ACK'd and empty
- **Drain-only**: no user appends, no public API for writing.
- **Cap concurrent drainers: `max_background_drainers=4`** (default). Excess orphans are queued and started as earlier drainers finish.
- **Drain failure policy**: drainer's reconnect cap exhausts, or auth fails, or segments are corrupt → drainer drops a `.failed` sentinel in the slot, releases the lock, exits. Future foreground startups skip slots with `.failed` until the user clears the sentinel manually. Bounded automatic retry, then human-in-the-loop.
- **No automatic cleanup of empty slot dirs.** Goal is data preservation; only ACK'd data is trimmed (within a slot, by the segment manager). Empty slot dirs are cheap and stay forever unless the user removes them.

### Visibility
- WS-only accessor `sender.getBackgroundDrainers()` returns a snapshot list: `{dir, framesPending, framesAcked, lastError, isFailed}`.
- Lets users observe orphan-drain progress without parsing logs.

### Per-sender threading cost
- Each engine (foreground + each background drainer) has its own `SegmentManager`. That's 1 manager thread + 1 I/O thread per engine. With `max_background_drainers=4`, worst case is 1 (foreground) + 4 (drainers) = 5 engines = 10 threads + 5 sockets per `Sender.fromConfig` call. Acceptable for typical deployments; users with hundreds of senders per JVM should set `max_background_drainers` low.

## Configuration knobs (connect string)
| Key | Default | Mode | Status |
|---|---|---|---|
| `sf_dir` | unset | both | existing (semantics: now a parent dir) |
| `sender_id` | `"default"` | SF | **NEW** |
| `sf_max_bytes` | 4 MiB | both | existing |
| `sf_max_total_bytes` | 128 MiB / 10 GiB | both | existing |
| `sf_durability` | `memory` | SF | existing (`flush`/`append` reserved) |
| `sf_append_deadline_millis` | 30000 | both | **NEW** (currently a constant) |
| `reconnect_max_duration_millis` | 300000 | both | **NEW** |
| `reconnect_initial_backoff_millis` | 100 | both | **NEW** |
| `max_backoff_millis` | already exists | both | reuse existing |
| `initial_connect_retry` | `false` | both | **NEW** |
| `close_flush_timeout_millis` | 5000 (0/-1 = fast close) | both | **NEW** |
| `drain_orphans` | `false` | SF | **NEW** |
| `max_background_drainers` | 4 | SF | **NEW** |

Each new knob also gets a `LineSenderBuilder` setter.

## Counter accessors (WS-only, on QwpWebSocketSender)
- `getTotalBackpressureStalls()` — already exists
- `getTotalReconnectAttempts()`
- `getTotalReconnectsSucceeded()`
- `getTotalFramesReplayed()`
- `getBackgroundDrainers()` — list of `{dir, framesPending, framesAcked, lastError, isFailed}`

## Stated assumptions (server contract)
- Server **dedups** replayed batches by `messageSequence`. Replay-after-reconnect produces duplicates; without server-side dedup, every reconnect = double-write. Legacy code already relied on this; the new design continues to.
- Server's dedup window must be ≥ a sender's `sf_max_total_bytes` worth of FSNs (else replay = double-write under sustained outage + full cap).
- Coordination/testing of the recovery + dedup contract is **outside this repo's scope**.

## Self-sufficient frames (locked 2026-04-27)
Every frame written through the cursor SF path **must carry its full schema definition and the complete symbol-dictionary delta from id 0**. No schema-by-id refs, no incremental delta-dicts. The bytes survive process restart and replay against fresh server connections (post-reconnect, post-restart, drainer adopting an orphan slot) — frames with refs to IDs the new server has never seen are unrecoverable. Costs more bytes per batch; pays for replay correctness across every recovery path. Producer-side `maxSentSchemaId` / `maxSentSymbolId` retention is treated as a no-op for the cursor path; the encode call always passes `confirmedMaxId=-1` and `useSchemaRef=false`.

## Decisions locked
1. ✅ flush() never waits for ACK (ACKs are async).
2. ✅ Reconnect cap is per-outage time-based, default 300s.
3. ✅ close() drains by default with 5s timeout; `close_flush_timeout_millis=0|-1` opts out for fast close.
4. ✅ Schema-reset is also fired on disk recovery (recovered state == post-reconnect state).
5. ✅ Encode-mid-reconnect race closed via single volatile `connectionGeneration` counter + retry loop in `flushPendingRows`.
6. ✅ Slot dir model: `sf_dir` is parent; per-sender slots `<sf_dir>/<sender_id>/`; default `sender_id="default"`.
7. ✅ Orphan adoption is opt-in (`drain_orphans=true`); foreground sender spawns background drainers per orphan, capped at `max_background_drainers`.
8. ✅ Drain failure → `.failed` sentinel; bounded retry + human-in-the-loop.
9. ✅ Initial connect terminal by default; opt-in retry via `initial_connect_retry=true`.
10. ✅ Auth failures (401/403/non-101) terminal even on reconnect.
11. ✅ Logging: WARN on outage entry/exit-attempt, INFO on reconnect success, ERROR on budget exhaustion; throttled.
12. ✅ Counters and orphan-drainer visibility on `QwpWebSocketSender` (WS-only).
13. ✅ No automatic cleanup of empty slot dirs — preserve goal of data-loss reduction.
14. ✅ Frames on disk are self-sufficient — every frame carries its full schema + full symbol-dict delta from id 0; refs forbidden.

## Open
None. Ready to implement.
