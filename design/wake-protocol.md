# QWP client wake protocol

The event-driven I/O loop (459f9092) replaced three polls with parks. A park
is only correct if every writer of every variable in its park predicate rings
the parked thread's doorbell (or the predicate is re-checked on a timer). This
file is that enumeration. Any new writer of a listed input must add itself
here and ring the listed doorbell; any new input to a park predicate must add
a row.

Line numbers are as of 3362671f.

## Doorbells

| Doorbell | Mechanism | Lost-wake safety |
|---|---|---|
| I/O loop | `WebSocketClient.wake()` → eventfd/pipe, level-triggered; `awaitReadableOrWake` also returns when `socket.hasBufferedInput()` | stateful: a ring before the wait ends it |
| Segment manager worker | `SegmentManager.wakeWorker()` → `LockSupport.unpark` | permit is kept; worker re-checks after every park |
| Ack waiters | `AckWaiters.signal()` → unpark every registered thread | waiter registers before its first check; permit kept |

## Consumer A — I/O loop idle wait

`CursorWebSocketSendLoop.ioLoop` step 5 (`:2838-2847`) → `awaitIdle` →
`WebSocketClient.awaitReadableOrWake(idleTimeoutMillis())`.

Park predicate: `!didWork && running && !recycledThisIteration && engine.publishStamp() == publishedBeforeWork`.
Blocks until: socket readable, doorbell, TLS user-space input, or next PING deadline.

| Input | Writer | Doorbell / cover | Fixed by |
|---|---|---|---|
| `SegmentRing.publishStamp` | `appendOrFsn` `:1163` (sole writer, after the bytes are published) | `CursorSendEngine.published` → `wakeIoIfParked`; Dekker on volatile `ioParked` (`:505`, set before the stamp re-check) | 5d7f6bab |
| active segment / successor link (loop follows `sendingSegment → successor`, `:3978`) | `appendOrFsn` rotation `:1119-1122` | none of its own. **Invariant: a rotation is always followed by a publish in the same `appendOrFsn` call.** `requestSyncBeforeRotation` and the oversized check both return before any mutation | d4fbb674 |
| socket bytes (ACK, DURABLE_ACK, server frames, EOF) | kernel | `poll(2)`; EOF inside a TLS record now returns `<0` so the readable-at-EOF spin cannot happen | a013e560 |
| TLS plaintext spill / undecrypted records | `JavaTlsClientSocket` | `hasBufferedInput()` checked before `poll` | 3119a0ee |
| `running` | `close()` `:1486` | `unpark` + `wakeIo` + `signalAckProgress` | — |
| `recycledThisIteration` (reconnect in steps 3/4) | `fail` path | step 5 skipped when set | b5e6d529 |
| PING deadlines (idle keepalive, durable-ack prod) | clock | `idleTimeoutMillis()` timed wait | — |
| orphan tail retirement (`trySendOne` `:3929`) | server ACK watermark | socket readable | — |
| `pendingDurable` in durable-ack mode | server | socket; with `durable_ack keepalive <= 0` only inbound traffic flushes it (pre-existing design, not a wake bug) | — |
| engine swap (symbol-dict recycle) | producer | the loop is closed and replaced; old client's doorbell is irrelevant after `swapClient`, which goes straight to `trySendOne` | — |

No open gaps.

## Consumer B — segment manager worker

`SegmentManager.workerLoop` `:1463-1525`. Untimed park unless a timer below is pending; `hasMoreTrimmable` skips the park.

| Input | Writer | Doorbell / cover | Fixed by |
|---|---|---|---|
| `rings` | `register` `:761` | `wakeWorker` (after the entry is visible) | — |
| `rings` | `deregister` `:623` | none needed (nothing to service); `budget.release` wakes refused siblings | — |
| `ring.needsHotSpare()` | rotation `appendOrFsn` `:1142` | `managerWakeup.run()` | — |
| `ring.needsHotSpare()` | high-water backup `appendOrFsn` `:1175` (once per active) | `managerWakeup.run()` | — |
| `ring.needsHotSpare()` | recovered ring opened with no spare | `register` wake | — |
| trimmable head: `ackedFsn >= oldestTrimmableLastSeq` | `acknowledge` `:1023` | wakes when `seq >= oldestTrimmableLastSeq` (volatile threshold, no ring monitor on the ACK path) | 38c5d69d |
| `oldestTrimmableLastSeq` | rotation `:1129`; `releasePinnedSegment` `:1657`; `switchIoPin` `:1878` (pin moved off a segment); worker's own removal `:1865`; recovery `:691` (pre-register) | each non-worker writer that can *increase* trimmability calls `wakeManager`; a pin being *added* cannot | — |
| pending / failed trim | worker | `hasMoreTrimmable` → no park; failed unlink/barrier → `trimRetryAtNanos` timed | 98ac6ae4 |
| budget room (spare refused) | `SegmentBudget.release` / `removeSideFileGauge` | `tryCharge(bytes, onRoom)` registers the manager; `wakeRefused` runs it | 15247811 |
| failed spare create | worker | `provisionRetryDeadlineNanos` timed | — |
| periodic data sync | clock | `nextDataSyncNanos` timed | — |
| sync-before-rotation request | `appendOrFsn` `:1082` | `wakeManager`; the producer then polls at 50 µs in `CursorSendEngine.appendBlocking` `:972` (poll, see Residuals) | — |
| ACK watermark: `ackedFsn > lastPersistedAck` | `acknowledge` `:1023` | wakes iff `before <= persistedAckFsn` (armed); disarmed → `WATERMARK_LAG_NANOS` timed; Dekker at `:1510` | 39ac01e9, 779a99bc, 1304f71d |
| `running` | `close` `:335` | `unpark` | — |
| durability failure | `servicePeriodicSync` | producer throws on next append; sync retry timed | — |

No open gaps. The ACK-watermark row is the one that took four commits: its
state is three variables in two objects under two different disciplines —
`SegmentRing.persistedAckFsn` (volatile, read by the ACK path), and
`RingEntry.ackWakeArmed` + `RingEntry.lastPersistedAck` (manager lock) — with
the invariant "`ackWakeArmed` ⇔ `persistedAckFsn == lastPersistedAck`"
maintained by hand at three sites (`:720`, `:1209-1210`, `:1281-1282`). See
Recommendation.

## Consumer C — ack waiters

`QwpWebSocketSender.awaitAckedFsn` `:1512-1548` and the `close()` drain `:4894-4941`.
Predicate: `getAckedFsn() >= target`, or a terminal: connection error, durability latch, closed.

| Input | Writer | Doorbell / cover | Fixed by |
|---|---|---|---|
| `engine.ackedFsn` | `SegmentRing.acknowledge` `:1026` (server ACKs, orphan-tail self-ack via `engine.acknowledge`) | `signalAckState` → `ackWaiters::signal`, wired on every engine incl. recycled (`:3392`, `:5834`) | cf4f4b79, 50a7f4bc |
| durability latch | `recordDurabilityFailure` `:1278` | `signalAckState` | 50a7f4bc |
| `terminalError` | `recordFatal` `:3009` | `signalAckProgress` | — |
| loop close | `close()` `:1487` | `signalAckProgress` | — |
| `fsnEpochBase` / `cursorEngine` swap / `lastRecycleDurableFsn` | recycle (producer thread) | no signal; a waiter re-reads all three on its next wake, and its target can only be reached by an ACK on the new engine, which signals. The clamp cannot strand a waiter: it is the durable value at recycle time, so any target at or below it was signalled by the ACK that set it | — |
| `recycleFailure` | `latchRecycleBreach` `:5895` (producer thread; plain field `:579`) | **none**, and not in the loop predicate — Open 2 | — |
| `closed` | `close()` `:1741` | **none**, and not in the loop predicate; `loop.close` `:1487` signals but the waiter re-parks on the same predicate — Open 1 | — |

## Fixed after the independent audit (98b00346, next commit)

1. **`awaitAckedFsn` cannot see `close()`.** A monitor thread in
   `awaitAckedFsn(T, 60_000)` while the producer closes with the drain skipped
   (`closeFlushTimeoutMillis <= 0`), timed out, or `T` in abandoned deferred
   frames: `close()` returns, the waiter stays parked for the rest of its
   timeout, then returns `false` instead of throwing "Sender is closed". Fix:
   check `closed` in the loop; `ackWaiters.signal()` right after `closed = true`.
2. **`recycleFailure` invisible to waiters.** Checked only on entry (`:1514`),
   never signalled, non-volatile. Same shape as 1; terminal state, reach
   requires a breached drain contract. Fix: volatile, check in
   `checkAwaitAckedFsnErrors`, signal after the latch.
3. **Interrupt spin in `awaitAckedFsn`** (`:1536-1545`): the flag is never
   cleared, so `parkNanos` returns at once on every pass and a caller with a
   pending interrupt burns a core until the ACK or the deadline. The close
   drain (`:4904`) already consumes-and-restores; do the same here.

## Residuals (known, bounded)

1. Producer backpressure (`appendBlocking` `:972`) polls at 50 µs for the
   spare install (`installHotSpare` `:1469`), the sync-request clear
   (`:1205`) and the durability latch (`:1275`); bounded by the 30 s append
   deadline. Converting it to a park needs those three writers to ring it.
   `maybeBlockForStarvedReset` (`:5972`, 2 s bound) and
   `awaitDeferredEngineClose` (`:5684`, 30 s bound) are the same shape; the
   first could register with `AckWaiters` instead.
2. The worker's doorbell is a one-bit park permit. Any contended j.u.c lock
   the worker blocks on between servicing and `park` (an SLF4J appender lock
   behind its LOG calls is the only candidate found) consumes a pending
   permit. In disk mode the `ackedFsn > lastPersistedAck` timed park covers
   it; in memory mode an ACK that completed a sealed segment during that
   window is trimmed one segment late. Cheap guard if wanted: skip the park
   when `firstTrimmable() != null`.
3. `CursorSendEngine.appendOrFsn(addr, len, spinDeadline)` (`:991`) has no
   callers in `core/src/main`.
4. `BackgroundDrainer` `:1264` polls `POLL_NANOS`; not the foreground path.

## Recommendation

Collapse the ACK-watermark arm state to one variable owned by the worker:
`SegmentRing.ackWakeMark` (volatile long), `Long.MAX_VALUE` = disarmed,
otherwise the FSN the mark was armed at. `acknowledge` wakes iff
`before <= ackWakeMark`; `ackWakeArmed` becomes `mark != MAX_VALUE`;
`lastPersistedAck` stays as plain watermark bookkeeping. That removes the
cross-object invariant that 779a99bc, 1304f71d and 3362671f were each
repairing, and makes the register seed a single store with no ordering story.
Model it in `design/tla` next to `IdleWait` before changing it: IdleWait is
the only wake path on this branch whose bug was found mechanically rather
than by a reviewer probe.
