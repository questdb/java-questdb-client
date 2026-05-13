# Cursor SF — remaining work

Branch: `vi_sf` (off `main`).
Spec: `design/qwp-cursor-durability.md` (decisions 1–14 locked).
Memory: project memory `project_sf_self_sufficient_frames.md` documents the "every frame on disk carries full schema" decision — load-bearing for replay/drainer correctness, do not undo without revisiting.

## What's already done on this branch

Every locked spec decision (1–14), every knob in the spec table, every counter accessor, plus four bugs uncovered along the way. Recent commits, newest first:

- `c25773f` background drainer pool — adopt orphan slots and replay them
- `fa5c838` recovery replays sealed segments from baseSeq, not active (3-bug fix: start-position, ackedFsn-seed, fileGeneration-seed)
- `520231c` cursor frames are self-sufficient — full schemas, full dict
- `b9b6e2f` orphan-slot scanner + .failed sentinel + drain_orphans knob
- `40f9742` initial-connect retry opt-in + replay/attempt counters
- `f152583` slot directory model — sender_id + advisory exclusive .lock
- `8828038` cursor reconnect policy — backoff cap + auth-terminal

Test count: 788 in `io.questdb.client.test.cutlass.qwp.client.**`, 0 failures, 1 skipped (pre-existing).

## TODO

### 1. Multi-host failover (HIGH — needs server access)

The connect-string parses `addr=h1:p1,h2:p2,h3:p3` and stores all hosts in `hosts/ports` lists, but `Sender.build()` only passes `hosts.getQuick(0)` and `ports.getQuick(0)` to `QwpWebSocketSender.connect`. Every reconnect, initial-connect retry, and drainer connect uses the same single host. If host A is down for the per-outage cap, host B is never tried.

**What to change:**
- `QwpWebSocketSender.buildAndConnect()` — currently builds `WebSocketClient` against `host:port` (single string fields). Either:
  - Take a list of (host, port) pairs and round-robin / try-in-order each attempt, OR
  - Take a `Supplier<HostPort>` that yields the next endpoint to try and let the sender / loop round-robin externally.
- The reconnect retry-with-backoff loop in `CursorWebSocketSendLoop.fail()` and the helper `connectWithRetry` should treat each host as one attempt — backoff applies *after* exhausting the host list once.
- `Sender.build()` plumbs the full list down (don't drop hosts 1..n).
- `BackgroundDrainer` inherits the same failover via the `ReconnectFactory` it gets from the sender.
- Auth-terminal still terminal across all hosts (one host returning 401 means config is wrong; trying others is unlikely to help — but spec doesn't pin this; could be argued either way).

**Why server access matters:** to verify failover actually crosses hosts, you want a real multi-server setup (or two `TestWebSocketServer` instances on different ports) with one going down mid-stream and traffic landing on the other. The existing `TestWebSocketServer` is fine for this — but server-side validation that frames arrive intact and dedup-by-messageSequence handles cross-host duplicates is the value-add of the server-side environment.

**Tests to add:**
- 3 hosts, kill the first connected one, expect reconnect to land on host 2 inside the cap.
- All hosts down at startup → init-connect retry exhausts → terminal.
- Auth failure on host 1 — does it fall through to host 2 or stay terminal? (Spec ambiguity; pick one and document.)

### 2. `sf_durability=flush` and `sf_durability=append` (deferred per spec)

Cursor today only supports `sf_durability=memory` (page cache) and rejects `flush`/`append` at build time. Spec line 1001:

```java
if (sfDurability != SfDurability.MEMORY) {
    throw new LineSenderException(... + "is not yet supported (deferred follow-up; use sf_durability=memory)");
}
```

**What to change:**
- `flush` semantics: producer returns from `flush()` only after the engine has called `Files.fsync(fd)` on the active segment up to the just-published cursor position.
- `append` semantics: every `appendBlocking` call fsyncs before returning the FSN.
- Plumb a per-segment `fsync()` method on `MmapSegment` (low-level Files.fsync wrapper exists already).
- Backpressure cost is significant — fsync per-batch (`flush`) is acceptable; fsync per-frame (`append`) is the slow setting.
- Re-enable the rejected paths in `Sender.build()`.

**Tests:**
- After `flush()` returns and a `kill -9` of the JVM, recovery picks up every flushed frame. Hard to write portably; a soft equivalent: after `flush()`, the file's `fsync` was called (instrumented).
- Throughput regression test for `append` mode (10x slowdown is expected).

### 3. Drainer + terminal upgrade error e2e test

Today the drainer's "exhausts cap → drops `.failed`" path is exercised only by unit-level reasoning. There's a synthetic `OrphanScanner.markFailed()` test, but no integration test where:
1. Ghost slot has data,
2. Drainer's connect attempts hit a 401-emitting fixture (or unreachable host),
3. Cap exhausts,
4. `.failed` sentinel ends up in the slot,
5. Future foreground scans skip it.

The blocker today: the drainer inherits its `ReconnectFactory` from the foreground sender, so they share a target host. To exercise the drainer-fails-while-foreground-succeeds path, the drainer needs a configurable `ReconnectFactory` distinct from the foreground's. OR: stand up two servers on different ports and have the foreground point at the live one while the drainer is wired to point at the dead one.

This is small once the multi-host failover work clarifies how connection params flow through the drainer.

### 4. Run the full `core` test suite

Only `io.questdb.client.test.cutlass.qwp.client.**` was run after each commit. A `mvn -pl core test` end-to-end would catch any unrelated regressions in non-QWP code paths. Last run before this branch: presumably clean (the changes are confined to QWP).

### 5. JMH benchmark sanity check

`core/src/test/java/io/questdb/client/test/cutlass/qwp/client/QwpIngressLatencyBenchmark.java` exists. Self-sufficient frames bloat per-batch bytes vs the prior delta-encoded format — the perf delta should be measured. Run, compare to a baseline from before commit `520231c`, document the result.

### 6. Cleanups (LOW)

- `connectionGeneration` retry loop in `QwpWebSocketSender.flushPendingRows` is now dead code — the race it guarded (encode using stale schema state mid-reconnect) can't fire because encode no longer reads `maxSentSchemaId` / `maxSentSymbolId`. Worth ripping out to shrink surface area, but it's harmless as-is (one volatile read per encode).
- `OrphanScanner.hasAnySegmentFile` reports a slot as a candidate orphan if any `.sfa` file exists, including stale empty hot-spares. The drainer no-ops on empty slots (engine.publishedFsn = -1 → ackedFsn already past), but log noise. Filter on actual frame content via a header read.
- README / public-API docs untouched. New connect-string keys, new builder methods, new accessors all have Javadoc but no top-level doc reference.

### 7. Spec coverage check

`design/qwp-cursor-durability.md` decision table claims `max_backoff_millis` is "reuse existing". I added `reconnect_max_backoff_millis` as a new key. If `max_backoff_millis` already exists somewhere in the codebase (likely for HTTP retries elsewhere), align names — either rename mine to match, or document that they're distinct.

## How to run things

```bash
# Compile everything
mvn -pl core compile test-compile

# QWP-only suite (fast, ~30s)
mvn -pl core test -Dtest='io.questdb.client.test.cutlass.qwp.client.**'

# Single test
mvn -pl core test -Dtest=ReconnectTest

# Full core suite
mvn -pl core test
```

Native lib for macOS-aarch64 is already in the repo
(`core/src/main/resources/io/questdb/client/bin/darwin-aarch64/libquestdb.dylib`);
no rebuild needed unless touching `Files.java` natives.

## Files to know

- `core/src/main/java/io/questdb/client/Sender.java` — top-level builder + connect-string parser. Scroll to `LineSenderBuilder` (line ~571) for the builder, `build()` for the WS branch (line ~989), and the connect-string switch (line ~2330).
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketSender.java` — main sender. `buildAndConnect()` is the host:port-bound connect path (line ~1408 area).
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/CursorWebSocketSendLoop.java` — I/O thread, reconnect retry loop, replay positioning.
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/CursorSendEngine.java` — engine + slot lock + recovery.
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/BackgroundDrainer.java` and `BackgroundDrainerPool.java` — orphan adoption.
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/OrphanScanner.java` and `SlotLock.java` — slot model.

## Notes on the testing environment

The QWP test suite uses `TestWebSocketServer` (in-process, hand-rolled WS server) for everything. It receives binary frames as opaque bytes — does NOT parse the QWP wire format. So tests assert wire behavior (frame counts, byte equivalence, connection lifecycle) but cannot assert server-side semantic correctness (does the server accept these schemas? are messageSequence dedups working?). Validating the wire-protocol bytes against a real QuestDB server is the part that needs the server-code repo.
