# QWP schema capability gating at the replay head

Date: 2026-09-29

Status: superseded on 2026-09-29; not implemented. Schema-validated rows now use ordinary QWP
data frames, so the store-and-forward queue holds one frame format and replay needs no
capability gating. See "Why this proposal was superseded" below and proposal 2 in
[schema-aware sender contract simplification](schema-aware-sender-contract-simplification.md).

## Why this proposal was superseded

A prototype of the rule in this document showed two defects.

- Deriving the reconnect requirement from the frame at `ackedFsn + 1` livelocks. `trySendOne()`
  reaches the schema frame before the ACKs of the legacy prefix arrive, the recycle drops those
  ACKs, and the replay head stays legacy. Against a legacy-only endpoint the loop reconnected
  about 3,200 times per second and re-sent the prefix on every cycle.
- The transaction boundary acceptance gate fails against 9.4.x servers, which acknowledge
  `FLAG_DEFER_COMMIT` frames individually. Servers from 10.0.0 withhold those ACKs.

Both defects come from keeping two frame families in one queue. The server ingested both
families identically and used the identity only to decide schema feedback, which it can also
decide from the schema version it last reported on the connection. Removing the schema frame
family removes the gating problem instead of relocating it.

The rest of this document is kept as the record of the rejected design.

This is a narrow follow-up to
[schema-aware sender contract simplification](schema-aware-sender-contract-simplification.md) and the implementation
history in the [schema-aware sender journal](schema-aware-sender-journal.md). It addresses capability selection for
already-encoded frames; it does not reopen the schema conversion or batch-snapshot contracts.

## Decision

Remove the engine-wide `maxSchemaFsn` / `requiresSchema()` watermark. Decide whether a connection must support
schema framing from the first published data frame that the send loop would transmit or replay.

The central rule is:

> Schema capability belongs to the frame at the transmission cursor, not to producer intent or to every frame
> anywhere in the store-and-forward queue.

This keeps mixed legacy/schema queues and the existing `AUTO`, `STRICT`, and `OFF` row-encoding policies. It changes
connection selection so a legacy peer may drain a legacy prefix. When a schema frame reaches the replay head, the
send loop reconnects to a schema-capable peer before transmitting that frame.

Do not move the current speculative latch into each append retry. That narrows the race window but preserves the
incorrect abstraction and can still recycle a healthy legacy connection for a frame that was never published.

## Scope

This proposal changes client-side capability gating for live sending, reconnect, and recovered store-and-forward
replay. It does not change:

- the QWP frame format or `FLAG_SCHEMA` value;
- row conversion, schema lookup, or schema binding;
- the rule that one pending batch uses one wire contract;
- frame sequence numbers, durable ACK semantics, or unchanged-byte replay;
- terminal versus retriable NACK policy;
- the configured meaning of `schema_mode=auto|strict|off` for newly encoded rows.

The code is unreleased, so the implementation can remove the current watermark contract and its tests directly.
No compatibility shim for `requiresSchema()` is warranted.

## Current design and source of complexity

`QwpWebSocketSender.bindingForEffectiveWrite()` may select either the legacy or schema contract at a batch boundary.
Consequently, one store-and-forward queue may legitimately contain both frame families.

The current implementation summarizes that heterogeneous queue with one global value:

- `CursorSendEngine.maxSchemaFsn` records the newest schema-framed FSN.
- `CursorSendEngine.requiresSchema()` returns `maxSchemaFsn > ackedFsn`.
- append paths publish the predicted next FSN before calling `SegmentRing.appendOrFsn()` so the I/O thread cannot
  observe a schema frame before it observes the capability requirement;
- failed and backpressured appends must restore the previous value;
- recovery scans frame flags to reconstruct `maxSchemaFsn`;
- foreground reconnect, background draining, initial connection, client swap, and `trySendOne()` all consult the
  global predicate.

This conflates three different states:

1. the producer intends to append a schema frame;
2. a schema frame has been completely published;
3. the next frame sent on a connection requires schema support.

Only the third state is relevant to connection compatibility. The first two are not sufficient: an append may fail,
and a published schema frame may sit behind a sendable legacy prefix.

### Why backpressure exposes the problem

`MmapSegment.tryAppend()` writes the frame envelope and payload, then advances the volatile `publishedCursor` last.
That cursor is already the safe producer-to-consumer publication barrier.

The schema watermark adds a second publication protocol. Because it must become visible before `publishedCursor`, it
is set speculatively. During append backpressure, the speculative value can remain visible for the entire producer
wait, up to the append deadline. The send loop can interpret it as real queued work, reject its current legacy peer,
and enter reconnect even though no schema frame was published.

Transactional rollback fixes the stale value after append failure. It cannot prevent the false value from affecting
the I/O thread while the append is waiting.

## Proposed design

### Frame classification

Add one small frame-classification helper on the consumer side:

```text
frameRequiresSchema(payloadAddress, payloadLength):
    return payloadLength >= QWP_HEADER_SIZE
        and payload magic is QWP
        and FLAG_SCHEMA is set
```

Call it only after the send loop has established that the complete frame lies below the segment's published offset.
The volatile `publishedCursor` read then guarantees visibility of the frame bytes. No producer-side latch or second
memory-ordering protocol is required.

Synthetic or foreign test frames remain non-schema frames unless they have a valid QWP header and magic, matching the
defensive classification already used by symbol-dictionary handling.

### Normal sending

`trySendOne()` should:

1. locate the next published frame;
2. validate its envelope and complete payload boundary;
3. inspect that frame's QWP flags;
4. if the frame requires schema and the installed client did not negotiate schema, recycle the connection without
   advancing `sendOffset` or the wire sequence;
5. otherwise send the frame normally.

Remove the early global `requiresSchema()` check. When no frame is published, there is no queued capability
requirement to act on.

### Reconnect and replay

Reconnect replays from `ackedFsn + 1`, not from the frame that was merely next before the connection failed.
Therefore endpoint selection must derive its requirement from the first published replay frame at that position.

The reconnect sequence becomes:

1. retire a recoverable orphan tail if applicable;
2. calculate `replayStart = ackedFsn + 1`;
3. position or peek the frame cursor at `replayStart`;
4. derive `reconnectRequiresSchema` from that published frame;
5. require schema negotiation for that connection attempt when `reconnectRequiresSchema` is true;
6. install the compatible client, establish dictionary catch-up state, and replay from `replayStart`.

`reconnectRequiresSchema` is local to the I/O loop and the current connection attempt. It is derived state, not a
producer-owned field. If the replay head changes because ACK progress was accepted before reconnect begins, derive it
again from the new `ackedFsn + 1` position.

If the queue is empty when an AUTO or OFF reconnect begins, no backlog capability is required. A concurrently
published schema frame is still safe: after publication, `trySendOne()` sees its flag and recycles an incompatible
legacy connection before sending it. At worst this costs one additional connection attempt.

### Recovered queues and background draining

Recovery already validates every frame and rebuilds other replay state. It no longer needs to retain the maximum
schema FSN. After the send cursor is positioned at the first unacknowledged frame, that frame controls capability.

The background drainer follows the same rule. It must not reject a legacy peer merely because a schema frame exists
later in the queue. It may drain the legacy prefix, then retain the schema frame and retry schema-capable endpoints
when that frame becomes the replay head.

### Schema modes

The row-encoding policies remain distinct from replay capability:

- `STRICT` keeps its existing row policy. Before any successful schema negotiation, the current contract may still
  use legacy encoding after connecting to a legacy peer. Once schema support has been negotiated, its sticky
  `hasNegotiatedSchema` state prevents new legacy rows. This policy does not make an empty queue or a legacy replay
  head require schema capability.
- `AUTO` continues to request schema support and uses schema encoding when metadata is obtainable; legacy fallback
  remains available under its existing rules.
- `OFF` continues to encode new rows with the legacy contract. If an existing recovered queue contains schema frames,
  those immutable frames still require schema support when they reach the replay head.

A schema-capable connection can transmit both legacy and schema frames. Reaching a later legacy frame never requires
a downgrade or reconnect.

## Required invariants

The implementation is correct only if all of these remain true:

1. The consumer never reads a frame byte at or above `publishedOffset()`.
2. A schema frame is never passed to `sendBinary()` on a connection that did not negotiate schema.
3. Capability failure does not advance the segment cursor, FSN mapping, or wire sequence.
4. Every reconnect repositions to `ackedFsn + 1` before classifying replay requirements.
5. A sent but unacknowledged schema frame is classified again during replay; inspecting only the formerly unsent
   cursor is incorrect.
6. Endpoint mismatch remains retriable and does not discard, acknowledge, or rewrite the frame.
7. Mode-specific row-selection state remains independent of frame-derived replay capability.
8. The producer never writes connection-capability state.

## Transaction boundary acceptance gate

Mixed wire families can occur across frames in one transactional publish: changing contract may flush a deferred
prefix before the later commit-bearing frame is produced. Head-frame gating may send that legacy prefix and then
recycle at a schema frame.

This is safe only if a deferred prefix cannot advance the durable store-and-forward ACK watermark before its covering
commit. On connection loss, the server must discard the incomplete transaction and the client must replay the whole
unacknowledged prefix from `ackedFsn + 1` to the schema-capable peer.

Arbitrary transport failure already requires this property, but it must be demonstrated by a focused mixed-family
transaction test before accepting this design. If a deferred prefix can become durably acknowledged independently,
the proposal is invalid until that protocol defect is fixed.

## Observable semantics

| Situation | Current global gating | Proposed head-frame gating |
| --- | --- | --- |
| Schema append is backpressured | Legacy connection may be recycled during the wait | Connection is unaffected until publication |
| Append fails or times out | Rollback must repair speculative state | No capability state changed |
| Queue is `legacy, legacy, schema` | Entire queue requires a schema-capable peer | Legacy peer may drain the first two frames |
| Schema frame is the replay head | Only schema-capable peer is accepted | Only schema-capable peer is accepted |
| Schema frame was sent but not ACKed | Global maximum keeps schema required | Replay-head inspection finds the same frame |
| Schema frame is ACKed | ACK comparison clears the requirement | Cursor naturally moves beyond the frame |
| Recovered mixed queue | Recovery computes maximum schema FSN | Recovery positions at the first replay frame |

Ordering is unchanged. Frames remain sent in FSN order, and a later legacy frame cannot bypass a blocked schema
frame.

## User impact

### Improvements

- A producer append that never publishes cannot disturb a healthy connection.
- Legacy backlog can make progress against a legacy endpoint even when a later schema frame is queued.
- Backpressure caused by a full queue is no longer amplified by a speculative capability mismatch.
- A failed or oversized schema append needs no reset or connection recovery beyond its normal append error.
- Existing persisted frame bytes remain replayable without a new metadata file or format version.

### Behavior changes

- A sender may report a successful legacy connection and later reconnect when a schema frame reaches the head.
- If the deployment has no schema-capable endpoint, legacy frames before the schema frame can be delivered before the
  queue stalls at the schema boundary. Current global gating stalls the whole unacknowledged queue earlier.
- Connection and retry metrics may show a capability-driven reconnect later in time and at a specific FSN rather than
  at publication of any schema frame.

The schema frame itself remains retained and retried. The producer observes the existing store-and-forward behavior:
it can continue buffering until capacity is exhausted, at which point normal backpressure applies.

Logging should identify the blocked replay FSN and state that its frame requires schema capability. Avoid describing
the entire engine or queue as schema-only.

## Trade-offs

### Additional connection churn at a format boundary

When AUTO is connected to a legacy peer and the queue contains a legacy prefix followed by a schema frame, the sender
uses the working connection for the prefix and reconnects at the schema boundary. The current implementation tries to
select a schema-capable peer before sending any part of that queue.

This can add one handshake at the boundary. It is bounded by actual mixed-family transitions encountered while using
an incompatible peer. Once connected to a schema-capable server, both frame families are accepted and no transition
requires another reconnect.

### Capability failure is discovered later

A deployment with no schema-capable endpoint discovers the problem when the schema frame reaches the replay head,
not merely when such a frame exists later in the queue. Earlier legacy work can complete first. This is intentional:
the later incompatibility is not a reason to block independently sendable earlier frames.

### Connection status is less predictive of the entire backlog

"Connected" means the peer can send the current replay head, not necessarily every later frame already persisted.
Operational reporting should describe the current blocked FSN when capability changes. It should not promise that one
connection is compatible with the complete future queue.

### More careful reconnect ordering

The current code can ask `engine.requiresSchema()` before cursor positioning. The replacement must position or peek
the replay head before endpoint admission. This adds a local ordering constraint inside the I/O loop, but removes the
cross-thread publication and rollback protocol.

## Complexity impact

### Removed

- the volatile `maxSchemaFsn` field;
- speculative schema-FSN assignment in both append APIs;
- failure and timeout rollback of the previous maximum;
- ACK-dependent `requiresSchema()` computation;
- recovery's `maxSchemaFsn` accumulator and accessor;
- global capability checks before a frame is known to exist;
- producer/I/O-thread reasoning about ordering two independent publication mechanisms;
- tests dedicated to latch restoration after oversized, exceptional, backpressured, and timed-out appends.

### Added or retained

- one constant-time QWP flag classification after a complete frame is observed;
- one I/O-thread-local reconnect requirement derived from the replay head;
- cursor positioning before capability-based endpoint admission;
- focused tests for head-frame selection and a mixed-family transaction boundary.

The main reduction is not line count. It removes a cross-thread state machine whose value can be neither fully
truthful before append nor safely late after append. The replacement derives a local decision from immutable,
published data.

## Performance impact

The predicted steady-state impact is neutral to slightly positive, but must not be presented as measured until an
implementation is benchmarked.

### Producer path

Expected improvement:

- no QWP header inspection in each append API solely for capability tracking;
- no volatile `maxSchemaFsn` write for schema frames;
- no save/restore branches around append retries;
- no additional work during backpressure spins or parks.

The CRC and payload copy remain the dominant per-frame append work.

### I/O path

The send loop already reads the frame header and payload boundary. Reading the flags byte from that already-hot frame
is constant time and allocation-free. It replaces volatile global checks currently performed before and after frame
publication observation.

There is no additional per-row work; the check is per persisted frame.

### Recovery

Recovery remains linear because it must validate frames and rebuild other replay state. Removing schema-maximum
tracking saves one flag branch and assignment per schema frame but does not change asymptotic complexity or expected
startup time materially.

### Network behavior

The possible regression is an extra connection handshake when a legacy peer drains a legacy prefix and then reaches
a schema frame. The possible improvement is useful progress instead of rejecting that peer for work it can perform.

Implementation validation should compare:

- append throughput for legacy and schema frames;
- send-loop throughput on an all-legacy and all-schema queue;
- connection attempts and delivered prefix length for a mixed queue against legacy-only and mixed-capability endpoint
  sets.

## Implementation outline

1. Add a shared, defensive `isSchemaFrame(payloadAddr, payloadLen)` helper near the existing QWP frame classifiers in
   `CursorWebSocketSendLoop`.
2. Move the capability check in `trySendOne()` to after complete frame publication and classify `frameAddr` directly.
3. Refactor reconnect admission to derive its requirement from the frame at `ackedFsn + 1`; do not derive replay
   requirements from row-selection mode.
4. Apply the same replay-head rule to initial recovered startup and `BackgroundDrainer`.
5. Remove `CursorSendEngine.maxSchemaFsn`, `requiresSchema()`, and all speculative append bookkeeping.
6. Remove `RecoveredFrameAnalysis.maxSchemaFsn` and its recovery wiring.
7. Remove or rewrite tests that assert the deleted global watermark.
8. Run the acceptance matrix below before deleting the old code path permanently.

Do not retain both mechanisms behind a flag. Two capability authorities would recreate the ambiguity this design is
intended to remove.

## Validation plan

### Unit and deterministic loop tests

- Empty queue on a legacy connection does not require schema.
- A failed, oversized, exceptional, backpressured, or timed-out schema append does not recycle the connection.
- A published schema head is never sent to a legacy client.
- A published legacy head is sent even when a later frame carries `FLAG_SCHEMA`.
- After the legacy prefix advances, the schema frame triggers reconnect without cursor or wire-sequence advancement.
- A sent but unacknowledged schema frame is classified again from `ackedFsn + 1` after reconnect.
- Once that frame is durably acknowledged, a later legacy replay head does not require schema.
- A malformed or synthetic non-QWP payload is not classified as schema merely because an arbitrary byte matches the
  flag bit.

### Recovery and background draining

- Restart with `legacy, schema, legacy` frames and each possible ACK watermark.
- OFF mode with recovered schema backlog drains the legacy prefix, then waits for a schema-capable endpoint.
- Background drainer preserves the blocked schema frame across repeated legacy-only reconnects.
- A schema-capable reconnect resumes at exactly the blocked FSN and drains the suffix.

### Transaction and delivery safety

- A deferred legacy prefix followed by a schema commit frame is interrupted at the capability boundary.
- The legacy peer does not durably advance the store-and-forward watermark for the incomplete transaction.
- The schema-capable peer receives replay from the first unacknowledged deferred frame.
- The final table contents use a dense or DEDUP-safe oracle that tolerates documented at-least-once replay.

### Mode and endpoint matrix

- AUTO with old server only.
- AUTO with new server only.
- AUTO failover from new to old with legacy at the replay head.
- AUTO failover from new to old with schema at the replay head.
- STRICT preserves its current pre-negotiation legacy behavior while still rejecting a legacy peer when the replay
  head itself is schema-framed.
- OFF emits only new legacy frames but can replay recovered schema frames on a capable peer.
- Mixed old/new endpoint set does not spin on the same incompatible endpoint without exercising normal endpoint
  rotation and backoff.

## Alternatives rejected

### Set and roll back around each append attempt

This reduces the duration of each speculative value but does not eliminate it. The I/O thread can still observe the
value during any failed append attempt, and backpressure repeats those windows. It also retains duplicate append
bookkeeping and the recovery watermark.

### Keep transactional rollback around the entire append call

This repairs state after failure but leaves a false global capability requirement visible throughout the blocking
wait. It is insufficient when the send loop is active concurrently.

### Pin one wire family for the entire sender or slot

This removes still more switching code, but changes the product contract substantially. AUTO would either become
legacy for the sender lifetime after an offline first write, or a schema-pinned sender would reject new-table writes
during outages when metadata is unavailable. It also requires a clear persisted contract or a configuration-mismatch
policy for recovered slots.

Head-frame gating removes the unsafe shared state without giving up AUTO's store-and-forward fallback. Stream-wide
pinning should be considered only as a separate product decision, not as the repair for this concurrency problem.

## Source map

- [`QwpWebSocketSender.java`](../core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketSender.java)
  - `bindingForEffectiveWrite()` and mode-specific schema lookup
- [`CursorSendEngine.java`](../core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/CursorSendEngine.java)
  - `maxSchemaFsn`, append bookkeeping, and `requiresSchema()`
- [`CursorWebSocketSendLoop.java`](../core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/CursorWebSocketSendLoop.java)
  - reconnect admission, `swapClient()`, cursor positioning, and `trySendOne()`
- [`MmapSegment.java`](../core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/MmapSegment.java)
  - `publishedCursor` publication ordering
- [`RecoveredFrameAnalysis.java`](../core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/RecoveredFrameAnalysis.java)
  - recovered schema maximum
- [`BackgroundDrainer.java`](../core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/BackgroundDrainer.java)
  - orphan replay capability selection
