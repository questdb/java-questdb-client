# Schema rejection: report, retire, continue

Status: Java phases one and two implemented and validated locally. Updated: 2026-09-08. Owner: Jaromir Hamala.
Supersedes the whole-group/journal revision; see Appendix B for what was
dropped and why.

This amends [`qwp-nack-policy-v2.md`](qwp-nack-policy-v2.md). Today a
schema-invalid batch latches a `TERMINAL` error, stays in the store-and-forward
log, is replayed and rejected again on every restart, and parks the slot. The
new selectable policy is `REJECT_AND_CONTINUE`: fail the handle that owns the data,
report the rejection, retire the affected frames from replay, and keep
independent data moving. Phase one retains `TERMINAL` as the default; phase two
flips the default in the same release as preserved copies.

## Decisions

1. Validate the NACK against frames actually sent on this connection. Never
   clamp an invalid sequence into a retirement candidate.
2. Compute the rejection span by scanning frame flags on the ring, not from a
   tracked scalar. Ordinary flushes retire the deferred prefix through the
   rejected frame. Transactional senders retire the whole transaction.
3. Retire in memory using the existing orphan-tail machinery in the send loop:
   stop before the sealed span, retain a non-dropping in-memory notification,
   wait for lower ACKs, self-acknowledge through the span, recycle, continue.
   Callback completion is not a retirement gate.
4. No journal. A crash before retirement replays the frames and the server
   can reject them again. Phase one has the crash-reporting exceptions below;
   it does not guarantee a callback for every observed NACK across restart.
5. The handle that published the span fails until returned or rebuilt. Its
   waits report the rejection. Other handles never see it.
6. Phase two preserves the span's bytes in the existing segment and dictionary
   formats. Phase three adds an offline reader. Neither gates phase one.

Retirement is not acceptance and not proof of rollback. Per-table force
commits, auto-created tables and auto-added columns can survive a rejection.
Application resubmission can duplicate rows.

| User | Phase-one default: `TERMINAL` | Explicit phase-one `REJECT_AND_CONTINUE` / phase-two default |
|---|---|---|
| Pooled producer | Existing preserve-and-halt behavior; the slot can remain poisoned. | Return and reborrow; the slot progresses. Ordinary unrejected prefix rows are retired too: lost in phase one, copied in phase two when export is enabled. |
| Standalone producer | Rebuild may encounter the same rejection. | Handle state resets on rebuild; unretired frames may re-reject. |
| Transactional producer after a crash | Existing recovery behavior remains unchanged; do not assume every open tail is re-sent. | A crash before preservation/notification can yield orphan retirement, no callback and no preserved bytes. |
| Handler author | Existing bounded best-effort notifications. | Non-dropping while running, but crash/shutdown can lose pending notifications even after retirement. Phase two supplies a ready preserved-copy path. |
| Operator | Must explicitly opt in to retirement without a copy. | Phase two defaults to preservation before retirement; export opt-out accepts payload loss. |

## Shipping phases

| Phase | Ships | Boundary |
|---|---|---|
| 1 | NACK validation, span computation, in-memory retirement/notifications, selectable `REJECT_AND_CONTINUE`, failed-handle ownership, recovery drain fix, two counters. | `TERMINAL` remains default. No new on-disk state; process-local queue identity only. Opt-in retirement discards prefix bytes and requires the source. |
| 2 | Span copy in existing segment/dictionary formats, durable slot epoch, build-time destination probe, opt-out and storage counters. | Flip the schema default to `REJECT_AND_CONTINUE` only when this ships; copy is default-on with `sf_dir` and gates retirement. |
| 3 | Offline reader over copied files; JSONL export. | Separate release. |

Phase one does not fix the pooled poison demo out of the box: its default
`TERMINAL` policy still parks the slot. The demo progresses only when explicitly
configured with `REJECT_AND_CONTINUE`, until phase two changes the default.

The default flip and preserved-copy support ship together, not in separate
releases. Explicit phase-one users accept that ordinary A/B/C/D split-flush
rejection at C retires A and B as well as C, while D can commit. Phase two does
not recover bytes already discarded by phase one. Memory-only phase-two users
without a destination, and export opt-outs, retain their own source data.

## Terms

| Term | Meaning |
|---|---|
| Frame / FSN | One locally published ingest frame and its sequence number. |
| Commit-bearing frame | A frame without `FLAG_DEFER_COMMIT`. Commits it and every deferred frame before it. |
| Group start | The frame after the last commit-bearing frame below the rejected FSN. |
| Rejection span | Inclusive FSN range retired for one rejection. |
| Retired | Locally acknowledged without server acceptance; never sent again. |
| Owning lease | The borrow (or the standalone sender's lifetime) that published the span. |

## Retirement mechanism

### Validate the NACK

`handleServerRejection` currently clamps out-of-range wire sequences so that an
error can still be attributed. Keep that for reporting; never feed a clamped or
pre-send sequence into retirement. A NACK retires data only when its sequence
maps to a data frame sent on this connection at or above the replay start.

### Compute the span

Add a read-only engine API for live per-FSN QWP header flags; recovery-only
`RecoveredFrameAnalysis` is not such an API. Pin or otherwise protect the read
against trimming and validate the frame. Scan forward from a captured safe floor
of `ackedFsn + 1` to just before the rejected FSN, retaining the latest
commit-bearing boundary. The group starts just after that boundary, or at the
floor if none exists. A cold lookup cache makes this scan and archive copying
linear without indexing frames during healthy publication. The latest published commit boundary is the wrong scalar: a NACK can
arrive after a later closer was published.

Ordinary senders retire `[group start, rejected FSN]`. Valid deferred
predecessors remain in the ring after server rollback, but this policy deliberately
retires them along with the rejected frame. Published successors replay and may
commit as a partial flush. The split-flush javadoc's partial-publication caveat
does not itself authorize that disposal; it is an explicit policy trade-off,
opt-in until preserved copies ship.

Transactional senders retire from group start through their published closer,
or through their published open tail if no closer exists. Capture transaction
mode with the owning generation; never use a later borrower's settings. A closed
span ends at the first commit-bearing frame at or after the rejection, not the
lease's last published frame: one lease can contain several transactions. It
can retire without waiting for lease return. An unclosed transaction follows
this producer-side sealing protocol:

1. On rejection, fail the owning generation and install a replay stop at the
   span's start. Do not report a provisional end as final.
2. The next producer call that observes the failure, or lease return, excludes
   further publication. Sender methods already require one producer thread.
3. Read `publishedFsn` and look for the first closer after the rejected frame.
   Use that closer, or the published tip if still open. This snapshot is the
   sealing point; the first exception carries immutable final bounds.
4. On return, skip flush and discard staged rows, then advance the generation
   and make the slot available to the next borrower.
5. Queue the final-span notification and let retirement proceed once lower ACKs
   and, in phase two, the preserved copy are ready.

A held or leaked failed lease that neither observes its failure nor returns
can stall its unclosed span indefinitely. Either producer observation or return
seals it; callback completion does not. A later borrower's
closer cannot commit its rows because the replay stop and retirement precede
sending beyond the sealed span. Validate the publication race protocol under
the phase-one gate in Open decisions.

Recovery does not persist the previous producer's transaction mode. Treat a
rejected recovered group conservatively as transactional: retire through its
first recovered commit-bearing frame, or its recovered open tail. Never use a
new producer's closer. This also retires the tail of an ordinary split flush
after restart; phase two preserves those rows in the copy.

### Retire in the send loop

`CursorWebSocketSendLoop` already stops at a recovered orphan tail, and
`retireRecoveredOrphanTailIfReady` self-acknowledges it once every lower frame
is acknowledged, then recycles the connection to re-anchor the arithmetic
wire-sequence-to-FSN mapping. Generalize that range so it can be set live by
the I/O thread on a NACK. Retain the final-span notification before advancing
the watermark, but do not wait for its callback. Phase two additionally waits
for the preserved copy to be durably published.

I/O-side order on a NACK (return-side sealing and callback execution are separate):

1. Validate. Compute the span. Latch the error on the owning lease.
2. Disconnect and recycle immediately; do not wait for lease return or
   notification capacity.
3. Replay from the watermark. Independent frames below the span send and
   acknowledge normally. Stop before the span.
4. Once the span is sealed and lower frames have their configured ACKs, retain
   its notification in the FIFO and signal the dispatcher. If the FIFO is full,
   stop here until capacity is available. Then advance the watermark through
   the span without waiting for callback completion. In phase two, the copy
   must be durable before notification enqueue and watermark advancement.
5. Recycle, continue with frames above the span.

A second schema rejection below an already pending retirement range cannot be
merged safely by this implementation. Log the second rejected FSN and fall back
to `TERMINAL`, retaining source bytes. This can occur when a predecessor is
re-rejected while replaying for durable ACKs. Invalid closer scans or failed
skipped-range dictionary reconstruction also fail closed instead of reconnecting
forever.

No sparse ACK map, no second watermark file, no change to the watermark or
segment encodings. Each connection still sends one contiguous range. The general path uses two
recycles, after NACK and after retirement; each may ship dictionary catch-up.
Setup can sometimes retire before any wire sequence is consumed, but do not
assume that shortcut when estimating rejection cost.

Retired frames may be the only carriers of dictionary deltas referenced by
successors. Disk-backed catch-up must use the persisted dictionary covering
those deltas, not only replayed frames. Memory mode uses the live dictionary
mirror/snapshot; verify it covers skipped, possibly unsent transactional frames
before reclaiming them. Full-dictionary frames remain self-sufficient. Neither
mode may lose symbols because their carrier frame was retired. This is new live
retirement behavior: recovered orphan tails have no successors requiring those
skipped deltas. Spike this first in memory and disk modes before committing to
the phase-one schedule; existing orphan tests are not sufficient evidence.

### Crash behaviour

Phase one does not guarantee at-least-once callbacks across restart. A pending
notification is memory-only and retirement does not wait for invocation:

- Before durable retirement, a closed span may replay and re-reject if the
  schema is unchanged, producing another callback opportunity.
- Without a closer, recovery may retire the open tail without sending it:
  zero callbacks and no preserved bytes are possible. This is not presented as
  a reporting guarantee equivalent to repeated `TERMINAL` rejection.
- After durable retirement but before callback invocation, a crash loses the
  notification and there is no replay to reconstruct it.
- A changed schema may allow replay to succeed without recreating the original
  rejection. Before watermark durability, duplicate reports remain possible.

Phase two preserves evidence once its directory is durably published; recovery
can report from that copy. A crash before copy publication retains the open-tail
reporting gap. Memory-only queues cannot reconstruct data lost with the process.
The default phase-one `TERMINAL` path is unchanged; these are the guarantees of
the explicit retirement policy, not reasons to claim unconditional delivery.

## Ownership and handle contract

A rejection belongs to the lease whose published FSN range contains the
rejected FSN. Each slot records the lease generation and its first published
FSN at borrow; failed/observed state and the return-side end snapshot above
are also required. Use the highest already published FSN plus one as the start. Row-level calls in
`QwpWebSocketSender.checkConnectionError` and the `PooledSender` wrapper both
check it, since row calls poll the delegate directly. Empty borrows retain no
lease record. `TERMINAL` does not allocate schema ownership state. Under
`REJECT_AND_CONTINUE`, the next borrow removes the returned handle's generation
and exception and coalesces adjacent returned ranges with the same transaction
mode. Ordinary ranges can always coalesce; transactional ranges can coalesce
only across a commit-bearing return boundary. The queued frame flags still
identify each closed transaction's end. An unfinished failed transaction keeps
its own end boundary, and a pending retirement keeps its owner object until
completion. Thus normal publishing borrows during an ACK stall retain one
historical range plus the current or most recently returned lease, rather than
one record per borrow. Active-lease lookup remains constant time. Resolved
progress prunes obsolete ranges.

An owned rejection fails the handle. Every subsequent publish, wait and drain
on that handle throws the same `LineSenderServerException` until the handle is
returned. A failed pooled lease needs a distinct `PooledSender.close()` path:
skip `flush()`, discard its staged rows, seal any pending span, and give the slot
back; only then throw the owned error if not already observed. Do not route this
known lease-local rejection through `discardBroken`. That path remains for real
sender/storage/cleanup failures. A stale or repeated close is a no-op. A healthy
close racing a newly owned rejection must take this same cleanup path rather
than discard the slot simply because its flush observed that rejection.

A standalone sender is one lifetime lease. Its failed close seals/discards local
work and releases resources but does not wait for rejection retirement. Skip
`drainOnClose` for this failure. Rebuild clears public handle state; if retirement
was not durable, replay and another rejection are allowed. Close is not a promise
that the old rejection can never recur. Preservation runs on the I/O thread.
Close waits for that thread within its
existing shutdown budget. If disk I/O outlasts the budget, close reports the
shutdown failure and the existing I/O-thread cleanup fallback retains the
engine and slot lock until the thread exits. Immediate rebuild can therefore
still encounter lock contention. There is no separate preservation worker or
preserver-specific deferred cleanup.

Callback completion no longer participates in retirement, so handler-initiated
close creates no callback/retirement dependency cycle. Preserve ordinary safe
dispatcher shutdown (never join the current thread); no synthetic callback-return
signal or special retirement-completion protocol is needed. Close/rebuild can
still encounter any frame whose retirement was not durable.

Waits on a failed handle throw even for FSNs the server accepted. That is
deliberate: a wait returning true for a retired FSN would be a false delivery
confirmation, which is worse than a spurious throw. The exception carries the
span so the caller can tell which batches are actually affected.

Healthy handles keep today's slot-wide wait and drain semantics. Retirement
advances the watermark, so a healthy borrower's drain completes past another
borrower's retired span rather than hanging. Rejections from earlier borrows
never fail a later handle. Consequently a healthy B waiting on A's retired FSN
can return true: for cross-borrow targets this is resolved progress, not delivery
confirmation. Acceptance conclusions are limited to the caller's own publications
while its handle is healthy. This ownership limit is part of the API contract;
it is why clearing A's owned failure would be different from letting B progress.
Do not describe a slot-wide wait on an arbitrary old FSN as proof of ingestion.

Pool startup recovery treats a retired span as progress. Today it reports
`RecoveryDrainOutcome.FAILED`, retries, and parks the slot on a failure streak;
that path is what poisons the scan.

## Reporting

Retain schema notifications in a per-slot sticky FIFO with capacity **256
entries**, separate from the ordinary drop-oldest deque. This is a fixed initial
implementation limit, not a new public setting. Count the callback currently in
progress against the 256; release its capacity only when invocation completes,
including when the handler throws. Ordinary overflow cannot evict schema entries.
Keep the same entry across reconnect attempts for an in-progress retirement.
Signal the dispatcher without waiting for user code. Callbacks run outside I/O,
pool and queue locks; log and contain thrown exceptions.

A stuck handler permits up to 256 retained rejections in that slot, including
its current invocation. The next rejection cannot retire until an entry completes
and frees capacity. Keep that rejection in the live retirement state and leave
its frames unretired; do not drop or overwrite a notification. The I/O loop
remains responsive and independent slots have their own FIFO capacity. A shared
dispatcher can still delay their handlers, eventually filling their FIFOs too.

Thus slow-handler behavior degrades to a callback-dependent retirement stall
only once the 256-entry allowance is exhausted. Failure to allocate/retain an
entry also leaves the span unretired. This bounds retained notification count,
not arbitrary server-message bytes. Shutdown may abandon the volatile backlog;
no durable delivery is promised.

Install an effective handler and dispatcher for every reporting path: custom
when supplied, otherwise the default one-line logger. `SenderPool` currently
creates its recovery dispatcher only with a custom handler and SFA enabled;
wire the default path too. Startup schema callbacks run asynchronously, never
on the thread calling `build()`. Recovery does not wait for callback completion while its FIFO has capacity.

Phase-one identity is slot ID + process-local queue instance + trigger FSN.
Allocate a new instance identity on queue recreation; it is not a cross-restart
deduplication key. Durable slot epoch and legacy initialization are phase-two
work because the preserved directory needs stable identity. No epoch sidecar or
migration is introduced in phase one.

The error carries: category, policy `REJECT_AND_CONTINUE`, rejected FSN, span
from/to, server message, table when known, and in phase two the preserved file
path once it is published. It states that the span will not be retried, that
server side effects may remain, and that resubmission needs the source.

## Policy and API

- Add `SenderError.Policy.REJECT_AND_CONTINUE`. Do not reuse `TERMINAL`
  (bytes preserved, sender halted) or `ABANDONED` (no throw, `DATA_LOSS` only).
  Update the default handler's log line and policy switches.
- Phase one keeps `TERMINAL` as the schema default; phase two switches to
  `REJECT_AND_CONTINUE` with preserved-copy support. Add one builder method, provisionally
  `schemaMismatchPolicy(TERMINAL | REJECT_AND_CONTINUE)`, as the escape hatch.
  Do not implement the resolver precedence chain or wire `on_schema_error` in
  this change; the connect-string key stays a consumed no-op as today. Correct
  `Policy` javadoc to describe the actually implemented builder override and
  default, removing the non-existent resolver/precedence claim. Document that
  the reserved connection-string key does not enable the escape hatch.
- `LineSenderServerException.getServerError()` exposes the rejected FSN via a
  new accessor plus the existing `getFromFsn()` and `getToFsn()` span. Include
  all three in the message.
- `getAckedFsn()` is documented as the resolved watermark: acceptance or local
  retirement. It already advances for recovered orphan tails. No second
  accessor.

Other categories are unchanged. `PARSE_ERROR` and `SECURITY_ERROR` remain
`TERMINAL` and can still poison a persistent slot; enabling them needs their
own attribution review, since malformed input can compromise the flag scan
that computes spans.

## Server baseline

The mechanism relies on two server behaviours, both present in the inspected
source (`QwpIngressUpgradeProcessor.handleBinaryMessage`): deferred rows are rolled
back before the NACK is sent, and frames after a NACK are consumed without
processing or reply until disconnect. `QwpSenderE2ETest.testDeferredCommitSchemaMismatchRollsBack`
covers the first. Post-NACK reply handling is whatever the existing retriable
recycle does today; this change adds no new handling. The server does not close
the connection after a NACK: client-side disconnect is required to resume. Older
servers without the unresolved-sequence gate can apply successors on that same
connection; ignoring their replies and replaying can duplicate rows. The minimum
supported-release note must explicitly exclude that behavior.

The supported baseline for this policy is QuestDB 10.0.0 or later. The 10.0.0
source tag contains both the unresolved-sequence gate and rollback before NACK.
The rollback E2E test passes against the local 10.0.1-SNAPSHOT checkout at
`496b24d996ea321015c7cfeabcbfc7e563e053e7`, using the current client artifact.
Its harness now needs to observe the owning handle's failure before close;
receiving the callback alone does not consume that failure. A temporary test
adaptation verified the final span and allowed the unchanged database rollback
assertions to run; the server checkout was then restored.
No capability negotiation is added.

## Phase two: preserve the span

Introduce a durable slot epoch for directory/deduplication identity. Preserve it
across restart of the same queue and change it when a clean queue restarts FSNs.
Initialize/migrate under the exclusive slot lock before creating copies; existing
identity metadata may be reused only if it provides that lifecycle. This sidecar
is identity metadata, not a phase-one rejection journal.

Before either retirement or ready-path callback enqueue, copy the span into
`<sf_dir>/<slot>/rejected/<slot-id>-<epoch>-fsn-<from>-<to>/` as a mini slot: one
segment file in the existing `MmapSegment` format holding the span's frames in
order, plus a frozen full dictionary snapshot covering the copied frames. The
I/O mirror folds skipped deltas before taking the snapshot, including in memory
mode. It may contain unused later entries; preserving this superset avoids a
decoder and reconstructing historical dictionary versions. Both
formats already carry magic, version and CRC and have Java/Rust fixtures.
Add rejection metadata sufficient for the promised recovery callback (epoch,
trigger/span, category/status and message); existing segment headers alone do
not contain it. Define and validate that metadata file without calling it an
existing SFA field. Final-directory publication covers all constituent files.

Rewrite the last copied frame's QWP flags to clear `FLAG_DEFER_COMMIT` and
recompute that frame's CRC, so the copy is a closed unit. Without that, the
existing recovery reader classifies a deferred-only copy as an orphan tail and
retires it unsent. Validation must confirm that a copied subset with its
dictionary snapshot replays through the existing reader; this is validation of
an existing format, not a new one.

Write into a process-unique temporary directory, sync, rename, sync the parent.
A directory at its final name is complete. Under the queue lifecycle lock,
remove unfinished directories matching that exact slot and epoch. Do not remove
other epochs' temporary directories: a shared memory-only destination cannot
prove that another queue has stopped. Such leftovers require operator cleanup.
Reuse an existing final directory
with the same identity on re-rejection after a crash. Copy synchronously on the
I/O thread with bounded buffers, after lower frames are acknowledged and the
rejection span is sealed. No data above the span may be sent until the copy is
complete, so a separate copy worker cannot advance ingestion. A large span,
full dictionary or slow filesystem delays I/O-thread responsiveness, including
keepalives and shutdown; reconnect handles an expired connection. Healthy
publication is unchanged.

Keep the completed-copy notification until the bounded FIFO admits it, avoiding
repeated archive validation and directory sync while the handler queue is full.
On copy failure keep the frames unretired, log the failure, and wait using the
stop-aware backoff before retrying, with bounded exponential pacing. A failed copy does not latch a
fatal sender error. Before retry, remove unfinished temporary directories for
this exact slot and epoch, so repeated failures do not accumulate partial copies.

Java settings are builder methods on `Sender` and `QuestDB`: preservation is
enabled by default with `sf_dir`; `dlqEnabled(false)` opts out;
`dlqDirectory(path)` selects `path/<slot>/rejected/` and gives memory-only
senders a destination. No new connection-string keys are introduced. Probe the
destination at build time, including for a lazy pool with no warm connections. Files are never deleted by the client. TLS does not protect these
bytes at rest. Quarantining a damaged slot also moves its `rejected/` directory;
previously reported paths then change. Use the quarantine path reported by the
`DATA_LOSS` event to locate those copies. A ready path guarantees completeness
when delivered, not a permanent location. Document alongside existing `sf_dir` behaviour and use
restrictive permissions. Each copy may carry the full dictionary; that is the
cost of not decoding and is counted in `dlq_bytes_written_total`; alarm on sustained growth of that
counter and filesystem free space, since no automatic retention bounds it.

Recovery: a preserved directory whose span overlaps an orphan tail at startup
may mean a crash before callback completion or retirement; it does not prove
that the callback never ran. Dispatch the callback from
the directory's rejection metadata and retire the tail. For an orphan-only slot,
retain that notification and retire locally before attempting a connection. An
unreachable server must not prevent this socket-free cleanup; callback execution
remains asynchronous.

Startup filters completed archive directories by their canonical slot, epoch
and FSN range before opening metadata. Damage in an unrelated, already-drained
archive cannot block recovery. An overlapping archive must pass metadata,
directory identity and replay-file validation. Proven corruption preserves the
`UnreplayableSlotException` type through startup cleanup so `Sender.build()`
can quarantine the whole slot, report `DATA_LOSS` and continue on a fresh one.
Operational storage failures do not become corruption verdicts.

With export disabled, a persistent schema fault retires indefinitely with one
paced report per span and no circuit breaker. That is the accepted cost of
opting out.

## Phase three: offline reader

A CLI or static helper opens a preserved directory with the existing recovery
reader and, once an ingest-frame decoder exists, exports JSONL: one metadata
line, one row per line, a row-count trailer. Resubmission after a schema fix
first copies the preserved directory to a separate working slot using
`RejectedMiniSlotArchive.copyToWorkingDirectory`, then opens that working slot.
Opening the original directly would let normal drain cleanup destroy the evidence.
This reuses
the replay path and needs no decoder. Decoder failures never touch ingestion.

## Rust and C/C++

Rust's SFA path under `questdb-rs/src/ingress/sender/` publishes non-deferred
frames (`qwp_ws_publisher.rs::encode_to_scratch` passes `false` to the defer
argument); `qwp_ws_sfa_queue.rs` is the backing queue. Its spans are singletons.
Do not generalize this to `column_sender/sender.rs`: that file also contains a
direct path with deferred split prefixes. Its `rebase_lease_observation` method
is the lease-rebase citation, not `db.rs`. Implement the same ownership,
failed-handle, callback-independent retirement and phased policy surface. `Drop` releases
the lease but cannot throw; the callback is the only report there. Phase one
adds no on-disk state, so a Java slot after retirement is readable by any
current client, subject to the existing format contract. Phase-two epoch
metadata does not encode replay decisions. Phase two's `rejected/` directory is ignorable by clients that
do not know it. No cross-client gate is needed.

## Pacing, metrics, healthy path

- Reconnect after a rejection uses the existing reconnect backoff, reset on
  any real ACK. Distinct rejected FSNs do not accumulate their own strike count;
  the same-FSN poison detector is unchanged and still escalates a frame that is
  rejected without ever being retired.
The Java observation methods are on `QwpWebSocketSender`:

| Counter | Accessor | Meaning |
|---|---|---|
| `schema_frames_retired_total` | `getSchemaFramesRetired()` | Frames locally retired; not accepted rows. |
| `schema_rejections_total` | `getSchemaRejections()` | Attributed schema NACKs. |
| `dlq_files_written_total` | `getDlqFilesWritten()` | Newly published archive directories; reuse is not counted. |
| `dlq_bytes_written_total` | `getDlqBytesWritten()` | Bytes in newly published archives, including dictionary copies. |
| `dlq_write_failures_total` | `getDlqWriteFailures()` | Preservation failures; source frames remain queued. |

- Healthy publication stays allocation-free per row and takes no new lock per
  frame. Generation boundaries are written at borrow/return; prove race safety for
  failure checks and sealing without a new per-frame lock. Existing ingestion benchmarks gate the change.

## Validation

- Reject a singleton, a middle deferred frame and a closing frame; span matches
  the commit-boundary scan, including when a later closer was already published.
- Ordinary split flush: prefix retired, successors commit as a partial batch,
  documented. Transactional: whole transaction retired, staged rows discarded,
  no partial commit, including when the closer was published before the NACK.
- Interrupted split flush from a failed lease: its deferred tail is retired;
  B's first closer never commits A's rows.
- Lower independent frames acknowledge before self-ack; frames above the span
  replay after recycle; dictionary catch-up sequences re-anchor correctly.
- Invalid, pre-send and catch-up NACKs never retire. A second schema NACK
  during pending retirement reports `TERMINAL` and retains the stopped range.
- Invalid transaction scans and skipped dictionary reconstruction fail closed;
  transient catch-up send failures preserve the existing cap-gap bookkeeping.
- Recovered rejected groups include their original closer or recovered tip,
  irrespective of the new producer's mode; a new closer is never included.
- Orphan-only slots retire without connecting, with a preserved metadata report
  retained asynchronously first when present. Copy failures retry with pacing
  and temporary cleanup, without advancing the watermark or latching fatal.
- Ownership: A's rejection after B borrowed fails neither B nor B's waits.
  Failed handle: publish, wait, empty wait and drain throw; first close throws
  only if unobserved, skips flush and returns the slot;
  repeated close is idempotent; reborrow is healthy. Standalone: rebuild on the
  same `sf_dir` clears handle state but may replay unretired frames.
- Slow/throwing handlers do not gate retirement below FIFO capacity; pending entries
  survive ordinary inbox overflow, remain distinct and dispatch when unblocked.
  Failure to retain an entry prevents that span's retirement without dropping
  earlier notifications. With one blocked callback, 256 retained entries permit
  progress; the next span waits until callback completion frees capacity. Test
  shutdown/crash loss of the volatile backlog.
- Crash before self-ack with closer published: replay, re-reject, callback
  opportunity. Crash after durable self-ack but before invocation can lose the
  callback. Crash without closer: orphan retirement, and in phase two the
  callback from the preserved directory. Crash after self-ack: no replay.
- Pool recovery with and without custom handlers drains through bad, bad, good.
- Handler closes pooled/standalone senders and the pool without self-join or
  retirement waits; rebuild before durable retirement can re-reject.
- Unclosed transactional span waits for return; generation/end snapshot excludes
  B's frames. Confirm pending span callbacks cannot report a provisional end.
- Live header reads respect the watermark floor and concurrent trimming;
  memory and disk catch-up include deltas carried only by retired frames.
- Phase-one process-local identities distinguish recreated queues without disk
  state. Phase-two epoch survives restart but changes on FSN reset.
- Phase-one defaults preserve/halt; explicit retirement loses ordinary prefix
  rows as documented. Phase-two default flips only with durable-copy support.
- A healthy cross-borrow wait reports resolution, not acceptance of retired data.
- Phase two: copy replays through the existing reader; flag rewrite and CRC;
  atomic publish; reuse on re-rejection; disk full; opt-out; probe.
- Both ACK levels; force commits and surviving schema side effects; benchmark
  non-regression.

## Local validation (2026-09-07)

- Full core suite: 3,480 tests, zero failures/errors, seven skipped.
- Final targeted integration run after the last boundary and lookup changes:
  181 tests, zero failures/errors. Includes pool return/reborrow, standalone
  orphan draining, preserved-tail startup reporting, dictionary continuity,
  archive reuse/recovery, bounded callbacks and trim-safe frame lookup.
- Examples reactor package succeeds.
- Server rollback E2E succeeds with the new owning-handle observation noted
  above. No server implementation changes are required.
- Fixed-work healthy producer check: one million rows, batches of 1,000,
  three alternating runs per build. Both builds measure zero producer bytes
  allocated per row; observed times are approximately 39–43 ns/row. This is a
  narrow memory-mode check, not a disk or end-to-end throughput claim.
- The rejected-range lookup previously scaled quadratically (1,000/2,000/4,000
  lookups took about 1.1/4.2/15.3 ms). The cold forward lookup cache removes the
  repeated scans, with no healthy publish-path index, lock or allocation.

### Review follow-up (2026-09-08)

- Full core suite: 3,489 tests, zero failures/errors, seven skipped.
- Final affected-suite run after the last test and policy-reporting corrections:
  98 tests, zero failures/errors. Covers recovered group boundaries,
  socket-free orphan retirement with and without preserved metadata, second
  schema NACK under durable ACK replay, invalid closer scans, missing skipped
  frames, preservation failure followed by successful retry, and existing
  dictionary, archive, orphan-tail and pool regressions.
- `git diff --check` passes.

### Synchronous preservation follow-up (2026-09-08)

- Removed the preservation worker, request/completion handshake and separate
  shutdown coordination. The I/O thread now owns copying and retirement.
- Retained a completed-copy notification while the FIFO is full, stop-aware
  retry pacing, and the existing I/O-thread cleanup fallback.
- Full core suite: 3,492 tests, zero failures/errors, seven skipped. Regression
  coverage includes successful retry, no repeated archive sync while the FIFO
  is full, and a blocked synchronous copy retaining the engine lock through a
  shutdown timeout until delegated cleanup finishes.
- `git diff --check` passes.

## Open decisions

| Item | Owner | Gate |
|---|---|---|
| Minimum server release: 10.0.0; local rollback E2E passed on 10.0.1-SNAPSHOT. | Server/QWP maintainer | Resolved |
| Java builder method is `schemaMismatchPolicy`; Rust/C/C++ implementation remains a separate client deliverable. | Client API maintainers | Java resolved |
| Copied subset/dictionary recovery, last-flag CRC and archive reuse verified by tests. | Persistence maintainer | Resolved |
| Skipped dictionary carrier recovery verified in memory and disk modes. | Persistence/I/O maintainers | Resolved |
| Live-header API, first-closer bounds and atomic return-side failure capture tested; healthy stop/ACK checks use volatile fields. | Persistence/pool maintainers | Resolved |
| CRC-protected `.slot-epoch`, fresh-namespace rotation under the slot lock, restart/reuse tests. | Persistence maintainer | Resolved |
| `RejectedMiniSlotArchive` metadata version 1, CRC, enum names, span and trigger. | Persistence maintainer | Resolved |
| Diagnostics for full notification FIFOs and unreturned failed leases. | Design owner | Phase 1 |

## Appendix A: source evidence

- `QwpWebSocketSender.flushPendingRowsSplit`: per-table frames with
  `FLAG_DEFER_COMMIT` on all but the last; its javadoc states the split is not
  atomic and can deliver a prefix twice.
- `QwpWebSocketSender.transactional`: auto-flush defers, explicit `flush()`
  commits; documented as committing atomically per table.
- `QwpWebSocketSender.checkConnectionError`: row-level calls poll the delegate,
  so ownership checks cannot live only in `PooledSender`.
- `CursorSendEngine.retireRecoveredOrphanTailIfReady` and
  `CursorWebSocketSendLoop.trySendOne`: existing stop, self-ack and recycle for
  a recovered deferred tail. This is the machinery phase one generalizes.
- `CursorWebSocketSendLoop.handleServerRejection`: clamps invalid NACK
  sequences for attribution; unsafe as a retirement input.
- `SenderPool` recovery drain and failure streak: the poisoned-scan path.
- `MmapSegment`, `PersistedSymbolDict`: formats reused for phase two; both
  versioned with CRC, and a foreign version fails recovery without quarantine.
- Server: `QwpIngressUpgradeProcessor.handleBinaryMessage` withholds ACKs for deferred
  frames, clears state before the NACK, and consumes later frames without
  reply. `QwpSenderE2ETest.testDeferredCommitSchemaMismatchRollsBack`.
- Rust: `questdb-rs/src/ingress/sender/qwp_ws_publisher.rs::encode_to_scratch`
  passes defer=false for SFA; `sender/qwp_ws_sfa_queue.rs` stores the frames.
  `column_sender/sender.rs::rebase_lease_observation` rebases lease observation;
  its direct split path uses deferred prefixes and is a different path.

## Appendix B: alternatives dropped

**Whole-group retirement for every ordinary split flush.** Prefix retirement
allows ordinary successors to deliver instead of discarding the entire flush.
It does not eliminate the publication/return linearization and tail sealing
needed for unclosed transactional or interrupted spans.

**Retire only the ordinary rejected frame.** Could preserve valid predecessors
still on the ring when an independent closer survives. It is not equivalent to
prefix disposal and is worth a separate policy decision. If the rejected frame
is itself the only closer, replayed predecessors have no closer; letting a later
borrow commit them changes the result again. Define that case, interruption and
no-successor behavior before selecting this alternative. This revision retains
prefix retirement explicitly rather than claiming those predecessors are lost
under either choice.

**Rejection journal.** Gave retained at-least-once callbacks across crashes and
handler-independent retirement, at the cost of a versioned side file,
compaction, delivery markers, and a downgrade and cross-client gate that
released readers cannot honor without a segment version bump. Re-rejection is
idempotent, so the journal bought a stronger callback guarantee than the data
path needs. Revisit if the documented zero-notification crash windows are unacceptable.

**Throw once, then clear.** Simpler handle lifecycle, but a slot-wide wait on
the rejected FSN would then return true after retirement: a false delivery
confirmation for its own failed publication. Rejected. A later healthy borrow
may still observe slot-wide resolution for that FSN; the ownership distinction
is intentional and does not establish acceptance of old data.

**New DLQ container format.** Replaced by a copy in the existing segment and
dictionary formats, which already have versioning, CRC and fixtures, and which
the existing recovery reader can replay after a one-byte flag rewrite.

**Server capability gate and distinct-FSN pacer.** Replaced by a stated
minimum server release and the existing reconnect backoff.

**Callback-gated retirement and phase-one epoch.** Callback completion is not a
retirement requirement: it adds handler-dependent stalls without durable payload
recovery. Phase one retains notifications in memory and accepts crash loss.
Durable epoch identity moves to phase two with preserved directories. Removing the per-rejection gate requires the 256-entry sticky FIFO; only
capacity exhaustion makes retirement wait for callback completion.
