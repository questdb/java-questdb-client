# Schema rejection: preserve, retire, continue

Status: uncommitted simplification experiment based on `26e3c3a` (PR #94).
Updated: 2026-09-09. No server or wire-format changes.

## Goal and scope

A schema-rejected QWP batch must not permanently disable a pooled sender slot.
Fail the active borrow which published the rejected data, preserve the affected
queued frames when configured, retire that range, and continue independent
queued work. Returning a failed borrow allows the next borrower to use the slot.
A standalone handle remains failed until closed and rebuilt.

This experiment preserves the PR's rejection-range rules, default policy,
notification capacity, and preserve-before-retirement ordering. It simplifies
borrow ownership and replaces general archive discovery with an exact lookup for
a recovered orphan range. It does not implement whole-group rejection for
ordinary split flushes or set aside entire queues.

## Behavioral changes from PR #94

| Area | Experiment |
|---|---|
| Completed borrow history | Keep only the current borrow and one pending rejection. No deque, coalescing, pruning, or per-ACK ownership maintenance. |
| Transaction mode | Fixed for a sender's borrow lifecycle, as configured by the builders. Switching it between borrows is rejected. |
| Archive identity | Deterministic from the source namespace, source segment generation token, and exact FSN range; no durable `.slot-epoch`. |
| Restart | Recover the live SFA queue, then check only the deterministic archive path for an exact recovered orphan range. No directory scan. |
| Callback reconstruction | A structurally valid completed copy for that still-live orphan range is queued before retirement. Other archives do not reconstruct callbacks. |
| Duplicate copies | A retry or restart for the same live identity and range reuses the completed copy. |
| Metadata | `rejection.properties` replaces the custom CRC-protected `rejection-meta.bin` format. Payload segments and dictionary retain their existing checked formats. |
| Interrupted copies | Recovery or preservation removes the deterministic staging tree for that exact live range. Unrelated and legacy staging trees remain untouched. |

There is no public archive reader or replay-copy API in the ingestion client.
Copy an archive directory to a separate working directory and use the existing
SFA recovery reader. Both old and new copies retain the same SFA payload formats;
the metadata filenames differ. Old PR archives and `.slot-epoch` files are left
untouched and cannot block startup. The internal exact-path lookup reads the
properties only to validate and reconstruct the matching orphan report; there is
no archive index or general notification log.

## Rejection boundaries

An FSN is a local frame sequence number. A commit-bearing frame has
`FLAG_DEFER_COMMIT` clear. The group starts after the last commit-bearing frame
below the rejected FSN, bounded below by the unresolved queue floor.

- Ordinary flush: retire the deferred prefix through the rejected frame.
  Published successors remain eligible for replay and may produce a partial flush.
- Transactional sender: retire through the first commit-bearing frame at or after
  the rejection. If no closer has been published, the producer must seal its
  published tail when it observes the failure or returns the borrow.
- Recovered data: transaction mode is unknown, so retire conservatively through
  the recovered group's closer or recovered open tail. Never use a new
  producer's closer to determine an old group's end.

Retirement does not imply server rollback. Schema changes and previously forced
commits can survive rejection, so manually replaying a copy can duplicate rows.

## Ownership without history

`SchemaRejectionState` holds the current borrow and one pending rejection.
The current record contains generation, first FSN, transaction mode, active/end
state, and its first immutable failure. A pending rejection may retain the record
of a returned failed borrow until its range retires; it does not retain any
other historical borrows.

The NACK must first be validated against frames actually sent on the current
connection. An active borrow owns it only if the rejected FSN is at or above
that borrow's first FSN. Older and recovered data produce asynchronous reports
without failing the current borrower. A returned borrow cannot receive another
producer-side exception, even if no new borrower has arrived yet.

Begin, return, rejection installation, and open-tail sealing synchronize on the
same state object. `failedGeneration` and `stopFsn` remain volatile observations
for the producer and I/O loop. An open rejected range is sealed before returning
its producer ownership. Successful normal pool return flushes a commit boundary;
an unclosed transactional return without a covering rejection is refused.
The check tolerates ACK/trim racing the frame lookup.

Historical transaction ends are recoverable from queued commit flags. For an
old live rejection, the scan stops before the active borrow, or at the most
recent return when the slot is idle. Recovered ranges use the engine's original
recovery boundary. Consequently no scan borrows a new producer's transaction
closer. This relies on normal returns closing transactions and exceptional open
returns sealing their rejection before reuse; arbitrary unclosed returns are
not a supported state transition.

## Retirement and preservation

The I/O loop performs the following sequence:

1. Validate the NACK and identify its range. Ambiguous or out-of-range responses
   never become local retirement targets.
2. Latch the owning borrow's failure and reconnect. Replay independent lower
   frames until they have their configured ACKs, stopping before the rejection.
3. Wait for the range to be sealed. Preserve dictionary deltas from every
   skipped frame, including unsent transaction successors, for later replay.
4. When preservation is configured, copy the range into its deterministic staging
   directory using existing segment, manifest, watermark, and dictionary formats.
   Clear the final copied frame's defer flag so the copy can replay independently.
5. Write diagnostic properties, sync the files and directory, rename the staging
   directory, and sync its parent. Only then is preservation complete.
6. Retain the final notification in the separate 256-entry schema queue. A full
   queue pauses retirement; callback completion otherwise does not gate it.
7. Advance the existing resolved watermark through the range, then resume from
   the next FSN on a correctly reanchored connection.

`RejectedMiniSlotArchive` writes copies and can read the one deterministic path
for an exact live range. There is no directory scan, separate preserver wrapper,
or slot-epoch lifecycle. The writer is used by one I/O thread. After rename
succeeds, it retains the result until the parent-directory sync succeeds, so a
transient barrier failure retries without creating repeated copies. A later
retry or restart reuses a completed valid copy and removes the exact crashed
staging tree. Earlier write failures retain source frames and retry with the send
loop's existing bounded backoff.

A damaged completed archive cannot block live queue startup. For an exact
recovered orphan range, the internal lookup validates the properties, segment,
manifest, watermark, and optional dictionary. A missing, malformed, unreadable,
or mismatched copy produces no report and the live orphan still retires. For
manual replay, always use a working copy because queue cleanup can remove drained
files and existing corruption handling can quarantine damaged working data.

## Crash and shutdown boundaries

- Before preservation completes: source frames remain queued. Closed groups can
  replay and be rejected again. Recovery may discard an unclosed orphan tail
  without another NACK; there is no universal callback or archive guarantee.
- After archive publication but before durable retirement: the copy survives;
  restart reuses it. If the exact range is a recovered orphan tail, its report is
  retained before retirement; a closed range can replay and be rejected again.
- After retirement but before callback delivery: a crash or bounded dispatcher
  shutdown can lose the callback. With no still-live range, startup does not
  reconstruct it from the archive.
- A blocked filesystem call can exceed the close budget. Existing delegated
  I/O-thread cleanup retains the source engine and lock until the worker exits.

With disk buffering, preservation defaults to the slot's `rejected/` directory.
An explicit DLQ base also supports memory queues. Memory queues without a
configured destination, or preservation disabled explicitly, retire without
copies. Completed archives have no automatic retention policy.

`getAckedFsn`, `awaitAckedFsn`, and `drain` retain PR #94's resolved-progress
semantics: locally retired data counts as progress, not server acceptance.
The owning handle still throws for its rejected publication. Other error
categories retain their existing policies, and `TERMINAL` remains selectable.

## Validation and remaining limits

Focused coverage includes delayed NACKs after many ordinary/transactional borrows,
return-versus-NACK races, failed open-tail sealing, recovered group boundaries,
successor dictionary continuity, source retention during write failure,
parent-sync retry without recopying, notification saturation, blocked-copy close,
offline archive replay, deterministic archive reuse and staging cleanup, recovered
orphan notification reconstruction, and repeated startup with damaged archived
copies.

Validation on OpenJDK 25:

- Before the final segment-token substitution, the full core suite ran 3,507
  tests with zero failures or errors and seven skipped. A broader schema-focused
  run at that stage passed 66 tests.
- After the segment-token change, focused `MmapSegmentTest`,
  `RejectedMiniSlotArchiveTest`, and `RejectedArchiveRecoveryTest` coverage passed
  39 tests.
- Examples compiled successfully. `git diff --check` passed.

The current production diff removes 507 lines relative to `26e3c3a`. The archive
class is 332 lines, down from 511, with the 73-line preserver wrapper and 116-line
slot-epoch class deleted. The stronger return invariant and explicit
archive behavior changes still require design review before adoption. No measured
throughput improvement is claimed. The real server rollback behavior and minimum
server version remain the PR's existing assumptions; local protocol tests use
the repository's test server.
