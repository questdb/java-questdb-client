# QWP Client NACK Policy v2 — no drop, no lists, no dead senders

Status: implemented (client side) on `feat/nack-policy-v2`.
Tandem: OSS core reserves `STATUS_NOT_WRITABLE = 0x0C` in `QwpConstants`.

## Problem (v1)

Mid-stream server NACKs were classified client-side into two policies, both
wrong for store-and-forward:

| v1 policy | Categories | Consequence |
|---|---|---|
| `DROP_AND_CONTINUE` | `SCHEMA_MISMATCH`, `WRITE_ERROR` | **silent data loss** — batch discarded, watermark advanced |
| `HALT` | `PARSE_ERROR`, `INTERNAL_ERROR`, `SECURITY_ERROR`, terminal WS closes, `UNKNOWN` | **sender permanently dead** — next producer call throws |

Concrete failures:

- **Failover killed ingestion.** A read-only/demoting node rejecting writes
  surfaced as `SECURITY_ERROR` → HALT mid-stream, while the *same cluster
  state* at connect time (421 role reject) retried forever under Invariant B.
- **Fail-closed on the unknown.** `UNKNOWN` → HALT meant any new server status
  byte killed old clients (`STATUS_CANCELLED` 0x0A and `STATUS_LIMIT_EXCEEDED`
  0x0B already hit this).
- **Transport codes drove policy.** WS close codes 1002/1003/1007/1008/1009
  were interpreted via a client-side list — a transport detail leaking into an
  application-layer decision, blind to middlebox closes.

## Design principles (v2)

1. **No silent data loss.** There is no drop policy. Every rejected byte is
   either replayed or halts loudly with the bytes preserved in the SF log.
2. **User code never stops on anything transient.** Outages, failover,
   read-only windows, unknown statuses: the producer keeps writing into SF,
   the client keeps retrying. The only inherent bound is SF disk capacity
   (`sf_append_deadline_millis` backpressure).
3. **The client doesn't guess.** Policy tiers are minimal and deterministic;
   *empirical* poison detection replaces close-code lists.

## The three policies (`SenderError.Policy`)

| Policy | Behavior | Categories |
|---|---|---|
| `RETRIABLE` | Recycle the connection (same `fail()` → `connectLoop` machinery as a wire failure: capped backoff + jitter, endpoint rotation via the reconnect factory, no wall-clock give-up), replay from `ackedFsn+1`. Dispatch to `SenderErrorHandler` is informational. | `WRITE_ERROR`, `INTERNAL_ERROR`, `UNKNOWN` (fail open), any WS close without a preceding terminal NACK |
| `RETRIABLE_OTHER` | Same replay semantics; the node cannot serve writes at all, so rotation matters more than backoff. A node-state verdict, not a frame verdict: never counts poison strikes, recycles through the zero-progress pacer (first recycle immediate, consecutive no-progress recycles paced). | `NOT_WRITABLE` (reserved wire byte 0x0C — see below) |
| `TERMINAL` | `recordFatal` latch: next producer call throws `LineSenderServerException`; drainer quarantines its slot. Reserved for rejections **deterministic under byte-identical replay**. Bytes stay on disk. | `SCHEMA_MISMATCH`, `PARSE_ERROR`, `SECURITY_ERROR` (ACL denial on a writable node), `PROTOCOL_VIOLATION` (poison escalation) |

## Poison-frame detector (replaces the WS close-code list)

WS close codes carry **zero policy semantics**. Every close is a transport
event → reconnect + replay. The guarded case — a frame that deterministically
kills the connection without a NACK (e.g. an intermediary frame-size limit) —
is caught *behaviorally*:

> A server-active rejection (RETRIABLE NACK, or non-orderly close after at
> least one send on the connection) of the same frame counts a strike.
> RETRIABLE_OTHER (`NOT_WRITABLE`) never counts one: it is a verdict on the
> node's ability to serve writes, not on the bytes, and an all-replica
> window emitting it on every rotated endpoint is transient by Invariant B
> — striking it would escalate that window to a producer-fatal terminal. Strikes are
> keyed on the rejected frame's FSN — the NACK-named frame, or the OK-level
> head-of-line frame for a close — never on the engine's trim watermark.
> `max_frame_rejections` (default 4; connect-string key or
> `LineSenderBuilder.maxFrameRejections(int)`) consecutive strikes escalate
> to a typed `PROTOCOL_VIOLATION` TERMINAL naming that FSN. The counter
> resets only on OK-level acceptance **at or beyond** the suspected frame:
> in durable-ack mode the trim watermark advances only on durable coverage,
> so every post-NACK recycle replays from the durable watermark and re-OKs
> frames *behind* the suspect — those re-OKs say nothing about the poisoned
> bytes and must not launder the count. Orderly closes (`NORMAL_CLOSURE`
> role-change handoff, `GOING_AWAY` restart drain) never count strikes.

Below the escalation threshold, a RETRIABLE NACK's recycle is **paced**: the
server is reachable (it just answered), so the reconnect succeeds immediately
and the failed-connect backoff never engages — the recycle parks *before*
the first connect attempt instead of running full recycle cycles
(TCP+TLS+upgrade+window replay) at server NACK rate. The dose is the
reconnect backoff proper: initial backoff, doubling with each consecutive
strike against the same frame, capped at the max backoff, plus jitter. The
escalation is also the safety margin for transient same-frame rejections
(e.g. sustained disk pressure): the widening replay gaps give the condition
time to clear before `max_frame_rejections` strikes escalate to the poison
terminal, while a NACK sequence that is making progress (different frame
each time) resets to the initial dose. RETRIABLE_OTHER recycles route
through the zero-progress pacer (`failExemptPaced`, both pre- and
post-send): the node cannot serve writes at all, so the FIRST recycle stays
immediate — endpoint rotation matters more than backoff for a genuine
failover — but consecutive recycles with no OK-level progress park for the
same doubling, capped dose, so an all-replica window does not drive full
recycle cycles at handshake RTT rate for its whole duration. Transport
failures reconnect immediately (first attempt), with backoff on failed
attempts as before.

This catches everything `isTerminalCloseCode` caught, plus middlebox closes
the list missed, and never false-positives on outages (those fail at connect,
not deterministically on one FSN). `isTerminalCloseCode` is retained for
diagnostics only.

## The read-only / demotion case

The server already handles it at the right layer (Invariant B work): the
read-only gate and the commit-path authorization refusal both set
`roleChangeClosePending` and close with a reconnect-eligible `NORMAL_CLOSURE`
instead of NACKing `SECURITY_ERROR` (`QwpIngressProcessorState`). The client
reconnects, hits the 421 role reject on the now-replica, and retries from SF
until a primary is reachable. Consequently:

- `SECURITY_ERROR` mid-stream can only mean **ACL denial on a writable node**
  → TERMINAL is correct.
- `STATUS_NOT_WRITABLE` (0x0C) is **reserved**, not emitted: a future server
  may NACK it mid-stream once deployed client fleets classify it as
  retriable-with-rotation. Until then the graceful close covers the case with
  no version-gating problem.

## Behavior changes (release notes)

- **`SCHEMA_MISMATCH`: silent drop → loud TERMINAL.** One table's schema
  drift now halts a multi-table sender — the conscious no-silent-loss trade.
- **`WRITE_ERROR`: silent drop → retry forever** (poison detector bounds the
  deterministic case). A persistent write error blocks the stream until SF
  backpressure; loud stall beats silent loss.
- **`UNKNOWN`: fatal → retry.** Future server statuses degrade to retry.
- **Terminal WS close codes: fatal-on-first-sight → reconnect + poison
  detection.**
- **Watermark purity:** `ackedFsn` now advances *only* on server OKs.
  `SenderProgressHandler` no longer needs the "settled ≠ durable because of
  drops" caveat.
- **Source-breaking:** `SenderError.Policy.{DROP_AND_CONTINUE, HALT}` →
  `{RETRIABLE, RETRIABLE_OTHER, TERMINAL}`; `Category.NOT_WRITABLE` added.

## Invariants to hold the line on

- **Replay idempotency:** NACK ⇒ batch atomically not applied server-side.
  RETRIABLE replay depends on it; violating it trades loss for duplication.
- **NACK-before-close:** a server rejecting bytes must NACK before closing;
  a close without a terminal NACK is treated as transport.
- **No watermark advance on any rejection**, ever (fuzz-tested:
  `CursorWebSocketSendLoopDurableAckFuzzTest` asserts trim never crosses a
  rejected frame).

## Test coverage

- `CursorWebSocketSendLoopErrorClassificationTest` — full category → policy
  matrix, incl. `NOT_WRITABLE` and fail-open `UNKNOWN`.
- `ServerErrorAckTerminalTest.testRetriableNackReplaysThenPoisonEscalates` —
  e2e: RETRIABLE NACK → reconnect+replay per strike → typed poison terminal
  after exactly the configured `max_frame_rejections` deliveries (default 4;
  `ServerErrorAckTerminalTest` also pins `max_frame_rejections=2` end-to-end);
  watermark untouched.
- `CursorWebSocketSendLoopDurableAckTest` — TERMINAL NACKs in durable mode:
  no placeholder, no trim, typed `checkError()` throw.
- `CloseTerminalConflationTest` — close() safety net with a RETRIABLE
  rejection preceding a gated TERMINAL.
