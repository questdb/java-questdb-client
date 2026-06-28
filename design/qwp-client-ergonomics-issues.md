# QWP client startup/failover — ergonomics issues

Tracked sharp edges surfaced while reviewing
[`qwp-client-startup-failover-behavior.md`](./qwp-client-startup-failover-behavior.md).
Each entry is grounded in source. "Candidate" = likely defect worth changing;
"Intended (revisit)" = deliberate contract that may still deserve reconsideration.

Severity legend: **P1** user-visible footgun likely to cause an outage or hang ·
**P2** confusing/surprising but recoverable · **P3** polish.

---

## ERG-1 — `initial_connect_retry` is implicitly promoted to SYNC (P1, Candidate)

**Symptom.** A user sets `reconnect_max_duration_millis` for resilience and,
without setting `initial_connect_retry`, their application now **blocks** on
startup for the entire budget when the server is down.

**Source.** `Sender.java` (~line 1451): if `initialConnectMode == null` and any
`reconnect_*` knob is set, the mode resolves to `SYNC`.

**Why it's bad.** Mode is inferred from an unrelated knob. The "make me more
resilient" action produces a "hang my boot" side effect. The code comment itself
acknowledges the knob "reads as a generic retry budget but the underlying path
only governs reconnects."

**Proposed fix.**
- Make initial-connect mode an explicit, independent choice; stop inferring it.
- If inference must stay for back-compat, log a `WARN` when a `reconnect_*` knob
  flips startup to `SYNC`, naming the knob and the resulting blocking behavior.

**Acceptance.** With only `reconnect_max_duration_millis` set and the server
down, `build()` either returns promptly (OFF default) or logs an explicit
warning before blocking. A test asserts the warning / non-blocking default.

---

## ERG-2 — `reconnect_max_duration_millis`: misleading name + inconsistent `0` (P2, Candidate)

**Symptom.** Two confusions:
1. The name implies "reconnect only" but it also bounds the **initial** connect
   in SYNC/ASYNC modes.
2. `reconnect_max_duration_millis=0` means **give up immediately**, whereas
   `idle_timeout_ms=0` and `max_lifetime_ms=0` in the same config surface mean
   **infinite**. There is no infinite-retry mode at all.

**Source.** `CursorWebSocketSendLoop.java:827` — `deadlineNanos = start + dur*1e6`,
loop `while (now < deadline)`; `0` ⇒ zero iterations. Contrast
`QuestDBBuilder.idleTimeoutMillis/maxLifetimeMillis` (`0 ⇒ Long.MAX_VALUE`).

**Why it's bad.** Same `0` token, opposite semantics depending on the knob;
tolerating a long maintenance window forces magic numbers like `86400000`.

**Proposed fix.**
- Adopt one `0` convention. Recommended: `0 ⇒ infinite`, matching the pool
  knobs, which also gives a real infinite-retry mode.
- Consider an alias `connect_retry_budget_ms` that reflects it covers initial +
  reconnect; keep the old key as a deprecated alias.

**Acceptance.** Documented, consistent `0` semantics across the config surface;
test covering `=0` behavior and (if added) infinite mode.

---

## ERG-3 — `failover` does not cover startup; queries have no async connect (P2, Candidate)

**Symptom.** Users expect `failover=on` to make startup resilient. It does not —
it only governs reconnect+replay during `execute()` after a connection exists.
Query initial connect is always synchronous and blocking, with no async/lazy
mode (unlike ingest).

**Source.** `QwpQueryClient.connect()` is synchronous; `failover_*` defaults at
`QwpQueryClient.java:139-141`; spec "Query client behavior".

**Why it's bad.** Expectation mismatch on a safety-critical knob; asymmetry
between ingest (3 modes) and query (1 mode) forces two mental models.

**Proposed fix.**
- Document `failover`'s scope prominently (done in the rewrite).
- Evaluate an async/lazy initial-connect mode for the query client to match
  ingest, or a unified `initial_connect` setting shared by both sides.

**Acceptance.** Either query supports a documented non-blocking initial-connect
mode, or the docs make the scope unambiguous and the limitation is explicitly
accepted.

---

## ERG-4 — No first-class write-only facade (P2, Candidate)

**Symptom.** A write-only user of `QuestDB` must still supply a query config they
never use **and** remember `query_pool_min=0` to avoid a build-time query
connection.

**Source.** `QuestDBBuilder.build()` hard-requires both `ingestConfig` and
`queryConfig`; no write-only path.

**Why it's bad.** Leaky and error-prone; the doc's own recommendation is "prefer
direct `Sender`," which is an admission the facade is awkward here.

**Proposed fix.**
- Add `QuestDB.builder().ingestConfig(...).writeOnly()` (or a `writeOnly()`
  shortcut) that skips the query pool entirely.
- Symmetric `readOnly()` is a natural follow-up.

**Acceptance.** A write-only facade builds with no query config and creates no
query pool; documented and tested.

---

## ERG-5 — A single endpoint's `401`/`403` aborts the whole walk (P2, Intended, revisit)

**Symptom.** One misconfigured endpoint returning `401`/`403` blocks startup
even when other listed endpoints would accept the credentials. Applies to both
ingest and query walks, including at startup.

**Source.** Ingest/query endpoint matrices; `CursorWebSocketSendLoop` treats
`QwpAuthFailedException` as terminal across all endpoints.

**Why it's debatable.** "Fail fast on bad credentials" is reasonable, but it is
asymmetric with how every *transport* failure is tolerated, and surprising
during rolling credential rotation or a single bad node.

**Proposed fix (revisit).**
- Keep terminal-on-auth as the contract, but make it a deliberately documented
  contract (done in the rewrite).
- Consider an opt-in (e.g. `auth_failure=continue`) that demotes auth failure to
  a per-endpoint skip for heterogeneous fleets.

**Acceptance.** Behavior documented as intentional; decision recorded on whether
an opt-in continue mode is warranted.

---

## ERG-6 — Facade can't reach error handler / connection listener / serverInfoTimeout (P2, Candidate)

**Symptom.** Through the `QuestDB` facade you cannot install a
`SenderErrorHandler` or `SenderConnectionListener` (ingest), nor set
`serverInfoTimeoutMs` (query). The latter has no config key at all.

**Source.** `LineSenderBuilder.errorHandler()/connectionListener()` exist only on
the direct sender builder; `serverInfoTimeoutMs` is a `QwpQueryClient` builder
field with no `ConfigSchema` key (`ConfigSchema.java` EGRESS section).

**Why it's bad.** The facade is the recommended high-level entry point, yet it
cannot configure observability hooks or a documented query timeout.

**Proposed fix.**
- Expose ingest error handler / connection listener on `QuestDBBuilder`
  (per-pool or shared).
- Add a `server_info_timeout_ms` config key so it is reachable from any conn
  string (and therefore the facade).

**Acceptance.** Both hooks and the timeout are reachable from the facade;
documented in the knob-availability matrix.

---

## ERG-7 — Simplest API has the worst error visibility (P1, Candidate)

**Symptom.** `Sender.fromConfig(cfg)` with `initial_connect_retry=async` swallows
terminal startup failures — they surface only on a later producer call or at
`close()`. The visible path requires switching to `Sender.builder(...)` and
installing a handler.

**Source.** Async terminal `SenderError` delivered to a configured
`SenderErrorHandler`; "even without a handler they are surfaced by later producer
calls or close-time safety net behavior."

**Why it's bad.** The nicest ergonomics and the worst observability are
inversely correlated for the single most important question: "did my writer ever
connect?"

**Proposed fix.**
- Default to a sane error sink (e.g. `WARN`/`ERROR` log on terminal async
  failure) even without a registered handler.
- Provide a lightweight status accessor (e.g. `wasEverConnected()` /
  `lastError()`) on the public `Sender` surface for poll-based checks.

**Acceptance.** A terminal async failure is observable without installing a
custom handler; documented and tested.

---

## ERG-8 — No client-side TCP connect timeout (P2, Intended, revisit)

**Symptom.** A black-holed host in the `addr` list blocks the endpoint walk
until the OS connect timeout, undercutting the resilience value of listing
multiple endpoints.

**Source.** `auth_timeout_ms` bounds only the post-connect upgrade/auth phase;
no separate application-level TCP connect timeout in the transport.

**Why it's debatable.** It is a transport limitation, but it directly defeats the
multi-endpoint failover use case at startup.

**Proposed fix (revisit).**
- Add a client-side connect timeout so the walk can abandon black-holed hosts
  and proceed to the next endpoint.

**Acceptance.** A black-holed first endpoint no longer blocks past a configurable
bound before the walk advances; documented and tested.

---

## Suggested sequencing

1. **ERG-1** and **ERG-7** (both P1) — they cause hangs and silent failures.
2. **ERG-2**, **ERG-4**, **ERG-6** (P2 Candidate) — naming/consistency and
   facade completeness.
3. **ERG-3**, **ERG-8** (P2, need design) — async query connect and connect
   timeout.
4. **ERG-5** — confirm/record the auth-terminal contract; opt-in continue mode
   only if a concrete fleet use case justifies it.
