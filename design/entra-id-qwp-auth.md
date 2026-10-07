# Entra ID app-only auth over QWP: findings and proposed design

Status: **implemented** on `feat/qwp-entra-token-provider` - all four steps of the Java plan (§10). §11 maps each step to the code and to the conformance tests, and lists where the code departs from this document's earlier design.

**Read these first:**

- **The spec wins.** The normative, language-neutral contract is now `design/qwp-token-provider-spec.md` (v0.4, all decisions resolved). Where the two documents differ, follow the spec. That includes the refresh-schedule clamps, backoff jitter, registry linger, and the Java names in its Appendix B, which supersede §7.2 here.
- **Java plan:** §10.
- **Line references** are as of `c9968f24` and may drift.

Code inspected:

- **Client:** `java-questdb-client` `main` @ `c9968f24`.
- **Server:** QuestDB Enterprise at the time of writing (private source, so this doc describes server behaviour only), and the open-source `questdb` core @ `c06b4c8267`.
- **Rust core** (what the Python client wraps): `c-questdb-client` `main` @ `5ea51a47`.
- **Spec:** `documentation` @ `7c35c02`, files `qwp-ingress-websocket.md` and `qwp-egress-websocket.md`. The spec pins client `67bb5e4` (2026-05-13).

Abbreviations for client files, all under `core/src/main/java/io/questdb/client/`:

| Abbrev. | File |
|---|---|
| `S` | `Sender.java` |
| `QWS` | `cutlass/qwp/client/QwpWebSocketSender.java` |
| `CWSL` | `cutlass/qwp/client/sf/cursor/CursorWebSocketSendLoop.java` |
| `BD` | `cutlass/qwp/client/sf/cursor/BackgroundDrainer.java` |
| `QQC` | `cutlass/qwp/client/QwpQueryClient.java` |
| `WSC` | `cutlass/http/client/WebSocketClient.java` |

## TL;DR

1. **The token-provider hook already exists and is wired into every QWP upgrade path.**
   - The hook is `HttpTokenProvider`, added in #52.
   - With a provider, Java pulls a fresh token on every connect round: the initial connect, reconnect, failover, orphan drainers, pool recovery, egress connect and egress failover.
   - Only a static `token=` / `httpToken()` is captured once.
   - So the work is not plumbing. What's missing is:
     - an expiry-aware, shared, proactively refreshing cache;
     - classifying provider failures at startup;
     - a "token rejected" signal from the client back to the provider;
     - a way to select a provider from a connect string;
     - spec updates and Rust/Python parity.
2. **The brief's premise ("401 is terminal") no longer matches Java, but it does match the Rust core.**
   - Since #66 and #52, a Java foreground sender that has connected once retries 401/403 forever while store-and-forward (SF) buffers.
   - 401/403 is terminal only during initialization, for orphan drainers (with a bounded ride-out for rotating credentials), and on egress.
   - The Rust core treats `AuthError` as terminal on reconnect and sends a static header. That is the Python customer's likely client, and it has exactly the failure described in the brief.
   - The public spec is stale on both points.
3. **The server works for app-only Entra tokens, but only in local-JWT mode.** Required settings:
   - `acl.oidc.groups.encoded.in.token=true`, `acl.oidc.groups.claim=roles`, `acl.oidc.sub.claim=oid`;
   - QuestDB app registration set to issue v2 tokens.

   The default UserInfo mode cannot work for app-only tokens. A QuestDB 401 also covers "IdP or JWKS unreachable", so it is not proof that the token is bad.
4. **Recommendation:** do (a) now (core cache and SPI) plus (c), a thin optional `questdb-client-azure` module. That module adapts `TokenCredential` and registers a connect-string provider through `ServiceLoader`. Defer (b), the built-in IMDS provider.

## 1. Current auth configuration

**Ingress (`Sender`)**

- **Schema and TLS:**
  - `ws::` or `wss::` (`S:3540-3549`); `wss` enables TLS.
  - TLS keys: `tls_verify=on|unsafe_off`, `tls_roots` (PEM or JKS), `tls_roots_password`. All are rejected on `ws::` (`S:4245-4250`).
  - Builder equivalents: `enableTls()` (`S:2124`), and `advancedTls().customTrustStore(..)` / `.disableCertificateValidation()` (`S:4572-4624`).
- **Auth keys:**
  - Keys are `username`/`password` (aliases `user`/`pass`) and `token`, registered in `impl/ConfigSchema.java:49-59`.
  - Unknown keys are rejected (`impl/ConfigView.java:74-86`).
  - The keys are applied in `fromConfigWebSocket` (`S:4029-4036`).
  - Cross-key rules in `validateWsConfig` (`S:4227-4255`): Basic auth needs both halves, and `token` is exclusive with Basic.
- **Builder:**
  - `httpToken` (`S:2284`), `httpUsernamePassword` (`S:2372`), `httpTokenProvider` (`S:2343`); all three are mutually exclusive.
  - `auth_timeout_ms` bounds the upgrade (default 15 s). `connect_timeout` bounds the TCP connect.
- **Where the header is built:** `buildWebSocketAuthHeader` (`S:3427-3460`).
  - Basic and static Bearer values go through `QwpWebSocketSender.fixedAuthHeader`, which tags them as constant.
  - A provider becomes a lambda that calls `getToken()`, snapshots it, runs `HttpTokenProvider.validateToken`, and returns `"Bearer " + token`.
  - That lambda is passed as `Supplier<String>` to `QwpWebSocketSender.connectWithCredentialSupplier` (`S:1468`, `S:1694`).
- **On the wire:**
  - `WSC.upgrade(path, timeout, authorizationHeader)` writes `Authorization: <value>` (`WSC:640-722`, header at `:700-704`).
  - The ingress path is `/write/v4` (`QWS:148`). The client never uses `/api/v4/write`.

**Egress (`QwpQueryClient`)**

- Same keys, read in `fromConfig` (`QQC:375-490`, auth at `:417-419` and `:485-486`).
- Programmatic API: `withBasicAuth`, `withBearerToken`, `withBearerTokenProvider` (`QQC:1127`, `:1147`, `:1177`; mutually exclusive), plus `withTls()` / `withTrustStore()` / `withInsecureTls()`.
- The header is built in `resolveAuthorizationHeader()` (`QQC:1933-1960`).
- The upgrade to `/read/v1` happens in `runUpgradeWithTimeout` (`QQC:1971-1996`).

**`QuestDB` facade**

- `QuestDBBuilder.httpTokenProvider` (`QuestDBBuilder.java:330`) rejects a provider combined with `token`, `username` or `password` in the config (`:190-194`). There is also `QuestDB.connect(cs, provider)` (`QuestDB.java:105`).
- One provider instance is shared by every pooled sender (`SenderPool.java:540-544`, `:2046-2051`) and every pooled query client (`QueryClientPool.java:599-600`).

## 2. Captured once, or re-read per upgrade?

| Path | Code | Credential pull |
|---|---|---|
| Static `token=` / `httpToken` / Basic | `S:3431-3439`, `FixedAuthHeader` `QWS:5328` | **Captured once.** An expired token is presented forever. |
| Provider: ingress initial connect (OFF/SYNC) | `ensureConnected` `QWS:3999-4055` → `buildAndConnect` → `connectWalk` | Once per round, before the endpoint walk (`QWS:3305-3351`), on the thread calling `build()`. |
| ASYNC initial connect, and every reconnect | `CWSL.connectLoop` (`:1731`) → `reconnectFactory.reconnect(cancellation)` | Once per round, on the cursor I/O thread. |
| Failover to another host | Inside the same round (`QWS:3352-3394`) | **The same token is reused for every endpoint in the round**; the next round re-pulls. This is deliberate: a token is cluster-wide. |
| SF replay after reconnect | Replay starts on the new socket after the upgrade | No extra auth; covered by the reconnect pull. |
| Orphan drainers (SF recovery of other slots) | Background `ReconnectSupplier`, `QWS:2860`; same supplier | Once per round, on a drainer-pool thread. |
| `SenderPool` startup-recovery delegates | `SenderPool.java:2079-2081` | OFF-mode build with the provider. |
| Egress connect | `QQC:754-786` | Once per `connect()` walk. |
| Egress mid-query failover | `QQC:1576-1683` → `reconnectViaTracker` `:1865-1931` | Once per failover reconnect. |

These behaviours are already pinned by tests:

- `WebSocketTokenProviderTest`: `testProviderRequeriedOnEveryReconnect`, `testThrowingProviderResolvedOncePerConnectRound`, and others.
- `QwpQueryClientTokenProviderTest.testProviderTokenReResolvedOnFailoverReconnect`.
- `SenderPoolSfTokenProviderTest`.

## 3. Failure classification

**Where failures are classified**

- `QwpUpgradeFailures.classify` (`:41-57`):
  - 421 with `X-QuestDB-Role` → role reject (transient);
  - 401 or 403 → `QwpAuthFailedException`;
  - anything else passes through as `WebSocketUpgradeException` or `HttpClientException`.
- A provider that throws is wrapped as `QwpCredentialUnavailableException` (`QWS:3334-3344`).
- The policy applied to each class is decided per phase:

| Phase | 401/403 | Other non-421 upgrade reject (404/426/5xx) | Provider threw |
|---|---|---|---|
| OFF initial connect (the default) | thrown from `build()` | thrown: latched as `terminalUpgradeError` (`QWS:3442-3447`), thrown at round end (`:3593-3596`) | the provider's own exception is thrown (`QWS:4040-4054`) |
| SYNC initial connect (`initial_connect_retry=on`, or any `reconnect_*` key set) | terminal, no retry (`CWSL:1041-1052`) | terminal, no retry | **fails fast** (`CWSL:1053-1065`); pinned by `testThrowingProviderFailsFastInSyncInitialConnect` |
| ASYNC, before the first connect | terminal: goes to the error inbox and is rethrown on `close()` (`CWSL:1825-1867` via `endpointPolicyFailureIsTerminal`, `:2111`) | terminal | retried forever (`CWSL:1928-1963`) |
| Foreground sender after its first connect | **retried forever** with capped backoff; a RETRIABLE `SECURITY_ERROR` is dispatched per attempt (`CWSL:1876-1886`) | retried forever | retried forever |
| Orphan drainer | constant credential: slot quarantined (`.failed`). Rotating credential: ride-out of ≥6 attempts **and** ≥ min(`reconnect_max_duration`, 5 min), capped at 256 attempts (`BD:97-160`, `:481-556`) | quarantined | retried forever |
| Egress connect / failover | thrown from `connect()` (`QQC:785`); during failover, `onError("auth failure during failover reconnect")` (`:1665-1676`) | next endpoint; error if every endpoint fails | `LineSenderException`; during failover, `onError("failover reconnect failed")` |

**Where the brief, the spec and the code disagree**

- **Spec vs Java.** The spec (`qwp-ingress-websocket.md:1055-1063`) says "401/403 terminal at any host" and "all other upgrade errors transient (404, 426, 503…)". It was accurate at `67bb5e4`: the reconnect loop issued `HALT` on 401/403 (verified). #66 (`37d4b0ab`) and #52 (`0b9b5766`) changed that to the table above. Also, non-421 4xx/5xx is **terminal** during initialization, contrary to the spec. For example, a single node answering 503 fails a SYNC `build()` immediately.
- **Rust core (Python).**
  - The header is a static `auth_header: Option<String>` (`questdb-rs/src/ingress.rs:497`, `:599`, `:664`).
  - `reconnect_error_is_terminal` treats `AuthError` as terminal on reconnect (`questdb-rs/src/ingress/sender/qwp_ws_driver.rs:2596-2603`).
  - So the brief's failure mode is real there.
- **Egress pool (suspected bug; not yet reproduced).** After a failed failover reconnect, the client is left with `connected=false` (`QQC:1623`). The worker still returns to the pool, and every later `execute()` throws `"QwpQueryClient not connected"` (`QQC:1581`). `reapIdle` never removes a worker while the pool is at `query_pool_min` (`QueryClientPool.java:500`). A 401 during failover is one way in. This needs a red test before anything else.

## 4. Threading and the initial-connect budget

**Which thread connects**

- **OFF/SYNC initial connect:** the thread calling `Sender.build()` or `QuestDB.build()` (pool pre-warm). Pool growth runs on the acquiring thread; recovery delegates run on the housekeeper thread. None of these paths has a `ConnectCancellation`.
- **ASYNC initial connect and every reconnect:** the sender's cursor I/O thread.
- **Orphan drainers:** `BackgroundDrainerPool` threads.
- **Egress:** the caller of `connect()`; failover runs on the thread executing `execute()` (in the pool, the `QueryWorker` dispatch thread).

**What can block within one attempt**

- **`getToken()`:** this is caller code, and nothing in the client bounds it. `close()` interrupts it, but only on I/O-thread and drainer paths (`CWSL` `ConnectCancellation.cancel`, near `:3764-3790`).
- **DNS:** bounded only by the OS.
- **TCP connect:** bounded by `connect_timeout`. The foreground default is 0, which means the OS SYN timeout of 60-130 s. Background connects default to 15 s (`QWS:3139-3141`).
- **TLS handshake:** bounded by `connect_timeout`, or by the request timeout when that is unset.
- **Upgrade:** bounded by `auth_timeout_ms`.
- Producers are not blocked by any of this until SF fills and append backpressure kicks in.

**How the budget interacts with a slow token fetch**

- `connectWithRetry` checks `reconnect_max_duration_millis` only *between* attempts (`CWSL:1029`).
- A slow `getToken()` therefore stretches `build()` past the budget: an attempt is never cut short.
- A throwing `getToken()` ends SYNC immediately.
- ASYNC and reconnect have no budget (Invariant B).
- **Entra consequence:**
  - The first `DefaultAzureCredential` call runs unbounded on the `build()` thread. That call includes chain probing, an IMDS cold start, and the SDK's own retries on 429.
  - An IMDS 429 that escapes the SDK fails OFF and SYNC startup.

## 5. Constraints from repo conventions

- **Dependencies:**
  - The core's only runtime dependency is `slf4j-api` (annotations are `provided`).
  - Java 8 is the API floor, and the code must also compile on JDK 11+.
  - So no `azure-core` or `azure-identity` in core. `ServiceLoader`, `CompletableFuture` and `ScheduledExecutorService` are all Java 8.
- **Allocation:**
  - Zero-GC is mandatory on per-row producer calls and the steady-state I/O loop (`.pi/skills/review-pr/SKILL.md:37`, `:583-590`).
  - Connect paths already allocate: a `WebSocketClient` per attempt, and `"Bearer " + token`.
  - A cache hit only needs to be cheap: no I/O and no locks.
- **Config flow:**
  - Ingress: string → `ConfigString` → `ConfigView` (strict `ConfigSchema` registry) → `fromConfigWebSocket` setters → `build()` → `connectWithCredentialSupplier(..., Supplier<String>, ...)`.
  - Egress: `QwpQueryClient.fromConfig` → `with*` setters.
  - The facade validates both sides (`QuestDBBuilder.java:184-199`).
  - New keys must go into `ConfigSchema` and are shared vocabulary with every client.
- **Test harnesses:**
  - `TestWebSocketServer`: captures `Authorization`, supports `setRejectWithStatus` and `setRejectWithRole` and drop handlers, and caps requests at 8 KB.
  - `MockOidcServer`: a raw-socket HTTP mock that can return JSON, chunked, dropped or dribbled responses.
  - `assertMemoryLeak`, and `HandOffCharSequence` for TOCTOU tests.
  - Scripted `ReconnectFactory` stubs, as in `BackgroundDrainerMidDrainAuthRejectTest`.
- **API compatibility:** `ExportedApiCompatibilityTest` pins exported signatures. Adding is fine; retyping is not.

## 6. Problems with ~2 KB rotating bearer tokens

**Size limits**

- **Client buffers:** fine. The upgrade request buffer starts at ≥64 KiB and grows (`WSC:181-186`, `WebSocketSendBuffer.java:504-515`). The response buffer is ≥64 KiB.
- **Server header buffer:**
  - Production default is 64,448 B (`PropServerConfiguration.java:1213`; the code says `32 * 2014`, a harmless typo for 1024).
  - `DefaultHttpContextConfiguration`, used by embedded and test servers, allows only 4,096 B. A 2 KB token fits; a role-heavy token may not.
  - `TestWebSocketServer` drops any request over 8,192 B.
- **Connect string:** no length limit. A `;` must be doubled, and control characters are rejected (`ConfStringParser.java:164-183`). JWTs are safe.

**Rotation**

- One token serves the whole round.
- With the foreground `connect_timeout=0`, a round across several black-holed hosts can run for minutes, so a near-expiry token can expire mid-round.
- The cache's freshness margin and the 401 retry (§7.7) cover this.

**Where a token could leak**

- **Already safe:**
  - `validateToken` never echoes the token (`HttpTokenProvider.java:70-80`).
  - `WebSocketUpgradeException` carries only the status line (`WSC:1346-1357`).
  - `QwpAuthFailedException` carries status, host and port only.
  - The header is never logged.
- **Watch:**
  - `QwpCredentialUnavailableException` uses the *provider's* exception message as its own. That message flows into `SenderError` (`CWSL:1954`) and into logs (`CWSL:1957`, `BD:720`), so provider error text must be token-free.
  - `HttpClient.Request.toString()` dumps the raw request, headers and body included (`HttpClient.java:580-586`). An HTTP-based token source must never log or wrap a `Request`: a client-credentials body carries `client_secret`.
  - Test-only config snapshots expose the token (`S:4330`, `QQC:972`).
  - The upgrade bytes stay in the native send buffer after the handshake until frames overwrite them. This is low risk.
- **Server-side:** server findings that touch security are reported privately (see `SECURITY.md`).

## 7. Design

### 7.1 Shape

```
TokenSource         fetchToken() -> ExpiringToken(token, expiresAt[, refreshAt])
     ▲                          Azure TokenCredential adapter (module), future IMDS, anything else
RefreshingTokenProvider         shared cache: background refresh, single-flight, jitter, backoff
     ▲ implements HttpTokenProvider
existing plumbing               Sender / QwpQueryClient / QuestDB pools: call sites unchanged
```

Keep `HttpTokenProvider` as the single integration point; it is already in every path in §2. Add an expiry-aware source SPI behind it. The cache belongs in **core**: it is generic (OIDC client credentials, Vault, …), and it is the part every client must mirror.

### 7.2 API (core, Java 8)

```java
// io.questdb.client.cutlass.auth (exported)
public final class ExpiringToken {          // name avoids clashing with azure-core AccessToken
    public ExpiringToken(String token, long expiresAtEpochMillis);              // validates printable ASCII
    public ExpiringToken(String token, long expiresAtEpochMillis, long refreshAtEpochMillis); // 0 = derive
    // toString(): "ExpiringToken{<redacted, 1834 chars>, expiresAt=...}"
}

@FunctionalInterface
public interface TokenSource {
    // Only ever called on the provider's refresher thread, one call at a time. May block.
    // Throw TokenUnavailableException to classify; any other RuntimeException counts as retryable.
    ExpiringToken fetchToken();
}

public final class RefreshingTokenProvider implements HttpTokenProvider, QuietCloseable {
    public static Builder builder(TokenSource source); // margins, coldWaitMillis, backoff, clock/scheduler seams
    public CharSequence getToken();                    // warm: volatile read, no I/O; cold: bounded wait
    public void onTokenRejected(CharSequence token, int httpStatus);  // forced refresh, rate-limited
    public boolean awaitFirstToken(long timeoutMillis);               // optional startup readiness gate
    public void close();                                              // stops the refresher thread
}

// io.questdb.client (exported)
public class TokenUnavailableException extends LineSenderException {
    public boolean isRetryable();          // IdP unreachable / timeout / 429 / 5xx -> true
    public long getRetryAfterMillis();     // -1 if none
}

// HttpTokenProvider (existing): one default method added; still a @FunctionalInterface
default void onTokenRejected(CharSequence token, int httpStatus) { }
```

Under option (a), the Azure adapter is user code (or a README snippet):

```java
TokenCredential cred = new DefaultAzureCredentialBuilder().build();
TokenRequestContext ctx = new TokenRequestContext().addScopes("api://<questdb-app-id>/.default");
RefreshingTokenProvider tokens = RefreshingTokenProvider.builder(() -> {
    com.azure.core.credential.AccessToken t = cred.getTokenSync(ctx);
    return new ExpiringToken(t.getToken(), t.getExpiresAt().toInstant().toEpochMilli());
}).build();
QuestDB db = QuestDB.builder().fromConfig("wss::addr=qdb1:9000,qdb2:9000;").httpTokenProvider(tokens).build();
```

### 7.3 Connect-string surface

```
wss::addr=qdb1:9000,qdb2:9000;token_provider=azure;azure_resource=api://<questdb-app-id>;azure_client_id=<uami-client-id>;
```

- **New keys,** reserved in the spec for all clients and registered as `COMMON` in `ConfigSchema`:
  - `token_provider=<name>`;
  - `azure_resource`, from which the scope `<resource>/.default` is derived;
  - optional `azure_client_id`, for a user-assigned managed identity or a workload-identity client ID.
- **Safe to log:** none of these values is secret, so they can live in `QDB_CLIENT_CONF`.
- **Exclusivity:** mutually exclusive with `token`, `username`, `password`, and a programmatic `httpTokenProvider`.
- **Resolution:**
  - Core defines a small `TokenProviderFactory` SPI, looked up by name through `ServiceLoader` (with a `uses` clause in `module-info`).
  - Core ships no Azure code. `token_provider=azure` without the module fails at parse time: `"token_provider=azure requires questdb-client-azure on the class path"`.
- **Sharing:**
  - A process-wide registry keyed by provider name plus canonical parameters hands out one ref-counted `RefreshingTokenProvider`.
  - Every `Sender`, `QwpQueryClient` and `QuestDB` built from equivalent strings therefore shares one cache, so a process makes at most one IMDS/Entra call at a time.
  - The last `close()` stops the refresher.

### 7.4 Options

| | (a) Hook and cache in core | (b) Built-in IMDS in core | (c) `questdb-client-azure` module |
|---|---|---|---|
| Runtime dependencies | none | none | `azure-core` (the user brings `azure-identity`) |
| Azure hosts covered | all, through the user's SDK | VM/VMSS IMDS only. Not App Service/Functions (`IDENTITY_ENDPOINT`), AKS workload identity (federated token exchange), or Arc | all, through `DefaultAzureCredential` |
| Service principal with client credentials | yes | no | yes |
| Connect-string-only deployments (e.g. Kafka connector) | no | yes | yes, through the SPI |
| Maintenance | small | we own an Azure protocol: retry rules for 404/410/429, `expires_on` formats, identity selectors | tracks the `azure-core` API; another artifact to build on JDK 8 and 11 |

**Recommendation:**

- Ship (a) and (c) together.
- Add (b) only if a zero-dependency, connect-string-only VM deployment appears. It would plug into the same cache as `token_provider=azure_imds`, with an endpoint override for tests.

### 7.5 Where it plugs in (changes only)

| Path | Change |
|---|---|
| All ingress and egress pulls | None to the call sites. A warm cache makes the pull a memory read. `buildWebSocketAuthHeader` returns a small named credential class (dynamic, with `onRejected`) instead of a lambda. |
| `connectWalk` 401 branch (`QWS:3427`) | If the credential is dynamic and the status is 401: call `onTokenRejected`, re-pull, and retry the **same endpoint once** if the token changed. No host-health penalty for the first 401. `AUTH_FAILED` fires only on the final failure. |
| SYNC `connectWithRetry` credential branch (`CWSL:1053`) | A retryable `TokenUnavailableException` becomes transient within the budget. Anything else still fails fast, so OIDC device flow behaviour is unchanged. |
| Foreground reconnect and orphan drainer | Nothing extra. Each attempt re-pulls, and `onTokenRejected` already fired in the walk, so the rotating-401 ride-out now actually receives new tokens. |
| Egress `connect()` / `reconnectViaTracker()` (`QQC:785`, `:1889`) | The same 401 retry-once. |
| Egress pool | Separate fix: do not return a worker whose failover reconnect failed. Red test first (§3). |
| `ConfigSchema`, `fromConfig` paths, `QuestDBBuilder` | Register the keys; resolve them through the registry; add mutual-exclusion checks. |

Keep "one pull per round" rather than one per endpoint. It is deliberate (`QWS:3310-3320`), it keeps provider failures cluster-wide, and the cache margin plus the 401 retry cover long rounds.

### 7.6 Caching and refresh

- **Single-flight.** One daemon refresher thread per provider is the only caller of `fetchToken()`, so each process makes at most one concurrent call per source. This is what keeps it within the IMDS limits of 5 concurrent requests and 20 requests per second.
- **Schedule.** Computed at fetch time:
  - `refreshAt = fetchedAt + (expiresAt - fetchedAt) × U(0.45, 0.55)`, clamped to the range `[fetchedAt + 30 s, expiresAt - 5 min]`.
  - A source hint wins if it is earlier (newer azure-core and MSAL expose a refresh-at time).
  - A service-principal token (60-90 min) is refreshed about every 30-45 min.
  - For a managed-identity token (~24 h), IMDS may keep returning the platform-cached token. The schedule then halves the remaining time on each fetch, which is a handful of calls per token lifetime.
- **Hand-out.**
  - A token with at least 60 s left is returned from memory: no I/O, no lock.
  - With no usable token, `getToken()` asks for a fetch and waits up to `coldWait` (default 30 s). The wait is interruptible, so `close()`'s interrupt still works. If it times out, it throws a retryable `TokenUnavailableException`.
  - An expired token is never handed out.
- **Failures.**
  - Retry with exponential backoff and full jitter, from 0.5 s up to a 60 s cap, honouring `retryAfterMillis`.
  - Keep serving the current token while it is above the hand-out floor.
  - Log at most one WARN per minute, without the token. Expose `lastFailure()`.
- **`onTokenRejected`.**
  - Applies only if the rejected token is still the current one, and at most once per 30 s.
  - It triggers an immediate fetch; the walk waits up to 5 s for a *different* token.
  - If the result is the same token (the managed-identity platform cache), it is accepted, and the walk falls back to the phase policy.
  - Don't oversell this: for managed identity, proactive refresh with a margin is the real fix. The forced refresh mainly helps client credentials and clock skew.
- **Startup.** The provider starts its first fetch at construction. `awaitFirstToken` lets an application gate `build()` on it. `lazy_connect`/ASYNC never blocks on it.
- **Clocks.** Wall clock for expiry, `nanoTime` for scheduling; validity is re-checked at hand-out.

### 7.7 Error classification and the 401 policy

- **Provider failures:**
  - **Transient** (retryable): unreachable, timeout, 429, 5xx, IMDS 404/410.
  - **Permanent:** identity not found, malformed configuration.
  - **Behaviour by phase:**
    - OFF fails fast after `coldWait`, which already absorbs short IMDS 429 storms.
    - SYNC retries transient failures within the budget and fails fast on permanent ones.
    - ASYNC, the established foreground sender and the drainers retry forever, as today.
- **401 with a refreshable credential:** one immediate retry with a *different* token, in every phase. After that, the existing phase policy applies.
- **403:** no forced refresh. 403 is authorization: an app-role or alias change, and for managed identity a new token may not appear for up to 24 h. The existing phase policy applies.
- **Spec impact (needs a cross-client decision):**
  1. Replace "401/403 terminal at any host" with the phase table in §3. Recommendation: adopt Java's policy. A QuestDB 401 can be an IdP outage, and SF promises Invariant B. The other option is to revert Java, which would recreate the brief's problem for every SF user.
  2. Add a "credential sources" section:
     - pull per upgrade round;
     - reuse within a round;
     - proactive refresh, a SHOULD;
     - never log tokens, a MUST;
     - printable-ASCII validation, a MUST.
  3. Add the 401 rule: a client SHOULD invalidate and retry the same endpoint once if a different credential is obtained. The retry counts toward neither backoff nor host health.
  4. Define the retryable/permanent split for credential-acquisition failures, including the SYNC change.
  5. Reserve `token_provider`, `azure_resource` and `azure_client_id`.
  6. Separately: fix the "all other upgrade errors are transient" text against the code, or fix the code (§3).

### 7.8 Redaction

- **Rules for the new code:**
  - **`toString()`:** never prints token text. This applies to `ExpiringToken`, the provider and registry entries; they show length, expiry, and at most an 8-hex-digit SHA-256 prefix for correlating rotations.
  - **Response bodies:** a source never logs or embeds one, because they contain `access_token`. On a parse failure it reports the field or shape only.
  - **Error text from the IdP or SDK:** passed through `DisplaySafe` filtering and capped at about 256 chars.
  - **Exceptions:** never carry an HTTP `Request` as message or cause.
- **Optional hardening:**
  - Zero the upgrade bytes in the send buffer after sending. It is cheap: under 4 KB.
  - Refuse `token_provider=*` on `ws::` (cleartext bearer), and WARN once for a programmatic provider on `ws::`. To decide in §9.

### 7.9 Language-neutral contract (for Python and Rust)

- **Source:** returns `(token, expires_at)`, plus an optional `refresh_at`.
  - Python: `lambda: cred.get_token(scope)` already returns `AccessToken(token, expires_on)`.
  - Rust: `Fn() -> Result<(String, SystemTime), TokenError>`.
  - C: a callback with out-parameters.
- **Cache:** the same semantics and defaults in every client, with one cache per source instance and the same registry for connect-string providers.
- **Python:** keeping the cache in the Rust core means the Python callable runs only on the refresher thread, rarely. That keeps GIL hand-offs off the I/O threads.
- **Rust core work:**
  - build the header per upgrade attempt instead of `auth_header: String`;
  - add the callback and classification;
  - decide the reconnect `AuthError` policy (see §7.7, item 1).

### 7.10 Test plan

**1. Unit tests for `RefreshingTokenProvider`.** Use a fake clock and scheduler and a scripted source. Cover:
- the schedule: the halving formula, the clamps, jitter bounds, hint precedence, and managed-identity convergence when the source keeps returning the same token;
- single-flight: 64 threads cold-starting at once produce one fetch;
- backoff and `Retry-After`;
- serving the current token until the floor;
- cold failure: a retryable exception is thrown within `coldWait`;
- an interrupt during the wait: the exception is thrown and the interrupt flag is preserved;
- `onTokenRejected`: rate limit, same-token result, and ignoring a stale token;
- that `close()` leaves no thread behind.

**2. Stub server that rejects expired tokens.** Extend `TestWebSocketServer` with `setAuthorizationValidator(Function<String,Integer>)`. Tokens look like `T<n>.<expiresAtMillis>`, and the server shares the test's fake clock, returning 401 once a token has expired.

- **Proactive refresh on reconnect:** advance the clock past T1's expiry and drop the socket. The reconnect must carry T2 with **no** 401 seen.
- **Stalled refresher:** stall the refresher so the reconnect still carries T1. Expect exactly one 401, then an immediate retry with T2 (no backoff gap), and the batch lands.
- **Initial-connect matrix:** OFF, SYNC and ASYNC × a cold provider failure that is retryable or permanent. SYNC must retry the retryable case within the budget. `testThrowingProviderFailsFastInSyncInitialConnect` must stay green.

**3. Failover and SF replay across a rotation.** Use two validating servers, A and B, with `addr=A,B` and `sf_dir` set.

- **Failover:** connect to A with T1, rotate, then kill A. B must receive T2, and the unacked frames are replayed exactly once (assert rows and FSNs at B). Add a variant where the refresher is stalled, so there is one 401, then the retry.
- **SF replay:** the server rejects T1 with 401 until the rotation while the producer keeps writing. Every row must arrive, with no `.failed` sentinel, no `DATA_LOSS`, and no terminal.
- **Orphan drainer:** restart with an orphan slot and `drain_orphans=on`. The drainer drains with the rotated token.

**4. Facade and egress.**
- A `QuestDB` handle with 4 senders and 2 query clients sharing one provider. Force a reconnect storm: `fetchToken` runs once, and every upgrade carries the same fresh token.
- Egress: failover in the middle of a query with a rotated token, and the 401 retry-once on `connect()`.
- A red test for the dead pooled egress worker (§3).

**5. Connect string.** Cover:
- parse and validation errors;
- mutual exclusion;
- the "module missing" message;
- registry sharing across `fromConfig` instances, and ref-count release;
- rejection on `ws::`, if adopted.

**6. Redaction.** Use a sentinel token and capture every logger with a logback `ListAppender`. Run the 401, provider-failure and malformed-response scenarios, then assert the sentinel appears nowhere in logs, exception messages, `SenderError` messages or `toString()` output.

**7. Module (c).** Test against a fake `TokenCredential`, with no network: the expiry/refresh-at mapping, and mapping exceptions to retryable or permanent. Add a manual smoke run on an Azure VM with a managed identity; this is not for CI.

**8. Fake IMDS (only if we do (b)).** Build it on `MockOidcServer`. Assert `Metadata: true`, `api-version`, `resource` and `client_id`. Script 200, 400, 404, 410, 429 with `Retry-After`, 500, malformed JSON, and both `expires_on` formats. Error messages must never echo the response body.

**9. General.** Run everything under `assertMemoryLeak`. Tests run on JDK 8; also compile on JDK 25.

**10. Server e2e (Enterprise, separate PR).** Extend the Enterprise OIDC tests with locally signed tokens shaped like Entra app-only v1 and v2 tokens. Assert:
- v2 with `sub.claim=oid` and `groups.claim=roles` is accepted;
- v1 with `aud=api://…` is rejected under the default audience;
- no roles gives 401;
- an expired token gives 401, and a rotated token then succeeds.

## 8. Server side (QuestDB Enterprise)

This section describes server behaviour only. Enterprise source is private, and security-relevant server findings are reported privately (see `SECURITY.md`).

**Local JWKS validation or UserInfo?** Both exist. The switch is `acl.oidc.groups.encoded.in.token`; its default, `false`, means UserInfo.

- **UserInfo cannot work for app-only tokens.** Entra's UserInfo is a Microsoft Graph endpoint: it accepts only Graph-audience user tokens.
- **With `true`, the token is validated locally.** The server looks up the token's `kid` in the identity provider's signing keys (JWKS), then:
  - verifies the signature;
  - checks `aud` against one exact string, `acl.oidc.audience`, which defaults to `acl.oidc.client.id`;
  - checks `exp`, with 60 s of leeway;
  - requires a non-empty `sub`.
- **Key rollover:** an unknown `kid` makes the server reload its keys.
- **Cache:** verified tokens are cached for 30 s (`acl.oidc.cache.ttl`).

**Can `acl.oidc.sub.claim` point at `oid` or `azp`?** Yes.

- Any top-level claim name works.
- `oid` exists in both v1 and v2 tokens; `azp` exists only in v2 (v1 has `appid`). **Use `oid`.**
- The server also requires a **non-empty groups claim**; without one it returns 401.
- Caveat: there is a single `sub.claim` for every login. A deployment that also has human users (UPN) and apps (no UPN) needs `oid`.

**Does `acl.oidc.groups.claim=roles` work with `EXTERNAL ALIAS`?** Yes.

- The claim can be an array or a single string.
- Its values are matched against `EXTERNAL ALIAS`. Role values containing dots need quotes: `CREATE GROUP ingest WITH EXTERNAL ALIAS 'QuestDB.Ingest'`.
- The service principal or managed identity must hold at least one app role.
- Authorization failures come back as 403 at the upgrade. Established connections keep their old authorization.

**How are v1 and v2 tokens handled?**

- **The practical pitfall is `aud`.** A v1 token's `aud` is the resource string as requested (`api://<id>`); a v2 token's `aud` is the client-ID GUID.
- **Fix:** set `requestedAccessTokenVersion: 2` (manifest `accessTokenAcceptedVersion`) on the QuestDB app registration. Every client (IMDS `resource=` or an SDK `.default` scope) then gets `aud=<GUID>`, which matches the default.
- **To verify against the tenant:** that the `/discovery/keys` and `/discovery/v2.0/keys` endpoints serve the same signing keys.

**Working configuration**

```
acl.oidc.enabled=true
acl.oidc.configuration.url=https://login.microsoftonline.com/<tenant>/v2.0/.well-known/openid-configuration
acl.oidc.client.id=<questdb-app-client-id>      # also the audience
acl.oidc.groups.encoded.in.token=true           # local JWKS validation; required for app-only
acl.oidc.groups.claim=roles
acl.oidc.sub.claim=oid
```

**Server follow-ups**

- **Client-facing:** spec Appendix C proposes two changes:
  - send a `WWW-Authenticate: Bearer` challenge on 401;
  - return 503, not 401, when the server cannot verify tokens.
- **Also useful:** a fallback list for `sub.claim`, and 403 instead of 401 when the roles claim is missing.
- **Security hardening:** tracked privately.

**What the client assumes about the server**

- Auth happens only at the upgrade. True: no re-check mid-stream.
- A token is valid on every node. True if the OIDC configuration is identical across the cluster.
- 401/403 mean credential problems. **Partly false:** JWKS and UserInfo outages also return 401.
- 421 with `X-QuestDB-Role` means a role reject.

## 9. Decisions

All resolved. See the spec's §12 (D1–D9).

## 10. Java implementation plan

Build in this order. Each step is one PR and must pass the conformance tests listed for it (spec §10).

**Constraints for every step** (see `CLAUDE.md`):

- Code must build on Java 8 and also compile on JDK 11+.
- Zero-GC applies to per-row producer calls and the steady-state I/O loop, not to connect paths.
- Tests run under `assertMemoryLeak`.
- `ExportedApiCompatibilityTest` must stay green: add API, never retype it.

1. **Token cache.**
   - Implements spec §4–§5, with the types named in spec Appendix B.
   - Self-contained: no connect-path changes.
   - Tests C1–C8, plus C20 for the cache.
2. **Client integration.** Implements spec §6 and §8.1–§8.3. Tests C9–C16 and C23.
   - Add the default method `HttpTokenProvider.onTokenRejected`.
   - `S.buildWebSocketAuthHeader` (`S:3427`) returns a named dynamic-credential class with `onRejected`, instead of a lambda.
   - Retry the same endpoint once on 401, in three places, and fire `AUTH_FAILED` only on the final outcome:
     - the `QWS.connectWalk` 401 branch (`QWS:3427`);
     - `QQC.connect` (`:785`);
     - `QQC.reconnectViaTracker` (`:1889`).
   - Classify failures at SYNC startup in `CWSL.connectWithRetry` (`:1053`), per D6 and D8.
   - Test harness:
     - add an authorization validator to `TestWebSocketServer` that picks the response status from the header;
     - add an expiring-token scheme driven by a fake clock.
3. **Connect string, registry and Azure module.** Implements spec §7. Tests C17–C19.
   - Register the keys in `ConfigSchema`.
   - Parse and validate the keys in:
     - `S.fromConfigWebSocket` and `validateWsConfig` (`S:4018`, `:4227`);
     - `QQC.fromConfig` and `validateConfig` (`:375`);
     - the exclusivity check in `QuestDBBuilder.build()` (`:190-194`).
   - Add the `uses` clause to `module-info.java`.
   - Release the lease when a sender or query client closes.
   - Add the new `azure/` reactor module.
4. **Health accessor and deadline.** Implements spec §8.4 and §8.5. Tests C21–C22.

**Separate tickets, outside this feature:**

- **Dead pooled egress worker** (§3). Start with a test that reproduces it. *Done after all - the spec makes recovery a MUST (§8.3, "Egress recovery"); see §11.*
- **Treat a 503 at the upgrade as transient in every phase** (§3, spec Appendix C). This must land before any server change to return 503.

## 11. Implementation status

Every step of §10 is implemented. Line references in §1-§7 predate it.

**Step 1 - token cache** (spec §3-§5, Appendix B). `io.questdb.client.cutlass.auth`:

- `ExpiringToken`, `TokenSource`, `TokenUnavailableException`, and `RefreshingTokenProvider` with its builder (every §5.1 parameter, plus clock, scheduler and jitter seams for tests).
- `CredentialRedaction`: the §9 token rendering (length + 8-hex SHA-256 prefix) and the sanitizing of library text (display-unsafe characters stripped, 256-character cap).
- The warm path of `getToken()` is a volatile read with no lock and no I/O. A refresh whose wall-clock time has passed but whose monotonic schedule has not (a host that slept) starts in the background.
- A cold caller starts one fetch and then waits; it does not start another each time a fetch completes. A source that keeps returning a token inside the hand-out floor is therefore not fetched in a tight loop. While waiting, the "fail immediately" rule of §5.3 is re-evaluated after each failed fetch.
- Tests: `RefreshingTokenProviderTest` (C1-C8, the cache half of C20).

**Step 2 - client integration** (spec §6, §8.1-§8.3).

- `HttpTokenProvider.onTokenRejected` (default no-op).
- `QwpWebSocketSender.tokenProviderAuthHeader` is the named dynamic credential that replaced the lambda in `Sender.buildWebSocketAuthHeader`.
- The one retry after a refreshable 401 is in `QwpWebSocketSender.connectWalk` (shared by the foreground and the orphan drainers) and in `QwpQueryClient.connect` and `reconnectViaTracker`. It records no health penalty and fires no event.
- `WWW-Authenticate` is captured by `WebSocketClient` and parsed by `cutlass.http.BearerChallenge`. `QwpAuthFailedException.isTokenRefreshable()` applies the §8.2 challenge rule; its message now starts with `auth-rejected`.
- SYNC startup (`CursorWebSocketSendLoop.connectWithRetry`) retries a credential failure only when `QwpCredentialUnavailableException.isRetryable()` (D6/D8) and the thread is not interrupted.
- Egress errors name the failure class. A provider failure on `connect()` is a `QwpCredentialUnavailableException` with a `credential-unavailable:` message.
- Egress recovery: after a failover reconnect fails, the next `execute()` reconnects instead of throwing "not connected". `QwpQueryClientDynamicCredentialTest.testPooledQueryClientRecoversAfterAFailedFailover` reproduced the §3 dead-pooled-worker bug before the fix (it failed with exactly that message).
- Tests: `WebSocketDynamicCredentialTest` (C9-C16, C23), `QwpQueryClientDynamicCredentialTest`, `BearerChallengeTest`. `TestWebSocketServer` gained an authorization validator, a `WWW-Authenticate` challenge, `dropAllConnections()`, and a TLS mode. The TLS mode uses a self-signed identity generated per JVM with `keytool` (`TestTls`), because `token_provider` is `wss::`-only.

**Step 3 - connect string, registry, Azure module** (spec §7).

- `ConfigSchema` registers `token_provider`, `azure_resource`, `azure_client_id` and the enum `azure_credential` (COMMON). `TokenProviderSpec` normalizes `azure_credential=default` away, so it shares a provider with a string that omits the key, and rejects `azure_client_id` with `azure_credential=environment`.
- `TokenProviderSpec.parse` enforces §7.2 on both clients and resolves the factory without fetching anything. It strips `/.default`, checks and lower-cases the client-ID GUID, and builds the registry key.
- `TokenProviderFactory` is the `ServiceLoader` SPI (`uses` in `module-info.java`). `TokenProviderRegistry` hands out ref-counted leases, with a 60 s linger, a daemon timer that exits when idle, and a test seam that replaces discovery.
- The non-QWP `Sender` schemas reject the keys (D9). The builders and `QuestDBBuilder` reject mixing them with an application-supplied provider.
- Leases: `Sender.build()` acquires one and hands it to `QwpWebSocketSender.setCredentialLease` (released on close, or released by `build()` if it fails). `QwpQueryClient` acquires one on its first `connect()` and releases it on close.
- New reactor module `azure/`, artifact `org.questdb:questdb-client-azure` (not `io.questdb`: the core artifact is `org.questdb:questdb-client`):
  - `AzureTokenProviderFactory` uses `DefaultAzureCredential` for `azure_credential=default`, where `azure_client_id` sets both the managed-identity and workload-identity client ID, and warns once when that chain starts. The other values build `ManagedIdentityCredential`, `WorkloadIdentityCredential` or `EnvironmentCredential` directly (spec Appendix B); a credential that cannot be built for lack of configuration fails every fetch as permanent instead of failing a client's `build()`.
  - `AzureTokenSource` bounds each request at 30 s and classifies per §7.5: `CredentialUnavailableException`, a managed identity that is not assigned (reported only in a nested cause when that credential is used alone), HTTP 400/401 and known AADSTS configuration codes are permanent; everything else is retryable, with `Retry-After` honoured, and names the socket-level cause of a network failure.
  - It never attaches a library exception: its response references the raw request.
  - Java 8 bytecode. It has no `module-info`; `Automatic-Module-Name: io.questdb.client.azure`, and an automatic module provides its `META-INF/services`.
  - Its release profiles mirror core's.
- Tests: `TokenProviderConfigTest` (C18, C19, the connect-string half of C24), `TokenProviderSharingTest` (C17, over TLS), `azure/AzureTokenSourceTest` (fake `TokenCredential`, no network), `azure/AzureCredentialSelectionTest` (the real factory, no network) and `azure/AzureManagedIdentityLibraryTest` (C24: the real library in a child JVM, against a local IMDS stub).

**Step 4 - health and deadline** (spec §8.4, §8.5).

- `io.questdb.client.ConnectionHealth` (with `State`, `FailureClass`, `Failure`, `Aggregate`) and `QwpConnectionHealthTracker`, which publishes immutable snapshots through a volatile field.
- The ingest walk reports rounds and upgrades; the I/O loop reports connection loss and terminal failure. Drainers never report.
- Accessors: `Sender.health()` (default throws `UnsupportedOperationException`), `QwpQueryClient.health()`, `QuestDB.health()` (aggregate of every pooled connection).
- `auth_failure_max_duration_millis` (INGRESS key) and `Sender.LineSenderBuilder.authFailureMaxDurationMillis(long)`. The clock lives in `CursorWebSocketSendLoop`, applies to FOREGROUND loops only, and is tracked even before the deadline is armed. The builder arms it right after connecting, so an `async` start is measured from its first failure.
- Tests: `ConnectionHealthTest` (C21, C22).

**Not done:** the spec documents (`qwp-ingress-websocket.md`, `qwp-egress-websocket.md`, the connect-string reference) live in the documentation repository. The Rust core and Python parity is tracked there too.
