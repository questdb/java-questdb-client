# QWP dynamic bearer credentials: cross-language specification

**Status:** v0.4, ready for implementation. All decisions are resolved (§12).

**Changes:**

- **v0.4:** `azure_credential` selects one Azure credential deterministically (§7.1), so the library itself reports an unreachable managed-identity endpoint (retryable) apart from a misconfigured credential (permanent) (§7.5, D10). A discovery chain's "no credential available" stays permanent, so a host without a credential still fails fast, and the client warns once that the chain does not ride out an endpoint outage. This resolves the conflict between §4 and §7.5 over an unreachable managed-identity endpoint.
- **v0.3:** decisions resolved, with D8 (§8.1) and D9 (§7.2) added; Java binding decided (Appendix B).
- **v0.2:** connection health (§8.4), the optional authentication-outage deadline (§8.5), `WWW-Authenticate` handling (§8.2), and Appendices C and D.

- **Implementations:** Java is the reference. Other clients implement this document; that includes the Rust core used by Python, Go, .NET and Node.
- **Freezing:** the spec freezes when the Java implementation merges. Any later change needs a version bump and a note to client maintainers.
- **Rationale and Java code locations:** `design/entra-id-qwp-auth.md`. Its §10 is the Java implementation plan.

**Conventions:**

- **MUST, SHOULD, MAY** are used as in RFC 2119.
- **[Dn]** marks a rule that follows from a decision recorded in §12.
- Durations are defaults that implementations MAY expose as options. They SHOULD be identical across clients.

## 1. Scope

**In scope:**

- obtaining, caching and refreshing bearer tokens for the QWP WebSocket upgrade, for both ingress (`/write/v4`) and egress (`/read/v1`);
- when clients obtain tokens, and how token-related failures are handled;
- connect-string keys that select a token provider, and the `azure` provider.

**Out of scope:**

- the protocol after the upgrade;
- ILP over HTTP and PGWire. A provider defined here MAY also serve them;
- server behaviour. Appendix A describes it and Appendix C proposes changes; both are informative only;
- interactive sign-in. The OIDC device flow is covered by `design/oidc-token-persistence.md`.

## 2. Terms

| Term | Meaning |
|---|---|
| Credential | The value of the `Authorization` header on the upgrade. It is either **static** (fixed at configuration: Basic auth, or a `token=` value) or **dynamic** (obtained from a token provider). |
| Token source | A function, supplied by the application or an integration, that obtains a new token from an identity platform (§4). |
| Token provider | The client-side component that connection code asks for the current token. The **refreshing provider** (§5) is a token provider that wraps a token source. |
| Upgrade attempt | One HTTP upgrade request sent to one endpoint. |
| Connect round | One walk over the configured endpoints. It ends at the first successful upgrade, or when every endpoint has failed. |
| Initialization | The phase of a sender before its first successful upgrade. |
| Established | The phase of a sender after its first successful upgrade. |
| Orphan drain | The background replay of a store-and-forward slot left behind by another sender. |
| Egress operation | One query execution, including any failover reconnects it triggers. |

## 3. Wire format

This section restates current behaviour; nothing here changes.

- **Where the credential goes:** it is sent only on the upgrade request, as `Authorization: Bearer <token>`. An established connection is never re-authenticated.
- **Token values:** a token is opaque.
  - A client MUST reject an empty or blank token.
  - A client MUST reject a token that contains any character outside printable ASCII (0x20–0x7E).
  - A client MUST NOT trim or otherwise alter a token.
- **Validating mutable strings:** where the language allows a token to change after it is returned, the client MUST validate a snapshot of it and send that same snapshot.
- **Prefix:** token sources and providers return the token without the `Bearer ` prefix.

## 4. Token source contract

`fetch() -> TokenResult | TokenError`

### TokenResult

| Field | Required | Meaning |
|---|---|---|
| `token` | yes | The token. |
| `expires_at` | yes | An absolute instant. |
| `refresh_at` | no | The earliest instant the platform suggests refreshing. |

Rules for `expires_at`:

- If the source receives a relative lifetime (such as `expires_in`), it SHOULD compute `expires_at` from its own local time of receipt. This makes the cache robust to clock skew.
- If the source cannot determine the expiry, it MUST supply a conservative value.

### TokenError

| Field | Meaning |
|---|---|
| `retryable` | Whether retrying can succeed. See classification below. |
| `retry_after` | Optional duration to wait before retrying. |
| `message` | Description of the failure. It MUST NOT contain a token or a raw response body (§9). |

Classification:

- **Retryable:** network failures, timeouts, HTTP 429, HTTP 5xx, and IMDS 404 or 410.
- **Permanent:** configuration that is missing or wrong. Examples: the credential the source was told to use is not configured, identity not found, invalid client.
- **Discovery:** a source that finds its credential by probing the environment cannot tell "nothing found" from a brief outage of a probed endpoint (§7.5). It MAY report "nothing found" as permanent, so a host without a credential fails fast, but it MUST then offer a way to select one credential deterministically, which reports the two apart **[D10]**.
- **When unsure:** treat the failure as retryable.
- An error that carries no classification MUST be treated as retryable.

### Rules

- **No concurrency:** a provider never calls `fetch()` concurrently with itself.
- **Where it runs:** implementations SHOULD run `fetch()` on a background context owned by the provider, never on a connection thread.
- **Blocking:** `fetch()` MAY block, but it SHOULD bound its own network operations. 30 s per attempt is RECOMMENDED.
- **Bad results:** the provider MUST treat a result as a retryable error in either of these cases:
  - the token fails the checks in §3;
  - `expires_at` is not later than the time the result was received.

## 5. Refreshing provider

### 5.1 Parameters

| Name | Default | Meaning |
|---|---|---|
| `refresh_margin` | 5 min | Upper bound on how close to expiry a scheduled refresh may run. |
| `min_refresh_interval` | 30 s | Lower bound on the time between a fetch and the next scheduled refresh. |
| `handout_floor` | 60 s | A token is **usable** only while `now < expires_at - handout_floor`. |
| `cold_wait` | 30 s | The longest a caller waits when no usable token is held. |
| `backoff_initial` | 0.5 s | First retry delay after a failed fetch. |
| `backoff_max` | 60 s | Largest retry delay after failed fetches. |
| `retry_after_max` | 5 min | Cap applied to a source's `retry_after`. |
| `forced_min_interval` | 30 s | Minimum spacing between forced refreshes (§5.5). |
| `forced_wait` | 5 s | The longest `on_rejected` waits for a forced refresh. |
| `registry_linger` | 60 s | How long a connect-string provider stays alive after its last lease is released (§7.4). |

### 5.2 Refresh schedule

After a successful fetch received at time `f`, with expiry `e`:

```
L = e - f
m = min(refresh_margin, L / 2)
p = min(min_refresh_interval, L / 2)
r = f + L * U(0.45, 0.55)        // jittered half-life; U is uniform
r = min(r, e - m)
r = max(r, f + p)
if refresh_at is present:
    r = min(r, max(refresh_at, f + p))
```

- **Proactive refresh:** the provider MUST start a fetch at `r` without waiting for a caller.
- **Effect:** while the source is healthy, even an idle client always holds a usable token.

*Informative examples:*

- A 60-minute service-principal token is refreshed at about 27–33 minutes.
- A 24-hour managed-identity token is refreshed at about 11–13 hours.
- If the platform keeps returning the same token, each refresh happens at about half of the remaining lifetime. So a token is fetched only a handful of times before it expires.

### 5.3 Handing out a token: `get_token()`

1. **Usable token held.** Return it at once, with no I/O and no waiting. If `now >= r` and no fetch is running or scheduled, start one in the background.
2. **No usable token held.** Proceed as follows:
   - Start a fetch now, unless one is already running or the provider is in failure backoff (§5.4).
   - If the earliest permitted fetch is later than `now + cold_wait`, fail immediately.
   - Otherwise wait until one of these happens: a fetch yields a usable token, `cold_wait` elapses, or the caller is cancelled.
   - On failure, raise a **token-unavailable** error. It takes the classification of the most recent failed fetch. If no fetch has failed (the wait timed out, or the caller was cancelled), it is retryable.
3. **Never hand out an unusable token.** The provider MUST NOT do so.
4. **Cancellation.** If the calling operation is cancelled (for example, because the client is closing):
   - the wait MUST end promptly with a retryable error;
   - the cancellation signal MUST be preserved for the caller. In Java, that is the thread's interrupt flag.
5. **Concurrency.** `get_token()` MUST be safe to call concurrently from any number of threads. Any number of concurrent callers without a usable token MUST cause at most one fetch.

### 5.4 Failures

**Backoff after the n-th consecutive failed fetch:**

```
base  = min(backoff_max, backoff_initial * 2^(n-1))
delay = base / 2 + U(0, base / 2)
if retry_after is present:
    delay = max(delay, min(retry_after, retry_after_max))
```

A successful fetch resets `n`.

**While fetches are failing:**

- **Keep retrying** while the provider is open, whatever the classification. An operator can fix a "permanent" error without a restart, for example by assigning a role.
- **Keep serving** the current token for as long as it remains usable.
- **Log** each failure as a warning, at most once per minute per provider. The log contains only the classification and the sanitized message.

### 5.5 Forced refresh: `on_rejected(token, status)`

Connection code calls this when an upgrade that presented `token` was rejected (§8.2).

**Ignore the call** in any of these cases:

- `status` is not 401 **[D2]**;
- `token` is no longer the provider's current token (it has already rotated);
- a forced refresh ran less than `forced_min_interval` ago.

**Otherwise:**

- Start a fetch now. Join a fetch that is already running, and respect any failure backoff.
- Wait up to `forced_wait` for the fetch to finish, then return.
- A forced fetch that returns the same token counts as a normal success.

*Informative:* platforms that cache tokens, such as Azure managed identity, usually return the same token again. A forced refresh mainly helps client-credential flows and clock skew; proactive refresh (§5.2) remains the main defence.

### 5.6 Lifecycle

- **Prefetch.** The provider SHOULD start its first fetch as soon as it is created.
- **Readiness.** It SHOULD offer `await_ready(timeout) -> bool`, which returns true once a usable token is held. Applications can use it to gate startup.
- **Close.**
  - `close()` stops scheduled fetches and wakes all waiters.
  - After `close()`, `get_token()` MUST fail with a permanent error.
  - A client MUST NOT close a provider that the application supplied.
- **Clocks.**
  - Expiry comparisons use wall-clock time.
  - Delays and waits use a monotonic clock.
  - Usability is re-evaluated on every call.

### 5.7 Observability

The provider SHOULD expose:

- the current token's expiry;
- the time of the last successful fetch;
- the last failure: its classification, sanitized message, and time.

None of these MAY reveal the token itself.

## 6. When the client obtains the token

- **At the start of every connect round.** A client using a dynamic credential MUST get it from the provider:
  - on the initial connect;
  - on every reconnect;
  - on every failover reconnect;
  - on every orphan-drain or recovery connect;
  - on every egress connect and egress failover reconnect.
- **Reuse.** The client MAY present that credential on every upgrade attempt within the round. It MUST NOT reuse it in a later round.
- **Provider failure.** If the provider fails, the round MUST end without contacting any endpoint. The failure is classified as **credential-unavailable** (§8).
- **Cancellation.** Getting the credential MUST be cancellable when the client shuts down.
- **Established connections.** Sending data on an established connection MUST NOT query the provider.

## 7. Connect-string keys

### 7.1 Keys [D3]

| Key | Values | Required | Notes |
|---|---|---|---|
| `token_provider` | `azure`. The name `azure_imds` is reserved **[D5]**. | no | Selects a provider. |
| `azure_resource` | `api://<app-id>` or `<app-id>` | when `token_provider=azure` | The application ID URI or the client ID of the QuestDB app registration. A trailing `/.default` MUST be stripped. The requested scope is `<azure_resource>/.default`. |
| `azure_client_id` | a GUID | no | The client ID of a user-assigned managed identity or of a workload identity. |
| `azure_credential` | `default`, `managed_identity`, `workload_identity`, `environment` | no | Which Azure credential `azure` uses (§7.5). `default`, the default, is the library's default credential: unless the platform's own selector (`AZURE_TOKEN_CREDENTIALS`) names one credential, it is a discovery chain, meant for development. Each other value selects that one credential and overrides the platform's selector. This deterministic mode is recommended in production **[D10]**. |

### 7.2 Validation

A client MUST reject the configuration, with an error that names the offending key, if any of these holds:

- `token_provider` is combined with `token`, `username`, `password`, or a provider supplied by the application;
- `token_provider` is empty, unknown, or not supported by this client. The error MUST list the supported values. A client MUST NOT silently connect without credentials;
- a provider-specific key is present but the selected provider does not accept it. For example, `azure_resource` without `token_provider=azure`;
- a provider key that is required is missing;
- `azure_credential` has a value that §7.1 does not list. The error MUST list the supported values;
- `azure_client_id` is combined with `azure_credential=environment`. That credential takes its client ID from the platform's configuration;
- `token_provider` is used with any schema other than `wss::`:
  - plain `ws::` is rejected because the token would cross the network in cleartext **[D4]**;
  - non-QWP schemas, such as ILP's `http::`, are out of scope **[D9]**.

**Ingress and egress.** Both MUST accept these keys, so a single string can configure both.

**Validating without connecting.** Validation that does not connect, such as a facade checking its configuration at build time, MUST NOT fetch a token.

### 7.3 Secrets

- **No secrets in the string.** The keys defined here are non-secret, so a connect string that uses them stays safe to log and to place in `QDB_CLIENT_CONF`.
- **Where secrets live.** Secrets, such as a service-principal secret or certificate, come from the platform's standard configuration. They never come from connect-string keys.

### 7.4 Sharing

- **One provider per configuration.** A process MUST NOT run more than one refreshing provider for the same `(token_provider, parameters)`.
- **Registry.** Clients resolve connect-string providers through a registry that is shared by the whole process. Its key is the provider name plus the normalized parameter values.
- **Leases.**
  - Every client instance built from a connect string acquires a lease and releases it on close. That covers each sender, each query client and each pooled connection.
  - When the last lease is released, the provider is closed after `registry_linger`, unless a new lease is acquired first.
- **Application-supplied providers.** Sharing is the application's responsibility: use one provider instance per identity per process.

### 7.5 The `azure` provider

- **How tokens are obtained:** through the language's Azure Identity library, for the scope `<azure_resource>/.default`. `azure_credential` (§7.1) decides which credential is used:
  - `default`: the library's default credential. Unless the platform's selector names one credential, this is a **discovery chain**. It tries an environment service principal, a workload identity, a managed identity and developer tools, in that order, and uses the first that works.
  - `managed_identity`, `workload_identity` or `environment`: that credential alone, called **deterministic mode**. A platform selector that names one credential has the same effect.
- **Why the mode matters:** to keep development machines fast, a discovery chain checks for the instance metadata service (IMDS) with one short request and no retries, and moves on when nothing answers. So it reports "no credential available" both on a machine without a managed identity and on an Azure host whose IMDS is briefly unreachable. In deterministic mode the library skips that check and retries the managed-identity endpoint with backoff, so the two cases are reported differently.
- **`azure_client_id`:** when set, it selects both the managed identity and the workload identity.
- **Expiry:** `expires_at`, and `refresh_at` when the library exposes it, come from the library's token result.
- **Classification of library errors [D10]:**
  - **Deterministic mode:**
    - **Permanent:** configuration that the selected credential reports as missing or wrong. That covers missing settings it requires (for example, no federated token file for a workload identity), an identity that is not found or not assigned to the host, and Entra rejecting the client, secret, assertion, application or tenant. This holds when the library reports the reason only in a nested cause.
    - **Retryable:** network errors, including a managed-identity endpoint that cannot be reached; timeouts; throttling; `404`, `410` and `5xx` responses from a managed-identity endpoint; and anything else.
  - **Discovery chain:**
    - "No credential available in the chain" is **permanent** (§4), so on a host without a credential a `sync` startup fails fast instead of waiting out `reconnect_max_duration_millis`. A brief managed-identity outage is reported the same way, so a chain does not ride one out at startup; deterministic mode does **[D10]**.
    - A failure that one credential in the chain reports for itself is classified as in deterministic mode.
    - A client SHOULD log once, when the provider starts, that a discovery chain does not retry a managed-identity outage, and recommend `azure_credential` for production.
- **Error text:** libraries often report every managed-identity failure with one generic message and keep the reason, such as "Identity not found", in a nested cause. The error SHOULD carry that innermost reason, sanitized as §9 requires.
- **Unsupported platforms:** a client whose platform has no Azure Identity library MAY leave `azure` unsupported, and must then reject it as described in §7.2.

*Informative bindings:*

- **Java:** the optional `questdb-client-azure` artifact, which is found through `ServiceLoader`. It uses `DefaultAzureCredential` for `default` and builds the selected credential itself otherwise (Appendix B).
- **Python:** `azure-identity` as an optional extra.
- **Rust and C:** not required.
- **Platform support:** the Azure Identity libraries read `AZURE_TOKEN_CREDENTIALS`. When it selects `ManagedIdentityCredential`, they skip the IMDS check and retry with backoff from .NET 1.16.0, Java 1.18.1, Python 1.25.1, JavaScript 4.13.0, Go 1.13.0 and C++ 1.13.2.

## 8. Failure handling

### 8.1 Classes

| Class | Trigger |
|---|---|
| credential-unavailable (retryable or permanent) | The provider failed while the client was getting the credential (§6). |
| auth-rejected 401 | The upgrade response was `401`. |
| auth-rejected 403 | The upgrade response was `403`. |

This specification does not change how other upgrade failures are handled: role rejects (`421`), transport errors, version mismatches, and other 4xx and 5xx responses.

**Exceptions from application-supplied providers [D8].**

- An exception from an application-supplied provider that is not a token-unavailable error counts as credential-unavailable, **permanent**. Startup therefore fails fast, as it does today for an OIDC device-flow provider that is not signed in.
- Only a token-unavailable error marked retryable is retried at SYNC startup.

### 8.2 One retry after a 401

When an upgrade attempt that presented a dynamic credential receives `401`, the client:

1. MUST call `on_rejected(presented token, 401)`;
2. MUST get the credential from the provider again;
3. if the new credential differs from the one presented, MUST retry the same endpoint once, immediately and without backoff. That retry:
   - MUST NOT count as an endpoint failure for health tracking or backoff;
   - is an ordinary upgrade attempt for any outcome other than `401`;
4. if the credential is unchanged, or the retry also receives `401`, applies the phase policy in §8.3.

Further rules:

- **Limit:** at most one such retry per connect round.
- **Events:** a client that emits connection events SHOULD report the authentication failure only for the round's final outcome.
- **No forced refresh on 403:** a `403` MUST NOT trigger a forced refresh **[D2]**.
- **Static credentials:** they never get this retry.
- **`WWW-Authenticate` challenge:** if the `401` carries a `WWW-Authenticate: Bearer` challenge with an `error` parameter, the client SHOULD take steps 1–3 only when `error` is `invalid_token` (RFC 6750 §3.1). Without such a challenge, the status code alone decides. Current QuestDB servers send no challenge; Appendix C proposes adding one.

### 8.3 Policy by phase

| Phase | credential-unavailable, retryable | credential-unavailable, permanent | 401 / 403 |
|---|---|---|---|
| Initialization, `initial_connect_retry=off` | fail startup | fail startup | fail startup |
| Initialization, `on` / `sync` | retry within `reconnect_max_duration_millis` **[D6]** | fail startup | fail startup |
| Initialization, `async` | retry indefinitely | retry indefinitely | fail, and report through the error handler |
| Established | retry indefinitely | retry indefinitely | retry indefinitely **[D1]** |
| Orphan drain, dynamic credential | retry indefinitely | retry indefinitely | ride out, then quarantine (rule below) |
| Orphan drain, static credential | n/a | n/a | quarantine on the first rejection |
| Egress operation | fail the operation | fail the operation | fail the operation |

**Retry indefinitely**

- **Meaning:** retry with capped exponential backoff for as long as the client is open, unless the optional deadline in §8.5 is configured. Store-and-forward MUST NOT drop a live producer.
- **Reporting:** each retried failure MUST be reported to the application's error channel, not only to logs. The report is a retriable, security-category error that names the failure class. The failure SHOULD also be visible in the connection health (§8.4).

**Ride out, then quarantine**

The slot is quarantined when either of these holds:

- at least 6 rejections, **and** the rejections have persisted for at least `min(reconnect_max_duration_millis, 5 min)`;
- 256 rejections within one episode.

An episode ends only when the drain makes acknowledgement progress. A failure of another class in between restarts the persistence clock but not the count.

**Egress recovery**

- After a failed connect or failover reconnect, the client MUST do one of two things: reconnect on its next operation, or be replaced by its pool. It MUST NOT remain permanently unusable.
- The error reported for the failed operation MUST name the failure class.

### 8.4 Connection health

Retrying indefinitely is only safe if an outage is easy to see. An application that registers no error handler must still be able to find out that its sender has not reached the server for an hour. Kafka shows the failure mode: when credentials expire, a consumer keeps failing authentication in the background while the application sees nothing (KAFKA-10840, Appendix D).

Each sender and query client SHOULD expose a snapshot of its connection health, with at least:

| Field | Meaning |
|---|---|
| `state` | `connecting` (no successful upgrade yet), `connected`, `reconnecting` (the connection was lost and is being re-established), `failed` (terminal) or `closed`. |
| `last_connected_at` | Time of the most recent successful upgrade, or none. |
| `outage_since` | Start of the current period without a connection, or none while connected. |
| `failed_rounds` | Number of failed connect rounds since `outage_since`. |
| `last_failure` | The most recent failed connect round: its class (`credential_unavailable`, `auth_rejected` with the status code, `role_rejected`, `transport` or `other`), a sanitized message, and the time. It is kept after the connection recovers. |

Rules:

- **Cheap and safe to poll.** Reading the snapshot MUST NOT wait on I/O or on connection threads, and MUST be safe from any thread, so that it can back a health endpoint.
- **No secrets.** The snapshot MUST NOT contain a credential (§9).
- **Pools and facades** SHOULD expose an aggregate: the number of connections in each `state`, the oldest `outage_since`, and the most recent `last_failure`.
- **Query clients** use the same fields. They connect on demand, so for them `reconnecting` means the last connect or failover reconnect failed and the next operation will try again.

### 8.5 Optional authentication-outage deadline [D7]

Some applications prefer a hard failure to an authentication outage that lasts indefinitely, much as Kafka's `delivery.timeout.ms` bounds how long a record may wait. A client MAY offer this. If it does, it MUST use this key and these semantics.

| Key | Values | Default |
|---|---|---|
| `auth_failure_max_duration_millis` | a positive duration in milliseconds | not set (no deadline) |

- **Which failures count.** Only *authentication-class* failures: credential-unavailable, and auth-rejected (`401` or `403`).
- **Where it applies.** Ingress senders, in the phases where §8.3 retries an authentication-class failure indefinitely: `async` initialization and established. It does not apply to orphan drains, which have their own rule, or to egress, which already fails the operation.
- **The clock.** It starts at the first authentication-class failure of the current outage. Only a successful upgrade resets it.
- **When it fires.** The sender becomes terminal when a connect round fails with an authentication-class failure and the clock has reached the configured duration.
  - Rounds that fail for other reasons neither reset the clock nor fire the deadline.
  - So a network outage that follows a single `401` does not fire it.
  - And a cluster that alternates between `401` and network errors cannot postpone it forever.
- **What happens then.**
  - The error names the failure class and the elapsed time. It is reported as terminal through the error handler and raised from later producer calls.
  - Unacknowledged data stays in on-disk store-and-forward storage, for a later sender or an orphan drain. In memory-only mode it is lost when the sender closes.

## 9. Security

- **Tokens.** A client MUST NOT write a token into logs, error messages, events, or the string representation of any object. A representation MAY include the token's length, its expiry, and a fingerprint of at most the first 8 hex digits of the token's SHA-256.
- **Response bodies.** A token source MUST NOT log or embed a token-endpoint response body. For a malformed response, it reports only the shape of the problem, such as a missing or invalid field.
- **Error text from libraries and endpoints.** Before such text enters client errors or logs, the client SHOULD:
  - remove control, bidirectional and other non-displayable characters;
  - truncate it to 256 characters.
- **Raw requests.** Objects that serialize raw HTTP requests MUST NOT be attached to errors or logs, because requests may carry secrets.
- **Cleartext.** A client SHOULD warn once when a provider supplied by the application is used over a non-TLS schema.

## 10. Conformance tests

Every client that implements this specification SHOULD pass these scenarios. They use:

- a fake clock;
- a scripted token source;
- a stub server that answers `401` to an expired token. Tokens can be shaped `T<n>.<expires_at_ms>` and checked against the shared fake clock.

| ID | Scenario | Expected result |
|---|---|---|
| C1 | Refresh schedule | Refresh times follow §5.2 for `L` = 24 h, 60 min and 5 min, with and without `refresh_at`. |
| C2 | Same token returned | Fetches converge as described in §5.2, and the token stays usable. |
| C3 | Cold burst | 64 concurrent callers with no usable token cause exactly one fetch. |
| C4 | Cold failure | A retryable error is raised within `cold_wait`. A permanent classification is passed through. |
| C5 | Backoff | Delays follow §5.4. `retry_after` is honoured and capped. |
| C6 | Hand-out floor | A token inside `handout_floor` is never handed out. |
| C7 | Cancellation | A cancelled wait returns promptly, and the cancellation signal is preserved. |
| C8 | Forced refresh | Rate limited. A stale token is ignored. A same-token result is accepted. |
| C9 | Reconnect after expiry | The reconnect carries the refreshed token, and no `401` is observed. |
| C10 | Stale token presented | Exactly one `401`, then an immediate retry with a new token: no backoff, no health penalty. |
| C11 | Persistent `401` | Each phase follows §8.3. |
| C12 | Initialization matrix | `off`, `sync` and `async`, each with a retryable and a permanent source failure, behave as in §8.3. |
| C13 | Provider outage while established | Retried indefinitely and reported. Recovers when the source recovers. |
| C14 | Failover across a rotation | The second endpoint receives the new token. Unacknowledged data is replayed exactly once. |
| C15 | Store-and-forward across a rotation | All rows are delivered. No quarantine and no data-loss report. |
| C16 | Orphan drain across a rotation | The slot drains with the new token. |
| C17 | Shared cache | N senders and query clients built from one string use one provider, with one fetch per refresh. |
| C18 | Connect-string validation | Every rule in §7.2 is enforced, and validation does not fetch a token. |
| C19 | Registry lifecycle | Lease release, linger, and re-acquiring a lease during the linger period. |
| C20 | Redaction | A sentinel token never appears in logs, errors, events, string representations or health snapshots, across C4, C10, C11 and a malformed source response. |
| C21 | Connection health | The snapshot moves through `connecting`, `connected`, `reconnecting` (with `401` and credential-unavailable failures) and back to `connected`. `outage_since` and `failed_rounds` reset on recovery; `last_failure` is kept. No credential appears in it. |
| C22 | Authentication-outage deadline | Unset: retries continue indefinitely. Set: the sender becomes terminal only on an authentication-class round once the duration has passed. Other failures in between neither reset nor fire it. A successful upgrade resets it. Orphan drains are unaffected, and on-disk data remains. |
| C23 | `WWW-Authenticate` challenge | A plain `401`, and a `401` with `Bearer error="invalid_token"`, both trigger the retry in §8.2. A `401` whose Bearer challenge carries another error does not. |
| C24 | Azure credential selection | With `azure_credential=managed_identity`, an unreachable managed-identity endpoint is retryable, so `sync` initialization retries it within its budget; an identity that is not assigned is permanent, so initialization fails fast. With `default`, a chain that finds nothing is permanent, and the warning in §7.5 is logged once, unless the platform's selector names one credential. |

## 11. Relation to existing specifications

- **QWP ingress and egress specs** (`qwp-ingress-websocket.md`, `qwp-egress-websocket.md`):
  - their statement that "authentication errors are terminal at any host" is replaced by §8 **[D1]**;
  - their authentication sections should point to §6.
- **Connect-string reference:** it should reserve the keys in §7.1, and `auth_failure_max_duration_millis` (§8.5).
- **OIDC device-flow token store** (`design/oidc-token-persistence.md`): unaffected. An `OidcDeviceAuth` instance remains a valid provider supplied by the application.

## 12. Decisions

All decisions below are resolved. Changing one after the Java implementation merges needs a version bump.

| ID | Question | Decision |
|---|---|---|
| D1 | How to treat 401/403 once a sender is established | Retry indefinitely, report it (§8.3), and expose it in the connection health (§8.4). This is Java's current behaviour and matches common practice (Appendix D). The Rust core must change to match. |
| D2 | Whether a 403 also triggers a forced refresh | No. Only a 401 does, as in RFC 6750 and the clients in Appendix D. |
| D3 | Key names | `token_provider`, `azure_resource`, `azure_client_id`. (`client_id` is already used by egress.) |
| D4 | Whether to reject `token_provider` on `ws::` | Yes. |
| D5 | A zero-dependency `azure_imds` provider | The name is reserved; it is implemented only on demand. |
| D6 | Whether SYNC initialization retries retryable provider failures | Yes. |
| D7 | Whether to offer the authentication-outage deadline, and its key | Yes, as an optional setting that is off by default: `auth_failure_max_duration_millis`. |
| D8 | How to classify an exception from an application-supplied provider that is not a token-unavailable error | Permanent (§8.1). |
| D9 | Whether `token_provider` applies to non-QWP schemas | No; `wss::` only (§7.2). |
| D10 | How the `azure` provider classifies "no credential available", and how to make it resilient | `azure_credential` selects one credential, so the library itself reports an unreachable endpoint (retryable) apart from a misconfigured credential (permanent). In a discovery chain, "no credential available" stays permanent. An IMDS outage looks the same (§7.5), but only `sync` initialization depends on the difference (§8.3), and making it retryable would make every host without a credential wait out `reconnect_max_duration_millis` (5 minutes by default, plus up to one `cold_wait`) before failing. Resilience is therefore opt-in through deterministic mode, which Azure Identity recommends in production anyway, and a chain warns once when it starts. This follows Azure Identity's split between fail-fast discovery and a resilient single credential (Appendix D). A local host check, as Google's library makes, was not adopted: Azure's SMBIOS asset tag identifies only the public cloud, not sovereign clouds or Azure Local. |

**Still to verify in a real Entra tenant.** These checks don't block implementation, but they should be done before the spec freezes:

- Managed-identity tokens from IMDS carry the client-ID GUID as `aud` once the app registration is set to v2 tokens.
- The v1 and v2 signing-key endpoints serve the same keys.
- Whether IMDS returns the same token when asked again. This affects only the convergence note in §5.2.
- How long `DefaultAzureCredential` takes on a cold start. This sizes `cold_wait`.
- On a VM, with `azure_credential=managed_identity`: an unreachable IMDS is reported as retryable, and an identity that is not assigned as permanent. So far this was checked only against a stub endpoint, with Azure Identity for Java 1.18.4.

## Appendix A. QuestDB Enterprise and Entra configuration (informative)

Describes QuestDB Enterprise behaviour at the time of writing.

**Server properties:**

```
acl.oidc.enabled=true
acl.oidc.configuration.url=https://login.microsoftonline.com/<tenant>/v2.0/.well-known/openid-configuration
acl.oidc.client.id=<questdb-app-client-id>   # also the expected audience
acl.oidc.groups.encoded.in.token=true        # local JWKS validation; required for app-only tokens
acl.oidc.groups.claim=roles
acl.oidc.sub.claim=oid
```

**Entra setup:**

- On the QuestDB app registration:
  - expose an application ID URI such as `api://<app-id>`;
  - define app roles whose allowed member type is Applications;
  - set `requestedAccessTokenVersion` to 2 (`accessTokenAcceptedVersion` in the manifest). Every token then carries the app's client ID as `aud`.
- Assign at least one app role to each managed identity or service principal.

**QuestDB setup:**

```sql
CREATE GROUP ingest WITH EXTERNAL ALIAS 'QuestDB.Ingest';
GRANT HTTP TO ingest;
GRANT INSERT ON trades TO ingest;
```

**Server behaviours clients should expect:**

- **401 does not always mean a bad token.** `401` is also returned when the server cannot fetch its signing keys (JWKS).
- **One audience.** `aud` must match a single exact string. v1 tokens carry the requested resource string as `aud`; v2 tokens carry the client ID.
- **Expiry.** `exp` is enforced with 60 s of leeway.
- **Roles are required.** A token without the roles claim is rejected with `401`.
- **Cache.** A verified token is cached for 30 s.
- **Role changes apply only to new connections,** and only once a new token carries them. Managed-identity tokens can live for about 24 h.

## Appendix B. Java binding (decided)

New types live in `io.questdb.client.cutlass.auth` unless noted.

| Spec concept | Java |
|---|---|
| TokenResult | `ExpiringToken(String token, long expiresAtEpochMillis)`, plus an overload that adds `long refreshAtEpochMillis` (0 means none) |
| Token source | `TokenSource`, a functional interface: `ExpiringToken fetchToken()` |
| Refreshing provider | `RefreshingTokenProvider implements HttpTokenProvider, QuietCloseable`, created with `RefreshingTokenProvider.builder(TokenSource)`. Methods: `getToken()`, `onTokenRejected(CharSequence, int)`, `awaitReady(long)`, `close()`. The builder exposes the §5.1 parameters, plus clock and scheduler seams for tests. |
| token-unavailable error | `TokenUnavailableException extends LineSenderException`, with `isRetryable()` and `getRetryAfterMillis()` (-1 means none) |
| `on_rejected` | `onTokenRejected(CharSequence token, int httpStatus)`, a default no-op method on the existing `io.questdb.client.HttpTokenProvider` |
| Provider factory | `TokenProviderFactory`, an SPI found through `ServiceLoader` (a `uses` clause in `module-info.java`). `TokenProviderSpec.parse` applies the §7.2 rules to a connect string and resolves the factory without fetching a token. |
| Registry | `TokenProviderRegistry`: process-wide (`global()`), hands out ref-counted `Lease`s, closes a provider `registry_linger` after its last lease |
| `azure` provider | The new reactor module `azure/`, artifact `org.questdb:questdb-client-azure` (the core client is `org.questdb:questdb-client`), package `io.questdb.client.azure`, classes `AzureTokenProviderFactory` and `AzureTokenSource`. It depends on `azure-identity` through `azure-sdk-bom`, keeps the Java 8 floor, and is released together with the client. |
| Connection health | `io.questdb.client.ConnectionHealth`, an immutable snapshot. Returned by `Sender.health()` (a default method; non-QWP senders throw `UnsupportedOperationException`), by `QwpQueryClient.health()`, and as a `ConnectionHealth.Aggregate` by `QuestDB.health()`. |
| Authentication-outage deadline | Key `auth_failure_max_duration_millis`; builder method `authFailureMaxDurationMillis(long)` |
| Credential selection | Key `azure_credential`, validated by `TokenProviderSpec` and handled by `AzureTokenProviderFactory`. `default` builds `DefaultAzureCredential`; the other values build `ManagedIdentityCredential`, `WorkloadIdentityCredential` or `EnvironmentCredential` directly, never touching the process environment. Setting `AZURE_TOKEN_CREDENTIALS` in the configuration passed to `DefaultAzureCredentialBuilder.configuration(...)` is not enough: in `azure-identity` 1.18 it selects the credential but keeps the chain's endpoint check, because each credential re-reads the selector from the process-wide configuration. A credential that cannot be built for lack of configuration fails every fetch as permanent. `AzureTokenSource` classifies the same way whatever the credential: it finds a managed identity's "not assigned" reason in a nested cause, and names the socket-level cause of a network failure, but never echoes another nested message (§9). |

**Python notes:**

- A token source maps to a callable that returns `azure.core.credentials.AccessToken`, which carries `token` and `expires_on` (in epoch seconds).
- Keep the provider in the Rust core, so that the callable runs only on the provider's refresh thread.
- A provider MUST NOT be used across `fork()`.

## Appendix C. Recommended server changes (informative)

These changes would let clients refresh only when a new token can help, and treat server-side outages as transient.

| Situation | Today | Recommended |
|---|---|---|
| The token is invalid: bad signature, expired, wrong audience, or malformed | `401`, no challenge | `401` with `WWW-Authenticate: Bearer error="invalid_token"`. An `error_description` is fine, but never the token. |
| The token is valid but grants nothing: no roles claim, or no mapped group | `401` without a roles claim; `403` without a mapped group | `403` with `WWW-Authenticate: Bearer error="insufficient_scope"` |
| The server cannot verify tokens: it cannot fetch its signing keys (JWKS) or reach UserInfo | `401` | `503`, optionally with `Retry-After` |

**Sequencing: land the client fix first.**

- Java currently treats any non-421 rejection of the upgrade as terminal during initialization.
- An orphan drain quarantines its slot on such a rejection immediately (`design/entra-id-qwp-auth.md` §3).
- If the server switched to `503` first, a signing-key outage would therefore quarantine orphan slots that today ride out a `401`.
- The client should treat a `503` at the upgrade as transient in every phase, as the public QWP spec already says.

Other server-side follow-ups, including security hardening, are tracked privately with the QuestDB Enterprise team (see `SECURITY.md`).

## Appendix D. Precedents for D1, D2 and D10 (informative)

| Source | Behaviour | Bearing on this spec |
|---|---|---|
| RFC 6750 §3.1 | `invalid_token` goes with `401`, and "the client MAY request a new access token and retry". `insufficient_scope` goes with `403`. | Refresh on `401` only (D2); the challenge rule in §8.2. |
| MongoDB driver auth spec (MONGODB-OIDC) | On an authentication failure with a cached token: clear that token from the cache (atomically, and only if it is unchanged), fetch a new one, and retry once. Other errors go to the user. At least 100 ms between calls to the token callback. | The closest match to §5.5 and §8.2. |
| Google auth library, `HttpCredentialsAdapter` | Refreshes and retries on a `401`, or on a Bearer `invalid_token` challenge. A plain `403` does not trigger it. | D2. |
| Kubernetes client-go, exec credential plugin | On a `401`, refreshes "unless they were rotated already", then returns the `401`. The next request uses the new credentials. | The stale-token guard in §5.5. |
| Azure SDK, `BearerTokenAuthenticationPolicy` | Retries only on a `401` that carries a Continuous Access Evaluation claims challenge. A plain `401` goes back to the caller. | A stricter variant of D2. |
| Kafka: KIP-152 and KAFKA-6516 | Authentication failures are non-retriable at the API. The client is not closed, though, and keeps reconnecting in the background; a request to stop that was closed Won't Fix. | D1. |
| Kafka: KAFKA-10840 (open) | When credentials expire, a consumer keeps failing authentication in the background and the application cannot see it. A proposed fix notes that Kafka Connect tasks report RUNNING meanwhile. | Why §8.4 exists. |
| Azure Identity: credential chains and the managed-identity retry strategy | `DefaultAzureCredential` runs managed identity in a "fail fast" mode, meant for the development inner loop: one IMDS probe with a short timeout and no retries. A "resilient" mode skips the probe and retries with exponential backoff; `AZURE_TOKEN_CREDENTIALS=ManagedIdentityCredential`, or the managed-identity credential used directly, enables it. Microsoft recommends a deterministic credential in production. | D10: a discovery chain's "no credential" is weak evidence of misconfiguration; `azure_credential` selects the resilient mode. |
| MongoDB driver auth spec (MONGODB-OIDC) | The application names its environment (`ENVIRONMENT:azure`, `gcp` or `k8s`), and the driver calls that metadata endpoint directly, with no discovery. | D10: deterministic selection. |
| Google auth library, `ComputeEngineCredentials` | Detection pings the metadata server three times with a short timeout, "for developer desktop scenarios", then checks the SMBIOS product name, which needs no network. So a slow metadata server on a real VM is not taken for "not on GCE". A `503` from the metadata server is retryable. | D10: a probe that gets no answer is not proof; the local check is not adopted (§12). |
| AWS SDK for Java v2, issue #3939; Apache Druid | On EC2, the default chain sometimes reports "Unable to load credentials from any of the providers in the chain", which AWS attributes to IMDS latency. Druid now treats that error as recoverable within its retry budget. | D10: "nothing found" in a chain is ambiguous. Druid chose retryable, at the cost of a slower failure when nothing is configured; this spec keeps it permanent and offers deterministic mode instead. |

Links:

- RFC 6750: https://www.rfc-editor.org/rfc/rfc6750#section-3.1
- MongoDB auth spec: https://github.com/mongodb/specifications/blob/master/source/auth/auth.md
- Google `HttpCredentialsAdapter`: https://github.com/googleapis/google-auth-library-java/blob/main/oauth2_http/java/com/google/auth/http/HttpCredentialsAdapter.java
- client-go exec plugin: https://github.com/kubernetes/client-go/blob/master/plugin/pkg/client/auth/exec/exec.go
- Azure `BearerTokenAuthenticationPolicy`: https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/core/azure-core/src/main/java/com/azure/core/http/policy/BearerTokenAuthenticationPolicy.java
- KIP-152: https://cwiki.apache.org/confluence/display/KAFKA/KIP-152+-+Improve+diagnostics+for+SASL+authentication+failures
- KAFKA-6516: https://issues.apache.org/jira/browse/KAFKA-6516
- KAFKA-10840: https://issues.apache.org/jira/browse/KAFKA-10840 (proposed fix: https://github.com/apache/kafka/pull/16418)
- Azure Identity best practices (deterministic credentials, the managed-identity retry strategy): https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication/best-practices
- Azure Identity for Java, credential chains and `AZURE_TOKEN_CREDENTIALS`: https://learn.microsoft.com/en-us/azure/developer/java/sdk/authentication/credential-chains
- Azure SDK design note on IMDS probing: https://gist.github.com/ahsonkhan/d6c4d3a9780bb058a729b76844923ef1
- Azure SDK release, October 2025 (resilient managed identity in C++, Go, Java, JavaScript and Python): https://devblogs.microsoft.com/azure-sdk/azure-sdk-release-october-2025/
- Identify an Azure VM from the guest (SMBIOS asset tag, public cloud only): https://learn.microsoft.com/en-us/azure/virtual-machines/identify-azure-vm-from-guest
- Google `ComputeEngineCredentials`: https://github.com/googleapis/google-auth-library-java/blob/main/oauth2_http/java/com/google/auth/oauth2/ComputeEngineCredentials.java
- AWS SDK for Java v2, issue #3939: https://github.com/aws/aws-sdk-java-v2/issues/3939
- Apache Druid, retry transient AWS credential resolution failures (#19558): https://www.mail-archive.com/commits@druid.apache.org/msg115181.html
