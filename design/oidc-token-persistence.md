# OIDC device-flow token persistence

Status: **implemented** in PR #52 (`OidcDeviceAuth`, RFC 8628 device flow) on branch
`ia_oidc_device_flow` — both the `TokenStore` SPI and the default `FileTokenStore` (Layer 1
atomic replace and Layer 2 lock-file critical section) shipped together. This document remains
the frozen cross-language on-disk contract (file name, JSON schema, atomic-write and lock-file
protocols) that other clients (e.g. Python) mirror; the design discussion below is retained as
the rationale of record. Code line references are indicative and may drift from the current source.

## Problem

`OidcDeviceAuth` keeps all token state in memory (`OidcDeviceAuth.java:165-175`):
`accessToken`, `idToken`, `refreshToken`, `expiresAtMillis`, `tokenTtlMillis`. Its
own javadoc says so: *"Token state is in-memory only and does not survive a process
restart."* Every restart of the host app therefore forces the human back through the
interactive device flow (open URL, enter code, authorize), even though a long-lived
**refresh token** that could mint a new access token silently was sitting in memory
seconds earlier.

Goal: optionally persist the token state so a restarted process resumes from the
refresh token (one silent token-endpoint round-trip) instead of re-prompting — without
weakening any of the trust/secret-handling guarantees PR #52 establishes.

## Goals

- **Survive restart without re-prompting.** A process that signed in, then restarted,
  obtains a usable token from a persisted refresh token with no human interaction.
- **Opt-in.** Default behaviour is unchanged (in-memory only). Persisting a long-lived
  credential to disk is a security trade the caller makes explicitly.
- **Pluggable.** A `TokenStore` SPI so an integrator can back persistence with an OS
  keychain / KMS / vault. Ship one default `FileTokenStore` (strict-perms file).
- **Language-neutral on-disk contract.** The Java client is the reference
  implementation; the Python client (and any other) will mirror it. The file location,
  name, JSON schema, and the multi-writer coordination protocol are therefore a *frozen
  cross-language contract*, specified below to the byte, not a Java-internal detail.
- **Correctly scoped.** A persisted entry is keyed by the identity it belongs to
  (endpoints + client id + scope + audience + groups-in-token mode); a token is never
  served for a different configuration.
- **Crash- and concurrency-safe at the file level.** A torn write or an overlapping
  writer never yields a half-read credential.
- **Upholds PR #52's invariants.** Tokens never reach logs or exceptions; a persisted
  file is treated as untrusted input and validated before any byte reaches a header.
- **Java 8 floor, zero third-party deps** (`java-questdb-client/CLAUDE.md`): reuse
  `JsonLexer`/`StringSink`/`MessageDigest`/`java.nio.file` only.

## Non-goals (this spec)

- **Encryption at rest with a built-in key.** A key stored next to the ciphertext is
  theatre; a key from an OS secret store needs native code we cannot take as a
  dependency. Confidentiality at rest is delegated to (a) filesystem permissions for the
  default store and (b) the `TokenStore` SPI for anyone who wants a keychain. Stated as a
  residual risk below, not solved here.
- **A new credential surface in connection strings / `QDB_CLIENT_CONF`.** The README
  already warns against putting tokens there; persistence is a separate, file-scoped
  channel.

## Background: the two code points that constrain the design

1. **Single write funnel.** Both the interactive flow (`runDeviceFlow` -> `pollOnce` ->
   `storeTokens`) and the silent refresh (`tryRefresh` -> `storeTokens`) commit token
   state in exactly one method, `storeTokens(TokenResponseParser)`
   (`OidcDeviceAuth.java:1261-1284`). That is the natural — and only — place to persist.

2. **Refresh is gated behind a non-null cached token.** In `getToken()`
   (`OidcDeviceAuth.java:444-454`) and `signIn()` (`OidcDeviceAuth.java:477-486`) the
   silent-refresh branch only runs when `cachedToken != null`:

   ```java
   final String cachedToken = groupsInToken ? idToken : accessToken;
   if (cachedToken != null) {
       if (System.currentTimeMillis() < expiresAtMillis - effectiveSkewMillis()) {
           return cachedToken;
       }
       if (refreshToken != null && tryRefresh()) {
           return selectToken();
       }
       // getToken(): throw "expired, can't refresh"; signIn(): fall through to device flow
   }
   ```

   Consequence: **restoring only a refresh token does not work with the current logic** —
   `signIn()` would skip the refresh and run a fresh device flow; `getToken()` would throw
   "no token has been obtained yet". This is the pivotal fact. Two ways out:

   - **(A) Persist the full token blob** (access + id + refresh + `expiresAtMillis` +
     `tokenTtlMillis`). On restore `cachedToken != null` holds, so the *existing, audited*
     expiry-then-refresh logic runs untouched: a still-valid access token is served with
     zero network; an expired one triggers exactly one silent refresh. **No change to the
     delicate `signIn`/`getToken`/`tryRefresh` flow.**
   - **(B) Persist the refresh token only** and lift the refresh attempt out from behind
     the `cachedToken != null` gate in both methods. Smaller on-disk secret footprint, but
     it modifies the security-sensitive control flow.

   **Recommendation: (A).** Minimal blast radius on reviewed code, and it makes a quick
   restart fully warm (no round-trip at all). The extra on-disk item is a *short-lived*
   access token; the long-lived secret (refresh token) is on disk under either option, so
   (A) does not change the qualitative risk. (B) is noted as a leaner alternative if we
   later decide the access token must never touch disk.

## API

New, all in `io.questdb.client.cutlass.auth`:

```java
public interface TokenStore {
    /** Load previously persisted tokens for this identity, or null if none / unreadable. */
    PersistedToken load(TokenStoreKey key);

    /** Persist (atomically replace) the tokens for this identity. Best-effort: an
     *  implementation reports failure by throwing; the caller treats persistence as
     *  non-fatal and continues with the in-memory token. */
    void save(TokenStoreKey key, PersistedToken token);

    /** Remove any persisted tokens for this identity. */
    void clear(TokenStoreKey key);
}
```

`TokenStoreKey` — the non-secret identity fingerprint, computed by `OidcDeviceAuth` from
its config so the store stays semantics-free:

```java
public final class TokenStoreKey {
    private final String clientId;
    private final String tokenEndpoint;            // origin + path, canonicalised
    private final String deviceAuthorizationEndpoint;
    private final String scope;
    private final String audience;                 // may be null
    private final boolean groupsInToken;
    // getters; equals/hashCode over all fields
    // hash(): hex SHA-256 of a canonical join of the fields, for use as a file name
}
```

`PersistedToken` — an immutable carrier mirroring the in-memory fields:

```java
public final class PersistedToken {
    private final String accessToken;   // nullable
    private final String idToken;       // nullable
    private final String refreshToken;  // nullable
    private final long expiresAtMillis; // absolute wall-clock; survives restart
    private final long tokenTtlMillis;
    // ctor + getters only
}
```

Builder wiring (default `null` => no persistence, preserving today's behaviour):

```java
OidcDeviceAuth.builder().clientId(...)....tokenStore(store).build();
OidcDeviceAuth.fromQuestDB(url, new DiscoveryOptions().tokenStore(store));
```

Convenience: `FileTokenStore.atDefaultLocation()` and `FileTokenStore.at(Path dir)`.

## `FileTokenStore` (default implementation)

- **Location.** `${questdb.client.oidc.token.store.dir}` if set, else
  `${user.home}/.questdb/oidc-tokens/`. The `questdb.client.oidc.*` system-property
  namespace already exists (`questdb.client.oidc.open.browser`), so this matches.
- **One file per identity**, named `<TokenStoreKey.hash()>.json`. A hashed name avoids
  leaking the endpoint/client id/scope through directory listings, and lets several
  identities coexist (multiple servers / users on one host).
- **Permissions.** Directory created `rwx------` (0700), file `rw-------` (0600), set
  *at creation* via `PosixFilePermissions.asFileAttribute(...)` so there is no
  world-readable window. On a non-POSIX FS (`setPosixFilePermissions`/attribute throws
  `UnsupportedOperationException`) fall back to the ACL-protected user-profile dir and
  log a one-line warning that OS-level perms were not enforced (Windows hardening via
  `AclFileAttributeView` is a future item).
- **File format and atomic write** — flat plaintext JSON; the exact schema, file naming,
  and write protocol are the frozen cross-language contract in
  *On-disk interop contract* below. Parsed with the existing `JsonLexer` + a small
  `JsonParser` (same pattern as `TokenResponseParser`); written by hand into a `StringSink`
  with `"`/`\`/control-char escaping.
- **Bounded, defensive read.** Cap the file at a sane size (reuse the 1 MiB
  `JSON_LEXER_MAX_VALUE_BYTES` rationale — an id token with many group claims is several
  KB). Parse failure, size overrun, `v` mismatch, or a **fingerprint that does not match
  the live config** => return `null` (treat as "no cache"), never throw into the sign-in
  path. The fingerprint re-check is defence in depth against a copied/renamed/hostile file
  whose name happens to collide.
- **clear():** `Files.deleteIfExists(target)`.

## Integration into `OidcDeviceAuth`

All four touch points sit under the existing `ReentrantLock`, so persistence I/O is
already serialised with sign-in/refresh/clear and needs no new locking.

1. **Lazy load**, once, at the top of the locked section in `signIn()` and `getToken()`,
   guarded by a `boolean storeLoadAttempted` flag:
   ```java
   if (tokenStore != null && !storeLoadAttempted) {
       storeLoadAttempted = true;            // set first: a bad file is not retried every call
       PersistedToken t = tokenStore.load(storeKey);
       if (t != null) {
           // validate the SERVED token kind exactly as a wire token (reuse validateTokenChars);
           // ignore the file on any failure rather than throw
           accessToken = t.getAccessToken();
           idToken = t.getIdToken();
           refreshToken = t.getRefreshToken();
           expiresAtMillis = t.getExpiresAtMillis();
           tokenTtlMillis = t.getTokenTtlMillis();
       }
   }
   ```
   Nice side effect: after a restart with a persisted refresh token, `getToken()` works as
   the *first* call (no explicit `signIn()` needed) — a clean fit for the
   `Sender...httpTokenProvider(auth::getToken)` pattern. It may cost one silent refresh
   round-trip, which is already inside `getToken()`'s documented contract.

2. **Save** at the end of `storeTokens(...)` (`OidcDeviceAuth.java:1261`), after the
   in-memory fields are set:
   ```java
   persistIfConfigured();   // builds a PersistedToken from the current fields, calls tokenStore.save
   ```
   - On the interactive sign-in: always write (the refresh token is new).
   - On a refresh: **write only when the refresh token changed** (rotation). A
     non-rotating IdP returns no new refresh token (`storeTokens` keeps the old one), so
     the on-disk refresh token is still valid and we skip the write — keeping `getToken()`
     cheap on the hot path. A rotating IdP issues a new refresh token, which we *must*
     persist or a later restart would replay a revoked one; that write is unavoidable.
   - **Best-effort:** wrap `save` so an I/O failure logs one warning and is swallowed —
     a disk problem must never fail an otherwise-valid sign-in. The token is good in
     memory regardless.

3. **clear()** in `clearCache()` (`OidcDeviceAuth.java:361`): after nulling the
   in-memory fields, call `tokenStore.clear(storeKey)` so the next `signIn()` genuinely
   re-prompts. Leave `storeLoadAttempted = true` so we do not immediately reload the file
   we just deleted.

4. **close()** (`OidcDeviceAuth.java:390`): no change. `FileTokenStore` holds no native
   resources; `TokenStore` is deliberately **not** `Closeable`.

The `storeKey` is built once in the constructor from the already-parsed config
(`clientIdEncoded` decodes back, or capture the raw values before encoding;
`tokenEndpoint`/`deviceAuthorizationEndpoint` `Endpoint` -> canonical origin+path).

## Threat model / security

Persisting a refresh token widens the attack surface versus memory-only; this is the
whole reason persistence is **opt-in**. Mitigations, mapped to PR #52's existing posture:

- **At-rest exposure.** Anyone who can read the file (the user, root, a backup) gets a
  credential valid until the IdP expires/revokes it. Mitigation: 0600 file in a 0700 dir,
  created with those perms (no open window). This matches what `gcloud`, `aws`, and `gh`
  do. Residual risk is explicit in the README ("enabling persistence stores a long-lived
  credential on disk; use a `TokenStore` backed by your OS keychain to avoid that").
- **Tampered/forged file = untrusted input.** A file is attacker-writable, so on load we
  (a) bound its size, (b) parse defensively and ignore garbage, (c) re-check the
  in-file fingerprint against the live config, and (d) run `validateTokenChars` on the
  served token before it can become an `Authorization: Bearer` value or a `_sso` password
  — exactly the CR/LF / non-ASCII rejection PR #52 applies to IdP responses
  (`OidcDeviceAuth.java:935-951`). A bad file degrades to an interactive sign-in; it never
  injects into a request or throws token bytes into a message.
- **Never log/echo secrets.** The store never logs token contents and never embeds file
  contents in an exception, upholding PR #52's "tokens never leak into logs or exceptions"
  rule. Only paths and `IOException` kinds appear in the one best-effort warning.
- **Wrong-identity serving.** Prevented by the `TokenStoreKey` (filename hash) plus the
  in-file fingerprint re-check; a token minted for server/scope/audience A is never served
  to a process configured for B.
- **Plaintext-transport interaction.** Unchanged — the IdP endpoints still require
  `https` (loopback excepted), so the refresh token only ever crossed the wire encrypted;
  persistence does not introduce a new cleartext path.

## File format and confidentiality (Q1: plaintext vs encoded)

**The file is plaintext JSON. Confidentiality at rest comes from filesystem
permissions (0600/0700), not from encoding or encryption.** Rationale:

- **Encoding (base64 / obfuscation) is not security and would not be added as if it
  were.** Anyone who can read the file can reverse base64 in one step; it protects
  nothing while implying protection — the opposite of PR #52's habit of being explicit
  about its trust boundaries. It would also hurt the two things plaintext buys us:
  cross-language interop and debuggability.
- **Built-in encryption is a non-goal because of key management** (see Non-goals): a key
  beside the ciphertext is theatre, and a key from an OS secret store needs native code
  we cannot depend on. Worse for this project specifically — a shared *encrypted* format
  would force the Java and Python clients to agree on a cipher *and* a key-derivation
  scheme to interoperate on one file. Plaintext JSON is the only format every language
  reads and writes with zero dependencies, which is exactly what "Java is the reference
  for Python" needs.
- **Real at-rest encryption is delivered through the `TokenStore` SPI** — a caller who
  needs it plugs in a keychain/KMS-backed store (macOS Keychain, Windows DPAPI, Linux
  Secret Service, Vault). If they also need cross-language sharing, they implement the
  same custom store in each client; that is their explicit choice, not our default.
- **This matches the ecosystem.** `gcloud`, `aws`, and `gh` all persist tokens as
  plaintext under owner-only permissions. The README will state the residual risk plainly
  ("persistence writes a long-lived credential to disk in plaintext, protected by file
  permissions; back the store with your OS keychain to avoid that").

Writer correctness: a refresh token is an opaque IdP string, so the JSON writer **must**
escape `"`, `\`, and control characters (`< 0x20` as `\uXXXX`); the existing `JsonLexer`
already decodes escapes on read (a PR #52 change). Base64-ing token *values* would dodge
escaping, but proper escaping is trivial and keeps the file readable — not worth it.

## On-disk interop contract (frozen cross-language spec)

Both clients MUST agree on these to the byte, or they will not share a file (a mismatch
is benign for *correctness* — the fingerprint re-check below still prevents wrong-identity
serving — but it defeats *sharing*, leaving each client to re-prompt).

- **Directory:** `${questdb.client.oidc.token.store.dir}` if set, else
  `${user.home}/.questdb/oidc-tokens/`. Created `rwx------` (0700).
- **File name:** `<hex>.json`, where `<hex>` is the lowercase hex SHA-256 of the
  UTF-8 **canonical identity string**, NUL-separated so no field can be confused with a
  separator:
  ```
  "questdb-oidc-token-v1" \0 clientId \0 canon(tokenEndpoint) \0
  canon(deviceAuthorizationEndpoint) \0 scope \0 (audience ?? "") \0 (groupsInToken?"1":"0")
  ```
  `canon(endpoint)` = `lower(scheme) "://" lower(host) ":" port path`, with the port
  always explicit (the device-flow default 443/80 when absent) and `path` the parsed
  path (no fragment). The hash is a *bucketing* key only; correctness rests on the
  in-file fingerprint, so slight normalization drift across languages costs at most a
  missed share, never a wrong token.
- **Schema** (file perms `rw-------`, 0600):
  ```json
  {
    "v": 1,
    "client_id": "questdb",
    "token_endpoint": "https://idp.example.com/as/token.oauth2",
    "device_authorization_endpoint": "https://idp.example.com/as/device_authz.oauth2",
    "scope": "openid",
    "audience": "api://billing",
    "groups_in_token": false,
    "access_token": "...",
    "id_token": "...",
    "refresh_token": "...",
    "expires_at_millis": 1730000000000,
    "token_ttl_millis": 300000
  }
  ```
  The first seven fields are the **non-secret fingerprint**; on load both clients re-check
  them against the live config and ignore the file on mismatch (defence in depth against a
  hash collision or a copied/renamed file). `expires_at_millis` is absolute wall-clock, so
  it is portable across a restart and across machines that share a clock.

  A field whose value is null - an absent `audience`, or a token kind the grant did not
  return (e.g. no `id_token`) - is **omitted entirely**, not written as JSON `null`. QuestDB's
  `JsonLexer` reports a bare `null` and a quoted `"null"` identically, so omission is the only
  encoding under which every present value round-trips verbatim (a token equal to the string
  `"null"` included); a reader treats an absent field as null. The Python client MUST do the
  same: omit null fields on write, and treat an absent field as null on read.

  The document MUST be a single flat JSON object. A reader rejects any other shape - an array
  anywhere (for example a top-level `[ {…} ]` wrapper) or a non-object root - rather than
  extract fields from a malformed structure. The Python client MUST do the same.
- **Write protocol (atomicity):** write a sibling temp file created with 0600, flush, then
  **atomically rename** over the target — Java `Files.move(tmp, target, ATOMIC_MOVE,
  REPLACE_EXISTING)`, Python `os.replace(tmp, target)`. Both are `rename(2)` on POSIX
  (atomic) and atomic on Windows. A crash or an overlapping reader sees the whole old or
  whole new file, never a torn credential. This is the one *mandatory* multi-writer
  guarantee and it interoperates trivially.

## Cross-process and cross-language coordination (Q2)

Two layers; the first is mandatory, the second handles the one case the first cannot.

**Layer 1 — atomic replacement (always; cross-language-safe).** The write protocol above
makes every update all-or-nothing, so any mix of processes and languages sharing one file
(two notebook kernels, a restart overlapping the old process, a Java writer and a Python
reader) is *integrity-safe*: no torn reads, no partial credential. For the common case —
an IdP that does **not** rotate refresh tokens — this is fully sufficient: every process
holds the same stable refresh token, each independently refreshes to mint its own access
token, and last-writer-wins on the file is harmless because access tokens are
interchangeable and the fingerprint fields are identical for one identity.

**Layer 2 — a lock-file critical section (for rotating refresh tokens).** When the IdP
*rotates* the refresh token on every refresh (Auth0 public clients, OAuth 2.1 BCP
guidance), bare last-writer-wins races: two processes load RT1, both refresh, the IdP
invalidates RT1, one wins and the loser's RT1 is now revoked → an unnecessary interactive
re-prompt. To eliminate it, serialise the *read-modify-write* of a refresh per identity:

- **Use a lock *file*, not an OS advisory lock.** Java `FileLock` maps to `fcntl` POSIX
  record locks on Unix while Python's `fcntl.flock` is BSD `flock`; the two **do not
  interoperate on Linux**. A lock file acquired with `O_CREAT|O_EXCL` (Java
  `FileChannel.open(..., CREATE_NEW, WRITE)`, Python `os.open(..., O_CREAT|O_EXCL|O_WRONLY)`
  / `open(p,"x")`) is a plain filesystem primitive that interoperates trivially. The contract
  mandates the lock-file scheme; OS advisory locks are out.
- **Lock file:** `<hex>.lock` beside the token file, containing a unique per-acquisition
  owner stamp — the holder's `pid@host`, a creation timestamp, and a random nonce.
  Acquire by an exclusive-create that writes the owner stamp in the SAME atomic open (create
  and stamp are one operation, not two — see the empty-lock note below); on contention, spin
  with short backoff up to a small acquire budget (~3s); if it still cannot be acquired,
  **proceed without it** (degrade to Layer 1) rather than fail a sign-in. A lock older than a staleness timeout (10 minutes)
  is treated as abandoned and stolen, so a crashed holder cannot wedge others. The window
  must dominate the worst-case time a live holder can hold the lock. That worst case has two
  parts: the refresh I/O under the lock — send + await + parse, plus a body drain on a parse
  failure, each separately bounded by the HTTP timeout (capped at 120s), so up to ~4×120s =
  ~480s — **plus the connection phase that precedes the send** — DNS resolution, the TCP
  connect, and the TLS handshake — which is **not** bounded by the HTTP timeout (the OS bounds
  the connect instead; a black-holed connect can run to the OS TCP-connect timeout, commonly
  ~2 minutes). So size the window above ~4×HTTP-timeout **plus a generous connection-stall
  allowance**, never just ~4×HTTP-timeout; the interactive wait is never held under the lock.
  10 minutes clears ~480s with ample headroom for a typical connection stall; a client that
  raises the HTTP timeout must raise this window in step. A client MUST NOT advertise a tighter
  guarantee than this (an earlier draft claimed ~480s alone, omitting the connection phase).
- **An empty/unstamped lock is reclaimable on a short grace, not the full staleness window.**
  Acquire writes the owner stamp in the SAME atomic exclusive-create (one `O_CREAT|O_EXCL` open,
  then the nonce), so a LIVE lock always carries a stamp and there is **no create→stamp gap** for
  a GC/safepoint pause to straddle. An empty `<hex>.lock` can therefore arise only from a crash
  mid-write — the exclusive create succeeded but the nonce write did not — a rare, narrow window
  entirely inside the single write call (no bytecode boundary between two separate syscalls where
  a pause is reported). Its mtime is fresh, which the staleness check would protect for the whole
  window, wedging peers into lock-free refreshes; so treat a lock that carries no readable owner
  stamp as stealable once it is older than a short grace (a few seconds) instead of the full
  window. A cross-machine clock skew wider than the grace (the age check compares the local clock
  against the file's mtime) could still pre-empt such a partial lock, but that never forges or
  tears a credential — Layer 1's atomic replacement always holds — it degrades to a concurrent
  refresh (a re-prompt on a rotating-refresh-token IdP), the same best-effort residual as running
  lock-free. The capture-then-verify steal below still aborts if the captured lock does not match
  what was judged stale. The Python client MUST mirror this atomic create-with-stamp and the
  empty-lock grace.
- **Release verifies ownership.** A holder releases by re-reading the lock and deleting it
  **only when it still carries that holder's own owner stamp**, never by bare path. Should
  a hold ever outrun the staleness window and be stolen and recreated by a peer, the
  original holder must not delete the peer's live lock on release (which would admit a
  third acquirer alongside the peer and break mutual exclusion). Each implementation
  checks only its own stamp; it never has to parse another implementation's stamp, so the
  random nonce keeps the check exact without coupling the language clients.
- **Protocol (under the existing in-process `ReentrantLock`, only when a refresh is
  needed):**
  1. acquire `<hex>.lock`;
  2. **re-read the token file** — another process may have just refreshed;
  3. if the freshly read served token is now valid, adopt it (re-running
     `validateTokenChars`) and **skip the network**;
  4. else POST the refresh with the current refresh token; `storeTokens()` writes the
     new token atomically *inside* the lock;
  5. release (delete `<hex>.lock` only if it still carries our own owner stamp).

  The interactive device flow does **not** hold the lock file (coordinating human prompts
  across processes is overkill and would hold a cross-process lock for up to 30 min); two
  cold processes may each prompt once, after which later processes read the persisted
  refresh token. Lock ordering is always in-process lock then lock file (leaf), so no
  deadlock.

- **SPI shape:** keep `TokenStore` simple for the no-coordination case and add one
  optional hook, e.g. `default <T> T inLock(TokenStoreKey, Supplier<T> action)` that just
  runs `action` (no lock). `FileTokenStore` overrides it with the lock-file protocol;
  `OidcDeviceAuth` wraps its refresh step in `inLock` and does the re-read-then-decide
  (steps 2–4) as the action body. A store with no cross-process concern stays a plain
  load/save/clear.

**Staging.** Layer 1 is required and small; Layer 2 is only needed for rotating IdPs.
Both can ship together, or Layer 1 first with Layer 2 as a fast-follow — but the lock-file
protocol above should be frozen into the spec now so the Python client implements the
same one. (See Decisions.)

## Decisions

Resolved:
- **Full token blob (option A)** — persist access + id + refresh + expiry; no change to
  the audited `signIn`/`getToken`/`tryRefresh` gate.
- **Plaintext JSON**, confidentiality via file permissions; encryption only via the SPI
  (Q1).
- **`System.err`** for the one best-effort persistence-failure warning.
- **Opt-in** (no store unless the caller sets one).
- **Ship `FileTokenStore`** as the default; keychain/KMS via the SPI.
- **Frozen on-disk contract** (path, hash, schema, atomic write, lock-file protocol),
  because the Python client will mirror it.

Resolved (shipped in PR #52):
- **Layer 2 (lock file) shipped together with Layer 1.** `FileTokenStore` implements both the
  mandatory atomic-replace integrity layer and the `O_CREAT|O_EXCL` lock-file critical section
  for rotating-refresh-token IdPs, as recommended — the rotating case is realistic and the
  lock-file code is modest.

## Testing strategy

- `FileTokenStore`: round-trip save/load; perms are 0600/0700 (skip on non-POSIX);
  ATOMIC_MOVE leaves no `.tmp`; corrupt/oversized/garbage file -> `load` returns null, no
  throw; fingerprint mismatch -> null; a token with CR/LF/non-ASCII -> rejected on load.
- `OidcDeviceAuth` against a fake `TokenStore` + the existing `MockOidcServer`:
  - sign in -> a second *new* instance with the same store skips the device flow and only
    hits the token endpoint (silent refresh) — assert the device-auth endpoint is never
    called.
  - quick restart with an unexpired persisted access token -> zero network.
  - rotating refresh token -> file rewritten each refresh; non-rotating -> written once.
  - `clearCache()` deletes the file -> next `signIn()` re-runs the device flow.
  - `save` throwing -> sign-in still returns a valid token (best-effort), warning emitted.
  - `getToken()` as the first call after a restore (no `signIn()`), refresh path only.
- `assertMemoryLeak` around tests that build a real `OidcDeviceAuth` (native lexer).

## Open questions

- **Default location on Windows** — `${user.home}/.questdb` is fine functionally, but the
  ACL hardening story there is unfinished: POSIX perms do not apply, so the file relies on
  the user-profile directory's default ACL. Tightening via `AclFileAttributeView`
  (owner-only) is a possible follow-up; the Python client will face the same gap.
- **Windows lock-file interop** — the `O_EXCL` lock-file scheme works on Windows
  (`CREATE_NEW`), but the staleness/steal heuristic must tolerate Windows' stricter
  delete-while-open semantics; verify before relying on Layer 2 cross-platform.

Notes carried from the discussion (not open):
- Python persistence does not exist yet and will be built **after** the Java client, using
  this as the base — hence the frozen contract. The single most important thing Python
  must copy verbatim is the **lock-file** coordination (not an OS advisory lock), since
  Java `FileLock` (`fcntl`) and Python `flock` do not interoperate.
