/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.cutlass.auth;

import io.questdb.client.cutlass.json.JsonException;
import io.questdb.client.cutlass.json.JsonLexer;
import io.questdb.client.cutlass.json.JsonParser;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.Os;
import io.questdb.client.std.str.DirectUtf8Sink;
import io.questdb.client.std.str.StringSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The default {@link TokenStore}: one plaintext JSON file per OIDC configuration under a directory, with
 * the refresh token protected at rest by file permissions (0600 file, 0700 directory) rather than by
 * encryption. This matches what {@code gcloud}, {@code aws} and {@code gh} do; for encryption at rest,
 * supply a {@link TokenStore} backed by an OS keychain or a secrets manager instead.
 * <p>
 * The default location is {@code ${user.home}/.questdb/oidc-tokens/}, overridable with the
 * {@code questdb.client.oidc.token.store.dir} system property. The file name is
 * {@code <TokenStoreKey.hash()>.json}, so several configurations coexist and the name leaks neither the
 * endpoint nor the client id. The on-disk format (file name, JSON schema, write protocol, lock-file
 * protocol) is a deliberately language-neutral contract so other QuestDB clients can share the file.
 * <p>
 * <b>One store, one active login.</b> {@link TokenStoreKey} names a CONFIGURATION - client id, endpoints,
 * scope, audience, groups-in-token mode - and no field of it names a subject, so two people signing in
 * through the same configuration address the same file and the later sign-in overwrites the earlier one.
 * Separate application users need separate stores ({@link #at(Path)} on a per-user directory, or a per-user
 * {@code questdb.client.oidc.token.store.dir}), not a reliance on the key to tell them apart. The default
 * location is per OS user already, so this only arises inside one OS user - a shared service account, or a
 * process signing in on behalf of several people.
 * <p>
 * <b>Not authenticated.</b> The file carries no MAC or signature, so {@link #load} cannot distinguish a
 * planted credential from its own: anyone able to WRITE the file can substitute a well-formed entry that
 * this store will adopt and the caller will present. Permissions are the control, not the format. What load
 * does reject is corruption and mix-ups - an oversized, malformed or unparseable file, an entry whose
 * recorded identity fields do not match the key being loaded, an entry with no usable token, a token
 * carrying control or non-ASCII characters - with the recorded expiry and lifetime clamped rather than
 * trusted, and (on POSIX) an entry discarded outright when the directory is writable by other local users.
 * <p>
 * <b>Integrity (always).</b> {@link #save} writes a sibling temp file then atomically renames it over the
 * target, so a crash or an overlapping reader - in any process or language - sees the whole old or whole
 * new file, never a torn credential.
 * <p>
 * <b>Rotating refresh tokens (Layer 2).</b> {@link #inLock} serialises the read-refresh-write of a token
 * refresh across processes with an {@code O_CREAT|O_EXCL} lock file ({@code <hash>.lock}) - not an OS
 * advisory lock, which a Java {@code FileLock} and a Python {@code flock} cannot reliably share. It steals
 * a stale lock left by a crashed holder, and degrades to running without the lock (Layer 1 still protects
 * integrity) rather than stall a sign-in if it cannot acquire one.
 * <p>
 * That degrade has a residual worth understanding: if a peer's refresh genuinely outlasts the acquire
 * budget (a slow or stalled IdP), or its lock is judged stale and stolen mid-refresh, two processes can
 * POST the same parent refresh token concurrently. On an IdP that does not detect refresh-token reuse this
 * costs only a redundant refresh; on one that DOES (for example Auth0's default), reusing one parent token
 * twice can revoke the whole token family, forcing every process to re-run the interactive device flow -
 * which, for a headless {@code getToken()} consumer with no interactive fallback, is a hard failure until a
 * human re-signs in. If that matters, widen the acquire budget / staleness window, or back the store with a
 * keychain or secrets manager instead.
 * <p>
 * The store never writes a token value into a log or an exception message; only file paths and IO error
 * kinds may surface.
 */
public final class FileTokenStore implements TokenStore {
    public static final String TOKEN_STORE_DIR_PROPERTY = "questdb.client.oidc.token.store.dir";
    // wait this long for the per-identity lock before giving up and running without it (Layer 1 still
    // guards integrity). Kept short because getToken() can take this lock on the latency-sensitive flush
    // path: a real refresh round-trip is sub-second, so a peer not done within this budget is treated as
    // too slow and we degrade to a lock-free refresh rather than stall the caller
    private static final long DEFAULT_LOCK_ACQUIRE_BUDGET_MILLIS = 3_000L;
    // treat a lock older than this as abandoned by a crashed holder and steal it. Must stay comfortably
    // above the longest a live holder can hold it (one refresh under the lock) so a live holder is never
    // stolen from. That refresh runs send + await + parse, plus a body drain on a parse failure, each
    // separately bounded by the client's HTTP timeout (so up to ~4x it; OidcDeviceAuth caps that timeout at
    // 120s, hence ~480s), PLUS the connection phase (DNS + TCP connect + TLS handshake), which the HTTP
    // timeout does NOT bound and which the OS bounds instead (a black-holed connect is ~tcp-connect-timeout,
    // commonly ~2 minutes on Linux). This 10-minute window leaves ample headroom above ~480s + a typical
    // connection stall; a pathological DNS/connection hang longer than that headroom can still let a peer
    // steal a live holder's lock mid-refresh, degrading to a concurrent refresh of the same parent refresh
    // token: a redundant refresh on most IdPs, but on a reuse-detecting one (e.g. Auth0 default) a possible
    // token-family revocation and re-prompt / headless hard-failure (see the class javadoc residual note)
    private static final long DEFAULT_LOCK_STALE_MILLIS = 600_000L;
    private static final FileAttribute<Set<PosixFilePermission>> DIR_ATTRS =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    // the same owner-only directory permissions as DIR_ATTRS, in the form setPosixFilePermissions wants, so
    // ensureDirectory can re-assert them on a directory that already exists with looser permissions
    private static final Set<PosixFilePermission> DIR_PERMS = PosixFilePermissions.fromString("rwx------");
    // steal an empty/unstamped lock once it has existed at least this long. A validly held lock always
    // carries an owner stamp (acquireLock stamps it immediately after the exclusive create); an empty lock is
    // therefore either a peer momentarily between its create and its stamp - microseconds, far below this
    // grace - or one a holder abandoned by crashing in that tiny window. Stealing on this short grace instead
    // of the full staleness window keeps a post-crash empty lock from wedging peers (into lock-free refreshes)
    // for the whole staleness window, while the grace stays well above the create->stamp gap so a peer
    // mid-stamp is never pre-empted (which would force the rightful holder to degrade)
    private static final long EMPTY_LOCK_STEAL_GRACE_MILLIS = 5_000L;
    private static final FileAttribute<Set<PosixFilePermission>> FILE_ATTRS =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"));
    // Length of the identity fingerprint every file this store writes is named after: TokenStoreKey.hash()
    // is a SHA-256 rendered as lowercase hex, so 64 characters. Used to tell this store's own files apart
    // from whatever else shares the directory - see discardUntrustedDirectoryContents().
    private static final int HASH_NAME_LENGTH = 64;
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final int JSON_LEXER_CACHE_SIZE = 1024;
    private static final int JSON_LEXER_MAX_VALUE_BYTES = 1 << 20;
    private static final long LOCK_POLL_SLICE_MILLIS = 50L;
    private static final Logger LOG = LoggerFactory.getLogger(FileTokenStore.class);
    // reject a token file larger than this; a real entry is a few KB even with a group-laden id token, so
    // anything past this is corrupt or hostile and is not read into memory
    private static final long MAX_FILE_BYTES = 1 << 20;
    // upper bound on the configurable lock acquire budget. getToken() can take this lock on the
    // latency-sensitive flush path, so a caller-supplied budget is kept short: a real peer refresh is
    // sub-second, and bounding the wait stops a misconfigured budget from stalling a flush before it degrades
    // to a lock-free refresh (Layer 1 still guards integrity). Stays well below DEFAULT_LOCK_STALE_MILLIS so a
    // waiter degrades long before it could begin stealing live locks.
    private static final long MAX_LOCK_ACQUIRE_BUDGET_MILLIS = 30_000L;
    // reject a lock file larger than this before reading it: the <hash>.lock file sits in the same
    // attacker-writable directory as the token file, and a real owner stamp (millis + UUID) is a few dozen
    // bytes, so anything past this cap is corrupt or hostile and is not read into memory
    private static final int MAX_LOCK_FILE_BYTES = 1 << 12;
    // Serializes same-identity critical sections WITHIN this JVM. Two OidcDeviceAuth instances for one
    // identity in a single process (e.g. an ILP Sender and a QwpQueryClient) have separate instance locks, so
    // only this shared lock stops them running the read-refresh-write concurrently and double-POSTing the same
    // parent refresh token - which a reuse-detecting IdP revokes the whole token family for. The cross-process
    // file lock's lock-free degrade must not license an intra-process race, so this in-process lock is taken
    // first and is never subject to that degrade.
    //
    // Keyed on the identity fingerprint, with one entry per identity that currently has a holder or a
    // waiter and nothing left behind once the last of them leaves. TokenStoreKey is public and inLock() is
    // public API, so how many distinct identities a process mints is the caller's business - one per end
    // user in a multi-tenant service is a perfectly ordinary shape - and an entry per identity EVER SEEN,
    // which an unpruned map gives, roots a 64-char hash plus a lock for the life of the JVM. Retiring on
    // the last release bounds the map by CONCURRENT identities instead, which is bounded by live threads.
    //
    // A fixed stripe table also bounds it, and was tried, but over-serializing is not the free trade it
    // looks: this lock is held across a whole token-endpoint round trip while the caller also holds its
    // OidcDeviceAuth instance lock, and the acquire has no budget. Two unrelated identities landing on one
    // stripe therefore do not merely "wait for each other" - one tenant's ILP flush blocks on another
    // tenant's stalled refresh for that holder's entire worst case, which getToken() sizes at six times
    // httpTimeoutMillis plus an OS connect stall, and every other caller on the blocked instance fails
    // meanwhile. That also made OidcDeviceAuth.getToken()'s "two instances sharing ONE IDENTITY" contract
    // untrue. Per-identity entries serialize exactly the same-identity pairs the double-POST rule needs and
    // nothing else, so the contract holds as written.
    //
    // compute()/computeIfPresent() apply their function atomically under the bin lock, so `users` needs no
    // synchronization of its own and retirement has no race: an arriving thread cannot observe an entry
    // that a departing one is removing.
    private static final ConcurrentHashMap<String, ProcessLock> PROCESS_LOCKS = new ConcurrentHashMap<>();
    // Windows can fail the atomic token-file rename with a transient AccessDeniedException (a sharing violation)
    // when a concurrent reader in any process holds the target open; retry the rename this many times on a short
    // backoff before giving up, so a routine read/write overlap does not needlessly degrade persistence. Kept
    // small - persistence is best-effort and the in-memory token is valid regardless.
    private static final int REPLACE_MAX_ATTEMPTS = 5;
    private static final long REPLACE_RETRY_SLEEP_MILLIS = 20L;
    private static final int SCHEMA_VERSION = 1;
    // set once if the platform cannot enforce owner-only POSIX permissions on the token files (e.g. Windows),
    // so the at-rest protection falls back to the directory's inherited ACL; warns the user exactly once
    // (compareAndSet, so a race between two threads still prints a single warning)
    private static final AtomicBoolean warnedNoPosixPerms = new AtomicBoolean();
    private static final AtomicBoolean warnedTightenedStoreDir = new AtomicBoolean();
    private static final AtomicBoolean warnedUnprotectedStoreDir = new AtomicBoolean();
    private final Path directory;
    private final long lockAcquireBudgetMillis;
    // Namespaces this store's entries in PROCESS_LOCKS, so two stores over DIFFERENT directories never
    // contend even when they share one OIDC configuration. Normalized once here rather than per acquire:
    // "a" and "./a" must not mint two locks over one directory, which would be the dangerous direction.
    // toAbsolutePath().normalize() rather than toRealPath(): the directory may not exist yet (the store
    // creates it lazily), and a key that changed once it did would be worse than one that ignores symlinks.
    // Two stores reaching one directory through different symlinks therefore still get separate in-process
    // locks; the cross-process lock file, which they DO share, remains the guard for that shape.
    private final String lockNamespace;
    private final long lockStaleMillis;

    public FileTokenStore(Path directory) {
        this(directory, DEFAULT_LOCK_ACQUIRE_BUDGET_MILLIS, DEFAULT_LOCK_STALE_MILLIS);
    }

    /**
     * Advanced constructor exposing the cross-process lock-file timings used by
     * {@link #inLock(TokenStoreKey, CriticalSection)}. Most callers should use {@link #FileTokenStore(Path)}
     * or the factories, which apply sensible defaults.
     *
     * @param directory               the directory to hold the token files
     * @param lockAcquireBudgetMillis  how long {@code inLock} waits to acquire a peer's lock before degrading
     *                                 to a lock-free refresh rather than stalling a sign-in. Must be positive
     *                                 and at most 30_000 (30s): {@code getToken()} can wait it out on the
     *                                 latency-sensitive flush path, so it is kept short
     * @param lockStaleMillis          a lock older than this is treated as abandoned by a crashed holder and
     *                                 stolen. It MUST exceed the longest a live holder can hold the lock, which
     *                                 is the under-lock refresh PLUS the connection phase that precedes it: the
     *                                 refresh runs send + await + parse plus a body drain, each bounded by the
     *                                 {@code OidcDeviceAuth} httpTimeoutMillis (so up to ~4x it, ~480s at the
     *                                 120s timeout cap), but establishing the connection - DNS resolution, the
     *                                 TCP connect, and the TLS handshake - is NOT bounded by httpTimeoutMillis;
     *                                 the OS bounds it instead (a black-holed connect runs to the OS TCP-connect
     *                                 timeout, commonly ~3 minutes). Size this window above ~6x httpTimeoutMillis
     *                                 plus a generous connection-stall allowance, or a peer can judge a live but
     *                                 connection-stalled holder stale and steal its lock mid-refresh, reopening
     *                                 the cross-process refresh race this lock exists to prevent. The store
     *                                 cannot see the client's timeout, so sizing this correctly is the caller's
     *                                 responsibility; the default is 600_000 (~480s worst-case refresh plus
     *                                 ample headroom for a typical connection stall).
     */
    public FileTokenStore(Path directory, long lockAcquireBudgetMillis, long lockStaleMillis) {
        if (directory == null) {
            throw new OidcAuthException("the token store directory is required");
        }
        if (lockAcquireBudgetMillis <= 0) {
            throw new OidcAuthException("the token store lockAcquireBudgetMillis must be positive");
        }
        if (lockAcquireBudgetMillis > MAX_LOCK_ACQUIRE_BUDGET_MILLIS) {
            // getToken() can wait out this budget on the latency-sensitive flush path, so an unbounded value
            // would let a misconfiguration stall a flush; keep it short - it degrades to a lock-free refresh
            throw new OidcAuthException()
                    .put("the token store lockAcquireBudgetMillis must not exceed ").put(MAX_LOCK_ACQUIRE_BUDGET_MILLIS);
        }
        // a non-positive staleness window makes every freshly created lock look abandoned, so acquirers would
        // steal each other's live locks; keep it well above one refresh round-trip (see the default)
        if (lockStaleMillis <= 0) {
            throw new OidcAuthException("the token store lockStaleMillis must be positive");
        }
        this.directory = directory;
        this.lockNamespace = directory.toAbsolutePath().normalize().toString();
        this.lockAcquireBudgetMillis = lockAcquireBudgetMillis;
        this.lockStaleMillis = lockStaleMillis;
    }

    /**
     * @param directory the directory to hold the token files; created on first write with owner-only
     *                  permissions
     * @return a store rooted at the given directory
     */
    public static FileTokenStore at(Path directory) {
        return new FileTokenStore(directory);
    }

    /**
     * @return a store at {@code ${questdb.client.oidc.token.store.dir}} if that system property is set,
     * otherwise at {@code ${user.home}/.questdb/oidc-tokens/}
     */
    public static FileTokenStore atDefaultLocation() {
        String override = System.getProperty(TOKEN_STORE_DIR_PROPERTY);
        Path dir = override != null && !override.isEmpty()
                ? Paths.get(override)
                : Paths.get(System.getProperty("user.home"), ".questdb", "oidc-tokens");
        return new FileTokenStore(dir);
    }

    @Override
    public void clear(TokenStoreKey key) {
        if (!Files.isDirectory(directory)) {
            return; // nothing is persisted yet; do not create the directory just to clear it
        }
        // Interrupt-neutral, for the reason load() and save() are, and more sharply. Those two abandon file
        // I/O; this one is a local DELETE whose entire purpose is to erase a secret, so there is nothing to
        // abandon on a cancellation and "we were cancelled" is not a reason to leave a plaintext refresh token
        // behind. Routed through inLock, a merely CARRIED interrupt flag - the standard state of a cancelled
        // or shutting-down thread, which is exactly where a sign-out runs - made inLock skip the action and
        // return false, which this method discarded: clear() returned normally, clearCache() reported success,
        // the file stayed on disk, and the next process start silently resumed the old identity.
        final boolean wasInterrupted = Thread.interrupted();
        try {
            // delete under the cross-process lock, like the read-refresh-write, so a peer's in-flight refresh
            // cannot resurrect the entry by atomically renaming a fresh file in just after we delete. inLock
            // cleans up its own lock file and degrades to lock-free if it cannot acquire one. Cross-process
            // clear is still best-effort: a peer holding a live in-memory token may legitimately re-persist
            // later - clearing forces a fresh sign-in for THIS process regardless, since the caller resets its
            // in-memory token state.
            final CriticalSection delete = () -> {
                try {
                    Files.deleteIfExists(tokenFile(key));
                } catch (IOException e) {
                    throw new OidcAuthException(e).put("could not remove the OIDC token store file");
                }
                // Also remove any write temp for this identity. A crash between createTempFile and the atomic
                // rename orphans a <hash><random>.tmp holding the FULL serialized entry - access, id and
                // refresh tokens in plaintext - and until now nothing here reclaimed it: sweepStaleTempFiles
                // runs only from save(), so a caller that clears and never signs in again left a live refresh
                // token on disk indefinitely, contradicting this method's contract. Sweep at ANY age, unlike
                // save()'s staleness-bounded sweep: clear() is an explicit "forget this credential", and a
                // temp a concurrent save is mid-rename on is a benign loser - its rename fails, persistence
                // is best-effort, and the caller is discarding the credential anyway.
                sweepTempFiles(key.hash(), 0L);
                return true;
            };
            if (!inLock(key, delete)) {
                // inLock declined to RUN the action - a live cancellation, or it could not coordinate at all.
                // It returns false only when the action never ran (this action always returns true), so this
                // cannot double-delete. Run it uncoordinated rather than return with the credential still on
                // disk: the cross-process lock only orders us against a peer's in-flight refresh, and losing
                // that ordering costs at worst a peer re-persisting later, which this method already documents
                // as best-effort. Leaving the secret behind is not a trade this call may make.
                delete.run();
            }
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean inLock(TokenStoreKey key, CriticalSection action) {
        // First serialize other threads of THIS JVM sharing the same identity: the cross-process file lock below
        // degrades to lock-free after lockAcquireBudgetMillis, which is a fine cross-process fallback but must
        // not let two threads of one process run the critical section at once (they would double-POST the same
        // rotating refresh token and get the whole family revoked on a reuse-detecting IdP). This lock is not
        // subject to the file lock's degrade. ReentrantLock is safe even though inLock's contract forbids
        // nesting - a mistaken re-entry cannot self-deadlock.
        // An interrupt CARRIED ON ENTRY is the caller's own state, not a signal aimed at any wait below:
        // preserve it and abort before touching a lock. This has to be tested BEFORE the acquire, not after
        // it: ReentrantLock.lockInterruptibly() begins with Thread.interrupted(), so it throws even on a FREE,
        // UNCONTENDED lock and CLEARS the flag - a carried interrupt was therefore misread as a live
        // cancellation by the catch below, which does not re-assert, so the caller's cancellation signal was
        // destroyed and the critical section skipped on a lock nobody held. Aborting here also avoids
        // acquiring a lock for a critical section we should not start, which would only delay the caller and
        // risk stranding a lock file for its whole staleness window.
        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            return false;
        }
        // Retain before the acquire and release in the outermost finally, so every exit - the interrupted
        // acquire below included - gives the claim back exactly once.
        // Namespaced by DIRECTORY as well as identity. TokenStoreKey names a CONFIGURATION and carries no
        // directory, so keying on it alone made two stores over per-user directories - the multi-user recipe
        // this class and the README both prescribe - queue on one lock while touching different files. That
        // lock is held across a whole token-endpoint round trip and its acquire has no budget, so one user's
        // stalled refresh blocked another's getToken() on the flush path, for exactly the reason the stripe
        // table considered above was rejected.
        final String lockIdentity = processLockIdentity(key);
        final ProcessLock processLock = retainProcessLock(lockIdentity);
        // lockInterruptibly, never lock(): a peer thread on this identity holds this for a whole refresh round
        // trip, and an interrupt is the ONLY lever that reaches a caller stuck behind it. QWP's
        // ConnectCancellation.cancel() interrupts a thread inside a credential pull precisely so close() can
        // unstick it; an uninterruptible acquire here sleeps through that, outlives close()'s shutdown budget,
        // and leaves the native client, the cursor engine and the slot lock to a delegated teardown.
        try {
            processLock.lock.lockInterruptibly();
        } catch (InterruptedException e) {
            // Interrupted WAITING for the process lock: a live cancellation, acted on by abandoning the
            // refresh. RE-ASSERT the flag before returning. A bare `false` is indistinguishable from "the
            // refresh ran and failed", which is the one thing the caller must not conclude here: signIn()
            // reads it that way and starts the interactive device flow -- a browser launch and a poll loop
            // that runs to the device-code lifetime on Os.sleep, which ignores interrupts -- on a thread
            // whose owner has already asked it to stop, and getToken() reads it that way and arms the
            // shared refresh back-off on a refresh that never happened, failing every other caller of this
            // instance for the next five seconds. Restoring the flag is what lets signIn()'s
            // throwIfInterrupted and getToken()'s post-refresh check tell the two apart. It is also what
            // load() and save() already do; only this method consumed the signal.
            //
            // Safe to restore here: nothing below this point performs interruptible I/O -
            // releaseProcessLock is a ConcurrentHashMap update, and no lock file was ever opened.
            releaseProcessLock(lockIdentity); // the acquire never happened, so give the claim straight back
            Thread.currentThread().interrupt();
            return false;
        }
        try {
            // A LIVE interrupt that landed between the acquire above and here - the carried case is already
            // handled before the acquire. Same answer either way: preserve it and abort before touching any
            // lock file, rather than clear it to push the FileChannel I/O through (a set flag turns that into
            // ClosedByInterruptException) and run the critical section anyway.
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                return false;
            }
            Path lock = null;
            // the unique owner nonce stamped into the lock when we acquired it, or null if we did not (or could
            // not) acquire one and are running lock-free; releaseLock deletes the lock only when it still carries
            // this nonce, so we never delete a lock a peer has since stolen
            String nonce = null;
            // set when an interrupt arrives while we poll for the cross-process lock; see acquireLock
            boolean cancelled = false;
            try {
                if (!ensureDirectory()) {
                    // As in save(). The action run under this lock is a refresh that re-reads the store, so
                    // an entry exposed before this call must not survive to be adopted by it.
                    discardUntrustedDirectoryContents();
                }
                lock = lockFile(key);
                nonce = acquireLock(lock);
            } catch (InterruptedException e) {
                // Arrived DURING the poll, so it is a live cancellation rather than carried state. Consumed
                // for the same reason as the process-lock wait above.
                cancelled = true;
            } catch (IOException | RuntimeException e) {
                // could not prepare the lock directory or file; run without the cross-process lock. Layer-1
                // atomic replacement still keeps every reader consistent - only a rotating-refresh-token race
                // across processes is left unguarded for this one refresh.
                //
                // RuntimeException as well as IOException: this is lock BOOKKEEPING, and none of it is a
                // reason to fail a sign-in the caller could otherwise complete. A SecurityManager denying
                // the directory or the lock file throws SecurityException, and a filesystem that cannot
                // carry POSIX permissions throws UnsupportedOperationException - both unchecked, both
                // previously escaping past the caller's degrade path and aborting signIn()/getToken()
                // outright, which is the opposite of what a best-effort store should do.
                nonce = null;
            }
            try {
                // The critical section is a fresh HTTP round trip - exactly the work a cancellation is trying
                // to stop - so never start it once an interrupt has been observed. isInterrupted() rather than
                // interrupted() for the late arrival: we did not catch that one, so it is not ours to clear.
                if (cancelled || Thread.currentThread().isInterrupted()) {
                    if (cancelled) {
                        // acquireLock's poll consumed the flag; put it back for the same reason the
                        // process-lock wait above does, so the caller can tell a cancelled wait from a
                        // failed refresh. The late-arrival case needs nothing - that flag is still set.
                        // Nothing below performs interruptible I/O: cancelled implies acquireLock threw,
                        // so nonce is null and the finally below skips releaseLock.
                        Thread.currentThread().interrupt();
                    }
                    return false;
                }
                return action.run();
            } finally {
                if (nonce != null) {
                    // Release under the same shield, and re-read the flag here rather than reusing the value
                    // above: the interrupt that matters usually arrives DURING action.run() (close() breaking
                    // a stuck credential pull). Without this, releaseLock's channel read throws
                    // ClosedByInterruptException, the lock file survives its whole staleness window, and every
                    // peer degrades to an unserialized refresh meanwhile.
                    boolean wasInterruptedInSection = Thread.interrupted();
                    try {
                        releaseLock(lock, nonce);
                    } catch (RuntimeException e) {
                        // This runs in a finally, AFTER the critical section returned. A throw here would
                        // replace the caller's completed refresh with an exception - the refresh happened,
                        // the token is live, and the caller would be told the sign-in failed. releaseLock
                        // already absorbs IOException; a SecurityManager denying the delete throws
                        // SecurityException, which is unchecked and was escaping. Same operator-visible
                        // warning, same degrade: peers run unserialized until the lock goes stale.
                        // sanitized: an IO error message embeds the operator-supplied store path, which is
                        // the one untrusted string these warnings put in front of a terminal
                        LOG.warn("could not release the OIDC token store lock; peers degrade to lock-free "
                                + "refresh until it goes stale [error={}]",
                                OidcDeviceAuth.sanitizeForDisplay(e.getMessage()));
                    } finally {
                        if (wasInterruptedInSection) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        } finally {
            processLock.lock.unlock();
            releaseProcessLock(lockIdentity);
        }
    }

    @Override
    public PersistedToken load(TokenStoreKey key) {
        // Every file operation below goes through FileChannel, an InterruptibleChannel: a thread that
        // merely CARRIES a set interrupt flag makes the first read throw ClosedByInterruptException and
        // closes the channel, and the flag survives. Two callers routinely arrive here with it set - an
        // ILP producer on a pooled or managed thread, where interrupt is the standard cancellation
        // signal, and the sender's own I/O thread, which close() interrupts to break a stuck credential
        // pull. Neither means "abandon the token store", so clear the flag for the duration of the file
        // I/O and restore it on the way out: the caller's cancellation signal survives intact, while the
        // store's reads stop being collateral damage.
        final boolean wasInterrupted = Thread.interrupted();
        try {
            // Assert the directory on the READ path too, not only on the write paths. adopt() rejects an
            // entry carrying only a refresh token, but a COMPLETE planted entry - a dummy access token, the
            // attacker's refresh token, and an expiry already in the past - takes the normal path and the
            // next silent refresh presents their credential. Closing that needs the container checked as
            // well as the artefact: a store directory another local user can write is one whose contents
            // were never ours to trust. Fail closed - a null return is the documented outcome for any
            // unusable entry and degrades to a refresh or an interactive sign-in.
            final boolean isDirectoryTrusted;
            try {
                isDirectoryTrusted = ensureDirectory();
            } catch (IOException e) {
                // THROW, do not return null. load()'s contract makes the two mean opposite things: null is
                // the definitive "there is nothing here", which latches storeLoadAttempted and ends the
                // reads for the life of the OidcDeviceAuth, while a throw reads as a transient fault and is
                // retried under the store-load back-off. What ensureDirectory() reports here is squarely
                // transient - Files.createDirectories failing because a home directory is not mounted yet,
                // EIO/ESTALE on an NFS home, a momentarily read-only or full filesystem - so answering null
                // told every later call that a store holding a perfectly good refresh token was empty. The
                // process then re-runs the interactive device flow, and for the headless getToken()
                // consumer this persistence exists to serve, that is a hard failure with no recovery short
                // of a restart. The sibling arm below already throws for readBounded's IOException, and
                // save() lets this very exception propagate; only this path disagreed.
                warnUnprotectedStoreDirOnce("it could not be restricted to owner-only access");
                throw new OidcAuthException(e).put("could not prepare the OIDC token store directory");
            }
            if (!isDirectoryTrusted) {
                // The directory was WRITABLE by other local users until the tightening a moment ago, so
                // anything already in it may have been planted rather than written by us. Tightening protects
                // what we write from here on and says nothing about what was there before. Discard rather
                // than merely skip: leaving a file behind hands it to the next load, which now sees an
                // owner-only directory and would trust it. ALL of them, not just this key's - the verdict is
                // spent by whoever observes it first, so the entries this call leaves are entries no later
                // call can distrust.
                discardUntrustedDirectoryContents();
                return null;
            }
            Path file = tokenFile(key);
            byte[] bytes;
            try {
                bytes = readBounded(file);
            } catch (NoSuchFileException e) {
                return null;
            } catch (IOException e) {
                throw new OidcAuthException(e).put("could not read the OIDC token store file");
            }
            if (bytes == null) {
                return null;
            }
            return parseAndVerify(key, bytes);
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void save(TokenStoreKey key, PersistedToken token) {
        // interrupt-neutral for the same reason as load(): a carried interrupt flag would otherwise abort
        // the write or the atomic rename half-way and leave the rotated refresh token unpersisted
        final boolean wasInterrupted = Thread.interrupted();
        try {
            byte[] content = serialize(key, token);
            try {
                if (!ensureDirectory()) {
                    // Same verdict load() acts on, and save() is just as often the first call to touch the
                    // store - a process that signs in and persists before it ever loads. Discarding the
                    // boolean here left every entry already in the directory looking, to every later load,
                    // like it had always been protected. The fresh token below is written afterwards, into
                    // the directory ensureDirectory has by now tightened, so persistence still works.
                    discardUntrustedDirectoryContents();
                }
                sweepStaleTempFiles(key.hash());
                Path target = tokenFile(key);
                Path tmp = createTempFile(key.hash());
                boolean moved = false;
                try {
                    writeAndFlush(tmp, content);
                    replaceTarget(tmp, target);
                    moved = true;
                } finally {
                    if (!moved) {
                        try {
                            Files.deleteIfExists(tmp);
                        } catch (IOException ignore) {
                            // best-effort: never let the cleanup failure replace the write/rename failure
                            // that is unwinding; sweepStaleTempFiles reclaims the orphan on a later save
                        }
                    }
                }
            } catch (IOException e) {
                throw new OidcAuthException(e).put("could not persist the OIDC token to the token store");
            }
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    long getLockStaleMillis() {
        // exposed package-private so OidcDeviceAuth.build() can verify this window dominates the worst-case time
        // a coordinated refresh holds the lock, before a peer could otherwise judge a live lock stale and steal it
        return lockStaleMillis;
    }

    private static void createLockFile(Path lock, String nonce) throws IOException {
        // Exclusively create the lock (O_CREAT|O_EXCL via CREATE_NEW), then write the owner nonce into that same
        // open channel before closing it. The file exists empty only for the tiny window between the create and
        // the stamp; a GC/safepoint pause (or a cross-machine clock skew) CAN land in that window, so what keeps
        // our freshly-created lock from being stolen as empty-and-stale is EMPTY_LOCK_STEAL_GRACE_MILLIS sitting
        // well above it, not the absence of the window. FileAlreadyExists means a peer already holds it.
        // releaseLock and stealIfStale verify this nonce before deleting. Keep the owner-only perms (and the
        // non-POSIX fallback) to match the store's other files.
        final byte[] bytes = nonce.getBytes(StandardCharsets.UTF_8);
        try {
            writeNewFile(lock, bytes, FILE_ATTRS);
        } catch (UnsupportedOperationException e) {
            warnNoPosixPermsOnce();
            writeNewFile(lock, bytes);
        }
    }

    private static void deleteCapturedLock(Path captured) {
        // best-effort cleanup of a lock we atomically captured during a steal; a leftover .tmp is reclaimed by
        // sweepStaleTempFiles on a later save
        try {
            Files.deleteIfExists(captured);
        } catch (IOException ignore) {
            // reclaimed by sweepStaleTempFiles later
        }
    }

    /**
     * Whether {@code name} starts with the 64-character lowercase-hex identity fingerprint every file this
     * store writes is named after.
     * <p>
     * This is the test for "we could have written this", used where a sweep has no single
     * {@link TokenStoreKey} to scope itself by and must therefore recognise the store's files by shape
     * rather than by an exact name. It is deliberately a prefix test: the entry is
     * {@code <hash>.json} but a write temp is {@code <hash><random>.tmp}, so only the leading fingerprint
     * is common to both.
     * <p>
     * Case matters. {@code TokenStoreKey} renders the digest through {@link #HEX}, which is lowercase, so
     * an uppercase-hex name is not one of ours and is left alone.
     */
    private static boolean hasStoreHashPrefix(String name) {
        if (name.length() < HASH_NAME_LENGTH) {
            return false;
        }
        for (int i = 0; i < HASH_NAME_LENGTH; i++) {
            final char c = name.charAt(i);
            if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
                return false;
            }
        }
        return true;
    }

    private static String newLockNonce() {
        // A per-acquisition owner stamp: the acquire time is a human-readable debugging aid, and the random
        // UUID guarantees two acquisitions never share a stamp even within one pid and one millisecond, so
        // releaseLock's ownership check is exact rather than probabilistic.
        //
        // NO pid@host, deliberately. The obvious way to get one on Java 8 is
        // ManagementFactory.getRuntimeMXBean().getName(), and that RESOLVES THE LOCAL HOSTNAME:
        // VMManagementImpl.getVmId() calls InetAddress.getLocalHost(), a full resolver round trip. Measured
        // at 3162ms on a macOS dev box whose mDNS cache was cold - inside an acquire whose entire budget was
        // 200ms, on the producer thread, holding this store's in-process lock and the caller's
        // OidcDeviceAuth lock. That is the documented bound (inLock degrades to a lock-free refresh after
        // lockAcquireBudgetMillis) broken by a debugging aid, and it is a once-per-JVM surprise: the value is
        // cached inside the MXBean afterwards, so the very first credential refresh in a process paid for it
        // and nothing later did. Java has no cheap hostname - unlike Python's socket.gethostname(), which is
        // gethostname(2) and does not resolve - so the field goes rather than the bound. Nothing reads it:
        // releaseLock and stealIfStale compare the stamp byte-wise against their own, and the cross-language
        // contract has each implementation check only its own stamp (design/oidc-token-persistence.md).
        //
        // Not even Compat.currentPid(), which SlotLock uses for exactly this kind of diagnostic: its Java 9+
        // variant is a free ProcessHandle.current().pid(), but its Java 8 variant IS the getName() call above,
        // parsed for the part before the '@'. The bound has to hold on every runtime this artifact supports,
        // not only on modern ones, so the stamp carries no process identity at all.
        return System.currentTimeMillis() + " " + UUID.randomUUID();
    }


    private static boolean nullableEquals(String keyValue, StringSink fileValue) {
        boolean fileHasValue = fileValue.length() > 0;
        if (keyValue == null) {
            return !fileHasValue;
        }
        return fileHasValue && Chars.equals(keyValue, fileValue);
    }

    private static PersistedToken parseAndVerify(TokenStoreKey key, byte[] bytes) {
        if (bytes.length == 0) {
            return null;
        }
        TokenFileParser parser = new TokenFileParser();
        try (DirectUtf8Sink mem = new DirectUtf8Sink(bytes.length);
             JsonLexer lexer = new JsonLexer(JSON_LEXER_CACHE_SIZE, JSON_LEXER_MAX_VALUE_BYTES)) {
            // bulk-copy the file bytes into native memory in one go rather than byte by byte
            mem.put(bytes, 0, bytes.length);
            long lo = mem.ptr();
            lexer.parse(lo, lo + mem.size(), parser);
            lexer.parseLast(); // reject a truncated document
        } catch (JsonException e) {
            // corrupt or truncated file: treat as no usable entry, fall back to refresh / interactive
            return null;
        }
        // schema and fingerprint must match the live identity; a mismatch is a hash collision or a file
        // copied from a different identity, so ignore it rather than serve the wrong identity's token. A
        // malformed shape (an array anywhere - the schema is a single flat object) is likewise rejected.
        if (parser.malformed || parser.version != SCHEMA_VERSION) {
            return null;
        }
        if (!Chars.equals(key.getClientId(), parser.clientId)
                || !Chars.equals(key.getTokenEndpoint(), parser.tokenEndpoint)
                || !Chars.equals(key.getDeviceAuthorizationEndpoint(), parser.deviceAuthorizationEndpoint)
                || !Chars.equals(key.getScope(), parser.scope)
                || !nullableEquals(key.getAudience(), parser.audience)
                || key.isGroupsInToken() != parser.groupsInToken) {
            return null;
        }
        String accessToken = parser.accessToken.length() > 0 ? parser.accessToken.toString() : null;
        String idToken = parser.idToken.length() > 0 ? parser.idToken.toString() : null;
        String refreshToken = parser.refreshToken.length() > 0 ? parser.refreshToken.toString() : null;
        return new PersistedToken(accessToken, idToken, refreshToken, parser.expiresAtMillis, parser.tokenTtlMillis);
    }

    private static long parseLongOrZero(CharSequence value) {
        // The frozen on-disk contract stores these as JSON numbers: an optional '-' followed by bare digits.
        // Numbers.parseLong is more permissive than that - it accepts '_' thousands separators and an 'L'/'l'
        // suffix - so "5L" and "1_000" would parse here and fail in every other language client reading the
        // same file, which is exactly the kind of silent divergence a frozen cross-language format exists to
        // prevent. Screen the value first so this client accepts only what the format actually allows; an
        // out-of-contract value falls back to 0 like any other unusable field.
        final int n = value.length();
        int i = n > 0 && value.charAt(0) == '-' ? 1 : 0;
        if (i == n) {
            return 0; // empty, or a bare "-"
        }
        for (; i < n; i++) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return 0;
            }
        }
        try {
            return Numbers.parseLong(value);
        } catch (NumericException e) {
            return 0;
        }
    }

    private static void putBooleanMember(StringSink sink, String name, boolean value) {
        sink.put(',');
        putName(sink, name);
        sink.put(Boolean.toString(value));
    }

    private static void putLongMember(StringSink sink, String name, long value) {
        sink.put(',');
        putName(sink, name);
        // write the digits unconditionally. sink.put(long) routes through Numbers.append(..., checkNaN=true),
        // which renders Long.MIN_VALUE as the literal JSON null - a bare null for a present, non-nullable
        // integer field would break the frozen cross-language contract (serialize() OMITS absent fields rather
        // than writing null, so a null here is indistinguishable from absent) and round-trips back to 0 via
        // parseLongOrZero. checkNaN=false emits the full number, so every long value round-trips verbatim.
        Numbers.append(sink, value, false);
    }

    private static void putName(StringSink sink, String name) {
        sink.put('"').put(name).put('"').put(':');
    }

    private static void putNullableStringMember(StringSink sink, String name, String value) {
        // omit the member entirely when the value is null, rather than write a JSON null - see serialize()
        if (value != null) {
            putStringMember(sink, name, value);
        }
    }

    private static void putString(StringSink sink, CharSequence value) {
        // a refresh token is an opaque IdP string, so escape the JSON string properly; the JsonLexer
        // decodes these on read. Non-ASCII passes through and is encoded as UTF-8 by getBytes below.
        sink.put('"');
        for (int i = 0, n = value.length(); i < n; i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    sink.put("\\\"");
                    break;
                case '\\':
                    sink.put("\\\\");
                    break;
                case '\b':
                    sink.put("\\b");
                    break;
                case '\f':
                    sink.put("\\f");
                    break;
                case '\n':
                    sink.put("\\n");
                    break;
                case '\r':
                    sink.put("\\r");
                    break;
                case '\t':
                    sink.put("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sink.put("\\u00").put(HEX[(c >> 4) & 0x0f]).put(HEX[c & 0x0f]);
                    } else {
                        sink.put(c);
                    }
            }
        }
        sink.put('"');
    }

    private static void putStringMember(StringSink sink, String name, CharSequence value) {
        sink.put(',');
        putName(sink, name);
        putString(sink, value);
    }

    // Drops this caller's claim on the identity's lock, retiring the entry when it was the last one, so the
    // map never outgrows the identities actually in flight. Pairs with retainProcessLock in a finally.
    private static void releaseProcessLock(String identity) {
        PROCESS_LOCKS.computeIfPresent(identity, (k, held) -> --held.users == 0 ? null : held);
    }

    // Claims the lock for this identity, creating the entry if this caller is the first to arrive. Registers
    // the claim BEFORE the acquire, so an entry cannot be retired out from under a thread that is queued on
    // it - which is what makes the retirement in releaseProcessLock safe.
    private static ProcessLock retainProcessLock(String identity) {
        return PROCESS_LOCKS.compute(identity, (k, existing) -> {
            final ProcessLock held = existing != null ? existing : new ProcessLock();
            held.users++;
            return held;
        });
    }

    private static byte[] readBounded(Path file) throws IOException {
        // read with a hard cap instead of Files.readAllBytes after a separate Files.size: the file is
        // attacker-writable, so a size-check-then-read races a concurrent grow - a file enlarged past the cap
        // between the two would make readAllBytes allocate gigabytes and throw OutOfMemoryError (an Error, which
        // the best-effort RuntimeException guard in OidcDeviceAuth.maybeLoadFromStore would not catch, so a bad
        // file would abort sign-in instead of degrading). Cap the buffer at the reported size (already bounded
        // by MAX_FILE_BYTES) plus one byte, so a file that grew past its reported size is rejected, not allocated.
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size <= 0 || size > MAX_FILE_BYTES) {
                // an empty or implausibly large file is not a usable entry; ignore it rather than read it in
                return null;
            }
            ByteBuffer buffer = ByteBuffer.allocate((int) size + 1);
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // read until EOF or the (size + 1)-byte buffer fills
            }
            int read = buffer.position();
            if (read == 0 || read > size) {
                // empty, or grew past its reported size between the stat and the read: treat as corrupt/hostile
                return null;
            }
            byte[] bytes = new byte[read];
            buffer.flip();
            buffer.get(bytes);
            return bytes;
        }
    }

    private static byte[] readLockHolder(Path lock) throws IOException {
        // read the lock's owner stamp with a hard cap rather than Files.readAllBytes: the <hash>.lock file
        // sits in the same attacker-writable directory as the token file, so an inflated lock would otherwise
        // make readAllBytes allocate without bound and throw OutOfMemoryError - an Error the best-effort
        // RuntimeException guards on the getToken()/signIn() refresh path would not catch, aborting the
        // sign-in (the same reason readBounded caps the token file). A real owner stamp is a few hundred
        // bytes; anything past the cap is corrupt or hostile, so report it as unreadable (null) rather than
        // read it into memory. Returns the exact bytes present, or null for an empty/oversized lock.
        try (FileChannel channel = FileChannel.open(lock, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size <= 0 || size > MAX_LOCK_FILE_BYTES) {
                return null;
            }
            ByteBuffer buffer = ByteBuffer.allocate((int) size);
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // read until EOF or the buffer fills
            }
            int read = buffer.position();
            if (read == 0) {
                return null;
            }
            byte[] bytes = new byte[read];
            buffer.flip();
            buffer.get(bytes);
            return bytes;
        }
    }

    private static void releaseLock(Path lock, String nonce) {
        // release our own lock only: re-read it (bounded - see readLockHolder) and delete it solely when it
        // still carries our nonce. A hold that outran lockStaleMillis may have been judged stale and stolen
        // (captured and recreated) by a peer; deleting by bare path would then remove the peer's live lock and
        // admit a third acquirer alongside it, defeating the mutual exclusion this lock exists to provide. A
        // microscopic window remains if a steal lands between the read and the delete, but that is bounded to
        // one syscall gap rather than the whole hold, so a misconfigured staleness window degrades to at most
        // the documented double-refresh rather than corrupting a peer's lock state.
        try {
            byte[] content = readLockHolder(lock);
            if (content != null && nonce.equals(new String(content, StandardCharsets.UTF_8))) {
                Files.deleteIfExists(lock);
            }
            // otherwise a peer now owns this lock file, or it is unreadable/oversized; leave it for that owner
            // (or the staleness steal) to reclaim
        } catch (NoSuchFileException e) {
            // already gone (stolen and not yet recreated, or removed elsewhere); nothing to release
        } catch (IOException e) {
            // Best-effort release, but no longer silent. A lock we could not delete blocks every peer's
            // coordinated refresh until it goes stale (lockStaleMillis - 10 minutes by default), and each
            // peer degrades to an unserialized refresh meanwhile: the rotating-refresh-token race this lock
            // exists to prevent. That is worth a line an operator can find, rather than surfacing later as
            // unexplained repeated sign-ins.
            // sanitized: see the sibling warning in inLock - the message embeds the store path
            LOG.warn("could not release the OIDC token store lock; peers degrade to lock-free refresh until "
                    + "it goes stale [error={}]", OidcDeviceAuth.sanitizeForDisplay(e.getMessage()));
        }
    }

    private static void replaceTarget(Path tmp, Path target) throws IOException {
        // atomically rename tmp over target. On Windows a concurrent reader in any process holding target open
        // can make the rename fail transiently with AccessDeniedException (a sharing violation); retry a few
        // times on a short backoff before giving up, so a routine read/write overlap does not needlessly degrade
        // persistence (best-effort - the in-memory token is still valid). POSIX rename over an open file never
        // hits this. AtomicMoveNotSupported (a rare filesystem) falls back to a plain replace, which still beats
        // leaving a partial write.
        AccessDeniedException lastDenied = null;
        for (int attempt = 0; attempt < REPLACE_MAX_ATTEMPTS; attempt++) {
            if (attempt > 0) {
                Os.sleep(REPLACE_RETRY_SLEEP_MILLIS);
            }
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                return;
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                return;
            } catch (AccessDeniedException e) {
                lastDenied = e;
            }
        }
        throw lastDenied;
    }

    /**
     * Re-asserts owner-only permissions on the store directory. The at-rest protection of the plaintext token
     * files is exactly these permissions, so a pre-existing directory another tool or a permissive umask left
     * loose is tightened rather than trusted as it stands. ensureDirectory runs this on every save and every
     * inLock, so it chmods only on detected drift - the common case costs one stat and no write syscall.
     * <p>
     * NOT best-effort on the failure that matters. An {@code IOException} here means the directory is not ours
     * to chmod, which is precisely the state in which the documented {@code 0700} protection does not hold and
     * another local user can create, replace or delete entries in it. Swallowing it left every caller believing
     * the protection applied. Callers degrade on the throw, each in the way that suits it: {@code save} refuses
     * to write a plaintext refresh token into a directory it cannot protect, {@code inLock} runs lock-free.
     * On a non-POSIX filesystem (Windows) the check is unavailable rather than failed, so it falls back to the
     * inherited ACL as before (owner-only hardening there, via AclFileAttributeView, is a separate follow-up).
     *
     * @param directory the store directory, which must already exist
     * @return {@code true} when the directory's content may be trusted, {@code false} when it was writable
     *         by group or other, so another local user could have planted an entry before this call
     *         tightened it
     * @throws IOException if the directory exists but its permissions cannot be read or set
     */
    private static boolean restrictToOwner(Path directory) throws IOException {
        try {
            final Set<PosixFilePermission> perms = Files.getPosixFilePermissions(directory);
            // Writable by group or other is the state that decides TRUST, and it is narrower than "not
            // owner-only": only write permission on a directory lets another local user create or replace an
            // entry in it, which is what load() would then adopt. The 0755 a default umask produces exposes
            // no token - the files themselves are 0600 - and everything in it was still put there by us, so
            // it is tightened for defence in depth but its content stays trusted.
            final boolean wasOtherWritable = perms.contains(PosixFilePermission.GROUP_WRITE)
                    || perms.contains(PosixFilePermission.OTHERS_WRITE);
            if (!DIR_PERMS.equals(perms)) {
                // Tightening is load-bearing - it IS the at-rest protection of the plaintext token files, so
                // it stays unconditional - but it changes a directory the operator chose and may share with
                // something else, so it must not be silent. Once per JVM, and never naming the path.
                if (warnedTightenedStoreDir.compareAndSet(false, true)) {
                    LOG.warn("the OIDC token store directory was not owner-only and has been tightened to "
                            + "0700; it holds plaintext refresh tokens, so it must not be shared with "
                            + "anything else. Point questdb.client.oidc.token.store.dir at a directory of "
                            + "its own if another tool needs access to that path.");
                }
                Files.setPosixFilePermissions(directory, DIR_PERMS);
            }
            return !wasOtherWritable;
        } catch (UnsupportedOperationException e) {
            // non-POSIX FS (e.g. Windows): cannot enforce owner-only perms; keep the inherited ACL
            warnNoPosixPermsOnce();
            return true;
        }
    }

    private static byte[] serialize(TokenStoreKey key, PersistedToken token) {
        // a null value (an absent audience, or a token kind the grant did not return) is omitted rather than
        // written as JSON null: the JsonLexer reports a bare null and a quoted "null" identically, so omitting
        // absent fields is the only encoding under which a present value - a token equal to "null" included -
        // round-trips back verbatim. "v" is the first member; every later member prepends its own comma.
        StringSink sink = new StringSink();
        sink.put('{');
        putName(sink, "v");
        sink.put(SCHEMA_VERSION);
        putStringMember(sink, "client_id", key.getClientId());
        putStringMember(sink, "token_endpoint", key.getTokenEndpoint());
        putStringMember(sink, "device_authorization_endpoint", key.getDeviceAuthorizationEndpoint());
        putStringMember(sink, "scope", key.getScope());
        putNullableStringMember(sink, "audience", key.getAudience());
        putBooleanMember(sink, "groups_in_token", key.isGroupsInToken());
        putNullableStringMember(sink, "access_token", token.getAccessToken());
        putNullableStringMember(sink, "id_token", token.getIdToken());
        putNullableStringMember(sink, "refresh_token", token.getRefreshToken());
        putLongMember(sink, "expires_at_millis", token.getExpiresAtMillis());
        putLongMember(sink, "token_ttl_millis", token.getTokenTtlMillis());
        sink.put('}');
        return sink.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void warnNoPosixPermsOnce() {
        // best-effort, once per JVM: the token store could not enforce 0600/0700, so the persisted refresh
        // token is protected only by the directory's inherited ACL. ASCII-only, and never includes a path or
        // token byte (a path could itself carry terminal-spoofing characters)
        if (!warnedNoPosixPerms.compareAndSet(false, true)) {
            return;
        }
        LOG.warn("the OIDC token store could not enforce owner-only (0600/0700) permissions on this "
                + "filesystem; the persisted refresh token is protected only by the directory's default ACL. "
                + "Back the store with an OS keychain for at-rest encryption.");
    }

    private static void warnUnprotectedStoreDirOnce(String reason) {
        // once per JVM, like warnNoPosixPermsOnce: the condition is a property of the directory, which every
        // identity in this process shares, and load() sits on the flush path via OidcDeviceAuth.getToken().
        // ASCII-only and never the path itself - an operator-supplied path can carry terminal-spoofing
        // characters, which is why warnNoPosixPermsOnce omits it too.
        if (!warnedUnprotectedStoreDir.compareAndSet(false, true)) {
            return;
        }
        LOG.warn("the OIDC token store directory is not owner-only, so a persisted token there cannot be "
                + "trusted: {}. Point questdb.client.oidc.token.store.dir at a directory only this user "
                + "can write, or supply a TokenStore backed by an OS keychain.", reason);
    }

    private static void writeAndFlush(Path file, byte[] content) throws IOException {
        // write the payload and force it to disk before the rename, so a crash between the write and the
        // atomic rename cannot leave the target pointing at unflushed (zero/partial) bytes - the temp file
        // is the durability point of the write-temp / flush / atomic-rename protocol
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    private static void writeNewFile(Path file, byte[] content, FileAttribute<?>... attrs) throws IOException {
        // exclusive-create (CREATE_NEW = O_CREAT|O_EXCL) with the given perms and write the content in one
        // open; FileAlreadyExistsException is raised when the file already exists
        try (FileChannel channel = FileChannel.open(file, EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), attrs)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
    }

    private String acquireLock(Path lock) throws InterruptedException {
        // returns the unique owner nonce stamped into the lock on success, or null if it could not be acquired
        // within the budget. releaseLock uses the nonce to verify ownership before deleting, so a hold that
        // outran lockStaleMillis (and was stolen by a peer) never deletes the peer's lock on release
        final String nonce = newLockNonce();
        // nanoTime, not currentTimeMillis: this is an elapsed budget, and the wall clock is adjustable. An
        // NTP step or an operator setting the date back stretches a millis-based deadline by the size of the
        // jump, so a caller that documents a bounded degrade - inLock() promises to fall back to a lock-free
        // refresh after lockAcquireBudgetMillis - would sit here for however long the clock moved instead.
        // nanoTime is monotonic and immune to that. Compare by DIFFERENCE rather than by ordering, so the
        // arithmetic stays correct across nanoTime's wraparound.
        final long deadlineNanos = System.nanoTime() + lockAcquireBudgetMillis * 1_000_000L;
        while (true) {
            try {
                // exclusive-create then stamp on the same open channel: the empty-file window between the two is
                // tiny and covered by EMPTY_LOCK_STEAL_GRACE_MILLIS, so a GC/safepoint pause mid-acquisition
                // cannot get our freshly-created lock stolen as empty-and-stale
                createLockFile(lock, nonce);
                return nonce;
            } catch (FileAlreadyExistsException e) {
                // the lock exists; if a crashed holder abandoned it, steal it - atomically and stamp-verified,
                // so a stealer never removes a peer's freshly-created live lock (see stealIfStale). Then fall
                // through to the bounded wait below rather than retry immediately: a steal contest between
                // several acquirers (or a misconfigured tiny lockStaleMillis) must not hot-spin.
                stealIfStale(lock);
                if (System.nanoTime() - deadlineNanos >= 0) {
                    return null; // give up and run without the lock rather than stall a sign-in
                }
                // Thread.sleep, not Os.sleep: Os.sleep catches InterruptedException and keeps sleeping to its
                // deadline WITHOUT re-asserting the flag, so a cancellation aimed at this poll was swallowed
                // outright and the whole budget elapsed regardless. Propagate it and let inLock abandon the
                // refresh - the budget can be tens of seconds, far past a QWP close()'s shutdown window.
                Thread.sleep(LOCK_POLL_SLICE_MILLIS);
            } catch (IOException e) {
                // Do NOT delete the lock here. That used to be justified by "the exclusive create succeeded
                // and only the nonce write failed, so the file is ours" - true for one of the failures this
                // arm catches, but not the others. From the second loop iteration onward a PEER's live lock
                // occupies the path, and plenty of "cannot create" failures are not
                // FileAlreadyExistsException: fd exhaustion (EMFILE/ENFILE), EACCES, EROFS, ENOSPC and a
                // Windows sharing violation all arrive as a plain IOException. deleteIfExists cannot tell
                // the two cases apart, and removing a peer's live lock admits a second holder - the
                // double-POST of one rotating refresh token this lock exists to prevent, which a
                // reuse-detecting identity provider answers by revoking the whole token family.
                //
                // A lock we genuinely did leave half-created is EMPTY, and stealIfStale already reclaims an
                // empty lock on the short EMPTY_LOCK_STEAL_GRACE_MILLIS grace, so leaving it behind costs at
                // most that grace. Degrade to a lock-free refresh instead.
                // sanitized: see the sibling warning in inLock - the message embeds the store path
                LOG.warn("could not acquire the OIDC token store lock; running this refresh without "
                        + "cross-process coordination [error={}]",
                        OidcDeviceAuth.sanitizeForDisplay(e.getMessage()));
                return null;
            }
        }
    }

    private Path createTempFile(String prefix) throws IOException {
        try {
            return Files.createTempFile(directory, prefix, ".tmp", FILE_ATTRS);
        } catch (UnsupportedOperationException e) {
            // non-POSIX filesystem (e.g. Windows): rely on the owner-only directory ACL instead
            warnNoPosixPermsOnce();
            return Files.createTempFile(directory, prefix, ".tmp");
        }
    }

    /**
     * Discards EVERY entry in the store directory, and warns once, after {@link #restrictToOwner(Path)} has
     * reported it was writable by other local users.
     * <p>
     * Discarding only the caller's own {@code <hash>.json} is not enough, because the verdict is destroyed by
     * the act of reporting it: restrictToOwner chmods the directory to 0700 as it returns, so whichever
     * caller touches the store first consumes the one observation. Every later load - in this process or the
     * next - then sees an owner-only directory and adopts whatever entry is sitting there, including one
     * planted while the directory stood open. One store directory holds one file per configuration and is
     * documented as belonging to the store alone, so once it has been writable by other local users nothing
     * in it can be told apart from a plant: all of it goes, and each identity re-signs in.
     * <p>
     * Best-effort by design. This runs on the flush path through {@code OidcDeviceAuth.getToken()}, so a
     * delete that fails must degrade to a refresh or an interactive sign-in rather than throw - and the
     * caller that triggered it fails closed either way, whether or not the file goes.
     */
    private void discardUntrustedDirectoryContents() {
        warnUnprotectedStoreDirOnce("it was writable by other local users; every entry found in it was "
                + "discarded rather than trusted, and a fresh sign-in is required");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                final String name = entry.getFileName().toString();
                // Only files THIS STORE could have written. Every one of them is named after a 64-hex
                // identity fingerprint - tokenFile() builds "<hash>.json" and writeTemp() asks
                // createTempFile() for "<hash><random>.tmp" - so a name without that prefix belongs to
                // whatever else shares the directory. Without this test the filter below reads as "any
                // .json file", and the operator who pointed questdb.client.oidc.token.store.dir at a
                // directory holding their own config loses it on the first load(). The directory being
                // group-writable is what brought us here; it is not a licence to delete files we did not
                // write. sweepTempFiles() already scopes itself this way, by hash prefix.
                if (!hasStoreHashPrefix(name)) {
                    continue;
                }
                // The entry is exactly "<hash>.json"; anything longer that merely starts with a hash is
                // not ours either. A write temp carries a random infix, so only its suffix is fixed.
                final boolean isEntry = name.length() == HASH_NAME_LENGTH + 5 && name.endsWith(".json");
                // A steal-captured lock (<hash>.lock.<uuid>.tmp) is a cross-process handover in flight, not
                // an orphaned write temp - sweepTempFiles skips it for the same reason. The .lock files
                // themselves stay too: they carry no token, and acquireLock already treats a hostile or
                // stale one as stealable.
                final boolean isWriteTemp = name.endsWith(".tmp") && !name.contains(".lock.");
                if (!isEntry && !isWriteTemp) {
                    continue;
                }
                try {
                    Files.deleteIfExists(entry);
                } catch (IOException ignore) {
                    // best-effort; skip this entry, the caller still fails closed
                }
            }
        } catch (IOException ignore) {
            // best-effort; an unreadable directory must not turn a fail-closed load into a thrown one
        }
    }

    /**
     * Creates the store directory owner-only when absent, and asserts it is owner-only either way.
     *
     * @return {@code true} when the directory's content may be trusted - see {@link #restrictToOwner(Path)}
     * @throws IOException if the directory cannot be created, or exists and cannot be made owner-only
     */
    private boolean ensureDirectory() throws IOException {
        if (!Files.isDirectory(directory)) {
            try {
                Files.createDirectories(directory, DIR_ATTRS);
            } catch (UnsupportedOperationException e) {
                warnNoPosixPermsOnce();
                Files.createDirectories(directory);
            }
        }
        // Verify UNCONDITIONALLY, including immediately after our own create, rather than only on the
        // pre-existing branch. createDirectories is a no-op when the directory already exists, and it applies
        // DIR_ATTRS only to directories it actually creates - so a peer that won the race between the
        // isDirectory check above and the call keeps whatever permissions IT chose, and "we called
        // createDirectories" is not evidence the directory is ours. That window is exactly the hostile
        // local pre-create this method exists to defeat. Re-asserting here also covers ordinary drift on a
        // pre-existing directory (another tool, a permissive umask), which is what the old branch handled.
        return restrictToOwner(directory);
    }

    private boolean isOlderThan(Path lock, long thresholdMillis) {
        try {
            FileTime modified = Files.getLastModifiedTime(lock);
            return System.currentTimeMillis() - modified.toMillis() > thresholdMillis;
        } catch (IOException e) {
            return false; // cannot determine the age; do not steal
        }
    }

    private Path lockFile(TokenStoreKey key) {
        return directory.resolve(key.hash() + ".lock");
    }

    /**
     * The key this store's entry for {@code key} takes in {@link #PROCESS_LOCKS}: the normalized store
     * directory and the identity fingerprint, NUL-separated.
     * <p>
     * Both halves are load-bearing. The fingerprint alone over-serializes, because it names a
     * <em>configuration</em> and says nothing about where the entry lives - so two stores following the
     * documented per-user-directory recipe would queue on one lock while touching different files, and that
     * lock is held across a whole token-endpoint round trip with no acquire budget. The directory alone
     * under-serializes, letting two identities in one directory run their read-refresh-write concurrently.
     * <p>
     * NUL is the separator for the reason {@code TokenStoreKey} uses it: a path can contain almost anything
     * else, and two different (directory, identity) pairs must never render to one string.
     */
    private String processLockIdentity(TokenStoreKey key) {
        return lockNamespace + '\0' + key.hash();
    }

    private void stealIfStale(Path lock) {
        // Steal a lock abandoned by a crashed holder, but never remove a peer's freshly-created LIVE lock. A
        // bare deleteIfExists(lock) removes whatever sits at the path at that instant - including a fresh lock
        // a peer created in the gap since we judged the old one stale - and would admit two holders at once.
        // Instead: read the current owner stamp, confirm the lock is stale, then capture it atomically into a
        // private name (rename is atomic, so among racing stealers exactly one captures it; the losers get
        // NoSuchFileException and fall back to the wait), then verify what we captured carries the same stamp
        // we judged stale. If a peer had already replaced it with a live lock we grabbed that instead, so we
        // put it back rather than steal it. This mirrors releaseLock's own-stamp check and shrinks the
        // residual race from the whole age-check->delete gap to the gap between the two renames.
        final byte[] before;
        try {
            before = readLockHolder(lock);
        } catch (IOException e) {
            return; // gone or unreadable; nothing to steal here - the create attempt or a peer settles it
        }
        // read the stamp first, then check age: if a peer replaces the lock with a fresh one in between, the
        // age check reads the fresh mtime and returns false, so we never proceed against a live lock.
        if (before != null) {
            // a stamped lock is a (claimed) live holder: steal only once it outlives the full staleness window
            if (!isOlderThan(lock, lockStaleMillis)) {
                return;
            }
        } else if (!isOlderThan(lock, EMPTY_LOCK_STEAL_GRACE_MILLIS)) {
            // an empty/unreadable lock is almost never a validly-held lock: acquireLock creates the lock and
            // stamps the owner nonce onto the same open channel (createLockFile via CREATE_NEW), so a live lock
            // carries its stamp within the tiny create->stamp window. An empty lock therefore means either a
            // crash mid-write (the exclusive create succeeded but the nonce write did not) or a peer momentarily
            // caught in that narrow window - a GC/safepoint pause CAN land there, which is exactly why the grace
            // exists. Steal it on the short empty-lock grace rather than the full staleness window, so a crash
            // orphan stops wedging peers for the whole window; the capture-verify below still confirms the lock
            // is unchanged before completing the steal. (A cross-machine clock skew wider than the grace could
            // still pre-empt such a partial lock, but that never forges or tears a credential - Layer-1's
            // atomic rename holds - it degrades to at most a concurrent refresh, the best-effort residual
            // inLock already accepts.)
            //
            // The grace is used verbatim, never clamped down to a smaller lockStaleMillis. It is the one thing
            // standing between a peer caught mid-stamp and having its live lock stolen, so the frozen
            // cross-language contract (design/oidc-token-persistence.md) states that a client MUST NOT shorten
            // it - and a store built with a staleness window under 5s would otherwise do exactly that,
            // silently. A short window is a legitimate way to say "steal an abandoned STAMPED lock quickly";
            // it is not a statement about the create-to-stamp gap, which is the same few microseconds however
            // the store is configured.
            return;
        }
        final Path captured = lock.resolveSibling(lock.getFileName().toString() + '.' + UUID.randomUUID() + ".tmp");
        try {
            Files.move(lock, captured, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // NoSuchFile: a peer already stole/removed it; AtomicMoveNotSupported or other IO: degrade and
            // leave the lock for the staleness path. Either way we have not removed a peer's live lock.
            return;
        }
        byte[] after = null;
        boolean afterReadOk = false;
        try {
            after = readLockHolder(captured);
            afterReadOk = true;
        } catch (IOException ignore) {
            // captured but cannot re-read; treated as a non-match below, so we restore rather than steal
        }
        // Confirm we captured the same stamp we judged stale (or the same empty/oversized junk), not a live
        // lock a peer recreated in the gap. afterReadOk is load-bearing and separate from "after == null":
        // readLockHolder returns null for a legitimately empty or oversized lock but THROWS on an IO error,
        // and folding those together let a failed re-read of an empty lock read as confirmedStale - so the
        // steal completed on the strength of an IO error rather than on evidence the lock was unchanged,
        // the exact opposite of what the catch above says it does.
        final boolean confirmedStale = afterReadOk
                && (before == null ? after == null : Arrays.equals(before, after));
        if (confirmedStale) {
            // genuinely the abandoned lock: drop it, so the next createLockFile can claim a fresh one
            deleteCapturedLock(captured);
            return;
        }
        // We captured a live lock a peer recreated in the gap (or could not re-read what we captured): put it
        // back rather than steal it.
        restoreCapturedLock(lock, captured);
    }

    /**
     * Puts a captured lock back at {@code lock} after the capture-verify decided it is NOT the abandoned lock
     * we judged stale - a peer recreated it in the gap, or we could not re-read what we captured.
     * <p>
     * Restores by hard-LINKING the capture back to the lock path, not by renaming it. {@code Files.move}
     * without {@code REPLACE_EXISTING} looks atomic but is not: it stats the target, then renames, and
     * {@code rename(2)} silently replaces. A third party that claims the freed path between those two steps
     * therefore had its live lock destroyed by the very call whose comment promised to leave it intact.
     * {@code link(2)} has no such gap - it fails outright when the target exists - and it preserves the peer's
     * exact bytes, which matters because {@link #releaseLock} verifies the stamp before deleting.
     * <p>
     * A residual remains and is not closeable with a lock file: if a third party did claim the path, we drop
     * our copy, so the recreating peer's lock file is gone while that peer still believes it holds the lock,
     * and for that one refresh two holders can run concurrently. A filesystem offers no atomic "delete or
     * rename only if the content is still X", so the capture-verify narrows the window to this multi-actor
     * race - our steal, a peer recreating, AND a third party claiming the freed path, all overlapping -
     * without eliminating it. Best-effort by design: it degrades to one extra refresh, a re-prompt on a
     * rotating-refresh-token identity provider, never a torn or forged credential (Layer 1's atomic rename
     * still holds).
     * <p>
     * Split out of {@link #stealIfStale} so it can be driven directly. Reaching it through {@code stealIfStale}
     * needs a peer to replace the lock file between the staleness read and the capture rename, which no test
     * can force without a production seam - so the whole restore path, the part that keeps a stealer from
     * destroying a peer's live lock, otherwise ran only in production.
     *
     * @param lock     the lock path to restore to
     * @param captured the private capture name the steal renamed the lock to
     */
    private void restoreCapturedLock(Path lock, Path captured) {
        try {
            Files.createLink(lock, captured);
            deleteCapturedLock(captured);
        } catch (FileAlreadyExistsException e) {
            // a third party owns the path now; leave their lock untouched and drop our copy
            deleteCapturedLock(captured);
        } catch (IOException | UnsupportedOperationException e) {
            // the filesystem does not support hard links, or the link failed for another reason. Fall back to
            // the plain move: it preserves the bytes but reopens the stat-then-rename window described above,
            // which is still better than abandoning the peer's lock outright.
            try {
                Files.move(captured, lock);
            } catch (IOException moveFailure) {
                deleteCapturedLock(captured);
            }
        }
    }

    private void sweepStaleTempFiles(String hashPrefix) {
        // a crash between createTempFile and the atomic rename orphans a <hash>*.tmp holding a
        // valid-at-the-time refresh token; unlike the lock file nothing ever steals it, so it would accumulate
        // across crashes. Best-effort sweep on save: delete only temps older than the lock-staleness window, so
        // a temp a concurrent writer is actively using (its mtime is seconds old) is never removed. A separate
        // random suffix per writer keeps concurrent saves from colliding, which is why temps are not a fixed name
        sweepTempFiles(hashPrefix, lockStaleMillis);
    }

    private void sweepTempFiles(String hashPrefix, long minAgeMillis) {
        // shared by save()'s staleness-bounded sweep and clear()'s unconditional one (minAgeMillis 0), which
        // must reclaim even a freshly orphaned temp because it holds a plaintext refresh token the caller has
        // just asked to forget
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, hashPrefix + "*.tmp")) {
            final long now = System.currentTimeMillis();
            for (Path tmp : stream) {
                // never sweep a steal-captured lock (<hash>.lock.<uuid>.tmp): it is a cross-process steal in
                // progress, not an orphaned write temp, and ATOMIC_MOVE preserves the stale lock's old mtime
                // onto it, so the age guard below would judge an in-flight capture sweepable and delete it -
                // destroying a lock the stealer may be about to restore to its live owner. A save write temp is
                // <hash><random>.tmp and never contains ".lock.", so this only excludes captures. A capture
                // orphaned by a crash mid-steal is rare and harmless (the canonical lock path is left free), so
                // it is deliberately not reclaimed here.
                if (tmp.getFileName().toString().contains(".lock.")) {
                    continue;
                }
                try {
                    if (now - Files.getLastModifiedTime(tmp).toMillis() >= minAgeMillis) {
                        Files.deleteIfExists(tmp);
                    }
                } catch (IOException ignore) {
                    // best-effort; skip this entry and let a later sweep retry
                }
            }
        } catch (IOException ignore) {
            // best-effort; a sweep failure must never fail a save
        }
    }

    private Path tokenFile(TokenStoreKey key) {
        return directory.resolve(key.hash() + ".json");
    }

    /**
     * One identity's in-process lock plus the number of callers currently holding or queued on it.
     * <p>
     * {@code users} is read and written only inside {@link ConcurrentHashMap#compute} /
     * {@link ConcurrentHashMap#computeIfPresent} remapping functions, which run under the bin lock, so it
     * needs no volatility or atomics of its own.
     */
    private static final class ProcessLock {
        final ReentrantLock lock = new ReentrantLock();
        int users;
    }

    private static final class TokenFileParser implements JsonParser {
        private static final int FIELD_ACCESS_TOKEN = 8;
        private static final int FIELD_AUDIENCE = 6;
        private static final int FIELD_CLIENT_ID = 2;
        private static final int FIELD_DEVICE_AUTHORIZATION_ENDPOINT = 4;
        private static final int FIELD_EXPIRES_AT_MILLIS = 11;
        private static final int FIELD_GROUPS_IN_TOKEN = 7;
        private static final int FIELD_ID_TOKEN = 9;
        private static final int FIELD_NONE = 0;
        private static final int FIELD_REFRESH_TOKEN = 10;
        private static final int FIELD_SCOPE = 5;
        private static final int FIELD_TOKEN_ENDPOINT = 3;
        private static final int FIELD_TOKEN_TTL_MILLIS = 12;
        private static final int FIELD_VERSION = 1;
        final StringSink accessToken = new StringSink();
        final StringSink audience = new StringSink();
        final StringSink clientId = new StringSink();
        final StringSink deviceAuthorizationEndpoint = new StringSink();
        final StringSink idToken = new StringSink();
        final StringSink refreshToken = new StringSink();
        final StringSink scope = new StringSink();
        final StringSink tokenEndpoint = new StringSink();
        long expiresAtMillis;
        boolean groupsInToken;
        long tokenTtlMillis;
        long version;
        private int depth;
        private int field = FIELD_NONE;
        private boolean malformed;

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_ARRAY_START:
                    // the on-disk schema is a single flat JSON object; an array anywhere (for example a
                    // top-level [ {..} ] wrapper) is a malformed or hostile shape - mark the document invalid
                    // rather than extract fields from it through the object-depth gate
                    malformed = true;
                    break;
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    depth--;
                    break;
                case JsonLexer.EVT_NAME:
                    if (depth == 1) {
                        if (Chars.equals("v", tag)) {
                            field = FIELD_VERSION;
                        } else if (Chars.equals("client_id", tag)) {
                            field = FIELD_CLIENT_ID;
                        } else if (Chars.equals("token_endpoint", tag)) {
                            field = FIELD_TOKEN_ENDPOINT;
                        } else if (Chars.equals("device_authorization_endpoint", tag)) {
                            field = FIELD_DEVICE_AUTHORIZATION_ENDPOINT;
                        } else if (Chars.equals("scope", tag)) {
                            field = FIELD_SCOPE;
                        } else if (Chars.equals("audience", tag)) {
                            field = FIELD_AUDIENCE;
                        } else if (Chars.equals("groups_in_token", tag)) {
                            field = FIELD_GROUPS_IN_TOKEN;
                        } else if (Chars.equals("access_token", tag)) {
                            field = FIELD_ACCESS_TOKEN;
                        } else if (Chars.equals("id_token", tag)) {
                            field = FIELD_ID_TOKEN;
                        } else if (Chars.equals("refresh_token", tag)) {
                            field = FIELD_REFRESH_TOKEN;
                        } else if (Chars.equals("expires_at_millis", tag)) {
                            field = FIELD_EXPIRES_AT_MILLIS;
                        } else if (Chars.equals("token_ttl_millis", tag)) {
                            field = FIELD_TOKEN_TTL_MILLIS;
                        } else {
                            field = FIELD_NONE;
                        }
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    if (depth == 1) {
                        switch (field) {
                            case FIELD_VERSION:
                                // keep the full long: an over-32-bit value (e.g. 1 + 2^32) must not narrow to
                                // SCHEMA_VERSION and slip through the schema gate, so compare it as a long
                                version = parseLongOrZero(tag);
                                break;
                            case FIELD_CLIENT_ID:
                                putValue(clientId, tag);
                                break;
                            case FIELD_TOKEN_ENDPOINT:
                                putValue(tokenEndpoint, tag);
                                break;
                            case FIELD_DEVICE_AUTHORIZATION_ENDPOINT:
                                putValue(deviceAuthorizationEndpoint, tag);
                                break;
                            case FIELD_SCOPE:
                                putValue(scope, tag);
                                break;
                            case FIELD_AUDIENCE:
                                putValue(audience, tag);
                                break;
                            case FIELD_GROUPS_IN_TOKEN:
                                groupsInToken = Chars.equals("true", tag);
                                break;
                            case FIELD_ACCESS_TOKEN:
                                putValue(accessToken, tag);
                                break;
                            case FIELD_ID_TOKEN:
                                putValue(idToken, tag);
                                break;
                            case FIELD_REFRESH_TOKEN:
                                putValue(refreshToken, tag);
                                break;
                            case FIELD_EXPIRES_AT_MILLIS:
                                expiresAtMillis = parseLongOrZero(tag);
                                break;
                            case FIELD_TOKEN_TTL_MILLIS:
                                tokenTtlMillis = parseLongOrZero(tag);
                                break;
                            default:
                                break;
                        }
                    }
                    field = FIELD_NONE;
                    break;
                default:
                    break;
            }
        }

        private static void putValue(StringSink sink, CharSequence tag) {
            // The writer omits a null/absent field entirely, so a value event means the field was present
            // with a real string: store it verbatim, including a value that is literally "null". A bare JSON
            // null in a hand-edited or non-conforming file lands here as "null" too, because JsonLexer
            // reports the two identically - which is exactly why the frozen format forbids a writer from
            // emitting one (design/oidc-token-persistence.md).
            //
            // Faithfully round-tripping whatever is on disk is this parser's job; deciding whether a value is
            // fit to be a credential is not. OidcDeviceAuth.adopt() makes that call, and refuses a served
            // token of "null" along with the blank and control-character ones, so a non-conforming writer
            // degrades to an interactive sign-in rather than to a "Bearer null" header the server answers
            // with 401. Nothing here rejects it: the fingerprint covers client_id, the endpoints, scope,
            // audience and groups_in_token - never the token - and "null" is four printable ASCII characters,
            // so the char check passes it too.
            sink.clear();
            sink.put(tag);
        }
    }
}
