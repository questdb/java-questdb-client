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
import java.lang.management.ManagementFactory;
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
 * The default {@link TokenStore}: one plaintext JSON file per identity under a directory, with the
 * refresh token protected at rest by file permissions (0600 file, 0700 directory) rather than by
 * encryption. This matches what {@code gcloud}, {@code aws} and {@code gh} do; for encryption at rest,
 * supply a {@link TokenStore} backed by an OS keychain or a secrets manager instead.
 * <p>
 * The default location is {@code ${user.home}/.questdb/oidc-tokens/}, overridable with the
 * {@code questdb.client.oidc.token.store.dir} system property. The file name is
 * {@code <TokenStoreKey.hash()>.json}, so several identities coexist and the name leaks neither the
 * endpoint nor the client id. The on-disk format (file name, JSON schema, write protocol, lock-file
 * protocol) is a deliberately language-neutral contract so other QuestDB clients can share the file.
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
    // attacker-writable directory as the token file, and a real owner stamp (pid@host + millis + UUID) is a
    // few hundred bytes, so anything past this cap is corrupt or hostile and is not read into memory
    private static final int MAX_LOCK_FILE_BYTES = 1 << 12;
    // Serializes same-identity critical sections WITHIN this JVM, keyed on the identity fingerprint. Two
    // OidcDeviceAuth instances for one identity in a single process (e.g. an ILP Sender and a QwpQueryClient)
    // have separate instance locks, so only this shared lock stops them running the read-refresh-write
    // concurrently and double-POSTing the same parent refresh token - which a reuse-detecting IdP revokes the
    // whole token family for. The cross-process file lock's lock-free degrade must not license an intra-process
    // race, so this in-process lock is taken first and is never subject to that degrade. Bounded by identity
    // count (a handful), so it never grows unbounded.
    private static final ConcurrentHashMap<String, ReentrantLock> PROCESS_LOCKS = new ConcurrentHashMap<>();
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
    private final Path directory;
    private final long lockAcquireBudgetMillis;
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
     *                                 timeout, commonly ~2 minutes). Size this window above ~4x httpTimeoutMillis
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
        // delete under the cross-process lock, like the read-refresh-write, so a peer's in-flight refresh
        // cannot resurrect the entry by atomically renaming a fresh file in just after we delete. inLock cleans
        // up its own lock file and degrades to lock-free if it cannot acquire one. Cross-process clear is still
        // best-effort: a peer holding a live in-memory token may legitimately re-persist later - clearing forces
        // a fresh sign-in for THIS process regardless, since the caller resets its in-memory token state.
        inLock(key, () -> {
            try {
                Files.deleteIfExists(tokenFile(key));
            } catch (IOException e) {
                throw new OidcAuthException(e).put("could not remove the OIDC token store file");
            }
            // Also remove any write temp for this identity. A crash between createTempFile and the atomic
            // rename orphans a <hash><random>.tmp holding the FULL serialized entry - access, id and refresh
            // tokens in plaintext - and until now nothing here reclaimed it: sweepStaleTempFiles runs only
            // from save(), so a caller that clears and never signs in again left a live refresh token on
            // disk indefinitely, contradicting this method's contract. Sweep at ANY age, unlike save()'s
            // staleness-bounded sweep: clear() is an explicit "forget this credential", and a temp a
            // concurrent save is mid-rename on is a benign loser - its rename fails, persistence is
            // best-effort, and the caller is discarding the credential anyway.
            sweepTempFiles(key.hash(), 0L);
            return true;
        });
    }

    @Override
    public boolean inLock(TokenStoreKey key, CriticalSection action) {
        // First serialize other threads of THIS JVM sharing the same identity: the cross-process file lock below
        // degrades to lock-free after lockAcquireBudgetMillis, which is a fine cross-process fallback but must
        // not let two threads of one process run the critical section at once (they would double-POST the same
        // rotating refresh token and get the whole family revoked on a reuse-detecting IdP). This lock is not
        // subject to the file lock's degrade. ReentrantLock is safe even though inLock's contract forbids
        // nesting - a mistaken re-entry cannot self-deadlock.
        final ReentrantLock processLock = PROCESS_LOCKS.computeIfAbsent(key.hash(), k -> new ReentrantLock());
        processLock.lock();
        try {
            Path lock = null;
            // the unique owner nonce stamped into the lock when we acquired it, or null if we did not (or could
            // not) acquire one and are running lock-free; releaseLock deletes the lock only when it still carries
            // this nonce, so we never delete a lock a peer has since stolen
            String nonce = null;
            // acquireLock does FileChannel I/O, so shield it from a carried interrupt flag exactly as load()
            // does - but shield ONLY the lock bookkeeping, never action.run(). The critical section is the
            // caller's own token refresh, and an interrupt is precisely the lever close() uses to break it.
            boolean wasInterrupted = Thread.interrupted();
            try {
                ensureDirectory();
                lock = lockFile(key);
                nonce = acquireLock(lock);
            } catch (IOException e) {
                // could not prepare the lock directory or file; run without the cross-process lock. Layer-1
                // atomic replacement still keeps every reader consistent - only a rotating-refresh-token race
                // across processes is left unguarded for this one refresh.
                nonce = null;
            } finally {
                if (wasInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
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
                    } finally {
                        if (wasInterruptedInSection) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        } finally {
            processLock.unlock();
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
                ensureDirectory();
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

    private static String newLockNonce() {
        // a per-acquisition owner stamp: the pid@host and the acquire time are human-readable debugging aids,
        // and the random UUID guarantees two acquisitions never share a stamp even within one pid and one
        // millisecond, so releaseLock's ownership check is exact rather than probabilistic
        return ManagementFactory.getRuntimeMXBean().getName() // typically pid@host
                + ' ' + System.currentTimeMillis()
                + ' ' + UUID.randomUUID();
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
            LOG.warn("could not release the OIDC token store lock; peers degrade to lock-free refresh until "
                    + "it goes stale [error={}]", e.getMessage());
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

    private static void restrictToOwner(Path directory) {
        // best-effort: the at-rest protection of the plaintext token files is exactly these owner-only
        // directory permissions, so re-tighten a pre-existing directory another tool/umask left loose rather
        // than trust whatever it had. ensureDirectory runs this on every save and every inLock, so only chmod
        // on detected drift - skip the write syscall in the common case where the permissions already match. On
        // a non-POSIX filesystem (Windows) this is unsupported and falls back to the directory's existing ACL
        // (owner-only hardening there, via AclFileAttributeView, is a separate follow-up)
        try {
            if (!DIR_PERMS.equals(Files.getPosixFilePermissions(directory))) {
                Files.setPosixFilePermissions(directory, DIR_PERMS);
            }
        } catch (UnsupportedOperationException e) {
            // non-POSIX FS (e.g. Windows): cannot enforce owner-only perms; keep the inherited ACL
            warnNoPosixPermsOnce();
        } catch (IOException ignore) {
            // the directory is not ours to inspect/chmod: keep the existing permissions
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

    private String acquireLock(Path lock) {
        // returns the unique owner nonce stamped into the lock on success, or null if it could not be acquired
        // within the budget. releaseLock uses the nonce to verify ownership before deleting, so a hold that
        // outran lockStaleMillis (and was stolen by a peer) never deletes the peer's lock on release
        final String nonce = newLockNonce();
        final long deadline = System.currentTimeMillis() + lockAcquireBudgetMillis;
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
                if (System.currentTimeMillis() >= deadline) {
                    return null; // give up and run without the lock rather than stall a sign-in
                }
                Os.sleep(LOCK_POLL_SLICE_MILLIS);
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
                LOG.warn("could not acquire the OIDC token store lock; running this refresh without "
                        + "cross-process coordination [error={}]", e.getMessage());
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

    private void ensureDirectory() throws IOException {
        if (Files.isDirectory(directory)) {
            // re-assert owner-only permissions on a pre-existing directory: createDirectories applies
            // DIR_ATTRS only when it creates the directory, so one left world/group-accessible by another
            // tool, a permissive umask, or a hostile local pre-create would otherwise expose the token files
            restrictToOwner(directory);
            return;
        }
        try {
            Files.createDirectories(directory, DIR_ATTRS);
        } catch (UnsupportedOperationException e) {
            warnNoPosixPermsOnce();
            Files.createDirectories(directory);
        }
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
        //
        // Restore by hard-LINKING our capture back to the lock path, not by renaming it. Files.move without
        // REPLACE_EXISTING looks atomic but is not: it stats the target, then renames, and rename(2) silently
        // replaces. A third party that claims the freed path between those two steps therefore had its live
        // lock destroyed by the very call whose comment promised to leave it intact. link(2) has no such gap -
        // it fails outright when the target exists - and it preserves the peer's exact bytes, which matters
        // because releaseLock verifies the stamp before deleting.
        //
        // A residual remains and is not closeable with a lock file: if a third party did claim the path, we
        // drop our copy, so the recreating peer's lock file is gone while that peer still believes it holds
        // the lock, and for that one refresh two holders can run concurrently. A filesystem offers no atomic
        // "delete or rename only if the content is still X", so the capture-verify narrows the window to this
        // multi-actor race - our steal, a peer recreating, AND a third party claiming the freed path, all
        // overlapping - without eliminating it. Best-effort by design: it degrades to one extra refresh, a
        // re-prompt on a rotating-refresh-token identity provider, never a torn or forged credential
        // (Layer 1's atomic rename still holds).
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
            // the writer omits a null/absent field entirely, so a value event means the field was present
            // with a real string: store it verbatim, including a value that is literally "null". A bare JSON
            // null in a hand-edited or non-conforming file lands here as "null" too, which is harmless - a
            // bogus token simply fails its fingerprint/char check and the entry falls back.
            sink.clear();
            sink.put(tag);
        }
    }
}
