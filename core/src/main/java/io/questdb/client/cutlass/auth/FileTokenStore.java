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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
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
import java.util.Set;
import java.util.UUID;

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
    // separately bounded by the client's HTTP timeout, so up to ~4x it; OidcDeviceAuth caps that timeout at
    // 120s, so the worst-case hold is ~480s and this 10-minute window stays safely above it
    private static final long DEFAULT_LOCK_STALE_MILLIS = 600_000L;
    private static final FileAttribute<Set<PosixFilePermission>> DIR_ATTRS =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    // the same owner-only directory permissions as DIR_ATTRS, in the form setPosixFilePermissions wants, so
    // ensureDirectory can re-assert them on a directory that already exists with looser permissions
    private static final Set<PosixFilePermission> DIR_PERMS = PosixFilePermissions.fromString("rwx------");
    private static final FileAttribute<Set<PosixFilePermission>> FILE_ATTRS =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"));
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final int JSON_LEXER_CACHE_SIZE = 1024;
    private static final int JSON_LEXER_MAX_VALUE_BYTES = 1 << 20;
    private static final long LOCK_POLL_SLICE_MILLIS = 50L;
    // reject a token file larger than this; a real entry is a few KB even with a group-laden id token, so
    // anything past this is corrupt or hostile and is not read into memory
    private static final long MAX_FILE_BYTES = 1 << 20;
    // upper bound on the configurable lock acquire budget. getToken() can take this lock on the
    // latency-sensitive flush path, so a caller-supplied budget is kept short: a real peer refresh is
    // sub-second, and bounding the wait stops a misconfigured budget from stalling a flush before it degrades
    // to a lock-free refresh (Layer 1 still guards integrity). Stays well below DEFAULT_LOCK_STALE_MILLIS so a
    // waiter degrades long before it could begin stealing live locks.
    private static final long MAX_LOCK_ACQUIRE_BUDGET_MILLIS = 30_000L;
    private static final int SCHEMA_VERSION = 1;
    // set once if the platform cannot enforce owner-only POSIX permissions on the token files (e.g. Windows),
    // so the at-rest protection falls back to the directory's inherited ACL; warns the user once
    private static volatile boolean warnedNoPosixPerms;
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
     *                                 stolen. It MUST exceed the longest a live holder can hold the lock: one
     *                                 refresh under the lock runs send + await + parse plus a body drain, each
     *                                 bounded by the {@code OidcDeviceAuth} httpTimeoutMillis, so up to ~4x it
     *                                 (~480s at the 120s timeout cap). Set it below that and a peer can steal a
     *                                 live holder's lock mid-refresh, reopening the cross-process refresh race
     *                                 this lock exists to prevent. The store cannot see the client's timeout,
     *                                 so sizing this correctly is the caller's responsibility; the default is
     *                                 600_000 (safely above the ~480s worst case).
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
        try {
            Files.deleteIfExists(tokenFile(key));
        } catch (IOException e) {
            throw new OidcAuthException(e).put("could not remove the OIDC token store file");
        }
    }

    @Override
    public boolean inLock(TokenStoreKey key, CriticalSection action) {
        Path lock = null;
        // the unique owner nonce stamped into the lock when we acquired it, or null if we did not (or could
        // not) acquire one and are running lock-free; releaseLock deletes the lock only when it still carries
        // this nonce, so we never delete a lock a peer has since stolen
        String nonce = null;
        try {
            ensureDirectory();
            lock = lockFile(key);
            nonce = acquireLock(lock);
        } catch (IOException e) {
            // could not prepare the lock directory or file; run without the lock. Layer-1 atomic
            // replacement still keeps every reader consistent - only a rotating-refresh-token race across
            // processes is left unguarded for this one refresh.
            nonce = null;
        }
        try {
            return action.run();
        } finally {
            if (nonce != null) {
                releaseLock(lock, nonce);
            }
        }
    }

    @Override
    public PersistedToken load(TokenStoreKey key) {
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
    }

    @Override
    public void save(TokenStoreKey key, PersistedToken token) {
        byte[] content = serialize(key, token);
        try {
            ensureDirectory();
            Path target = tokenFile(key);
            Path tmp = createTempFile(key.hash());
            boolean moved = false;
            try {
                writeAndFlush(tmp, content);
                try {
                    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    // a rare filesystem without atomic rename; a plain replace still beats a partial write
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                }
                moved = true;
            } finally {
                if (!moved) {
                    Files.deleteIfExists(tmp);
                }
            }
        } catch (IOException e) {
            throw new OidcAuthException(e).put("could not persist the OIDC token to the token store");
        }
    }

    private static void createLockFile(Path lock) throws IOException {
        try {
            Files.createFile(lock, FILE_ATTRS);
        } catch (UnsupportedOperationException e) {
            warnNoPosixPermsOnce();
            Files.createFile(lock);
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

    private static long parseLongOrZero(CharSequence value) {
        try {
            return Numbers.parseLong(value);
        } catch (NumericException e) {
            return 0;
        }
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

    private static void putBooleanMember(StringSink sink, String name, boolean value) {
        sink.put(',');
        putName(sink, name);
        sink.put(Boolean.toString(value));
    }

    private static void putLongMember(StringSink sink, String name, long value) {
        sink.put(',');
        putName(sink, name);
        sink.put(value);
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

    private static void releaseLock(Path lock, String nonce) {
        // release our own lock only: re-read it and delete it solely when it still carries our nonce. A hold
        // that outran lockStaleMillis may have been judged stale and stolen (deleted and recreated) by a peer;
        // deleting by bare path would then remove the peer's live lock and admit a third acquirer alongside it,
        // defeating the mutual exclusion this lock exists to provide. A microscopic window remains if a steal
        // lands between the read and the delete, but that is bounded to one syscall gap rather than the whole
        // hold, so a misconfigured staleness window degrades to at most the documented double-refresh rather
        // than corrupting a peer's lock state.
        try {
            byte[] content = Files.readAllBytes(lock);
            if (nonce.equals(new String(content, StandardCharsets.UTF_8))) {
                Files.deleteIfExists(lock);
            }
            // otherwise a peer now owns this lock file; leave it for that owner (or staleness) to reclaim
        } catch (NoSuchFileException e) {
            // already gone (stolen and not yet recreated, or removed elsewhere); nothing to release
        } catch (IOException ignore) {
            // best-effort release; a leftover lock goes stale and the next acquirer steals it
        }
    }

    private static void restrictToOwner(Path directory) {
        // best-effort: the at-rest protection of the plaintext token files is exactly these owner-only
        // directory permissions, so tighten a pre-existing directory rather than trust whatever it had. On a
        // non-POSIX filesystem (Windows) this is unsupported and falls back to the directory's existing ACL
        // (owner-only hardening there, via AclFileAttributeView, is a separate follow-up)
        try {
            Files.setPosixFilePermissions(directory, DIR_PERMS);
        } catch (UnsupportedOperationException e) {
            // non-POSIX FS (e.g. Windows): cannot enforce owner-only perms; keep the inherited ACL
            warnNoPosixPermsOnce();
        } catch (IOException ignore) {
            // the directory is not ours to chmod: keep the existing permissions
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
        if (warnedNoPosixPerms) {
            return;
        }
        warnedNoPosixPerms = true;
        System.err.println("questdb client: the OIDC token store could not enforce owner-only (0600/0700) "
                + "permissions on this filesystem; the persisted refresh token is protected only by the "
                + "directory's default ACL. Back the store with an OS keychain for at-rest encryption.");
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

    private static boolean writeLockHolder(Path lock, String nonce) {
        // stamp the lock with the owner nonce. Unlike the staleness mtime (which only needs to be recent),
        // this content is what releaseLock checks before deleting, so it must be written reliably; report a
        // failure so acquireLock drops an unverifiable lock rather than hold one it cannot safely release.
        // Writing also refreshes the mtime, which is what isStale reads
        try {
            Files.write(lock, nonce.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            return true;
        } catch (Exception e) {
            return false;
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
                createLockFile(lock);
                if (writeLockHolder(lock, nonce)) {
                    return nonce;
                }
                // the exclusive create won the lock but the owner nonce could not be stamped, so releaseLock
                // could not later prove ownership and would risk deleting a peer's lock; drop the file we just
                // created and degrade to a lock-free refresh rather than hold an unverifiable lock
                try {
                    Files.deleteIfExists(lock);
                } catch (IOException ignore) {
                    // another acquirer may have removed it; the next createLockFile settles the race
                }
                return null;
            } catch (FileAlreadyExistsException e) {
                if (isStale(lock)) {
                    // a crashed holder left the lock behind; steal it
                    try {
                        Files.deleteIfExists(lock);
                    } catch (IOException ignore) {
                        // another acquirer may have removed it; the next createLockFile settles the race
                    }
                    // fall through to the bounded wait below rather than retry immediately: a steal contest
                    // between several acquirers (or a misconfigured tiny lockStaleMillis) must not hot-spin
                }
                if (System.currentTimeMillis() >= deadline) {
                    return null; // give up and run without the lock rather than stall a sign-in
                }
                Os.sleep(LOCK_POLL_SLICE_MILLIS);
            } catch (IOException e) {
                return null; // unexpected IO; degrade to no lock
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

    private boolean isStale(Path lock) {
        try {
            FileTime modified = Files.getLastModifiedTime(lock);
            return System.currentTimeMillis() - modified.toMillis() > lockStaleMillis;
        } catch (IOException e) {
            return false; // cannot determine the age; do not steal
        }
    }

    private Path lockFile(TokenStoreKey key) {
        return directory.resolve(key.hash() + ".lock");
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
        int version;
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
                                version = (int) parseLongOrZero(tag);
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
