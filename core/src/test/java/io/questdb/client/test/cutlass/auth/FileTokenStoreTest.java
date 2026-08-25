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

package io.questdb.client.test.cutlass.auth;

import io.questdb.client.cutlass.auth.FileTokenStore;
import io.questdb.client.cutlass.auth.OidcAuthException;
import io.questdb.client.cutlass.auth.PersistedToken;
import io.questdb.client.cutlass.auth.TokenStore;
import io.questdb.client.cutlass.auth.TokenStoreKey;
import io.questdb.client.std.Os;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Coverage for {@link FileTokenStore}.
 * <p>
 * PLATFORM SCOPE. CI runs Linux only, so the store's Windows-motivated arms are covered here to the extent a
 * POSIX host can reach them, and no further:
 * <ul>
 *     <li>the {@code AccessDeniedException} retry in {@code replaceTarget} - the sharing violation a Windows
 *     reader holding the target open produces - IS exercised, by denying the rename with directory
 *     permissions instead (testReplaceTargetRetriesADeniedRenameThenSucceeds and its give-up sibling). Those
 *     two skip when the process is root, which bypasses the permission bits they rely on;</li>
 *     <li>the {@code UnsupportedOperationException} fallbacks around {@code FILE_ATTRS}/{@code DIR_ATTRS}
 *     ({@code createTempFile}, {@code createLockFile}, {@code ensureDirectory}, {@code restrictToOwner}) are
 *     NOT exercised. They fire only where the filesystem cannot carry POSIX permissions, which a POSIX host
 *     cannot produce without a synthetic {@code FileSystemProvider}; the suite has no such fixture and none
 *     of these tests reach them;</li>
 *     <li>the {@code AtomicMoveNotSupportedException} fallback in {@code replaceTarget} is likewise
 *     unreachable here - every filesystem these tests run on supports an atomic rename.</li>
 * </ul>
 * Closing the last two needs a Windows CI agent, or a filesystem-provider fixture that reports neither POSIX
 * attributes nor atomic moves.
 */
public class FileTokenStoreTest {

    private static final Set<PosixFilePermission> OWNER_ONLY_DIR_PERMS =
            PosixFilePermissions.fromString("rwx------");

    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testAdvancedConstructorRejectsNonPositiveTimings() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            // a non-positive acquire budget or staleness window is rejected: a tiny/zero staleness would make
            // every freshly created lock look abandoned, so acquirers would steal each other's live locks
            try {
                new FileTokenStore(dir, 0, 1000);
                Assert.fail("a zero lock acquire budget must be rejected");
            } catch (OidcAuthException expected) {
                // expected
            }
            try {
                new FileTokenStore(dir, -1, 1000);
                Assert.fail("a negative lock acquire budget must be rejected");
            } catch (OidcAuthException expected) {
                // expected
            }
            try {
                new FileTokenStore(dir, 1000, 0);
                Assert.fail("a zero lock staleness window must be rejected");
            } catch (OidcAuthException expected) {
                // expected
            }
            try {
                new FileTokenStore(dir, 1000, -1);
                Assert.fail("a negative lock staleness window must be rejected");
            } catch (OidcAuthException expected) {
                // expected
            }
        });
    }

    @Test
    public void testAdvancedConstructorRejectsOverCapAcquireBudget() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            // the acquire budget is capped: getToken() can wait it out on the latency-sensitive flush path, so
            // an unbounded budget would let a misconfiguration stall a flush; the cap also keeps a waiter
            // degrading well before it could begin stealing live locks
            try {
                new FileTokenStore(dir, 30_001, 600_000);
                Assert.fail("an over-cap lock acquire budget must be rejected");
            } catch (OidcAuthException expected) {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("lockAcquireBudgetMillis"));
            }
            // the cap boundary itself is accepted
            new FileTokenStore(dir, 30_000, 600_000);
        });
    }

    @Test
    public void testArrayWrappedJsonReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            // a valid entry, then the same object wrapped in a top-level array. The wrapper leaves the
            // fingerprint fields untouched, so only the non-object-root rejection - not a fingerprint mismatch -
            // can reject it: the parser must refuse a shape that is not a single flat JSON object
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Assert.assertNotNull("the plain object must load", store.load(key));
            byte[] obj = Files.readAllBytes(tokenFile(dir, key));
            byte[] wrapped = new byte[obj.length + 2];
            wrapped[0] = '[';
            System.arraycopy(obj, 0, wrapped, 1, obj.length);
            wrapped[wrapped.length - 1] = ']';
            Files.write(tokenFile(dir, key), wrapped);
            Assert.assertNull("an array-wrapped object must be rejected as a malformed shape", store.load(key));
        });
    }

    @Test
    public void testAudienceNullVersusEmptyFingerprint() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey nullAud = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid", null, false);
            TokenStoreKey withAud = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid", "api://billing", false);

            // a null audience round-trips: the writer omits the member, and nullableEquals matches an absent
            // file value against a null key audience
            store.save(nullAud, sampleToken("ACCESS-1", "REFRESH-1"));
            Assert.assertNotNull(store.load(nullAud));
            byte[] nullAudBytes = Files.readAllBytes(tokenFile(dir, nullAud));

            store.save(withAud, sampleToken("ACCESS-2", "REFRESH-2"));
            byte[] withAudBytes = Files.readAllBytes(tokenFile(dir, withAud));

            // place each file under the *other* key's name to isolate the in-file audience fingerprint check
            // from the hash-based file naming: a recorded audience must not match a null-audience key, and an
            // absent audience must not match an audience-bearing key
            Files.write(tokenFile(dir, nullAud), withAudBytes);
            Assert.assertNull("a recorded audience must not match a null-audience key", store.load(nullAud));
            Files.write(tokenFile(dir, withAud), nullAudBytes);
            Assert.assertNull("an absent audience must not match an audience-bearing key", store.load(withAud));
        });
    }

    @Test
    public void testClearDeletesFile() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Assert.assertTrue(Files.exists(tokenFile(dir, key)));

            store.clear(key);
            Assert.assertFalse(Files.exists(tokenFile(dir, key)));
            Assert.assertNull(store.load(key));
            // clearing a missing entry is a no-op, not an error
            store.clear(key);
        });
    }

    @Test
    public void testClearErasesTheEntryOnAnInterruptCarryingThread() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-SECRET"));
            Assert.assertTrue(Files.exists(tokenFile(dir, key)));

            // A sign-out runs on shutdown and cleanup paths, which is exactly where a thread carries an
            // interrupt flag - the standard cancellation idiom re-asserts it. Routed through inLock, the
            // carried flag made the delete never run, and clear() discarded the false return: the plaintext
            // refresh token stayed on disk with no exception and no warning, and the next process start
            // silently resumed the old identity. A local delete has nothing to abandon on a cancellation.
            Thread.currentThread().interrupt();
            final boolean flagSurvived;
            try {
                store.clear(key);
                flagSurvived = Thread.currentThread().isInterrupted();
            } finally {
                Thread.interrupted(); // do not leak the flag into the next test
            }

            // erasure first: it is the claim this test exists for, so it is the one a regression must break
            Assert.assertFalse("clear() must erase the credential even on an interrupt-carrying thread",
                    Files.exists(tokenFile(dir, key)));
            Assert.assertNull(store.load(key));
            Assert.assertTrue("the caller's cancellation signal must survive clear()", flagSurvived);
        });
    }

    @Test
    public void testClearOnEmptyStoreIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir(); // a non-existent subdirectory
            FileTokenStore store = new FileTokenStore(dir);
            // clearing an identity that was never saved must be a no-op and must not create the store directory
            // just to run the now-locked delete
            store.clear(sampleKey());
            Assert.assertFalse("clear must not create the store directory", Files.exists(dir));
        });
    }

    @Test
    public void testConcurrentStealContentionDegradesCleanly() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            TokenStoreKey key = sampleKey();
            // a lock abandoned by a crashed holder, backdated well past the staleness window
            Path lock = lockFile(dir, key);
            Files.write(lock, "crashed-holder-stamp".getBytes(StandardCharsets.UTF_8));
            Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis() - 600_000));

            // Several "processes" run the FULL inLock path against the one abandoned lock, and it must
            // degrade CLEANLY: every contender runs its critical section (none is starved or wedged), each
            // under a lock it actually holds, and no atomic-capture temp file leaks.
            //
            // SCOPE NOTE: these four contenders do NOT race at the file-lock layer. inLock() takes the
            // in-process PROCESS_LOCKS entry for key.hash() before any lock-file logic, and that map is
            // static, so four distinct FileTokenStore instances on one identity are serialized whatever the
            // file lock does - the first reclaims the abandoned lock, and each of the rest finds the path free
            // and creates its own. The genuinely concurrent capture race is
            // testConcurrentStealersLeaveExactlyOneWinner, which drives stealIfStale directly because that is
            // the only way to reach it inside one JVM.
            //
            // It deliberately does NOT assert strict mutual exclusion. stealIfStale is best-effort by design
            // and documents a three-actor residual - a peer recreating the lock in the isStale->capture gap
            // while a second captures that fresh live lock and a third claims the momentarily-free path, all
            // at once - under which two holders can briefly run concurrently. That residual needs three or
            // more contenders and degrades only to one extra token refresh (a re-prompt on a
            // rotating-refresh-token IdP), never a torn or forged credential, since the Layer-1 atomic-rename
            // write is independent of the lock.
            // testSameProcessContendersSerializeAndBothStealStaleLock covers the two-contender same-JVM case
            // (where PROCESS_LOCKS, not the file-lock capture, provides the exclusion it asserts).
            final int threads = 4;
            AtomicInteger ran = new AtomicInteger();
            // the stamp on the lock file while each contender runs, so the assertions below can tell an
            // acquisition apart from a no-op
            List<String> stampWhileRunning = Collections.synchronizedList(new ArrayList<>());
            TokenStore.CriticalSection section = () -> {
                stampWhileRunning.add(readLockStamp(lock));
                Os.sleep(100);
                ran.incrementAndGet();
                return true;
            };

            // A contender that THREW instead of running would die on its own thread and leave the counts
            // below looking like a clean degrade, so carry the first failure back to the test thread.
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            Thread[] ts = new Thread[threads];
            for (int i = 0; i < threads; i++) {
                // a generous acquire budget so a contender waits for the lock rather than giving up early
                FileTokenStore store = new FileTokenStore(dir, 30_000, 60_000);
                ts[i] = new Thread(() -> {
                    try {
                        store.inLock(key, section);
                    } catch (Throwable t) {
                        workerError.compareAndSet(null, t);
                    }
                }, "steal-contender-" + i);
            }
            for (Thread t : ts) {
                t.start();
            }
            for (Thread t : ts) {
                joinOrFail(t, "a steal contender");
            }

            Assert.assertNull("a contender failed instead of running its critical section: " + workerError.get(),
                    workerError.get());
            Assert.assertEquals("every contender must run its critical section", threads, ran.get());
            // Teeth the run count alone does not have: threads==ran holds even with the whole acquire deleted,
            // since inLock() runs the section lock-free when it cannot get a lock. A contender that never
            // acquired would have run with the crashed holder's stamp still in place (or with no lock file at
            // all), so require a live stamp - one that is neither absent nor the crashed holder's - under
            // every critical section.
            Assert.assertEquals(threads, stampWhileRunning.size());
            for (String stamp : stampWhileRunning) {
                Assert.assertNotNull("a contender ran with no lock file at all, so it never acquired one", stamp);
                Assert.assertNotEquals("a contender ran while the crashed holder's lock was still in place",
                        "crashed-holder-stamp", stamp);
            }
            Assert.assertFalse("the last holder must have released its lock", Files.exists(lock));
            assertNoCaptureTempFiles(dir, key);
        });
    }

    @Test
    public void testConcurrentStealersLeaveExactlyOneWinner() throws Exception {
        assertMemoryLeak(() -> {
            // The capture race stealIfStale is written for: several stealers judge the same abandoned lock
            // stale at once, exactly one wins the ATOMIC_MOVE capture and drops it, and the losers take the
            // NoSuchFileException arm and fall back to the wait rather than deleting anything. inLock() cannot
            // reach this race inside one JVM - PROCESS_LOCKS serializes same-identity threads ahead of every
            // lock-file syscall - so drive the steal itself. Reflection is the seam: the test tree is a
            // separate io.questdb.client.test.* package with its own module-info, so package-private access
            // is structurally unavailable.
            Path dir = storeDir();
            createStoreDir(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            Files.write(lock, "crashed-holder-stamp".getBytes(StandardCharsets.UTF_8));
            Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis() - 600_000));

            Method stealIfStale = FileTokenStore.class.getDeclaredMethod("stealIfStale", Path.class);
            stealIfStale.setAccessible(true);

            final int stealers = 8;
            CyclicBarrier start = new CyclicBarrier(stealers);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread[] ts = new Thread[stealers];
            for (int i = 0; i < stealers; i++) {
                // one store per "process", as in the sibling test
                FileTokenStore store = new FileTokenStore(dir, 30_000, 60_000);
                ts[i] = new Thread(() -> {
                    try {
                        start.await(10, TimeUnit.SECONDS);
                        stealIfStale.invoke(store, lock);
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }, "steal-contender");
            }
            for (Thread t : ts) {
                t.start();
            }
            for (Thread t : ts) {
                joinOrFail(t, "a stealer");
            }

            Assert.assertNull("a stealer failed outright: " + failure.get(), failure.get());
            Assert.assertFalse("the abandoned lock must be gone - one stealer captures it and drops it, and a "
                    + "loser must not restore what it never captured", Files.exists(lock));
            assertNoCaptureTempFiles(dir, key);
        });
    }

    @Test
    public void testRestoreCapturedLockPutsAPeersLockBackByteForByte() throws Exception {
        assertMemoryLeak(() -> {
            // The arm testConcurrentStealersLeaveExactlyOneWinner cannot reach. Its three observables - no
            // stealer threw, the lock is gone, no capture temp survives - all hold under the bare
            // deleteIfExists(lock) that stealIfStale's own comment says must never be used, because a bare
            // delete also removes the lock and leaves no temp. What separates the two is what happens when the
            // capture-verify says "this is NOT the lock we judged stale": the capture must go BACK, with the
            // peer's exact bytes, because releaseLock verifies the stamp before deleting and a peer whose
            // stamp we corrupted can no longer release its own lock.
            //
            // Driving stealIfStale itself cannot get here: confirmedStale is false only when a peer replaces
            // the file between the staleness read and the ATOMIC_MOVE, an interleaving no test can force
            // without a production seam. So drive the restore directly.
            Path dir = storeDir();
            createStoreDir(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            Path captured = dir.resolve(lock.getFileName().toString() + ".capture.tmp");

            byte[] peerStamp = "peer-owner-nonce-9f3c".getBytes(StandardCharsets.UTF_8);
            Files.write(captured, peerStamp);

            Method restore = FileTokenStore.class.getDeclaredMethod(
                    "restoreCapturedLock", Path.class, Path.class);
            restore.setAccessible(true);
            restore.invoke(new FileTokenStore(dir, 30_000, 60_000), lock, captured);

            Assert.assertTrue("the peer's lock must be back at the lock path", Files.exists(lock));
            Assert.assertArrayEquals("the peer's owner stamp must survive byte for byte, or releaseLock's "
                            + "own-stamp check will refuse to let that peer release its own lock",
                    peerStamp, Files.readAllBytes(lock));
            Assert.assertFalse("the capture copy must not be left behind", Files.exists(captured));
            assertNoCaptureTempFiles(dir, key);
        });
    }

    @Test
    public void testRestoreCapturedLockLeavesAThirdPartysLockUntouched() throws Exception {
        assertMemoryLeak(() -> {
            // The reason the restore links rather than renames. Files.move without REPLACE_EXISTING stats the
            // target and then renames, and rename(2) replaces silently - so a third party that claimed the
            // freed path between those two steps would have its live lock destroyed by the very call whose
            // comment promises to leave it intact. createLink fails outright instead. Here the third party has
            // already claimed the path when the restore runs, which is the deterministic end of that race and
            // needs no interleaving to reproduce.
            Path dir = storeDir();
            createStoreDir(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            Path captured = dir.resolve(lock.getFileName().toString() + ".capture.tmp");

            byte[] thirdPartyStamp = "third-party-live-nonce".getBytes(StandardCharsets.UTF_8);
            Files.write(lock, thirdPartyStamp);
            Files.write(captured, "our-captured-copy".getBytes(StandardCharsets.UTF_8));

            Method restore = FileTokenStore.class.getDeclaredMethod(
                    "restoreCapturedLock", Path.class, Path.class);
            restore.setAccessible(true);
            restore.invoke(new FileTokenStore(dir, 30_000, 60_000), lock, captured);

            Assert.assertArrayEquals("a third party's LIVE lock must survive the restore untouched - "
                            + "overwriting it admits two holders at once",
                    thirdPartyStamp, Files.readAllBytes(lock));
            Assert.assertFalse("our capture copy must be dropped, not left to accumulate",
                    Files.exists(captured));
            assertNoCaptureTempFiles(dir, key);
        });
    }

    @Test
    public void testProcessLocksDoNotGrowWithTheIdentityCount() throws Exception {
        assertMemoryLeak(() -> {
            // TokenStoreKey is public and inLock() is public API, so a process mints as many identities as its
            // caller needs - one per end user in a multi-tenant service. An unpruned map roots a 64-char hash
            // plus a lock for every identity EVER SEEN, for the life of the JVM. Bound it by the identities
            // actually in flight instead: once the last caller on an identity leaves, its entry goes.
            Field field = FileTokenStore.class.getDeclaredField("PROCESS_LOCKS");
            field.setAccessible(true);
            java.util.Map<?, ?> locks = (java.util.Map<?, ?>) field.get(null);

            Path dir = storeDir();
            createStoreDir(dir);
            FileTokenStore store = new FileTokenStore(dir, 30_000, 600_000);
            AtomicInteger ran = new AtomicInteger();
            final int identities = 500;
            for (int i = 0; i < identities; i++) {
                TokenStoreKey key = new TokenStoreKey("client-" + i, "https://idp.example.com:443/token",
                        "https://idp.example.com:443/device", "openid", null, false);
                Assert.assertTrue(store.inLock(key, () -> {
                    ran.incrementAndGet();
                    // the entry has to EXIST while its critical section runs, or the lock is serializing
                    // nothing; the retirement below is only interesting because of this
                    Assert.assertFalse("the identity's lock must be held for the critical section",
                            locks.isEmpty());
                    return true;
                }));
            }
            Assert.assertEquals("every identity must still have run its critical section", identities, ran.get());

            // Nothing is in flight now, so nothing may be left behind. This is the assertion the old
            // stripe-table version could not make: it asserted the table was the same ARRAY of the same
            // LENGTH, which is true of any immutable array whether or not the code under test works.
            Assert.assertEquals("no entry may outlive its last caller [left=" + locks + "]", 0, locks.size());
            // and it must be usable, not merely empty: a fresh identity still serializes
            Assert.assertTrue(store.inLock(sampleKey(), () -> true));
            Assert.assertEquals("the fresh identity must be retired too", 0, locks.size());
        });
    }

    @Test(timeout = 60_000)
    public void testSameIdentityInDifferentDirectoriesDoesNotSerialize() throws Exception {
        assertMemoryLeak(() -> {
            // The mirror of testUnrelatedIdentitiesDoNotSerializeOnEachOther, along the other axis. That one
            // varies the identity within one directory; this one keeps the identity and varies the
            // directory - which is the shape this class's javadoc and the README actually prescribe for
            // signing several application users in at once: "a store each, on a per-user directory".
            //
            // Those two stores hold DIFFERENT files, so serializing them buys nothing and costs everything
            // the sibling test describes: the lock spans a whole token-endpoint round trip, its acquire has
            // no budget, and getToken() sits on the ILP flush path.
            //
            // Same CyclicBarrier trick: it trips only when both callers are inside their critical section at
            // once, which two callers sharing one lock can never be.
            // distinct directories, not storeDir() twice - that helper returns one fixed path, and two
            // stores over ONE directory are the case that must keep serializing
            Path dirA = temp.getRoot().toPath().resolve("oidc-tokens-user-a");
            Path dirB = temp.getRoot().toPath().resolve("oidc-tokens-user-b");
            createStoreDir(dirA);
            createStoreDir(dirB);
            FileTokenStore storeA = new FileTokenStore(dirA, 30_000, 600_000);
            FileTokenStore storeB = new FileTokenStore(dirB, 30_000, 600_000);
            TokenStoreKey key = sampleKey();

            CyclicBarrier bothInside = new CyclicBarrier(2);
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            AtomicInteger ran = new AtomicInteger();
            TokenStore.CriticalSection section = () -> {
                try {
                    ran.incrementAndGet();
                    bothInside.await(20, TimeUnit.SECONDS);
                    return true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            Thread tA = new Thread(() -> {
                try {
                    Assert.assertTrue(storeA.inLock(key, section));
                } catch (Throwable t) {
                    workerError.compareAndSet(null, t);
                }
            }, "store-a");
            Thread tB = new Thread(() -> {
                try {
                    Assert.assertTrue(storeB.inLock(key, section));
                } catch (Throwable t) {
                    workerError.compareAndSet(null, t);
                }
            }, "store-b");
            tA.start();
            tB.start();
            joinOrFail(tA, "store A");
            joinOrFail(tB, "store B");

            Assert.assertNull("one identity in two directories must not queue on a single in-process lock; "
                    + "the barrier times out when they share one", workerError.get());
            Assert.assertEquals("both critical sections must have run", 2, ran.get());
        });
    }

    @Test(timeout = 60_000)
    public void testUnrelatedIdentitiesDoNotSerializeOnEachOther() throws Exception {
        assertMemoryLeak(() -> {
            // The in-process lock owes exactly one guarantee: two callers on the SAME identity must not run
            // the read-refresh-write concurrently and double-POST a rotating refresh token. It owes unrelated
            // identities nothing, and serializing them is not the free trade it looks - the lock is held
            // across a whole token-endpoint round trip while the caller also holds its OidcDeviceAuth
            // instance lock, and the acquire has no budget. One tenant's ILP flush blocking on another
            // tenant's stalled refresh is a stall the flush path cannot see coming.
            //
            // MORE identities than a 64-entry stripe table has stripes, so the pigeonhole principle - not a
            // probability - guarantees a collision under any fixed table of that size. Every identity must
            // still be able to sit inside its critical section at once.
            final int identities = 65;
            Path dir = storeDir();
            createStoreDir(dir);
            FileTokenStore store = new FileTokenStore(dir, 30_000, 600_000);

            CyclicBarrier allInside = new CyclicBarrier(identities);
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            AtomicInteger inside = new AtomicInteger();
            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < identities; i++) {
                TokenStoreKey key = new TokenStoreKey("tenant-" + i, "https://idp.example.com:443/token",
                        "https://idp.example.com:443/device", "openid", null, false);
                Thread t = new Thread(() -> {
                    try {
                        Assert.assertTrue(store.inLock(key, () -> {
                            try {
                                inside.incrementAndGet();
                                // Trips only once every identity is holding its own lock. Two identities
                                // sharing one lock can never both get here, so a stripe table deadlocks the
                                // barrier and the await below times out.
                                allInside.await(30, TimeUnit.SECONDS);
                                return true;
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }));
                    } catch (Throwable e) {
                        workerError.compareAndSet(null, e);
                        allInside.reset(); // unblock the peers so the test fails loudly, not by timing out
                    }
                }, "tenant-lock-" + i);
                t.setDaemon(true);
                workers.add(t);
                t.start();
            }
            for (Thread t : workers) {
                joinOrFail(t, "tenant lock holder");
            }
            if (workerError.get() != null) {
                throw new AssertionError("unrelated identities did not hold their locks concurrently; "
                        + identities + " identities, " + inside.get() + " got inside", workerError.get());
            }
            Assert.assertEquals("every identity must have entered its critical section",
                    identities, inside.get());
        });
    }

    @Test
    public void testOneConfigurationHoldsOneActiveLogin() throws Exception {
        assertMemoryLeak(() -> {
            // The store is keyed on a CONFIGURATION - client id, endpoints, scope, audience,
            // groups-in-token mode - and no field of TokenStoreKey names a subject. Two people signing in
            // through the same configuration therefore address the same file, and the later sign-in
            // overwrites the earlier one: a store holds a single active login, which is the boundary the
            // README and the FileTokenStore javadoc now state. Anyone reading "one file per identity" as
            // "one file per person" would size a multi-user deployment on a guarantee that does not exist.
            FileTokenStore store = new FileTokenStore(storeDir());
            TokenStoreKey first = sampleKey();
            TokenStoreKey second = sampleKey(); // a separate instance, identical configuration
            Assert.assertEquals("identical configurations must address the same entry",
                    first.hash(), second.hash());

            store.save(first, sampleToken("ACCESS-ALICE", "REFRESH-ALICE"));
            store.save(second, sampleToken("ACCESS-BOB", "REFRESH-BOB"));

            PersistedToken loaded = store.load(first);
            Assert.assertNotNull(loaded);
            Assert.assertEquals("the later sign-in must own the entry", "ACCESS-BOB", loaded.getAccessToken());
            Assert.assertEquals("REFRESH-BOB", loaded.getRefreshToken());
            // and the first login is gone rather than merged or kept alongside
            Assert.assertEquals("one configuration keeps one entry, not one per person",
                    "ACCESS-BOB", store.load(second).getAccessToken());

            // separating them is the caller's job, and a separate store directory is what does it
            Path aliceDir = temp.getRoot().toPath().resolve("alice");
            FileTokenStore aliceStore = FileTokenStore.at(aliceDir);
            aliceStore.save(first, sampleToken("ACCESS-ALICE", "REFRESH-ALICE"));
            Assert.assertEquals("a per-user store keeps a per-user login",
                    "ACCESS-ALICE", aliceStore.load(first).getAccessToken());
            Assert.assertEquals("and does not disturb the shared one",
                    "ACCESS-BOB", store.load(first).getAccessToken());
        });
    }

    @Test
    public void testReplaceTargetGivesUpAfterTheRetryBudget() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to deny the rename",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        Assume.assumeFalse("a root process bypasses the directory permissions this denial relies on",
                "root".equals(System.getProperty("user.name")));
        assertMemoryLeak(() -> {
            // The other half of the Windows sharing-violation arm: a denial that never clears must surface,
            // not be retried forever or swallowed. save() turns the throw into its best-effort degrade.
            Path dir = storeDir();
            createStoreDir(dir);
            Path tmp = Files.write(dir.resolve("payload.tmp"), "NEW".getBytes(StandardCharsets.UTF_8));
            Path target = Files.write(dir.resolve("payload.json"), "OLD".getBytes(StandardCharsets.UTF_8));

            Method replaceTarget = FileTokenStore.class.getDeclaredMethod("replaceTarget", Path.class, Path.class);
            replaceTarget.setAccessible(true);
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-x------"));
            try {
                long start = System.currentTimeMillis();
                try {
                    replaceTarget.invoke(null, tmp, target);
                    Assert.fail("a rename denied on every attempt must be reported, not swallowed");
                } catch (InvocationTargetException e) {
                    Assert.assertTrue("the LAST denial must be the one rethrown, was: " + e.getCause(),
                            e.getCause() instanceof AccessDeniedException);
                }
                // 5 attempts means 4 backoff sleeps of 20ms; a shape that gave up on the first denial
                // (the pre-retry behaviour, and what Windows would routinely trip over) returns at once
                long elapsed = System.currentTimeMillis() - start;
                Assert.assertTrue("it must have spent the whole retry budget, took " + elapsed + "ms",
                        elapsed >= 80);
            } finally {
                // restore, or the temp-folder rule cannot delete the tree
                Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
            }
            Assert.assertEquals("a failed replace must leave the previous entry intact",
                    "OLD", new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
        });
    }

    @Test
    public void testReplaceTargetPreservesAnInterruptDeliveredDuringItsBackoff() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to deny the rename",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        Assume.assumeFalse("a root process bypasses the directory permissions this denial relies on",
                "root".equals(System.getProperty("user.name")));
        assertMemoryLeak(() -> {
            // replaceTarget's retry backoff is the one interruptible wait on the save path. save() parks and
            // restores only the flag it saw on ENTRY, so an interrupt arriving mid-save has to survive this
            // sleep on its own - and that interrupt is exactly what PoolHousekeeper.stop() delivers to break a
            // recovery step. Os.sleep() swallowed it: it catches InterruptedException, sleeps on to its
            // deadline and never re-asserts the flag, so the stop signal was destroyed and the caller's later
            // isInterrupted() checks read false.
            Path dir = storeDir();
            createStoreDir(dir);
            Path tmp = Files.write(dir.resolve("payload.tmp"), "NEW".getBytes(StandardCharsets.UTF_8));
            Path target = Files.write(dir.resolve("payload.json"), "OLD".getBytes(StandardCharsets.UTF_8));

            Method replaceTarget = FileTokenStore.class.getDeclaredMethod("replaceTarget", Path.class, Path.class);
            replaceTarget.setAccessible(true);
            // Deny the rename the same way the sibling test does, and prove the denial bites on THIS host
            // before resting on it: without a denial the first attempt succeeds, no backoff runs, and the
            // assertion below would pass without exercising anything.
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-x------"));
            try {
                try {
                    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    Assert.fail("the rename must be denied while the store directory is not writable");
                } catch (AccessDeniedException expected) {
                    // as intended - every attempt inside replaceTarget will now be denied, so it reaches its
                    // backoff and stays there for the whole budget
                }

                // Set the flag BEFORE the call rather than racing a second thread into the 20ms window: the
                // first backoff observes it either way, and this leaves nothing to time.
                Thread.currentThread().interrupt();
                try {
                    replaceTarget.invoke(null, tmp, target);
                    Assert.fail("a permanently denied rename must surface its AccessDeniedException");
                } catch (InvocationTargetException e) {
                    Assert.assertTrue("expected the denial to propagate, got " + e.getCause(),
                            e.getCause() instanceof AccessDeniedException);
                }
                Assert.assertTrue("replaceTarget must hand back an interrupt delivered during its backoff, "
                                + "or a stop signal aimed at the save path is lost",
                        Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted(); // do not leak the flag into the next test
                // whatever happened above, leave the tree deletable
                Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
            }
        });
    }

    @Test
    public void testReplaceTargetRetriesADeniedRenameThenSucceeds() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to deny the rename",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        Assume.assumeFalse("a root process bypasses the directory permissions this denial relies on",
                "root".equals(System.getProperty("user.name")));
        assertMemoryLeak(() -> {
            // replaceTarget retries a denied rename because on WINDOWS a concurrent reader holding the target
            // open makes the atomic replace fail transiently with AccessDeniedException. CI is Linux-only, so
            // the denial is produced the one way a POSIX host can: rename(2) needs write permission on the
            // containing directory, so taking it away denies the move exactly as the sharing violation does,
            // and restoring it mid-retry stands in for the Windows reader closing its handle.
            Path dir = storeDir();
            createStoreDir(dir);
            Path tmp = Files.write(dir.resolve("payload.tmp"), "NEW".getBytes(StandardCharsets.UTF_8));
            Path target = Files.write(dir.resolve("payload.json"), "OLD".getBytes(StandardCharsets.UTF_8));

            Method replaceTarget = FileTokenStore.class.getDeclaredMethod("replaceTarget", Path.class, Path.class);
            replaceTarget.setAccessible(true);
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-x------"));
            // Prove the denial is real on THIS host before the test rests on it. Without this the whole test
            // passes vacuously wherever the mode bits do not bite - the first attempt inside replaceTarget
            // simply succeeds and no retry is ever exercised.
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                Assert.fail("the rename must be denied while the store directory is not writable");
            } catch (AccessDeniedException expected) {
                // exactly what a Windows sharing violation produces, and what the retry loop is written for
            }
            Thread reopener = new Thread(() -> {
                // after the first backoff (20ms) but well inside the 5-attempt budget
                Os.sleep(30);
                try {
                    Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
                } catch (IOException e) {
                    throw new AssertionError("could not restore the directory permissions", e);
                }
            }, "denial-clearer");
            reopener.setDaemon(true);
            reopener.start();
            try {
                replaceTarget.invoke(null, tmp, target);
            } finally {
                reopener.join(10_000);
                // whatever happened above, leave the tree deletable
                Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
            }

            Assert.assertEquals("the retry must complete the replace once the denial clears",
                    "NEW", new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
            Assert.assertFalse("an atomic move consumes the temp file", Files.exists(tmp));
        });
    }

    @Test
    public void testSameProcessContendersSerializeAndBothStealStaleLock() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            TokenStoreKey key = sampleKey();
            // a lock abandoned by a crashed holder, backdated well past the staleness window
            Path lock = lockFile(dir, key);
            Files.write(lock, "crashed-holder-stamp".getBytes(StandardCharsets.UTF_8));
            Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis() - 600_000));

            // Two threads of the SAME JVM contend for the one abandoned lock. SCOPE NOTE: the mutual exclusion
            // asserted below (overlaps==0, maxInside==1) is provided by the in-process PROCESS_LOCKS entry,
            // which inLock() takes for key.hash() BEFORE any file-lock logic - so it would hold
            // even if the file-lock steal were broken. What this test genuinely proves is that two same-process
            // contenders each steal the stale lock and run their critical section (ran==2), serialized, without
            // leaving an orphaned capture temp (assertNoCaptureTempFiles). The CROSS-process capture-verify in
            // stealIfStale - that among separate OS PROCESSES exactly one steal wins - is masked by PROCESS_LOCKS
            // here and cannot be exercised in a single JVM; it is verified by inspection, and a two-holder
            // outcome is a documented best-effort residual anyway. The N-way degrade path is
            // testConcurrentStealContentionDegradesCleanly.
            final int threads = 2;
            AtomicInteger inside = new AtomicInteger();
            AtomicInteger maxInside = new AtomicInteger();
            AtomicInteger overlaps = new AtomicInteger();
            AtomicInteger ran = new AtomicInteger();
            TokenStore.CriticalSection section = () -> {
                int now = inside.incrementAndGet();
                maxInside.accumulateAndGet(now, Math::max);
                if (now > 1) {
                    overlaps.incrementAndGet();
                }
                Os.sleep(100);
                inside.decrementAndGet();
                ran.incrementAndGet();
                return true;
            };

            // A contender that THREW would never enter the section, so `inside` never rises and the
            // exclusion assertions below pass on a test that proved nothing. Carry the first failure back.
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            Thread[] ts = new Thread[threads];
            for (int i = 0; i < threads; i++) {
                // a generous acquire budget so a contender waits for the lock rather than degrading to a
                // lock-free run (a degraded action runs without the lock and could legitimately overlap)
                FileTokenStore store = new FileTokenStore(dir, 30_000, 60_000);
                ts[i] = new Thread(() -> {
                    try {
                        store.inLock(key, section);
                    } catch (Throwable t) {
                        workerError.compareAndSet(null, t);
                    }
                }, "same-process-contender-" + i);
            }
            for (Thread t : ts) {
                t.start();
            }
            for (Thread t : ts) {
                joinOrFail(t, "a same-process contender");
            }

            Assert.assertNull("a contender failed instead of running its critical section: " + workerError.get(),
                    workerError.get());
            Assert.assertEquals("every contender must run its critical section", threads, ran.get());
            Assert.assertEquals("same-process contenders must never overlap (PROCESS_LOCKS serializes them)", 0, overlaps.get());
            Assert.assertEquals("at most one holder at a time", 1, maxInside.get());
            assertNoCaptureTempFiles(dir, key);
        });
    }

    @Test
    public void testControlCharactersRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            FileTokenStore store = new FileTokenStore(storeDir());
            TokenStoreKey key = sampleKey();
            // a refresh token carrying every control-escape branch of the JSON writer - the short escapes
            // (\b \f \n \r \t) and the \\u00XX arm - plus a quote and a backslash must round-trip byte for byte;
            // the served-token char check lives in OidcDeviceAuth, so the store itself must preserve these
            String refresh = "R\b\f\n\r\t\"\\Z";
            // also exercise the hex-escape branch: control chars below 0x20 that are not one of the short escapes
            refresh = refresh + (char) 0x01 + (char) 0x1f;
            store.save(key, new PersistedToken("ACCESS-1", null, refresh, 1L, 1000L));

            PersistedToken loaded = store.load(key);
            Assert.assertNotNull(loaded);
            Assert.assertEquals("ACCESS-1", loaded.getAccessToken());
            Assert.assertEquals(refresh, loaded.getRefreshToken());
        });
    }

    @Test
    public void testClearRemovesOrphanedWriteTemps() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));

            // A crash between createTempFile and the atomic rename orphans this: it holds the FULL
            // serialized entry - access, id and refresh tokens in plaintext. save()'s sweep only reclaims
            // temps past the staleness window and only ever runs from save(), so a caller that cleared and
            // never signed in again left a live refresh token on disk indefinitely, contradicting clear()'s
            // "removes any persisted entry for this identity".
            Path orphan = dir.resolve(key.hash() + "9999.tmp");
            Files.write(orphan, "{\"refresh_token\":\"REFRESH-1\"}".getBytes(StandardCharsets.UTF_8));

            store.clear(key);

            Assert.assertFalse("clear must remove the token file", Files.exists(tokenFile(dir, key)));
            Assert.assertFalse("clear must also reclaim an orphaned write temp holding the refresh token",
                    Files.exists(orphan));
        });
    }

    @Test
    public void testCorruptFileReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            createStoreDir(dir);
            Files.write(tokenFile(dir, key), "this is not json {{{".getBytes(StandardCharsets.UTF_8));
            Assert.assertNull(store.load(key));
        });
    }

    @Test
    public void testEmptyAudienceNormalizesToNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            // an empty-string audience is normalised to null: getAudience() reports null, it shares the
            // null-audience identity hash/file, and its save->load round-trips (the pre-fix "" broke its own
            // round-trip because the writer recorded "audience":"" but the fingerprint treated it as absent)
            TokenStoreKey emptyAud = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid", "", false);
            TokenStoreKey nullAud = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid", null, false);
            Assert.assertNull("an empty audience must normalise to null", emptyAud.getAudience());
            Assert.assertEquals("null and empty audiences must share one identity hash", nullAud.hash(), emptyAud.hash());

            store.save(emptyAud, sampleToken("ACCESS-1", "REFRESH-1"));
            PersistedToken loaded = store.load(emptyAud);
            Assert.assertNotNull("an empty-audience key must load the entry it just saved", loaded);
            Assert.assertEquals("ACCESS-1", loaded.getAccessToken());
        });
    }

    @Test
    public void testEmptyFileReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            createStoreDir(dir);
            Files.write(tokenFile(dir, key), new byte[0]);
            Assert.assertNull(store.load(key));
        });
    }

    @Test
    public void testEmptyLockStolenAfterGraceWithinStaleWindow() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            // a holder that crashed between creating its lock and stamping it leaves an empty, unstamped lock.
            // It must be reclaimable on the short empty-lock grace, not held un-stealable until the full
            // staleness window elapses: here the staleness window is large (60s) but the empty lock is backdated
            // only past the grace, so a steal here can only come from the empty-lock-grace path. Without that
            // path the empty lock would not be stale (10s < 60s) and would wedge this acquirer into a lock-free
            // degrade, leaving the lock in place.
            FileTokenStore store = new FileTokenStore(dir, 2000, 60_000);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            Files.createFile(lock); // empty, unstamped
            Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis() - 10_000));

            AtomicBoolean ran = new AtomicBoolean();
            boolean result = store.inLock(key, () -> {
                ran.set(true);
                return true;
            });

            Assert.assertTrue("the action must run", ran.get());
            Assert.assertTrue(result);
            Assert.assertFalse("an empty (unstamped) lock past the grace must be stolen and acquired (then released),"
                    + " not wedge the acquirer for the full staleness window", Files.exists(lock));
        });
    }

    @Test
    public void testEmptyLockGraceIsNotShortenedByASmallStaleWindow() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            // A staleness window far below the 5s empty-lock grace. The grace is the ONLY thing standing
            // between a peer caught between its exclusive create and its stamp and having its live lock
            // stolen, which is why the frozen cross-language contract says a client MUST NOT shorten it.
            // Clamping the grace down to lockStaleMillis did exactly that, silently.
            FileTokenStore store = new FileTokenStore(dir, 300, 100);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            Files.createFile(lock); // empty: a peer momentarily between its create and its stamp
            Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis() - 1_000));

            AtomicBoolean ran = new AtomicBoolean();
            store.inLock(key, () -> {
                ran.set(true);
                return true;
            });

            Assert.assertTrue("inLock must still run, degrading to lock-free", ran.get());
            Assert.assertTrue("a 1s-old empty lock is inside the 5s grace and must not be stolen",
                    Files.exists(lock));
            Assert.assertEquals("the peer's lock must be left exactly as it was", 0, Files.size(lock));
        });
    }

    @Test
    public void testEnsureDirectoryTightensPreExistingDirPerms() throws Exception {
        Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            // a pre-existing, world-accessible store directory (a permissive umask, a prior tool, or a hostile
            // local pre-create) must be tightened to owner-only before a token is written into it
            createStoreDir(dir);
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));

            FileTokenStore store = new FileTokenStore(dir);
            store.save(sampleKey(), sampleToken("ACCESS-1", "REFRESH-1"));

            Assert.assertEquals("a pre-existing directory must be re-restricted to owner-only",
                    PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(dir));
        });
    }

    @Test
    public void testFingerprintMismatchReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            // save under one identity, then read with a key that hashes the same name but differs in the
            // stored fingerprint - simulated by writing the saved bytes under a *different* key's file name
            TokenStoreKey saved = sampleKey();
            store.save(saved, sampleToken("ACCESS-1", "REFRESH-1"));
            byte[] bytes = Files.readAllBytes(tokenFile(dir, saved));
            TokenStoreKey other = new TokenStoreKey("other-client", saved.getTokenEndpoint(),
                    saved.getDeviceAuthorizationEndpoint(), saved.getScope(), null, false);
            Files.write(tokenFile(dir, other), bytes);
            // the file exists under other.hash(), but its in-file fingerprint says client_id=questdb, so the
            // load for `other` must reject it rather than serve questdb's token
            Assert.assertNull(store.load(other));
        });
    }

    @Test
    public void testFrozenSchemaEndpointsCarryAnExplicitPort() throws Exception {
        assertMemoryLeak(() -> {
            // The file NAME hash is pinned by testHashMatchesFrozenCrossLanguageContract; the file BODY was
            // not. The two endpoint fields are part of the fingerprint and are compared with an exact string
            // compare, not a URL compare, so they must carry the canonical rendering - port always explicit -
            // that design/oidc-token-persistence.md specifies. A peer client (the Python one) that writes the
            // default port implicitly produces a file this client silently ignores: load() returns null, the
            // process re-prompts and re-persists in its own encoding, and the two never converge. That is
            // invisible in every other test, because they all round-trip through this client's own writer.
            Path dir = storeDir();
            createStoreDir(dir);
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            String withPort = "{\"v\":1,\"client_id\":\"questdb\","
                    + "\"token_endpoint\":\"https://idp.example.com:443/token\","
                    + "\"device_authorization_endpoint\":\"https://idp.example.com:443/device\","
                    + "\"scope\":\"openid\",\"groups_in_token\":false,"
                    + "\"access_token\":\"ACCESS-1\",\"refresh_token\":\"REFRESH-1\","
                    + "\"expires_at_millis\":1730000000000,\"token_ttl_millis\":300000}";
            Files.write(tokenFile(dir, key), withPort.getBytes(StandardCharsets.UTF_8));
            Assert.assertNotNull("the documented encoding must load", store.load(key));

            // the same document with the default ports omitted - the shape a naive reading of the schema
            // example invites - must NOT load, which is exactly why the spec pins the explicit port
            String withoutPort = withPort
                    .replace("https://idp.example.com:443/token", "https://idp.example.com/token")
                    .replace("https://idp.example.com:443/device", "https://idp.example.com/device");
            Files.write(tokenFile(dir, key), withoutPort.getBytes(StandardCharsets.UTF_8));
            Assert.assertNull("an implicit default port must not match the canonical fingerprint",
                    store.load(key));
        });
    }

    @Test
    public void testHashMatchesFrozenCrossLanguageContract() throws Exception {
        assertMemoryLeak(() -> {
            // the file name is a frozen cross-language contract (the Python client mirrors it byte for byte):
            // lowercase-hex SHA-256 of "questdb-oidc-token-v1" and the six identity fields, NUL-separated, a
            // null audience rendered as "" and groups_in_token as '1'/'0'. Pin it to golden values so a change
            // to the prefix, separator, field order, or null/boolean encoding that would silently stop two
            // clients sharing one file is caught here.
            TokenStoreKey withAudience = new TokenStoreKey("questdb",
                    "https://idp.example.com:443/as/token", "https://idp.example.com:443/as/device",
                    "openid", "api://billing", false);
            Assert.assertEquals("eee1a742a27499d176bcdaed8635c14a3edbdef1d68b61c05c3c2158a5bfbcca", withAudience.hash());

            // a null audience hashes as an empty field, not the literal "null"
            TokenStoreKey nullAudience = new TokenStoreKey("questdb",
                    "https://idp.example.com:443/as/token", "https://idp.example.com:443/as/device",
                    "openid", null, false);
            Assert.assertEquals("1dca0e8192ae529b94c1ac5493f09f8a45e641e4e0ec316333c0cbfeeccfef0e", nullAudience.hash());

            // groups_in_token participates in the identity, so it flips the hash to a different file
            TokenStoreKey groups = new TokenStoreKey("questdb",
                    "https://idp.example.com:443/as/token", "https://idp.example.com:443/as/device",
                    "openid", "api://billing", true);
            Assert.assertEquals("5193f668130b28cd9430f5271011f1044b3b1c1e78bfc4f45d7688a3d9b1ceb0", groups.hash());
            Assert.assertNotEquals(withAudience.hash(), groups.hash());
        });
    }

    @Test
    public void testKeyIsUsableAsAMapKey() throws Exception {
        assertMemoryLeak(() -> {
            // TokenStore's contract says entries are keyed by TokenStoreKey, and its javadoc invites a
            // custom store backed by a keychain or a vault. Without value equality that reads as an
            // invitation to a Map that never hits: OidcDeviceAuth builds its key once per instance, so a
            // Map-backed store looks correct until a second instance - or a restart - rebuilds an equal key,
            // misses, and sends the user back through the device flow on every refresh. The bundled
            // FileTokenStore is unaffected only because it keys by hash() for the file name.
            TokenStoreKey a = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid groups", "api://billing", true);
            TokenStoreKey sameIdentity = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid groups", "api://billing", true);
            TokenStoreKey otherClient = new TokenStoreKey("other", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid groups", "api://billing", true);

            Assert.assertEquals("two keys naming one identity must be equal", a, sameIdentity);
            Assert.assertEquals("equal keys must share a hashCode", a.hashCode(), sameIdentity.hashCode());
            Assert.assertNotEquals("a different client id is a different identity", a, otherClient);
            Assert.assertNotEquals(a, null);
            Assert.assertNotEquals(a, "not a key");

            Map<TokenStoreKey, String> byKey = new HashMap<>();
            byKey.put(a, "entry");
            Assert.assertEquals("a rebuilt key must find the entry the original stored", "entry",
                    byKey.get(sameIdentity));
            Assert.assertNull("a different identity must not read another's entry", byKey.get(otherClient));
            byKey.put(sameIdentity, "replaced");
            Assert.assertEquals("an equal key must replace, not duplicate", 1, byKey.size());

            // equality means "the same store entry", so it follows the constructor's null/empty audience
            // normalisation rather than the raw arguments - the two below share one file, and now one
            // Map slot too
            TokenStoreKey emptyAud = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid", "", false);
            TokenStoreKey nullAud = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid", null, false);
            Assert.assertEquals("keys addressing one entry must be equal", emptyAud, nullAud);
            Assert.assertEquals(emptyAud.hashCode(), nullAud.hashCode());
        });
    }

    @Test
    public void testInLockAbandonsFileLockWaitOnInterrupt() throws Exception {
        assertMemoryLeak(() -> {
            // The lock-file poll used Os.sleep, which catches InterruptedException, keeps sleeping to its own
            // deadline and never re-asserts the flag - so a cancellation aimed at this wait was swallowed and
            // the whole budget elapsed regardless. The budget maxes out at 30s, the same as QWP's close()
            // shutdown budget, so a caller stuck here made close() time out and delegate the teardown of the
            // native client, the cursor engine and the store-and-forward slot lock.
            Path dir = storeDir();
            createStoreDir(dir);
            FileTokenStore store = new FileTokenStore(dir, 30_000, 600_000);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            // a live peer's stamped lock: not empty, so the empty-lock grace does not apply, and far inside
            // the staleness window, so it is never stolen - the waiter can only poll
            Files.write(lock, "live-peer-nonce".getBytes(StandardCharsets.UTF_8));

            AtomicBoolean ran = new AtomicBoolean();
            AtomicReference<Boolean> result = new AtomicReference<>();
            AtomicReference<Throwable> waiterError = new AtomicReference<>();
            AtomicBoolean flagLeftSet = new AtomicBoolean();
            Thread waiter = new Thread(() -> {
                try {
                    result.set(store.inLock(key, () -> {
                        ran.set(true);
                        return true;
                    }));
                    flagLeftSet.set(Thread.currentThread().isInterrupted());
                } catch (Throwable t) {
                    // without this the throw dies on this thread and the assertions below read it as
                    // "result was never set" - a null-vs-FALSE mismatch that names nothing
                    waiterError.compareAndSet(null, t);
                }
            }, "file-lock-waiter");
            waiter.setUncaughtExceptionHandler((t, e) -> waiterError.compareAndSet(null, e));
            waiter.setDaemon(true);
            waiter.start();
            // Read off the waiter's own stack that it is INSIDE the poll before interrupting it. The latch
            // this replaced counted down at the top of the thread body, so it proved only that the thread had
            // been scheduled: an interrupt landing before the call becomes a CARRIED flag, which inLock
            // answers by returning false without ever entering the wait, and every assertion below would then
            // pass on a path this test does not mean to exercise.
            awaitInside(waiter, "acquireLock");

            long start = System.currentTimeMillis();
            waiter.interrupt();
            waiter.join(10_000);
            long elapsed = System.currentTimeMillis() - start;

            Assert.assertNull("the waiter failed instead of abandoning its wait: " + waiterError.get(),
                    waiterError.get());
            Assert.assertFalse("the waiter must not still be polling out the 30s budget", waiter.isAlive());
            Assert.assertTrue("the interrupt must cut the poll short, took " + elapsed + "ms", elapsed < 5_000);
            Assert.assertFalse("the refresh must not start once the wait was cancelled", ran.get());
            Assert.assertEquals("an abandoned wait reports no refresh", Boolean.FALSE, result.get());
            // The false above is not self-describing: a refresh that RAN and failed returns the same value.
            // Only the restored flag separates them, and OidcDeviceAuth acts on the difference - a bare
            // false sends signIn() into the interactive device flow (a browser, then a poll loop on Os.sleep
            // that ignores interrupts) on a thread its owner just cancelled, and makes getToken() arm the
            // instance-wide refresh back-off over a credential that is fine. This assertion used to require
            // the opposite, on the reasoning that consuming the signal was "acting on it"; consuming it is
            // what made the two cases indistinguishable.
            Assert.assertTrue("inLock must leave the interrupt flag set when a cancellation abandoned its "
                    + "wait, or the caller cannot tell that apart from a failed refresh", flagLeftSet.get());
            Assert.assertTrue("the peer's live lock must be left alone", Files.exists(lock));
        });
    }

    @Test
    public void testInLockAbandonsProcessLockWaitOnInterrupt() throws Exception {
        assertMemoryLeak(() -> {
            // The in-process lock that serializes same-identity threads was taken with lock(), which no
            // interrupt can break. A peer thread holds it for a whole refresh round trip, so a caller behind
            // it was unreachable by the one lever QWP's ConnectCancellation has.
            Path dir = storeDir();
            createStoreDir(dir);
            FileTokenStore holderStore = new FileTokenStore(dir, 30_000, 600_000);
            FileTokenStore waiterStore = new FileTokenStore(dir, 30_000, 600_000);
            TokenStoreKey key = sampleKey();

            CountDownLatch holding = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Thread holder = new Thread(() -> holderStore.inLock(key, () -> {
                holding.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }), "process-lock-holder");
            holder.setDaemon(true);
            holder.start();
            Assert.assertTrue("the holder must enter its critical section", holding.await(5, TimeUnit.SECONDS));

            AtomicBoolean ran = new AtomicBoolean();
            AtomicReference<Boolean> result = new AtomicReference<>();
            AtomicBoolean flagAfterReturn = new AtomicBoolean();
            AtomicReference<Throwable> waiterError = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                try {
                    result.set(waiterStore.inLock(key, () -> {
                        ran.set(true);
                        return true;
                    }));
                    // sampled INSIDE the thread and immediately after the return, because that is the
                    // instant OidcDeviceAuth inspects it to tell "the wait was cancelled" from "the
                    // refresh ran and failed"
                    flagAfterReturn.set(Thread.currentThread().isInterrupted());
                } catch (Throwable t) {
                    // see the sibling test: a throw here must arrive as itself, not as a missing result
                    waiterError.compareAndSet(null, t);
                }
            }, "process-lock-waiter");
            waiter.setUncaughtExceptionHandler((t, e) -> waiterError.compareAndSet(null, e));
            waiter.setDaemon(true);
            waiter.start();
            // inside inLock is the right point here: it is where the process lock is taken, and inLock's
            // carried-interrupt check has already run by then, so the interrupt below is unambiguously the
            // LIVE cancellation this test is about. See the sibling test for what the latch could not prove.
            awaitInside(waiter, "inLock");

            long start = System.currentTimeMillis();
            waiter.interrupt();
            waiter.join(10_000);
            long elapsed = System.currentTimeMillis() - start;

            Assert.assertNull("the waiter failed instead of abandoning its wait: " + waiterError.get(),
                    waiterError.get());
            Assert.assertFalse("the waiter must not still be blocked on the process lock", waiter.isAlive());
            Assert.assertTrue("the interrupt must break the process-lock wait, took " + elapsed + "ms",
                    elapsed < 5_000);
            Assert.assertFalse("the refresh must not start once the wait was cancelled", ran.get());
            Assert.assertEquals("an abandoned wait reports no refresh", Boolean.FALSE, result.get());
            // Same contract as the file-lock sibling: false alone cannot be told from a failed refresh, and
            // OidcDeviceAuth answers a failed refresh with the interactive device flow.
            Assert.assertTrue("inLock must leave the interrupt flag set when a cancellation abandoned its "
                    + "wait, or the caller cannot tell that apart from a failed refresh",
                    flagAfterReturn.get());

            release.countDown();
            holder.join(10_000);
            Assert.assertFalse("the holder must finish its critical section", holder.isAlive());
        });
    }

    @Test
    public void testInLockHonoursItsAcquireBudgetBehindALivePeerLock() throws Exception {
        assertMemoryLeak(() -> {
            // The budget is a PROMISE to the caller: inLock waits at most lockAcquireBudgetMillis for a peer's
            // lock and then runs the critical section lock-free, because getToken() reaches this on an ILP
            // producer's flush path. Anything blocking added inside the acquire breaks that promise silently -
            // as ManagementFactory.getRuntimeMXBean().getName() did in the owner stamp, resolving the local
            // hostname (InetAddress.getLocalHost()) for 3.2s inside a 200ms budget, once per JVM, on the first
            // credential refresh. That one was caught end-to-end by
            // OidcDeviceAuthPersistenceTest.testGetTokenDegradesWhenStoreLockHeld; this pins the same bound
            // directly on the store, where such a call would live.
            Path dir = storeDir();
            createStoreDir(dir);
            FileTokenStore store = new FileTokenStore(dir, 200, 600_000);
            TokenStoreKey key = sampleKey();
            // a live peer's stamped lock: neither the empty-lock grace nor the staleness steal applies, so the
            // acquire can only poll it out and degrade
            Files.write(lockFile(dir, key), "live-peer-nonce".getBytes(StandardCharsets.UTF_8));

            AtomicBoolean ran = new AtomicBoolean();
            long start = System.nanoTime();
            boolean result = store.inLock(key, () -> {
                ran.set(true);
                return true;
            });
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;

            Assert.assertTrue("a peer's lock must not stop the critical section, only unserialize it", ran.get());
            Assert.assertTrue(result);
            Assert.assertTrue("the whole budget must be spent polling, was " + elapsedMillis + "ms",
                    elapsedMillis >= 200);
            // Generous, because this is a wall-clock bound on a shared machine: it must ride out a GC pause or
            // a scheduling hiccup, while still failing on the kind of multi-second blocking call it exists to
            // keep out of the acquire.
            Assert.assertTrue("the acquire must degrade on its budget, not stall, was " + elapsedMillis + "ms",
                    elapsedMillis < 2_000);
            Assert.assertTrue("the peer's live lock must be left alone", Files.exists(lockFile(dir, key)));
        });
    }

    @Test
    public void testInLockPreservesACarriedInterruptFlag() throws Exception {
        assertMemoryLeak(() -> {
            FileTokenStore store = new FileTokenStore(storeDir());
            AtomicBoolean ran = new AtomicBoolean();

            // The root cause behind clear() losing a credential, pinned on its own. The lock is FREE and
            // uncontended here, so nothing is being waited on: a carried flag is the caller's own state and
            // must survive. ReentrantLock.lockInterruptibly() begins with Thread.interrupted(), so testing
            // the flag only after the acquire read it as a live cancellation and consumed it - which also
            // made getToken() report "could not be refreshed" on a reachable endpoint while destroying the
            // caller's signal.
            Thread.currentThread().interrupt();
            boolean result;
            boolean flagSurvived;
            try {
                result = store.inLock(sampleKey(), () -> {
                    ran.set(true);
                    return true;
                });
                flagSurvived = Thread.currentThread().isInterrupted();
            } finally {
                Thread.interrupted(); // do not leak the flag into the next test
            }

            Assert.assertTrue("a carried interrupt must survive inLock", flagSurvived);
            Assert.assertFalse("inLock must not start the critical section for a cancelled caller", ran.get());
            Assert.assertFalse("and must report that the action did not run", result);
        });
    }

    @Test
    public void testInLockDegradesWhenDirectoryUnusable() throws Exception {
        assertMemoryLeak(() -> {
            // a regular file standing where the store directory's parent must be makes ensureDirectory throw
            // IOException; inLock must still run the action lock-free rather than fail a sign-in
            Path blocker = temp.getRoot().toPath().resolve("blocker");
            Files.write(blocker, new byte[]{1});
            FileTokenStore store = new FileTokenStore(blocker.resolve("oidc-tokens"));

            AtomicBoolean ran = new AtomicBoolean();
            boolean result = store.inLock(sampleKey(), () -> {
                ran.set(true);
                return true;
            });

            Assert.assertTrue("must run the action even when the directory cannot be created (degrade)", ran.get());
            Assert.assertTrue(result);
        });
    }

    @Test
    public void testInLockDegradesWhenHeldByFreshLock() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            // small acquire budget, large staleness: a fresh foreign lock cannot be acquired or stolen
            FileTokenStore store = new FileTokenStore(dir, 200, 60_000);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            Files.createFile(lock); // a live holder's fresh lock

            AtomicBoolean ran = new AtomicBoolean();
            long start = System.currentTimeMillis();
            boolean result = store.inLock(key, () -> {
                ran.set(true);
                return true;
            });
            long elapsed = System.currentTimeMillis() - start;

            Assert.assertTrue("must run the action even when it cannot lock (degrade)", ran.get());
            Assert.assertTrue(result);
            Assert.assertTrue("must not steal a fresh foreign lock", Files.exists(lock));
            Assert.assertTrue("must wait out the acquire budget before degrading, was " + elapsed, elapsed >= 150);
        });
    }

    @Test
    public void testInLockIsMutuallyExclusiveAcrossInstances() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            TokenStoreKey key = sampleKey();
            // two instances over one directory model two concurrent users of one identity; a generous acquire
            // budget makes a contender wait rather than degrade, and a large staleness window stops either from
            // stealing the other's live lock - so the two critical sections must run strictly one at a time. In a
            // single JVM the in-process lock (keyed on the identity) is what serializes them; it stands in for the
            // cross-process file lock that only genuinely separate processes would exercise.
            FileTokenStore storeA = new FileTokenStore(dir, 10_000, 600_000);
            FileTokenStore storeB = new FileTokenStore(dir, 10_000, 600_000);

            AtomicInteger inside = new AtomicInteger();
            AtomicInteger maxInside = new AtomicInteger();
            AtomicInteger overlaps = new AtomicInteger();
            AtomicInteger ran = new AtomicInteger();
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            TokenStore.CriticalSection section = () -> {
                int now = inside.incrementAndGet();
                maxInside.accumulateAndGet(now, Math::max);
                if (now > 1) {
                    overlaps.incrementAndGet();
                }
                Os.sleep(200);
                inside.decrementAndGet();
                ran.incrementAndGet();
                return true;
            };

            // a barrier forces the two threads to genuinely contend, rather than one running and finishing before
            // the other starts (which would satisfy the overlap check without ever exercising mutual exclusion)
            CyclicBarrier barrier = new CyclicBarrier(2);
            Thread tA = new Thread(() -> {
                try {
                    barrier.await();
                    storeA.inLock(key, section);
                } catch (Throwable t) {
                    workerError.compareAndSet(null, t);
                }
            });
            Thread tB = new Thread(() -> {
                try {
                    barrier.await();
                    storeB.inLock(key, section);
                } catch (Throwable t) {
                    workerError.compareAndSet(null, t);
                }
            });
            tA.start();
            tB.start();
            joinOrFail(tA, "contender A");
            joinOrFail(tB, "contender B");

            // capture a worker throwable on the main thread: without this, a contender that THREW instead of
            // waiting would die silently and leave the other holder looking (falsely) like correct exclusion
            Assert.assertNull("a contender thread failed instead of running its critical section", workerError.get());
            Assert.assertEquals("both critical sections must have run", 2, ran.get());
            Assert.assertEquals("the two critical sections must never overlap", 0, overlaps.get());
            Assert.assertEquals("at most one holder at a time", 1, maxInside.get());
        });
    }

    @Test
    public void testInLockReleaseDoesNotDeleteAStolenLock() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            // a tiny staleness window so our own in-progress hold is judged stale and a peer can steal it
            FileTokenStore store = new FileTokenStore(dir, 1000, 50);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);

            // our critical section outlives the 50ms staleness window; while we are still inside it, a peer
            // process judges our lock stale, steals it (deletes and recreates) and writes its own owner stamp.
            // releaseLock must verify ownership and leave the peer's live lock intact, not delete it by bare
            // path - otherwise a third acquirer could enter alongside the peer, defeating mutual exclusion.
            store.inLock(key, () -> {
                Os.sleep(120);
                try {
                    Files.deleteIfExists(lock);
                    Files.write(lock, "peer-owner-stamp".getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });

            Assert.assertTrue("releaseLock must not delete a lock a peer has stolen", Files.exists(lock));
            Assert.assertEquals("the peer's lock content must survive our release",
                    "peer-owner-stamp", new String(Files.readAllBytes(lock), StandardCharsets.UTF_8));
        });
    }

    @Test
    public void testInLockReleasesLockWhenActionThrows() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);

            RuntimeException boom = new RuntimeException("action failed");
            try {
                store.inLock(key, () -> {
                    Assert.assertTrue("the lock must be held while the action runs", Files.exists(lock));
                    throw boom;
                });
                Assert.fail("the action's exception must propagate out of inLock");
            } catch (RuntimeException e) {
                Assert.assertSame(boom, e);
            }
            Assert.assertFalse("inLock must release the lock even when the action throws", Files.exists(lock));
        });
    }

    @Test
    public void testInLockReleasesLockWhenSectionLeavesThreadInterrupted() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);

            // The critical section is a token refresh, and close() breaks a drainer stuck in one by
            // interrupting its thread - so inLock's release routinely runs with the flag already set.
            // releaseLock reads the lock's owner stamp through a FileChannel, an InterruptibleChannel:
            // with the flag set that read throws ClosedByInterruptException, which releaseLock swallows,
            // so the lock file survives its whole staleness window (10 minutes by default) while every
            // peer degrades to an unserialized refresh - the rotating-refresh-token race the lock exists
            // to prevent. Release must therefore be interrupt-neutral.
            boolean released = store.inLock(key, () -> {
                Assert.assertTrue("the lock must be held while the action runs", Files.exists(lock));
                Thread.currentThread().interrupt();
                return true;
            });

            Assert.assertTrue(released);
            try {
                Assert.assertFalse("inLock must release the lock even when the section leaves the thread "
                        + "interrupted", Files.exists(lock));
                Assert.assertTrue("the caller's interrupt must be preserved, not consumed",
                        Thread.currentThread().isInterrupted());
            } finally {
                // never leak the flag into the rest of the suite
                Thread.interrupted();
            }
        });
    }

    @Test
    public void testInLockRunsActionAndManagesLockFile() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);

            AtomicBoolean ran = new AtomicBoolean();
            boolean result = store.inLock(key, () -> {
                ran.set(true);
                Assert.assertTrue("lock file must exist while the action runs", Files.exists(lock));
                return true;
            });

            Assert.assertTrue(ran.get());
            Assert.assertTrue(result);
            Assert.assertFalse("lock file must be released after the action", Files.exists(lock));
        });
    }

    @Test
    public void testInLockStealsStaleLock() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            // staleness threshold 100ms; the pre-created lock is backdated well past it
            FileTokenStore store = new FileTokenStore(dir, 2000, 100);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            Files.createFile(lock);
            Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis() - 10_000));

            AtomicBoolean ran = new AtomicBoolean();
            boolean result = store.inLock(key, () -> {
                ran.set(true);
                return true;
            });

            Assert.assertTrue("must steal the stale lock and run", ran.get());
            Assert.assertTrue(result);
            Assert.assertFalse("having acquired the stolen lock, it must be released", Files.exists(lock));
        });
    }

    @Test
    public void testLiteralNullStringTokenRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            FileTokenStore store = new FileTokenStore(storeDir());
            TokenStoreKey key = sampleKey();
            // a token whose value is exactly the 4 characters "null" must survive the round trip: the writer
            // omits absent fields rather than emitting a JSON null, so a present "null" is unambiguous on read
            PersistedToken saved = new PersistedToken("null", null, "null", 1_730_000_000_000L, 300_000L);
            store.save(key, saved);

            PersistedToken loaded = store.load(key);
            Assert.assertNotNull(loaded);
            Assert.assertEquals("null", loaded.getAccessToken());
            Assert.assertNull("an absent id token must stay null, not become the string \"null\"", loaded.getIdToken());
            Assert.assertEquals("null", loaded.getRefreshToken());
        });
    }

    @Test
    public void testLoadAndSaveSurviveACarriedInterruptFlag() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();

            // Every file operation here goes through FileChannel, an InterruptibleChannel: a thread that
            // merely CARRIES a set interrupt flag makes the first read or write throw
            // ClosedByInterruptException, and the flag survives. Callers arrive that way routinely - an ILP
            // producer on a pooled thread where interrupt is the cancellation signal, and the sender's own
            // I/O thread, which close() interrupts to break a stuck credential pull. Neither means "abandon
            // the token store", so the store must clear the flag around its own I/O and restore it after.
            Thread.currentThread().interrupt();
            try {
                store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
                Assert.assertTrue("save must complete with the flag set", Thread.currentThread().isInterrupted());

                PersistedToken loaded = store.load(key);
                Assert.assertNotNull("load must complete with the flag set, not throw on the channel", loaded);
                Assert.assertEquals("ACCESS-1", loaded.getAccessToken());
                Assert.assertEquals("REFRESH-1", loaded.getRefreshToken());
                Assert.assertTrue("the caller's interrupt must be preserved, not consumed",
                        Thread.currentThread().isInterrupted());
            } finally {
                // never leak the flag into the rest of the suite
                Thread.interrupted();
            }
        });
    }

    @Test
    public void testLoadMissingReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            FileTokenStore store = new FileTokenStore(storeDir());
            Assert.assertNull(store.load(sampleKey()));
        });
    }

    @Test
    public void testLoadDiscardsOnlyTheStoresOwnFilesFromAWorldWritableDirectory() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            // Discarding every ENTRY in an untrusted directory is right - the sibling test pins it. What the
            // discard must not do is decide "entry" means "any .json", because the directory it is emptying
            // is one the operator chose and may share. questdb.client.oidc.token.store.dir pointed at an
            // existing config directory that happens to be group-writable is enough: one getToken() then
            // deletes files the client never wrote.
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));

            // Files a stranger owns, chosen to sit either side of the shape test: a plain name, a name that
            // is hex but too short to be a fingerprint, a full-length hex name that is not a fingerprint of
            // ANY key, and a foreign temp. (No uppercase-hex case: the store renders its digests lowercase,
            // but on a case-insensitive filesystem such a name is the same file as the real entry, so the
            // assertion would be about the filesystem rather than about the filter.)
            Path plainJson = dir.resolve("my-important-settings.json");
            Path shortHexJson = dir.resolve("abc123.json");
            Path foreignTemp = dir.resolve("scratch-notes.tmp");
            Files.write(plainJson, "{\"keep\":true}".getBytes(StandardCharsets.UTF_8));
            Files.write(shortHexJson, "{\"keep\":true}".getBytes(StandardCharsets.UTF_8));
            Files.write(foreignTemp, "keep".getBytes(StandardCharsets.UTF_8));

            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));

            Assert.assertNull("an entry from a directory other local users could write must not be adopted",
                    store.load(key));
            Assert.assertFalse("the store's own entry is still discarded - that half is unchanged",
                    Files.exists(tokenFile(dir, key)));

            Assert.assertTrue("a file the store never wrote must survive: " + plainJson.getFileName(),
                    Files.exists(plainJson));
            Assert.assertTrue("a short hex name is not a 64-char fingerprint: " + shortHexJson.getFileName(),
                    Files.exists(shortHexJson));
            Assert.assertTrue("a foreign .tmp is not a store write temp: " + foreignTemp.getFileName(),
                    Files.exists(foreignTemp));
        });
    }

    @Test
    public void testLoadRejectsAndDiscardsAnEntryFromAWorldWritableDirectory() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Assert.assertNotNull("baseline: an entry written into an owner-only directory is trusted",
                    store.load(key));

            // adopt() already rejects an entry carrying ONLY a refresh token, but a COMPLETE plant - a dummy
            // access token, the attacker's refresh token, an expiry already in the past - takes the normal
            // path and the next silent refresh presents their credential. Closing that needs the container
            // checked too: an entry sitting in a directory other local users can WRITE was never ours to
            // trust, whatever it contains.
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));

            Assert.assertNull("an entry from a directory other local users could write must not be adopted",
                    store.load(key));
            Assert.assertFalse("the untrusted entry must be discarded, not left for the next load to adopt",
                    Files.exists(tokenFile(dir, key)));
            Assert.assertEquals("load() must tighten the store directory, as the write paths already do",
                    PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(dir));
            Assert.assertNull(store.load(key));

            // the store stays usable: a fresh sign-in persists and loads normally over the tightened directory
            store.save(key, sampleToken("ACCESS-2", "REFRESH-2"));
            PersistedToken reloaded = store.load(key);
            Assert.assertNotNull(reloaded);
            Assert.assertEquals("REFRESH-2", reloaded.getRefreshToken());
        });
    }

    @Test
    public void testWorldWritableVerdictIsNotConsumedByWhicheverIdentityLoadsFirst() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            // restrictToOwner reads the permissions, chmods to 0700, and returns the verdict it computed
            // BEFORE the chmod - so the verdict is destroyed by the act of reporting it. One store directory
            // holds one file per configuration, and identity A's load tightens the directory for everybody:
            // by the time identity B loads, the directory is 0700, B's verdict is "trusted", and B adopts
            // whatever <hashB>.json happens to be sitting there. A discarded its own entry and left B's.
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey a = sampleKey();
            TokenStoreKey b = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid profile", null, false);
            Assert.assertNotEquals("the two identities must address different files", a.hash(), b.hash());

            store.save(a, sampleToken("ACCESS-A", "REFRESH-A"));
            store.save(b, sampleToken("ACCESS-B", "REFRESH-B"));

            // the window: while this stands, any local user can replace either entry
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));
            byte[] planted = Files.readAllBytes(tokenFile(dir, b));
            Files.write(tokenFile(dir, b),
                    new String(planted, StandardCharsets.UTF_8)
                            .replace("REFRESH-B", "REFRESH-PLANTED")
                            .getBytes(StandardCharsets.UTF_8));

            // A loads first and correctly refuses - and tightens the directory on the way through
            Assert.assertNull("A must refuse an entry from a world-writable directory", store.load(a));
            Assert.assertEquals("A's load tightens the directory for every later caller",
                    PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(dir));

            // B now loads over a directory that LOOKS owner-only, because A made it so
            Assert.assertNull("B must not adopt an entry that was exposed in the same window, merely "
                    + "because A's load already spent the directory's untrusted verdict", store.load(b));
            Assert.assertFalse("and the exposed entry must be discarded, not left for the next load",
                    Files.exists(tokenFile(dir, b)));

            // the store stays usable for both identities over the tightened directory
            store.save(a, sampleToken("ACCESS-A2", "REFRESH-A2"));
            store.save(b, sampleToken("ACCESS-B2", "REFRESH-B2"));
            Assert.assertEquals("REFRESH-A2", store.load(a).getRefreshToken());
            Assert.assertEquals("REFRESH-B2", store.load(b).getRefreshToken());
        });
    }

    @Test
    public void testWorldWritableVerdictSurvivesASaveTouchingTheDirectoryFirst() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            // The same verdict, spent by a WRITE path instead. save() and inLock() call ensureDirectory too
            // and discarded its boolean outright, so a save arriving before any load tightened the directory
            // and left every planted entry in it looking like it had always been protected.
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey a = sampleKey();
            TokenStoreKey b = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid profile", null, false);
            store.save(b, sampleToken("ACCESS-B", "REFRESH-B"));

            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));
            byte[] planted = Files.readAllBytes(tokenFile(dir, b));
            Files.write(tokenFile(dir, b),
                    new String(planted, StandardCharsets.UTF_8)
                            .replace("REFRESH-B", "REFRESH-PLANTED")
                            .getBytes(StandardCharsets.UTF_8));

            // a save for an unrelated identity is the first thing to touch the directory
            store.save(a, sampleToken("ACCESS-A", "REFRESH-A"));
            Assert.assertEquals("the save tightens the directory, as it always did",
                    PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(dir));

            Assert.assertNull("an entry exposed before that save must not be adopted afterwards",
                    store.load(b));
        });
    }

    @Test
    public void testAnUntrustedSentinelKeepsAnOwnerOnlyDirectoryDistrusted() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            // The invariant the concurrent fix rests on, pinned directly: while the .untrusted sentinel is
            // present, the directory is distrusted whatever its permission bits say. It reconstructs, without
            // threads, the state a second caller observes mid-race - a peer detected the world-writable
            // directory, dropped the sentinel and chmodded to 0700, but has not yet swept - an owner-only
            // directory that still holds a valid-looking entry AND the sentinel. A loader that trusts on the
            // bits alone adopts the entry; it must not.
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Assert.assertNotNull("baseline: an entry in an owner-only directory is trusted", store.load(key));
            Assert.assertEquals("baseline: the directory is owner-only", OWNER_ONLY_DIR_PERMS,
                    Files.getPosixFilePermissions(dir));

            // mark untrusted while leaving the permissions owner-only: the "tightened but not yet swept" state
            Path sentinel = dir.resolve(".untrusted");
            Files.write(sentinel, new byte[0]);

            Assert.assertNull("a marked directory must be distrusted even while it looks owner-only",
                    store.load(key));
            Assert.assertFalse("the entry under a marked directory must be discarded",
                    Files.exists(tokenFile(dir, key)));
            Assert.assertFalse("a complete sweep must clear the sentinel", Files.exists(sentinel));

            // recovery: with the sentinel gone the store trusts the owner-only directory again
            store.save(key, sampleToken("ACCESS-2", "REFRESH-2"));
            PersistedToken reloaded = store.load(key);
            Assert.assertNotNull(reloaded);
            Assert.assertEquals("REFRESH-2", reloaded.getRefreshToken());
        });
    }

    @Test
    public void testConcurrentLoadDistrustsTheDirectoryTightenedButNotYetSwept() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            // The concurrent case the sibling sequential verdict tests cannot reach. restrictToOwner tightens
            // the directory to 0700 and returns the verdict in one breath, but the planted entries are not
            // swept until discardUntrustedDirectoryContents runs afterwards. A SECOND caller - another thread
            // or process - that reads the permissions in the gap between the chmod and the sweep sees an
            // owner-only directory with the plant still in it and, on the permission bits alone, would trust
            // it. The .untrusted sentinel dropped before the chmod is what it must distrust through instead.
            //
            // beforeUntrustedDiscardHook drops us into exactly that gap: it fires after the tighten-and-mark,
            // before the sweep. From inside it a fresh store (no hook) loads the OTHER identity over the
            // tightened directory - the second caller - and must refuse the plant. No real concurrency can
            // force a peer into this sub-syscall gap deterministically, which is why the seam exists (as
            // beforeCaptureHook does for stealIfStale).
            Path dir = storeDir();
            FileTokenStore first = new FileTokenStore(dir);
            FileTokenStore second = new FileTokenStore(dir);
            TokenStoreKey a = sampleKey();
            TokenStoreKey b = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid profile", null, false);
            Assert.assertNotEquals("the two identities must address different files", a.hash(), b.hash());

            first.save(a, sampleToken("ACCESS-A", "REFRESH-A"));
            first.save(b, sampleToken("ACCESS-B", "REFRESH-B"));

            // the window: while this stands, any local user can replace either entry
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));
            byte[] planted = Files.readAllBytes(tokenFile(dir, b));
            Files.write(tokenFile(dir, b),
                    new String(planted, StandardCharsets.UTF_8)
                            .replace("REFRESH-B", "REFRESH-PLANTED")
                            .getBytes(StandardCharsets.UTF_8));

            AtomicReference<PersistedToken> secondSaw = new AtomicReference<>();
            AtomicReference<Set<PosixFilePermission>> permsInGap = new AtomicReference<>();
            AtomicBoolean plantPresentInGap = new AtomicBoolean();
            Field hookField = FileTokenStore.class.getDeclaredField("beforeUntrustedDiscardHook");
            hookField.setAccessible(true);
            hookField.set(first, (Runnable) () -> {
                try {
                    permsInGap.set(Files.getPosixFilePermissions(dir));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                plantPresentInGap.set(Files.exists(tokenFile(dir, b)));
                secondSaw.set(second.load(b));
            });

            // A refuses its own identity AND, through the hook, drives the concurrent B into the gap
            Assert.assertNull("A must refuse an entry from a world-writable directory", first.load(a));

            Assert.assertEquals("the directory was already tightened to 0700 when B observed it",
                    OWNER_ONLY_DIR_PERMS, permsInGap.get());
            Assert.assertTrue("the plant was still present when B observed it - the sweep had not run yet",
                    plantPresentInGap.get());
            Assert.assertNull("B must NOT adopt the plant it saw over the tightened directory: the .untrusted "
                    + "sentinel dropped before the chmod carries the distrust the chmod would otherwise erase",
                    secondSaw.get());
            Assert.assertFalse("the plant must be discarded, not left for the next load",
                    Files.exists(tokenFile(dir, b)));

            // the store recovers over the tightened directory: the sentinel is gone and both identities work
            first.save(a, sampleToken("ACCESS-A2", "REFRESH-A2"));
            first.save(b, sampleToken("ACCESS-B2", "REFRESH-B2"));
            Assert.assertEquals("REFRESH-A2", first.load(a).getRefreshToken());
            Assert.assertEquals("REFRESH-B2", first.load(b).getRefreshToken());
        });
    }

    @Test
    public void testADanglingSymlinkAtTheSentinelNameKeepsTheDirectoryDistrusted() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            // The sibling test plants the sentinel as a REGULAR FILE, which is the only shape this store
            // writes - and so the only shape it ever proved the distrust through. The party the sentinel
            // defends against is the one who can write this directory, and they choose the shape. A DANGLING
            // symlink at the name reports absent to any link-following test, so a verdict built on
            // Files.exists reads "no sentinel" and trusts the directory on its permission bits alone - the
            // mechanism disabled outright, with no race to win and nothing in any log. The exclusive-create
            // mark cannot displace it either: O_CREAT|O_EXCL answers EEXIST for a symlink exactly as it does
            // for a peer's mark, so markUntrusted reads the squatter as "already marked" and returns happy.
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-PLANT"));
            Assert.assertNotNull("baseline: an entry in an owner-only directory is trusted", store.load(key));

            Path sentinel = dir.resolve(".untrusted");
            Files.createSymbolicLink(sentinel, dir.resolve("no-such-target"));
            Assert.assertTrue("the fixture must be a symlink", Files.isSymbolicLink(sentinel));
            Assert.assertFalse("the fixture must DANGLE - that is what a link-following test misreads",
                    Files.exists(sentinel));

            Assert.assertNull("a symlink standing at the sentinel's name must distrust the directory just as "
                            + "a regular file does; a link-following presence test hands an attacker who can "
                            + "write this directory a way to switch the sentinel off",
                    store.load(key));
            Assert.assertFalse("the entry under a distrusted directory must be discarded",
                    Files.exists(tokenFile(dir, key)));
        });
    }

    @Test
    public void testMarkUntrustedDisplacesASymlinkSquattingTheSentinelName() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            // The other half. Distrusting THROUGH a squatter (the sibling test) keeps THIS caller safe, but
            // the sentinel's job is to carry the verdict to a CONCURRENT one across the chmod. That peer
            // reads the name itself, so the name has to end up holding a mark rather than the attacker's
            // symlink. markUntrusted must therefore tell a peer's mark from a squatter - which the
            // FileAlreadyExistsException alone cannot do - and displace the squatter.
            //
            // beforeUntrustedDiscardHook drops us into the chmod-to-sweep gap, the same seam and the same
            // window as testConcurrentLoadDistrustsTheDirectoryTightenedButNotYetSwept, and asserts what a
            // peer arriving there would find at the name.
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));

            Path sentinel = dir.resolve(".untrusted");
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));
            Files.createSymbolicLink(sentinel, dir.resolve("no-such-target"));

            AtomicBoolean regularFileInGap = new AtomicBoolean();
            AtomicBoolean stillASymlinkInGap = new AtomicBoolean();
            Field hookField = FileTokenStore.class.getDeclaredField("beforeUntrustedDiscardHook");
            hookField.setAccessible(true);
            hookField.set(store, (Runnable) () -> {
                regularFileInGap.set(Files.isRegularFile(sentinel, LinkOption.NOFOLLOW_LINKS));
                stillASymlinkInGap.set(Files.isSymbolicLink(sentinel));
            });

            Assert.assertNull("the world-writable directory must be distrusted", store.load(key));

            Assert.assertFalse("markUntrusted must not leave the attacker's symlink standing at the name: a "
                            + "peer reading it in this gap follows it, finds nothing, and trusts the plant",
                    stillASymlinkInGap.get());
            Assert.assertTrue("the sentinel name must hold a real mark - a regular file - once markUntrusted "
                            + "has run, so a concurrent caller in the chmod-to-sweep gap distrusts",
                    regularFileInGap.get());
            Assert.assertFalse("a complete sweep must still clear the mark it wrote",
                    Files.exists(sentinel, LinkOption.NOFOLLOW_LINKS));
        });
    }

    @Test
    public void testLoadThrowsRatherThanReportsEmptyWhenTheDirectoryIsUnusable() throws Exception {
        assertMemoryLeak(() -> {
            // Same fixture as testInLockDegradesWhenDirectoryUnusable: a regular file standing where the
            // store directory's parent must be makes ensureDirectory throw IOException. That fault is
            // TRANSIENT in the field - a home directory not mounted yet, EIO/ESTALE on an NFS home, a
            // momentarily read-only or full filesystem.
            Path blocker = temp.getRoot().toPath().resolve("blocker");
            Files.write(blocker, new byte[]{1});
            FileTokenStore store = new FileTokenStore(blocker.resolve("oidc-tokens"));

            try {
                PersistedToken token = store.load(sampleKey());
                Assert.fail("load must not report a definitive empty store for a transient directory fault; "
                        + "returned " + token);
            } catch (OidcAuthException expected) {
                Assert.assertTrue(expected.getMessage(),
                        expected.getMessage().contains("could not prepare the OIDC token store directory"));
            }
            // Why the distinction is not cosmetic: null is load()'s DEFINITIVE answer. OidcDeviceAuth
            // latches storeLoadAttempted on it and never reads the store again for the life of the
            // instance, so a momentary mount fault at the first getToken() would send a process that owns a
            // good refresh token back through the interactive device flow - a hard failure for the headless
            // consumer this persistence exists to serve. A throw is retried under the store-load back-off.
            // save() already lets this same exception propagate; only load() disagreed.
        });
    }

    @Test
    public void testLoadTrustsAWorldREADABLEDirectory() throws Exception {
        Assume.assumeTrue("POSIX permissions are needed to loosen the store directory",
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            // The 0755 a default umask produces is NOT the attack surface: no other user can create or
            // replace a file in it, and the entry itself is 0600. Distrusting it would discard honest tokens
            // - and make every negative assertion in this suite pass for the wrong reason.
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));

            PersistedToken loaded = store.load(key);
            Assert.assertNotNull("a merely world-READABLE directory must not invalidate its entry", loaded);
            Assert.assertEquals("REFRESH-1", loaded.getRefreshToken());
            Assert.assertEquals("and it is still tightened on the way through",
                    PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(dir));
        });
    }

    @Test
    public void testLockFilePermissionsOwnerOnly() throws Exception {
        Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            // the lock file is created owner-only too: it briefly records an owner stamp and sits beside the
            // 0600 token file, so it must not widen the directory's exposure. Assert while the lock is held; inLock
            // deletes it on return and propagates a thrown AssertionError after releasing it.
            store.inLock(key, () -> {
                try {
                    Assert.assertEquals("the lock file must be owner-only (0600)",
                            PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(lock));
                } catch (java.io.IOException e) {
                    throw new AssertionError(e);
                }
                return true;
            });
        });
    }

    @Test
    public void testLongFieldsSerializeAsDigitsNotBareNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            // the two long fields are present, non-nullable integers, so Long.MIN_VALUE must serialize as its
            // digits. serialize() reserves an omitted member (not a bare null) for an absent value, so a null
            // here would be indistinguishable from absent and breaks the frozen cross-language contract; the
            // reader would also round-trip that null back to 0 (parseLongOrZero), a silent corruption.
            store.save(key, new PersistedToken("ACCESS-1", null, "REFRESH-1", Long.MIN_VALUE, Long.MIN_VALUE));

            String json = new String(Files.readAllBytes(tokenFile(dir, key)), StandardCharsets.UTF_8);
            Assert.assertTrue("expires_at_millis must be written as digits, not a bare null [json=" + json + ']',
                    json.contains("\"expires_at_millis\":-9223372036854775808"));
            Assert.assertTrue("token_ttl_millis must be written as digits, not a bare null [json=" + json + ']',
                    json.contains("\"token_ttl_millis\":-9223372036854775808"));

            // and the extreme value round-trips verbatim rather than collapsing to 0 on read
            PersistedToken loaded = store.load(key);
            Assert.assertNotNull(loaded);
            Assert.assertEquals(Long.MIN_VALUE, loaded.getExpiresAtMillis());
            Assert.assertEquals(Long.MIN_VALUE, loaded.getTokenTtlMillis());
        });
    }

    @Test
    public void testNoLeftoverTempFileAfterSave() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            store.save(key, sampleToken("ACCESS-2", "REFRESH-2")); // overwrite

            File[] files = dir.toFile().listFiles();
            Assert.assertNotNull(files);
            int jsonCount = 0;
            for (File f : files) {
                Assert.assertFalse("leftover temp file: " + f.getName(), f.getName().endsWith(".tmp"));
                if (f.getName().endsWith(".json")) {
                    jsonCount++;
                }
            }
            Assert.assertEquals(1, jsonCount);
        });
    }

    @Test
    public void testOutOfContractNumberIsRejectedNotQuietlyParsed() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Path file = tokenFile(dir, key);
            String json = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);

            // QuestDB's Numbers.parseLong accepts an 'L' suffix and '_' thousands separators. JSON allows
            // neither, and neither does the frozen cross-language format - so "1L" is a schema version only
            // THIS client can read, and accepting it would let a file diverge silently from every other
            // language client sharing the directory. It must read as unusable instead.
            String tampered = json.replace("\"v\":1,", "\"v\":1L,");
            Assert.assertNotEquals("the fixture must actually have been tampered with", json, tampered);
            Files.write(file, tampered.getBytes(StandardCharsets.UTF_8));

            Assert.assertNull("a number only this client's parser accepts must not satisfy the schema gate",
                    store.load(key));
        });
    }

    @Test
    public void testOversizedFileReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            // Baseline: a normal, fingerprint-matching token loads. This proves the oversized file below is
            // rejected by the size cap ALONE, not by a fingerprint/version/parse mismatch (the flaw in the old
            // all-spaces file, which parsed to version 0 and would return null even with the cap removed).
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Assert.assertNotNull("a normal valid token must load", store.load(key));
            // A valid, fingerprint-matching token whose two large fields push the FILE past MAX_FILE_BYTES
            // (1 MiB) while each field stays under the per-value lexer limit (also 1 MiB), so without the size
            // cap this parses and loads. readBounded caps on channel.size() before reading, so the oversized
            // file is rejected up front - the real point of the cap (avoid an unbounded read / OOM on an
            // attacker-grown file), which the guard now demonstrably enforces.
            char[] big = new char[600_000];
            Arrays.fill(big, 'a');
            String bigField = new String(big);
            store.save(key, sampleToken(bigField, bigField));
            Assert.assertTrue("the test file must exceed the 1 MiB size cap to isolate it",
                    Files.size(tokenFile(dir, key)) > (1 << 20));
            Assert.assertNull("an oversized but otherwise valid, fingerprint-matching file must be rejected by the size cap",
                    store.load(key));
        });
    }

    @Test
    public void testOversizedStaleLockIsStolen() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            // Stale window 60s, lock backdated only 10s: a STAMPED (readable) lock this fresh would NOT be
            // stolen (10s < 60s). So the steal below can only happen because the oversized lock reads as
            // unreadable/null via MAX_LOCK_FILE_BYTES and is stolen on the shorter empty-lock grace (5s < 10s).
            // This isolates the read cap: remove it and readLockHolder reads the 64 KiB as a live stamp -> the
            // lock is judged fresh, not stolen, acquisition degrades lock-free, and the lock file is NOT
            // released, failing the Files.exists assertion below. The old 100ms window stole on staleness
            // regardless of the cap, so the cap was untested.
            FileTokenStore store = new FileTokenStore(dir, 2000, 60_000);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            // a corrupt/hostile lock far larger than the read cap. The steal reads the owner stamp with a hard
            // cap (not Files.readAllBytes, which on an attacker-grown lock could OutOfMemoryError on the refresh
            // path): a bounded read reports an oversized lock as unreadable, which the steal treats as abandoned
            // junk - it must still acquire, not wedge.
            byte[] huge = new byte[64 * 1024];
            Arrays.fill(huge, (byte) 'x');
            Files.write(lock, huge);
            Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis() - 10_000));

            AtomicBoolean ran = new AtomicBoolean();
            boolean result = store.inLock(key, () -> {
                ran.set(true);
                return true;
            });

            Assert.assertTrue("an oversized stale lock must be stolen, not wedge acquisition", ran.get());
            Assert.assertTrue(result);
            Assert.assertFalse("the acquired (stolen) lock must be released", Files.exists(lock));
            assertNoCaptureTempFiles(dir, key);
        });
    }

    @Test
    public void testPerFieldFingerprintMismatchReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey saved = sampleKey();
            store.save(saved, sampleToken("ACCESS-1", "REFRESH-1"));
            byte[] bytes = Files.readAllBytes(tokenFile(dir, saved));

            // each key differs from the saved fingerprint in exactly one field; writing the saved bytes under
            // the differing key's file name isolates the in-file fingerprint re-check (the file is found, but
            // its recorded identity does not match), so a hash collision or a copied file never serves another
            // identity's token. groups_in_token (id-token vs access-token credential) and audience (the
            // distinct nullableEquals path) are the riskiest fields.
            TokenStoreKey[] mismatches = {
                    new TokenStoreKey("questdb", "https://idp.example.com:443/OTHER-token",
                            "https://idp.example.com:443/device", "openid", null, false),
                    new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                            "https://idp.example.com:443/OTHER-device", "openid", null, false),
                    new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                            "https://idp.example.com:443/device", "openid groups", null, false),
                    new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                            "https://idp.example.com:443/device", "openid", "api://other", false),
                    new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                            "https://idp.example.com:443/device", "openid", null, true),
            };
            for (TokenStoreKey other : mismatches) {
                Files.write(tokenFile(dir, other), bytes);
                Assert.assertNull("a fingerprint mismatch must be rejected: " + other.hash(), store.load(other));
            }
        });
    }

    @Test
    public void testPermissionsOwnerOnly() throws Exception {
        Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Assert.assertEquals(PosixFilePermissions.fromString("rw-------"),
                    Files.getPosixFilePermissions(tokenFile(dir, key)));
            Assert.assertEquals(PosixFilePermissions.fromString("rwx------"),
                    Files.getPosixFilePermissions(dir));
        });
    }

    @Test
    public void testSaveFailureLeavesNoTempFileAndThrows() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            // make the atomic rename fail: the target path already exists as a NON-EMPTY directory, which a
            // file-over-directory replace cannot overwrite, driving save() down its IOException path
            Path target = tokenFile(dir, key);
            Files.createDirectories(target);
            Files.createFile(target.resolve("blocker"));

            try {
                store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
                Assert.fail("save must throw when it cannot replace the target");
            } catch (OidcAuthException expected) {
                // the write-temp / flush / atomic-rename protocol must surface a wrapped failure, never a raw
                // IOException, and never a half-written credential
            }

            // the temp file is the durability point of the protocol; a failed save must clean it up rather than
            // leave a *.tmp credential fragment behind
            boolean hasTmp;
            try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(dir, "*.tmp")) {
                hasTmp = entries.iterator().hasNext();
            }
            Assert.assertFalse("a failed save must not leave a *.tmp file behind", hasTmp);
        });
    }

    @Test
    public void testSaveThenLoadRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            FileTokenStore store = new FileTokenStore(storeDir());
            TokenStoreKey key = sampleKey();
            PersistedToken saved = new PersistedToken("ACCESS-1", "ID-1", "REFRESH-1", 1_730_000_000_000L, 300_000L);
            store.save(key, saved);

            PersistedToken loaded = store.load(key);
            Assert.assertNotNull(loaded);
            Assert.assertEquals("ACCESS-1", loaded.getAccessToken());
            Assert.assertEquals("ID-1", loaded.getIdToken());
            Assert.assertEquals("REFRESH-1", loaded.getRefreshToken());
            Assert.assertEquals(1_730_000_000_000L, loaded.getExpiresAtMillis());
            Assert.assertEquals(300_000L, loaded.getTokenTtlMillis());
        });
    }

    @Test
    public void testSchemaVersionMismatchReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            createStoreDir(dir);
            // a future schema version with an otherwise-matching fingerprint must be ignored, not served: the
            // version is the forward-compat guard the frozen cross-language contract rests on
            String v2 = "{\"v\":2,\"client_id\":\"questdb\","
                    + "\"token_endpoint\":\"https://idp.example.com:443/token\","
                    + "\"device_authorization_endpoint\":\"https://idp.example.com:443/device\","
                    + "\"scope\":\"openid\",\"groups_in_token\":false,"
                    + "\"access_token\":\"ACCESS-1\",\"refresh_token\":\"REFRESH-1\","
                    + "\"expires_at_millis\":1730000000000,\"token_ttl_millis\":300000}";
            Files.write(tokenFile(dir, key), v2.getBytes(StandardCharsets.UTF_8));
            Assert.assertNull("a future schema version must be rejected", store.load(key));

            // sanity: the identical body at the live version IS accepted, proving the rejection is the version
            // and not a malformed document
            Files.write(tokenFile(dir, key), v2.replace("\"v\":2", "\"v\":1").getBytes(StandardCharsets.UTF_8));
            Assert.assertNotNull("the same body at the live schema version must load", store.load(key));
        });
    }

    @Test
    public void testSpecialCharactersAndNullsRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            FileTokenStore store = new FileTokenStore(storeDir());
            // a non-null audience that needs JSON escaping, and null access/id tokens
            TokenStoreKey key = new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                    "https://idp.example.com:443/device", "openid groups", "api://q\"uote\\slash", true);
            PersistedToken saved = new PersistedToken(null, null, "REFRESH-\t-1", 42L, 60_000L);
            store.save(key, saved);

            PersistedToken loaded = store.load(key);
            Assert.assertNotNull(loaded);
            Assert.assertNull(loaded.getAccessToken());
            Assert.assertNull(loaded.getIdToken());
            Assert.assertEquals("REFRESH-\t-1", loaded.getRefreshToken());
            Assert.assertEquals(42L, loaded.getExpiresAtMillis());
        });
    }

    @Test
    public void testStaleTempFilesAreSweptOnSave() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            // 1s staleness window so the test does not have to wait
            FileTokenStore store = new FileTokenStore(dir, 3_000, 1_000);
            TokenStoreKey key = sampleKey();
            // an orphan temp left by a crashed save (backdated past the staleness window) must be reaped on the
            // next save; a fresh temp (recent mtime - a concurrent writer's) must be left untouched
            Path staleTmp = dir.resolve(key.hash() + "stale.tmp");
            Files.createFile(staleTmp);
            Files.setLastModifiedTime(staleTmp, FileTime.fromMillis(System.currentTimeMillis() - 10_000));
            Path freshTmp = dir.resolve(key.hash() + "fresh.tmp");
            Files.createFile(freshTmp);

            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));

            Assert.assertFalse("a stale orphan temp must be swept on save", Files.exists(staleTmp));
            Assert.assertTrue("a fresh temp (a concurrent writer's) must not be swept", Files.exists(freshTmp));
            Assert.assertNotNull("the save must still succeed", store.load(key));
        });
    }

    @Test
    public void testStealIfStaleRestoresALockAPeerRecreatedInTheCaptureGap() throws Exception {
        assertMemoryLeak(() -> {
            // The arm that ran only in production. stealIfStale judges a lock stale, captures it with an
            // ATOMIC_MOVE, then re-reads the capture to confirm it took the stamp it judged rather than a
            // LIVE lock a peer recreated in the gap between those two steps. Its own comment says a bare
            // deleteIfExists(lock) "would admit two holders at once", yet replacing the whole
            // capture/verify/restore with exactly that left the suite green: the two
            // testRestoreCapturedLock* cases drive restoreCapturedLock DIRECTLY by reflection, so they
            // pass unchanged when nothing calls it.
            //
            // Reaching the arm needs the peer to land inside that gap, which no amount of concurrency can
            // force deterministically -- hence beforeCaptureHook, which runs there and nowhere else.
            Path dir = storeDir();
            createStoreDir(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            byte[] peerLive = "peer-owner-stamp".getBytes(StandardCharsets.UTF_8);
            Files.write(lock, "crashed-holder-stamp".getBytes(StandardCharsets.UTF_8));
            Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis() - 600_000));

            FileTokenStore store = new FileTokenStore(dir, 30_000, 60_000);
            Field hookField = FileTokenStore.class.getDeclaredField("beforeCaptureHook");
            hookField.setAccessible(true);
            // In the gap: the abandoned lock is replaced by a peer's freshly-created live one.
            hookField.set(store, (Runnable) () -> {
                try {
                    Files.write(lock, peerLive);
                    Files.setLastModifiedTime(lock, FileTime.fromMillis(System.currentTimeMillis()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            Method stealIfStale = FileTokenStore.class.getDeclaredMethod("stealIfStale", Path.class);
            stealIfStale.setAccessible(true);
            stealIfStale.invoke(store, lock);

            Assert.assertTrue("the peer's LIVE lock must survive the capture gap; removing it admits two "
                    + "holders at once, which is the double-POST of one rotating refresh token that a "
                    + "reuse-detecting provider answers by revoking the whole family", Files.exists(lock));
            Assert.assertArrayEquals("the peer's lock must go back byte for byte, or releaseLock's "
                            + "owner-stamp check refuses to delete it and the peer wedges every later acquire",
                    peerLive, Files.readAllBytes(lock));
            assertNoCaptureTempFiles(dir, key);
        });
    }

    @Test
    public void testSweepDoesNotDeleteStealCapturedLock() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            // a 1s staleness window so an old file is well past it
            FileTokenStore store = new FileTokenStore(dir, 3_000, 1_000);
            TokenStoreKey key = sampleKey();
            store.save(key, sampleToken("ACCESS-0", "REFRESH-0")); // create the directory
            // a steal in another process captures a stale lock by atomically renaming it to
            // <hash>.lock.<uuid>.tmp; ATOMIC_MOVE preserves the stale lock's old mtime onto the capture. Even
            // though that name matches the <hash>*.tmp write-temp glob and is past the staleness window, the
            // save's temp-sweep must NOT delete it - it is a live cross-process steal in progress, and deleting
            // it would destroy a lock the stealer may be about to restore to its live owner.
            Path capture = dir.resolve(key.hash() + ".lock." + java.util.UUID.randomUUID() + ".tmp");
            Files.write(capture, "stale-owner-stamp".getBytes(StandardCharsets.UTF_8));
            Files.setLastModifiedTime(capture, FileTime.fromMillis(System.currentTimeMillis() - 10_000));

            store.save(key, sampleToken("ACCESS-1", "REFRESH-1")); // runs sweepStaleTempFiles

            Assert.assertTrue("the temp-sweep must not delete an in-flight steal-captured lock", Files.exists(capture));
        });
    }

    @Test
    public void testTokenStoreKeyRejectsNullRequiredFields() throws Exception {
        assertMemoryLeak(() -> {
            // the identity fields are required; a null must fail fast with a clear OidcAuthException rather than
            // surface later as a raw NullPointerException inside save()/load() (audience stays optional)
            try {
                new TokenStoreKey(null, "https://idp/token", "https://idp/device", "openid", null, false);
                Assert.fail("a null clientId must be rejected");
            } catch (OidcAuthException expected) {
                // required identity field
            }
            try {
                new TokenStoreKey("questdb", null, "https://idp/device", "openid", null, false);
                Assert.fail("a null tokenEndpoint must be rejected");
            } catch (OidcAuthException expected) {
                // required identity field
            }
            try {
                new TokenStoreKey("questdb", "https://idp/token", null, "openid", null, false);
                Assert.fail("a null deviceAuthorizationEndpoint must be rejected");
            } catch (OidcAuthException expected) {
                // required identity field
            }
            try {
                new TokenStoreKey("questdb", "https://idp/token", "https://idp/device", null, null, false);
                Assert.fail("a null scope must be rejected");
            } catch (OidcAuthException expected) {
                // required identity field
            }
        });
    }

    @Test
    public void testTruncatedJsonReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            createStoreDir(dir);
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            // a crash mid-write on a filesystem without atomic rename, or a torn read, can leave a valid JSON
            // prefix cut off before the closing brace. parseLast() must reject the truncated document rather
            // than serve a half-parsed credential
            Files.write(tokenFile(dir, key),
                    "{\"v\":1,\"client_id\":\"questdb\"".getBytes(StandardCharsets.UTF_8));
            Assert.assertNull("a truncated-but-prefix-valid file must be ignored", store.load(key));
        });
    }

    @Test
    public void testVersionOverflowReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            // a tampered version that narrows to SCHEMA_VERSION when cast to int (1 + 2^32) must not pass the
            // schema gate: the parser keeps the version as a long and compares it as a long
            store.save(key, sampleToken("ACCESS-1", "REFRESH-1"));
            Assert.assertNotNull("the valid entry must load", store.load(key));
            byte[] valid = Files.readAllBytes(tokenFile(dir, key));
            String tampered = new String(valid, StandardCharsets.UTF_8).replace("\"v\":1", "\"v\":4294967297");
            Files.write(tokenFile(dir, key), tampered.getBytes(StandardCharsets.UTF_8));
            Assert.assertNull("a version that truncates to 1 as an int must be rejected", store.load(key));
        });
    }

    private static TokenStoreKey sampleKey() {
        return new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                "https://idp.example.com:443/device", "openid", null, false);
    }

    private static PersistedToken sampleToken(String access, String refresh) {
        return new PersistedToken(access, null, refresh, System.currentTimeMillis() + 300_000L, 300_000L);
    }

    private void assertNoCaptureTempFiles(Path dir, TokenStoreKey key) throws Exception {
        // a successful steal deletes its atomic-capture file and a restore moves it back; neither must leak a
        // <hash>.lock.<uuid>.tmp behind (an orphan is otherwise only reclaimed by a later save's sweep)
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, key.hash() + "*.tmp")) {
            for (Path p : stream) {
                Assert.fail("a steal must not leak a capture temp file: " + p.getFileName());
            }
        }
    }

    private static void awaitInside(Thread t, String method) throws InterruptedException {
        // poll the thread's own stack for the named FileTokenStore frame: the only evidence that a helper
        // thread has actually ENTERED the call, as opposed to having been scheduled at all
        final long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (StackTraceElement frame : t.getStackTrace()) {
                if (FileTokenStore.class.getName().equals(frame.getClassName())
                        && method.equals(frame.getMethodName())) {
                    return;
                }
            }
            Thread.sleep(5);
        }
        Assert.fail("the waiter never entered FileTokenStore." + method + " [state=" + t.getState() + ']');
    }

    /**
     * Creates the store directory owner-only, the way {@code FileTokenStore} itself creates it - NOT the
     * way the JVM's umask happens to.
     * <p>
     * A fixture that calls {@code Files.createDirectories(dir)} bare inherits the umask, so on a host with
     * a group-writable one (002, the default on the Linux CI agents) the directory arrives {@code
     * rwxrwxr-x}. {@code load()} then reads it as a directory another local user could have planted an
     * entry in, discards the entry and returns null BEFORE it opens the file - which fails every test whose
     * first assertion is that a valid entry loads, and, far worse, silently satisfies every test asserting
     * that some malformed entry does NOT load. Those pass for the wrong reason: proved by feeding
     * {@code testCorruptFileReturnsNull} a perfectly valid document, which fails the test at umask 022 and
     * passes it at 002.
     * <p>
     * A test that wants a loose directory sets the permissions itself right after this call; that is an
     * explicit statement rather than a property of whoever ran the build.
     */
    private static void createStoreDir(Path dir) throws Exception {
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            Files.createDirectories(dir); // Windows: no POSIX bits to set, and load() trusts it either way
            return;
        }
        Files.createDirectories(dir, PosixFilePermissions.asFileAttribute(OWNER_ONLY_DIR_PERMS));
        // createDirectories applies the attribute only to directories it actually creates, so assert
        // rather than assume: a fixture that silently reverted to the umask must not go unnoticed again.
        Assert.assertEquals("the fixture must not leave the store directory at the mercy of the umask",
                OWNER_ONLY_DIR_PERMS, Files.getPosixFilePermissions(dir));
    }

    private static void joinOrFail(Thread t, String what) throws InterruptedException {
        // never a bare join(): a contender that wedges on a lock it should have degraded out of would hang
        // the suite until the 20-minute surefire timeout, reported as an opaque stall with no failing
        // assertion. Bounded, then asserted, so the wedge fails as itself.
        t.join(30_000);
        Assert.assertFalse(what + " did not finish within 30s [state=" + t.getState() + ']', t.isAlive());
    }

    private Path lockFile(Path dir, TokenStoreKey key) {
        return dir.resolve(key.hash() + ".lock");
    }

    private String readLockStamp(Path lock) {
        // the owner nonce a live holder stamped into the lock, or null when there is no lock file at all. An
        // IO error here is a harness fault, so it fails loudly rather than reading as "no lock"
        try {
            return Files.exists(lock) ? new String(Files.readAllBytes(lock), StandardCharsets.UTF_8) : null;
        } catch (IOException e) {
            throw new AssertionError("could not read the lock stamp: " + lock, e);
        }
    }

    private Path storeDir() {
        // a non-existent subdirectory so the store creates it (and we can assert its permissions)
        return temp.getRoot().toPath().resolve("oidc-tokens");
    }

    private Path tokenFile(Path dir, TokenStoreKey key) {
        return dir.resolve(key.hash() + ".json");
    }
}
