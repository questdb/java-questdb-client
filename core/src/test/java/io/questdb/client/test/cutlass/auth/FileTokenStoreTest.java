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
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class FileTokenStoreTest {
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
    public void testCorruptFileReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            Files.createDirectories(dir);
            Files.write(tokenFile(dir, key), "this is not json {{{".getBytes(StandardCharsets.UTF_8));
            Assert.assertNull(store.load(key));
        });
    }

    @Test
    public void testEmptyFileReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            Files.createDirectories(dir);
            Files.write(tokenFile(dir, key), new byte[0]);
            Assert.assertNull(store.load(key));
        });
    }

    @Test
    public void testEnsureDirectoryTightensPreExistingDirPerms() throws Exception {
        Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            // a pre-existing, world-accessible store directory (a permissive umask, a prior tool, or a hostile
            // local pre-create) must be tightened to owner-only before a token is written into it
            Files.createDirectories(dir);
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
            Files.createDirectories(dir);
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
            Files.createDirectories(dir);
            TokenStoreKey key = sampleKey();
            // two instances over one directory model two processes; a generous acquire budget makes a
            // contender wait for the lock rather than degrade, and a large staleness window stops either from
            // stealing the other's live lock - so the two critical sections must run strictly one at a time
            FileTokenStore storeA = new FileTokenStore(dir, 10_000, 600_000);
            FileTokenStore storeB = new FileTokenStore(dir, 10_000, 600_000);

            AtomicInteger inside = new AtomicInteger();
            AtomicInteger maxInside = new AtomicInteger();
            AtomicInteger overlaps = new AtomicInteger();
            TokenStore.CriticalSection section = () -> {
                int now = inside.incrementAndGet();
                maxInside.accumulateAndGet(now, Math::max);
                if (now > 1) {
                    overlaps.incrementAndGet();
                }
                Os.sleep(200);
                inside.decrementAndGet();
                return true;
            };

            Thread tA = new Thread(() -> storeA.inLock(key, section));
            Thread tB = new Thread(() -> storeB.inLock(key, section));
            tA.start();
            tB.start();
            tA.join();
            tB.join();

            Assert.assertEquals("the two critical sections must never overlap", 0, overlaps.get());
            Assert.assertEquals("at most one holder at a time", 1, maxInside.get());
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
            Files.createDirectories(dir);
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
    public void testLoadMissingReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            FileTokenStore store = new FileTokenStore(storeDir());
            Assert.assertNull(store.load(sampleKey()));
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
    public void testOversizedFileReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            Files.createDirectories(dir);
            byte[] big = new byte[(1 << 20) + 1];
            java.util.Arrays.fill(big, (byte) ' ');
            Files.write(tokenFile(dir, key), big);
            Assert.assertNull(store.load(key));
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

    private static TokenStoreKey sampleKey() {
        return new TokenStoreKey("questdb", "https://idp.example.com:443/token",
                "https://idp.example.com:443/device", "openid", null, false);
    }

    private static PersistedToken sampleToken(String access, String refresh) {
        return new PersistedToken(access, null, refresh, System.currentTimeMillis() + 300_000L, 300_000L);
    }

    private Path lockFile(Path dir, TokenStoreKey key) {
        return dir.resolve(key.hash() + ".lock");
    }

    private Path storeDir() {
        // a non-existent subdirectory so the store creates it (and we can assert its permissions)
        return temp.getRoot().toPath().resolve("oidc-tokens");
    }

    private Path tokenFile(Path dir, TokenStoreKey key) {
        return dir.resolve(key.hash() + ".json");
    }
}
