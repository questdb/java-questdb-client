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
    public void testInLockReleaseDoesNotDeleteAStolenLock() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            Files.createDirectories(dir);
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
    public void testLockFilePermissionsOwnerOnly() throws Exception {
        Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assertMemoryLeak(() -> {
            Path dir = storeDir();
            FileTokenStore store = new FileTokenStore(dir);
            TokenStoreKey key = sampleKey();
            Path lock = lockFile(dir, key);
            // the lock file is created owner-only too: it briefly records pid@host and sits beside the 0600
            // token file, so it must not widen the directory's exposure. Assert while the lock is held; inLock
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
            Files.createDirectories(dir);
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
            Files.createDirectories(dir);
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
            Files.createDirectories(dir);
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
            Files.createDirectories(dir);
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
