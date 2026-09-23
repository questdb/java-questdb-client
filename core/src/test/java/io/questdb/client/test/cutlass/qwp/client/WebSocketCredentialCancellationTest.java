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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.FileTokenStore;
import io.questdb.client.cutlass.auth.OidcDeviceAuth;
import io.questdb.client.cutlass.auth.PersistedToken;
import io.questdb.client.cutlass.auth.TokenStoreKey;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.test.tools.NoBrowserLaunch;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLockContentionException;
import io.questdb.client.std.ObjList;
import io.questdb.client.test.cutlass.auth.MockOidcServer;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Connect cancellation against the BUILT-IN credential path — a real {@link OidcDeviceAuth} over a real
 * {@link FileTokenStore} — rather than a test double.
 * <p>
 * QWP's close() cannot reach a credential pull through {@code closeTraffic()}: the pull is caller code
 * owning no socket. Its only lever is an interrupt, which
 * {@code CursorWebSocketSendLoop.ConnectCancellation.cancel()} sends to the thread published as being
 * inside the pull. Whether that lever WORKS depends entirely on what the pull is blocked in, and every
 * existing test blocks it in an interruptible test double, so the shipped path went unchecked: it waited
 * on an uninterruptible {@code ReentrantLock.lock()} and polled the lock file through {@code Os.sleep},
 * which catches {@code InterruptedException} and keeps sleeping to its own deadline. The lock-acquire
 * budget caps at 30s, the same as close()'s shutdown budget, so a sender closing while another
 * same-identity instance held the lock burned the whole budget and then gave up on its I/O thread,
 * delegating teardown of the native client, the cursor engine and the store-and-forward slot lock.
 * <p>
 * The token endpoint is never reached in these tests: the pull cannot get past the store lock. That is
 * asserted, because reaching it would mean the wait had already been abandoned for a lock-free refresh
 * and the test would no longer be exercising the blocked path.
 */
public class WebSocketCredentialCancellationTest {
    private static final String DEVICE_PATH = "/device";
    // Issued lifetime and remaining life of the seeded entry. effectiveSkewMillis caps the clock-skew
    // margin at half the issued lifetime, so this reads as valid for (12s - 10s) = ~2s and stale after:
    // long enough for the foreground connect to be a cache hit, short enough to force the reconnect's pull
    // into a refresh without stubbing the clock.
    private static final long SEED_REMAINING_MILLIS = 12_000L;
    private static final long SEED_TTL_MILLIS = 20_000L;
    private static final String TOKEN_PATH = "/token";

    // the credential pull can reach the device-code prompt; see NoBrowserLaunch for why this is a rule
    @ClassRule
    public static final NoBrowserLaunch NO_BROWSER = new NoBrowserLaunch();

    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 120_000)
    public void testCloseBreaksAForegroundReconnectBlockedInTheBuiltInCredentialPull() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger tokenEndpointCalls = new AtomicInteger();
            try (MockOidcServer idp = new MockOidcServer((method, path, body) -> {
                if (TOKEN_PATH.equals(path)) {
                    tokenEndpointCalls.incrementAndGet();
                }
                return MockOidcServer.json(200, "{}");
            })) {
                Path dir = storeDir();
                Files.createDirectories(dir);
                // The maximum permitted acquire budget, which is exactly QWP's close() shutdown budget: this
                // is the wait an interrupt has to be able to cut short.
                FileTokenStore store = new FileTokenStore(dir, 30_000, 600_000);
                TokenStoreKey key = keyFor(idp);
                store.save(key, seededEntry());

                DropAfterFirstAckHandler wire = new DropAfterFirstAckHandler();
                try (TestWebSocketServer server = new TestWebSocketServer(wire)) {
                    server.start();
                    Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                    AtomicBoolean armed = new AtomicBoolean();
                    CountDownLatch blockedPullStarted = new CountDownLatch(1);
                    try (OidcDeviceAuth auth = authFor(idp, store)) {
                        Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                                .address("localhost:" + server.getPort())
                                .reconnectInitialBackoffMillis(20)
                                .reconnectMaxBackoffMillis(20)
                                .httpTokenProvider(() -> {
                                    // The wrapper only reports that a pull started; the blocking is all done by
                                    // the real OidcDeviceAuth/FileTokenStore underneath.
                                    if (armed.get()) {
                                        blockedPullStarted.countDown();
                                    }
                                    return auth.getToken();
                                })
                                .build();
                        boolean closed = false;
                        try {
                            // the seeded entry is still valid, so the foreground connect is a cache hit and
                            // never touches the store lock
                            Assert.assertEquals("Bearer ACCESS-SEED",
                                    server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                            // A peer holds the identity's lock: stamped (so the empty-lock grace does not
                            // apply) and far inside the staleness window (so it is never stolen). Every later
                            // refresh can only poll for it.
                            Path lock = dir.resolve(key.hash() + ".lock");
                            Files.write(lock, "live-peer-nonce".getBytes(StandardCharsets.UTF_8));

                            armed.set(true);
                            // let the seeded token fall inside its clock-skew margin, so the reconnect's pull
                            // has to refresh rather than serve the cache
                            // stale once now >= expiresAt - skew, i.e. after (remaining - ttl/2)
                            Thread.sleep(SEED_REMAINING_MILLIS - SEED_TTL_MILLIS / 2 + 500L);

                            // one batch: the server acks it, then drops the socket -> foreground reconnect ->
                            // credential pull -> refresh -> blocked polling for the store lock
                            sender.table("foo").longColumn("v", 1L).atNow();
                            sender.flush();
                            Assert.assertTrue("the reconnect must reach the credential pull",
                                    blockedPullStarted.await(30, TimeUnit.SECONDS));

                            long startNanos = System.nanoTime();
                            sender.close();
                            closed = true;
                            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                            // the shutdown budget is 30s; anything near it means close() waited the store lock
                            // out instead of breaking it. A generous ceiling keeps this off the CI flake line
                            // while still failing the pre-fix behaviour by a wide margin.
                            Assert.assertTrue("close() must break the store-lock wait, not sit out the shutdown "
                                    + "budget; took " + elapsedMillis + "ms", elapsedMillis < 15_000);
                            Assert.assertEquals("the pull never got past the store lock, so the IdP must not "
                                    + "have been reached", 0, tokenEndpointCalls.get());
                        } finally {
                            if (!closed) {
                                sender.close();
                            }
                        }
                    }
                }
            }
        });
    }

    @Test(timeout = 120_000)
    public void testCloseStopsAnOrphanDrainerBlockedInTheBuiltInCredentialPull() throws Exception {
        assertMemoryLeak(() -> {
            // The orphan drainer's INITIAL connect -- the one it makes before any CursorWebSocketSendLoop
            // exists, so before the loop's ConnectCancellation is in play. Its lever is a different one:
            // BackgroundDrainerPool.close() ends its stop grace with executor.shutdownNow(), which
            // interrupts the drainer thread. That interrupt only accomplishes anything if what the thread
            // is blocked in honours it, and a credential pull sat in the token store's uninterruptible
            // waits -- so the drainer sat out the store's whole 30s lock-acquire budget while close() gave
            // up on it, leaving the orphan slot locked by a thread nobody was waiting for any more.
            String sfDir = temp.getRoot().toPath().resolve("sf").toString();
            AtomicInteger tokenEndpointCalls = new AtomicInteger();
            try (MockOidcServer idp = new MockOidcServer((method, path, body) -> {
                if (TOKEN_PATH.equals(path)) {
                    tokenEndpointCalls.incrementAndGet();
                }
                return MockOidcServer.json(200, "{}");
            })) {
                // Phase 1: a ghost sender leaves un-acked frames behind, so phase 2 has an orphan to adopt.
                try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                    silent.start();
                    Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                    try (Sender ghost = Sender.fromConfig("ws::addr=localhost:" + silent.getPort()
                            + ";sf_dir=" + sfDir + ";sender_id=ghost;close_flush_timeout_millis=0;")) {
                        ghost.table("foo").longColumn("v", 7L).atNow();
                        ghost.flush();
                    }
                }
                ObjList<String> orphans = OrphanScanner.scan(sfDir, "primary");
                Assert.assertEquals("phase 1 must leave exactly one orphan slot", 1, orphans.size());

                Path dir = storeDir();
                Files.createDirectories(dir);
                FileTokenStore store = new FileTokenStore(dir, 30_000, 600_000);
                TokenStoreKey key = keyFor(idp);
                // No access token, only a refresh token: adopt() keeps the refresh token and leaves the cache
                // empty, so EVERY pull -- the foreground's and the drainer's initial one alike -- goes into
                // inLock rather than hitting a cache.
                store.save(key, new PersistedToken(null, null, "REFRESH-SEED", 0L, 0L));
                // the peer's live lock is in place BEFORE the sender is built, so the drainer's very first
                // connect blocks
                Files.write(dir.resolve(key.hash() + ".lock"), "live-peer-nonce".getBytes(StandardCharsets.UTF_8));

                try (TestWebSocketServer server = new TestWebSocketServer(new SilentHandler())) {
                    server.start();
                    Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                    CountDownLatch drainerPullStarted = new CountDownLatch(1);
                    try (OidcDeviceAuth auth = authFor(idp, store)) {
                        // ASYNC so build() itself does not sit in the blocked foreground pull and never hand
                        // back a sender to close; the orphan drainers still start inside build().
                        Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                                .address("localhost:" + server.getPort())
                                .storeAndForwardDir(sfDir)
                                .senderId("primary")
                                .drainOrphans(true)
                                .initialConnectMode(Sender.InitialConnectMode.ASYNC)
                                .reconnectInitialBackoffMillis(20)
                                .reconnectMaxBackoffMillis(20)
                                .httpTokenProvider(() -> {
                                    // positively confirm the DRAINER (not just the foreground loop) reaches
                                    // the pull; without this the test could pass on a drainer that never
                                    // started and prove nothing about the initial-connect path
                                    if (Thread.currentThread().getName().contains("orphan-drainer")) {
                                        drainerPullStarted.countDown();
                                    }
                                    return auth.getToken();
                                })
                                .build();
                        boolean closed = false;
                        try {
                            Assert.assertTrue("the orphan drainer must reach its initial credential pull",
                                    drainerPullStarted.await(30, TimeUnit.SECONDS));
                            // let it settle into the store's lock wait
                            Thread.sleep(500L);

                            long startNanos = System.nanoTime();
                            sender.close();
                            closed = true;
                            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                            Assert.assertTrue("close() must stop a drainer blocked in the built-in credential "
                                    + "pull, not leave it to the store's 30s budget; took " + elapsedMillis
                                    + "ms", elapsedMillis < 15_000);
                        } finally {
                            if (!closed) {
                                sender.close();
                            }
                        }
                    }
                    Assert.assertEquals("the pull never got past the store lock, so the IdP must not have "
                            + "been reached", 0, tokenEndpointCalls.get());
                    // The drainer must have released the orphan slot's lock on the way out: a slot still
                    // locked by an abandoned drainer thread cannot be adopted by anyone, which is the durable
                    // cost of close() giving up on it.
                    Assert.assertTrue("the abandoned orphan slot must be adoptable again after close()",
                            awaitSlotAdoptable(sfDir + "/ghost", 10_000));
                }
            }
        });
    }

    private static OidcDeviceAuth authFor(MockOidcServer idp, FileTokenStore store) {
        return OidcDeviceAuth.builder()
                .clientId("questdb")
                .deviceAuthorizationEndpoint(idp.httpUrl(DEVICE_PATH))
                .tokenEndpoint(idp.httpUrl(TOKEN_PATH))
                .scope("openid")
                .allowInsecureTransport(true)
                .tokenStore(store)
                .prompt(challenge -> {
                })
                .build();
    }

    private static TokenStoreKey keyFor(MockOidcServer idp) {
        return new TokenStoreKey(
                "questdb",
                idp.httpUrl(TOKEN_PATH),
                idp.httpUrl(DEVICE_PATH),
                "openid",
                null,
                false);
    }

    private static PersistedToken seededEntry() {
        // an access token the foreground connect can serve straight from cache, plus the refresh token that
        // sends the next pull into inLock once it goes stale
        return new PersistedToken("ACCESS-SEED", null, "REFRESH-SEED",
                System.currentTimeMillis() + SEED_REMAINING_MILLIS, SEED_TTL_MILLIS);
    }

    private static boolean awaitSlotAdoptable(String slotPath, long timeoutMillis) throws Exception {
        // the slot lock is an flock held by the drainer's engine, so a fresh acquire succeeds only once the
        // drainer has genuinely let go; SlotLock.acquire throws SlotLockContentionException while it has not
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try (SlotLock probe = SlotLock.acquire(slotPath)) {
                Assert.assertNotNull(probe);
                return true;
            } catch (SlotLockContentionException e) {
                Thread.sleep(50);
            }
        }
        return false;
    }

    private Path storeDir() {
        return temp.getRoot().toPath().resolve("oidc-tokens");
    }

    /** Never acks, so a sender's frames stay un-acked on disk. */
    private static final class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
        }
    }

    /**
     * Acks the first binary frame, then closes the socket — a deterministic drop that drives the foreground
     * loop into a reconnect, and so into a fresh credential pull.
     */
    private static final class DropAfterFirstAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicInteger received = new AtomicInteger();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            int n = received.incrementAndGet();
            try {
                if (n == 1) {
                    client.sendBinary(okFrame(0L));
                    client.close();
                }
            } catch (IOException ignored) {
                // best-effort: the connection died under us
            }
        }

        private static byte[] okFrame(long wireSeq) {
            ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 2).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(wireSeq);
            bb.putShort((short) 0);
            return bb.array();
        }
    }
}
