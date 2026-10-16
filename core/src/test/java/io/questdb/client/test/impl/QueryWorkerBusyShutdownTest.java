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

package io.questdb.client.test.impl;

import io.questdb.client.Completion;
import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.impl.QueryClientPool;
import io.questdb.client.impl.QueryWorker;
import io.questdb.client.impl.QuestDBImpl;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A pool shutdown whose bounded join expires while the worker's dispatch thread is still inside
 * {@code execute()} must not close the query client under it. {@code QwpQueryClient.close()} frees what a
 * running {@code execute()} uses -- its bind buffer and the connection a reconnect walk is building -- and a
 * walk parked in a native connect or WebSocket upgrade ignores the shutdown interrupt. Closing anyway let the
 * walk reach a healthy endpoint, then write the binds into the freed buffer (SIGSEGV) or publish an I/O
 * thread and connection on the closed client (leak). The dispatch thread now closes the client itself once
 * the call returns.
 */
public class QueryWorkerBusyShutdownTest {
    private static final String SENDER_CFG = "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    @Test(timeout = 60_000)
    public void testShutdownLeavesTheCloseToADispatchThreadStillInsideExecute() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer(new AtomicBoolean())) {
                AtomicInteger closes = new AtomicInteger();
                AtomicReference<Thread> dispatchThread = new AtomicReference<>();
                CountDownLatch inHandler = new CountDownLatch(1);
                CountDownLatch releaseHandler = new CountDownLatch(1);
                QuestDBImpl db = new QuestDBImpl(
                        SENDER_CFG, "ws::addr=localhost:" + server.getPort() + ";",
                        0, 1, 1, 1,
                        10_000L, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
                        slot -> {
                            throw new AssertionError("no sender expected");
                        },
                        client -> {
                            client.setBeforeCloseHookForTest(closes::incrementAndGet);
                            client.connect();
                        });
                try {
                    Query q = db.borrowQuery();
                    // The handler runs on the dispatch thread inside execute() and ignores the shutdown
                    // interrupt, like a reconnect walk parked in a native wait.
                    q.sql("SELECT 1").handler(new Handler() {
                        @Override
                        public void onExecDone(short opType, long rowsAffected) {
                            dispatchThread.set(Thread.currentThread());
                            inHandler.countDown();
                            awaitUninterruptibly(releaseHandler);
                        }
                    }).submit();
                    Assert.assertTrue("the query never reached its handler", inHandler.await(10, TimeUnit.SECONDS));

                    db.close(); // the join gives up after SHUTDOWN_JOIN_MILLIS with the thread still in execute()
                    Assert.assertTrue(dispatchThread.get().isAlive());
                    Assert.assertEquals("the pool must not close the client while its dispatch thread is inside "
                            + "execute()", 0, closes.get());

                    releaseHandler.countDown();
                    dispatchThread.get().join(TimeUnit.SECONDS.toMillis(10));
                    Assert.assertFalse("the dispatch thread must exit once execute() returns",
                            dispatchThread.get().isAlive());
                    Assert.assertEquals("the dispatch thread must close its client on the way out", 1, closes.get());
                } finally {
                    releaseHandler.countDown();
                    db.close();
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testShutdownDuringAReconnectWalkWithBinds() throws Exception {
        assertShutdownDuringAReconnectWalkIsSafe(true);
    }

    @Test(timeout = 60_000)
    public void testShutdownDuringAReconnectWalkWithoutBinds() throws Exception {
        assertShutdownDuringAReconnectWalkIsSafe(false);
    }

    /**
     * The failure from the review: a pooled client whose failover reconnect failed reconnects on its next
     * execute(). The walk stalls on an endpoint that accepts TCP but never answers the upgrade, outlasting the
     * 5 s shutdown join, then reaches a healthy endpoint after QuestDB.close() returned.
     */
    private static void assertShutdownDuringAReconnectWalkIsSafe(boolean binds) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicBoolean dropNextQuery = new AtomicBoolean();
            AtomicBoolean tokenOutage = new AtomicBoolean();
            try (TestWebSocketServer healthy = startServer(dropNextQuery);
                 StallingEndpoint stalling = new StallingEndpoint()) {
                // auth_timeout_ms bounds the stalled upgrade: above the 5 s shutdown join, so the walk outlives
                // it, and short enough to keep the test quick
                String cfg = "ws::addr=localhost:" + healthy.getPort() + ",localhost:" + stalling.port()
                        + ";sender_pool_min=0;query_pool_min=1;query_pool_max=1;failover_backoff_initial_ms=0"
                        + ";auth_timeout_ms=6500;";
                QuestDB db = QuestDB.connect(cfg, () -> {
                    if (tokenOutage.get()) {
                        throw new IllegalStateException("token outage");
                    }
                    return "TOKEN";
                });
                Thread dispatchThread = dispatchThread(db);
                QwpQueryClient client = pooledWorker(db).client();
                try {
                    // a failover whose reconnect fails leaves the pooled client disconnected
                    dropNextQuery.set(true);
                    tokenOutage.set(true);
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT 1").handler(new Handler()).submit().await();
                        Assert.fail("the failover reconnect must fail");
                    } catch (QueryException expected) {
                        // the slot reconnects on its next execute()
                    }
                    tokenOutage.set(false);
                    Assert.assertFalse(client.isConnected());

                    // the next execute() walks the endpoints and parks on the stalling one
                    Query q = db.borrowQuery();
                    q.sql(binds ? "SELECT $1" : "SELECT 1").handler(new Handler());
                    if (binds) {
                        q.binds(b -> b.setLong(0, 42L));
                    }
                    Completion completion = q.submit();
                    Assert.assertTrue("the reconnect walk never reached the stalling endpoint",
                            stalling.awaitAccept(10, TimeUnit.SECONDS));
                    Assert.assertFalse(completion.isDone());

                    db.close(); // returns after the 5 s join, with the walk still parked
                    dispatchThread.join(TimeUnit.SECONDS.toMillis(30)); // the walk resumes, then the thread exits
                    Assert.assertFalse("the dispatch thread must exit", dispatchThread.isAlive());
                    Assert.assertFalse("a closed client must not stay connected", client.isConnected());
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (healthy.liveConnectionCount() > 0 && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    Assert.assertEquals("the connection the walk opened must be closed", 0, healthy.liveConnectionCount());
                } finally {
                    db.close();
                    dispatchThread.join(TimeUnit.SECONDS.toMillis(30));
                }
            }
        });
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (!latch.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("test never released the handler");
                    }
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static byte[] buildExecDone(byte[] queryRequest) {
        int bodyLen = 1 + 8 + 1 + 1; // msg_kind + request_id + op_type + rows_affected varint
        byte[] frame = new byte[QwpConstants.HEADER_SIZE + bodyLen];
        ByteBuffer bb = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 'Q').put((byte) 'W').put((byte) 'P').put((byte) '1');
        bb.put((byte) 1);       // version
        bb.put((byte) 0);       // flags
        bb.putShort((short) 0); // table_count
        bb.putInt(bodyLen);     // payload_length
        bb.put(QwpEgressMsgKind.EXEC_DONE);
        bb.put(queryRequest, 1, 8); // echo request_id verbatim
        bb.put((byte) 0);       // op_type
        bb.put((byte) 0);       // rows_affected = 0
        return frame;
    }

    private static Thread dispatchThread(QuestDB db) throws Exception {
        Field f = QueryWorker.class.getDeclaredField("thread");
        f.setAccessible(true);
        return (Thread) f.get(pooledWorker(db));
    }

    @SuppressWarnings("unchecked")
    private static QueryWorker pooledWorker(QuestDB db) throws Exception {
        QueryClientPool pool = ((QuestDBImpl) db).getQueryPoolForTesting();
        Field f = QueryClientPool.class.getDeclaredField("all");
        f.setAccessible(true);
        List<QueryWorker> all = (List<QueryWorker>) f.get(pool);
        Assert.assertEquals(1, all.size());
        return all.get(0);
    }

    private static TestWebSocketServer startServer(AtomicBoolean dropNextQuery) throws IOException, InterruptedException {
        TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                if (data.length == 0 || data[0] != QwpEgressMsgKind.QUERY_REQUEST) {
                    return;
                }
                if (dropNextQuery.compareAndSet(true, false)) {
                    // a transport failure mid-query; close from another thread, ClientHandler.close() joins
                    // this read thread
                    Thread dropper = new Thread(client::close, "drop-connection");
                    dropper.setDaemon(true);
                    dropper.start();
                    return;
                }
                try {
                    client.sendBinary(buildExecDone(data));
                } catch (IOException e) {
                    // surfaces to the client as a transport error
                }
            }
        });
        server.setSendServerInfo(true);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static class Handler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }
    }

    /**
     * Completes TCP connects and never answers the WebSocket upgrade, like an overloaded or wedged node.
     */
    private static final class StallingEndpoint implements AutoCloseable {
        private final CountDownLatch accepted = new CountDownLatch(1);
        private final List<Socket> held = new CopyOnWriteArrayList<>();
        private final ServerSocket serverSocket;

        StallingEndpoint() throws IOException {
            serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread acceptor = new Thread(() -> {
                try {
                    while (true) {
                        held.add(serverSocket.accept());
                        accepted.countDown();
                    }
                } catch (IOException ignored) {
                    // closed
                }
            }, "stalling-endpoint");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            for (Socket s : held) {
                s.close();
            }
        }

        boolean awaitAccept(long timeout, TimeUnit unit) throws InterruptedException {
            return accepted.await(timeout, unit);
        }

        int port() {
            return serverSocket.getLocalPort();
        }
    }
}
