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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.network.IOOperation;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketReadinessWaiter;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import io.questdb.client.test.tools.TestWebSocketClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The I/O thread sends a schema lookup's DESCRIBE request, and a failed send on
 * that thread ends the connection: the loop reconnects and replays every unacked
 * frame. The request must therefore wait for the socket as long as a data frame
 * does, while the producer stops waiting at its own lookup budget.
 * <p>
 * Transport fault injection: {@link StallSocket} answers the real WebSocket
 * upgrade, then accepts nothing while stalled. The client's write wait blocks
 * until the test releases it and then ends as the OS wait would. The frames
 * each connection's socket received are the oracle. Three data frames are on
 * the first connection, unacked, before each scenario starts, so a reconnect
 * shows as their replay.
 */
public class CursorWebSocketSchemaSendTimeoutTest {
    private static final int FRAME_COUNT = 3;
    // The I/O timeout of the clients, the one data frames use. Not the 600_000 ms
    // of DefaultHttpClientConfiguration, so a send that used that default shows.
    private static final int IO_TIMEOUT_MILLIS = 60_000;
    private static final int PAYLOAD_LEN = 8;
    private static final String UNACKED_FRAMES = "[data:0, data:1, data:2]";

    @Test(timeout = 30_000L)
    public void testCloseBreaksBlockedDescribeAndReleasesLookup() throws Exception {
        runScenario(false, h -> {
            // Nothing releases the stall, and the budget outlasts the test: only
            // close() can end the send and the lookup.
            h.firstSocket.stall(30_000);
            Lookup lookup = h.lookup("t", 60_000);
            Thread ioThread = h.firstSocket.awaitWriteBlocked();
            FutureTask<Void> close = new FutureTask<>(h.loop::close, null);
            Thread closer = new Thread(close, "describe-closer");
            closer.start();
            assertLookupFails(lookup, "t", "schema coordinator is closed");
            close.get(10, TimeUnit.SECONDS);
            closer.join(TimeUnit.SECONDS.toMillis(5));
            Assert.assertEquals("traffic must close exactly once", 1, h.first.trafficCloseCount.get());
            Assert.assertSame("the closer must break the traffic", closer, h.first.trafficCloseThread.get());
            ioThread.join(TimeUnit.SECONDS.toMillis(5));
            Assert.assertFalse("I/O thread did not exit", ioThread.isAlive());
            Assert.assertNull(h.loop.getTerminalError());
            Assert.assertEquals(0, h.reconnects.get());
        });
    }

    @Test(timeout = 30_000L)
    public void testDescribeSendErrorAbandonsLookup() throws Exception {
        assertLookupAbandonedAtReconnect(Fault.SEND_ERROR, 30_000);
    }

    @Test(timeout = 30_000L)
    public void testDescribeSendErrorReconnects() throws Exception {
        assertDescribeResentAfterReconnect(Fault.SEND_ERROR, 30_000);
    }

    @Test(timeout = 30_000L)
    public void testDescribeStalledPastIoTimeoutAbandonsLookup() throws Exception {
        assertLookupAbandonedAtReconnect(Fault.STALL_PAST_IO_TIMEOUT, 30_000);
        assertLookupAbandonedAtReconnect(Fault.STALL_PAST_IO_TIMEOUT, Long.MAX_VALUE);
    }

    @Test(timeout = 30_000L)
    public void testDescribeStalledPastIoTimeoutReconnects() throws Exception {
        // A finite budget, and the largest, which waits indefinitely: either way the
        // send gives up once the socket made no progress for the I/O timeout.
        assertDescribeResentAfterReconnect(Fault.STALL_PAST_IO_TIMEOUT, 30_000);
        assertDescribeResentAfterReconnect(Fault.STALL_PAST_IO_TIMEOUT, Long.MAX_VALUE);
    }

    @Test(timeout = 30_000L)
    public void testDescribeStalledPastLookupBudgetKeepsConnection() throws Exception {
        runScenario(false, h -> {
            // budget 1_000 < stall 30_000 < I/O timeout 60_000
            h.firstSocket.stall(30_000);
            Lookup lookup = h.lookup("t", 1_000);
            h.firstSocket.awaitWriteBlocked();
            assertLookupFails(lookup, "t", "schema lookup timed out");
            // The lookup ended on its own budget; the DESCRIBE still waits for the socket.
            Assert.assertEquals(UNACKED_FRAMES, h.firstSocket.wire());
            h.firstSocket.release();
            h.publish(FRAME_COUNT);
            h.awaitFrameOnEitherWire("data:" + FRAME_COUNT);
            Assert.assertEquals("[data:0, data:1, data:2, describe:t, data:3]", h.firstSocket.wire());
            Assert.assertEquals("[]", h.secondSocket.wire());
            Assert.assertEquals(0, h.reconnects.get());
            h.loop.checkError();
        });
    }

    @Test(timeout = 30_000L)
    public void testDescribeStalledWithinLookupBudgetGetsItsReply() throws Exception {
        runScenario(false, h -> {
            // stall 10_000 < budget 30_000 < I/O timeout 60_000
            h.firstSocket.stall(10_000);
            Lookup lookup = h.lookup("t", 30_000);
            h.firstSocket.awaitWriteBlocked();
            h.firstSocket.release();
            QwpSchemaResponse response = lookup.response();
            Assert.assertEquals("[data:0, data:1, data:2, describe:t]", h.firstSocket.wire());
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getResult());
            Assert.assertEquals(1, response.getTableId());
            Assert.assertSame(response, h.loop.peekSchema("t"));
            Assert.assertEquals("[]", h.secondSocket.wire());
            Assert.assertEquals(0, h.reconnects.get());
        });
    }

    @Test(timeout = 30_000L)
    public void testLookupQueuedBehindBlockedDescribeGetsItsOwnReply() throws Exception {
        runScenario(false, h -> {
            // budget 1_000 < stall 30_000 < I/O timeout 60_000
            h.firstSocket.stall(30_000);
            Lookup late = h.lookup("a", 1_000);
            h.firstSocket.awaitWriteBlocked();
            assertLookupFails(late, "a", "schema lookup timed out");
            // Queued while the I/O thread is still inside the send of "a".
            Lookup queued = h.lookup("b", 30_000);
            queued.awaitWaiting();
            h.firstSocket.release();
            QwpSchemaResponse response = queued.response();
            Assert.assertEquals("[data:0, data:1, data:2, describe:a, describe:b]", h.firstSocket.wire());
            List<Long> requestIds = h.firstSocket.describeRequestIds();
            Assert.assertEquals(2, requestIds.size());
            Assert.assertNotEquals(requestIds.get(0), requestIds.get(1));
            // The socket answers each DESCRIBE with its ordinal as the table id: "b" got
            // its own reply, and the late reply to "a" did not reach the cache.
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getResult());
            Assert.assertEquals(2, response.getTableId());
            Assert.assertNull(h.loop.peekSchema("a"));
            Assert.assertEquals("[]", h.secondSocket.wire());
            Assert.assertEquals(0, h.reconnects.get());
        });
    }

    /**
     * The loop's default, which STRICT keeps: a lookup survives the connection
     * loss, and the next connection sends it again.
     */
    private static void assertDescribeResentAfterReconnect(Fault fault, long budgetMillis) throws Exception {
        runScenario(false, h -> {
            String scenario = fault + ", budget=" + budgetMillis;
            QwpSchemaResponse response = h.lookUpThrough(fault, budgetMillis).response();
            Assert.assertEquals(scenario, UNACKED_FRAMES, h.firstSocket.wire());
            Assert.assertEquals(scenario, "[data:0, data:1, data:2, describe:t]", h.secondSocket.wire());
            Assert.assertEquals(scenario, 1, h.reconnects.get());
            Assert.assertEquals(scenario, QwpSchemaProtocol.RESULT_KNOWN, response.getResult());
            Assert.assertEquals(scenario, 1, response.getTableId());
            Assert.assertSame(scenario, response, h.loop.peekSchema("t"));
            Assert.assertNull(scenario, h.loop.getTerminalError());
        });
    }

    /**
     * AUTO, the default schema mode, drops the lookup when the connection is
     * lost, so the producer falls back at once instead of waiting its budget out.
     */
    private static void assertLookupAbandonedAtReconnect(Fault fault, long budgetMillis) throws Exception {
        runScenario(true, h -> {
            String scenario = fault + ", budget=" + budgetMillis;
            assertLookupFails(h.lookUpThrough(fault, budgetMillis), "t", "connection lost during schema lookup");
            h.publish(FRAME_COUNT);
            String replayed = "[data:0, data:1, data:2, data:3]";
            Assert.assertEquals(scenario, replayed, h.secondSocket.awaitWire(replayed));
            Assert.assertEquals(scenario, UNACKED_FRAMES, h.firstSocket.wire());
            Assert.assertEquals(scenario, 1, h.reconnects.get());
            Assert.assertNull(scenario, h.loop.peekSchema("t"));
            Assert.assertNull(scenario, h.loop.getTerminalError());
        });
    }

    private static void assertLookupFails(Lookup lookup, String table, String detail) throws Exception {
        LineSenderSchemaException e = lookup.failure();
        Assert.assertEquals(lookup.description, LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, e.getReason());
        Assert.assertEquals(
                lookup.description,
                "schema lookup failed [reason=SCHEMA_UNAVAILABLE, table=" + table + ", detail=" + detail + ']',
                e.getMessage());
    }

    private static void runScenario(boolean isAbandoningLookupOnDisconnect, Scenario scenario) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            StallSocket firstSocket = new StallSocket();
            StallSocket secondSocket = new StallSocket();
            long payload = Unsafe.malloc(PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
                 StallClient first = StallClient.upgraded(firstSocket);
                 StallClient second = StallClient.upgraded(secondSocket)) {
                AtomicInteger reconnects = new AtomicInteger();
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(first, engine, 0L, () -> {
                    reconnects.incrementAndGet();
                    return second;
                }, 0L, 0L);
                loop.setAbandonSchemaLookupOnDisconnect(isAbandoningLookupOnDisconnect);
                Harness h = new Harness(engine, loop, payload, first, firstSocket, secondSocket, reconnects);
                try {
                    for (int i = 0; i < FRAME_COUNT; i++) {
                        h.publish(i);
                    }
                    loop.start();
                    Assert.assertEquals(UNACKED_FRAMES, firstSocket.awaitWire(UNACKED_FRAMES));
                    scenario.run(h);
                } finally {
                    firstSocket.release();
                    loop.close();
                    h.joinLookups();
                }
            } finally {
                Unsafe.free(payload, PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private enum Fault {
        // The socket accepts nothing for longer than the client's I/O timeout.
        STALL_PAST_IO_TIMEOUT,
        // send() fails, as on a reset connection.
        SEND_ERROR
    }

    @FunctionalInterface
    private interface Scenario {
        void run(Harness h) throws Exception;
    }

    private static final class Harness {
        private final CursorSendEngine engine;
        private final StallClient first;
        private final StallSocket firstSocket;
        private final CursorWebSocketSendLoop loop;
        private final List<Lookup> lookups = new ArrayList<>();
        private final long payload;
        private final AtomicInteger reconnects;
        private final StallSocket secondSocket;

        private Harness(
                CursorSendEngine engine,
                CursorWebSocketSendLoop loop,
                long payload,
                StallClient first,
                StallSocket firstSocket,
                StallSocket secondSocket,
                AtomicInteger reconnects
        ) {
            this.engine = engine;
            this.loop = loop;
            this.payload = payload;
            this.first = first;
            this.firstSocket = firstSocket;
            this.secondSocket = secondSocket;
            this.reconnects = reconnects;
        }

        private void awaitFrameOnEitherWire(String frame) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!firstSocket.frames().contains(frame) && !secondSocket.frames().contains(frame)) {
                Assert.assertTrue(frame + " reached neither wire", System.nanoTime() < deadline);
                Thread.yield();
            }
        }

        private void joinLookups() throws InterruptedException {
            for (int i = 0, n = lookups.size(); i < n; i++) {
                lookups.get(i).join();
            }
        }

        /**
         * Starts a lookup of table "t" whose DESCRIBE meets {@code fault} on the
         * first connection.
         */
        private Lookup lookUpThrough(Fault fault, long budgetMillis) throws InterruptedException {
            if (fault == Fault.SEND_ERROR) {
                firstSocket.breakConnection();
                return lookup("t", budgetMillis);
            }
            firstSocket.stall(2 * IO_TIMEOUT_MILLIS);
            Lookup lookup = lookup("t", budgetMillis);
            firstSocket.awaitWriteBlocked();
            firstSocket.release();
            return lookup;
        }

        private Lookup lookup(String table, long budgetMillis) {
            Lookup lookup = new Lookup(loop, table, budgetMillis);
            lookups.add(lookup);
            return lookup;
        }

        private void publish(long value) {
            Unsafe.getUnsafe().putLong(payload, value);
            Assert.assertEquals(value, engine.appendBlocking(payload, PAYLOAD_LEN));
        }
    }

    private static final class Lookup {
        private final String description;
        private final FutureTask<QwpSchemaResponse> task;
        private final Thread thread;

        private Lookup(CursorWebSocketSendLoop loop, String table, long budgetMillis) {
            description = "lookup of " + table + " with budget " + budgetMillis + " ms";
            task = new FutureTask<>(() -> loop.resolveSchema(table, budgetMillis));
            thread = new Thread(task, "schema-lookup-" + table);
            thread.start();
        }

        /**
         * Waits until the lookup is queued: resolveSchema() waits only for the
         * reply, after it queued the request.
         */
        private void awaitWaiting() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (thread.getState() != Thread.State.TIMED_WAITING) {
                Assert.assertTrue(description + " never waited", thread.isAlive() && System.nanoTime() < deadline);
                Thread.yield();
            }
        }

        private LineSenderSchemaException failure() throws Exception {
            try {
                QwpSchemaResponse response = task.get(10, TimeUnit.SECONDS);
                throw new AssertionError(description + " returned table id " + response.getTableId());
            } catch (ExecutionException e) {
                if (e.getCause() instanceof LineSenderSchemaException) {
                    return (LineSenderSchemaException) e.getCause();
                }
                throw new AssertionError(description + " failed with an unexpected error", e.getCause());
            }
        }

        private void join() throws InterruptedException {
            thread.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse(description + " did not finish", thread.isAlive());
        }

        private QwpSchemaResponse response() throws Exception {
            return task.get(10, TimeUnit.SECONDS);
        }
    }

    private static final class StallClient extends TestWebSocketClient {
        private final StallSocket socket;
        private final AtomicInteger trafficCloseCount = new AtomicInteger();
        private final AtomicReference<Thread> trafficCloseThread = new AtomicReference<>();

        private StallClient(StallSocket socket) {
            super(new DefaultHttpClientConfiguration() {
                @Override
                public int getTimeout() {
                    return IO_TIMEOUT_MILLIS;
                }
            }, (nf, log) -> socket);
            this.socket = socket;
        }

        @Override
        public void closeTraffic() {
            trafficCloseThread.compareAndSet(null, Thread.currentThread());
            trafficCloseCount.incrementAndGet();
            super.closeTraffic();
        }

        private static StallClient upgraded(StallSocket socket) {
            StallClient client = new StallClient(socket);
            boolean isUpgraded = false;
            try {
                client.requestQwpSchema();
                client.connect("localhost", 9000);
                client.upgrade("/write/v4", 1_000, null);
                Assert.assertTrue(client.isQwpSchemaEnabled());
                isUpgraded = true;
                return client;
            } finally {
                if (!isUpgraded) {
                    client.close();
                }
            }
        }

        @Override
        protected void ioWait(int timeout, int op) {
            if (op == IOOperation.WRITE && !socket.awaitWritable(timeout)) {
                // The timeout the production wait reports.
                dieWaiting(0);
            }
        }

        @Override
        protected void setupIoWait() {
        }
    }

    /**
     * Fault injection only: answers the WebSocket upgrade with schema support,
     * then records the frames the client sends. While stalled it accepts
     * nothing (send() returns 0, a full socket); once broken every send fails.
     * Each DESCRIBE gets a KNOWN reply whose table id is the ordinal of that
     * DESCRIBE on this socket.
     */
    private static final class StallSocket implements Socket {
        private final CountDownLatch releaseWrite = new CountDownLatch(1);
        private final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        private final CountDownLatch writeBlocked = new CountDownLatch(1);
        private volatile Thread blockedWriter;
        private int describeReplyCount;
        private byte[] inbound = new byte[0];
        private int inboundPosition;
        private volatile boolean isBroken;
        private volatile boolean isStalled;
        private boolean isUpgraded;
        private volatile int stallMillis;

        @Override
        public void close() {
        }

        @Override
        public void closeTraffic() {
            // As shutdown(2) would: the blocked wait returns and the next send fails.
            isBroken = true;
            releaseWrite.countDown();
        }

        @Override
        public int getFd() {
            return 1;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void of(int fd) {
        }

        @Override
        public synchronized int recv(long bufferPtr, int bufferLen) {
            int count = Math.min(bufferLen, inbound.length - inboundPosition);
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putByte(bufferPtr + i, inbound[inboundPosition++]);
            }
            return count;
        }

        @Override
        public synchronized int send(long bufferPtr, int bufferLen) {
            if (!isUpgraded) {
                isUpgraded = true;
                String request = new String(copy(bufferPtr, bufferLen), StandardCharsets.US_ASCII);
                appendInbound(upgradeResponse(header(request, "Sec-WebSocket-Key:")));
                return bufferLen;
            }
            if (isBroken) {
                return -1;
            }
            if (isStalled) {
                return 0;
            }
            byte[] bytes = copy(bufferPtr, bufferLen);
            wire.write(bytes, 0, bytes.length);
            answerNewDescribes();
            notifyAll();
            return bufferLen;
        }

        @Override
        public void startTlsSession(CharSequence peerName, SocketReadinessWaiter waiter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsTls() {
            return false;
        }

        @Override
        public int tlsIO(int readinessFlags) {
            return 0;
        }

        @Override
        public boolean wantsTlsWrite() {
            return false;
        }

        private static byte[] copy(long address, int length) {
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(address + i);
            }
            return bytes;
        }

        private static String header(String request, String name) {
            int start = request.indexOf(name) + name.length();
            int end = request.indexOf("\r\n", start);
            return request.substring(start, end).trim();
        }

        private static byte[] knownSchemaReply(long requestId, int tableId) {
            byte[] column = "x".getBytes(StandardCharsets.UTF_8);
            int payloadLength = 1 + 8 + 1 + 4 + 8 + 2 + 2 + 2 + column.length + 4 + 2;
            ByteBuffer message = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payloadLength)
                    .order(ByteOrder.LITTLE_ENDIAN);
            message.putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                    .putShort((short) 0).putInt(payloadLength).put(QwpSchemaProtocol.KIND_SCHEMA)
                    .putLong(requestId).put((byte) QwpSchemaProtocol.RESULT_KNOWN).putInt(tableId)
                    .putLong(1).putShort((short) -1).putShort((short) 1)
                    .putShort((short) column.length).put(column).putInt(ColumnType.LONG).putShort((short) 0);
            byte[] payload = message.array();
            // A server frame is unmasked: FIN + binary opcode, then a 7-bit length.
            byte[] frame = new byte[2 + payload.length];
            frame[0] = (byte) 0x82;
            frame[1] = (byte) payload.length;
            System.arraycopy(payload, 0, frame, 2, payload.length);
            return frame;
        }

        private static byte[] upgradeResponse(String key) {
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                String accept = Base64.getEncoder().encodeToString(sha1.digest(
                        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)));
                return ("HTTP/1.1 101 Switching Protocols\r\n"
                        + "Upgrade: websocket\r\n"
                        + "Connection: Upgrade\r\n"
                        + "Sec-WebSocket-Accept: " + accept + "\r\n"
                        + "X-QWP-Schema: enabled\r\n"
                        + "\r\n").getBytes(StandardCharsets.US_ASCII);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private void answerNewDescribes() {
            List<Long> requestIds = new ArrayList<>();
            decode(new ArrayList<>(), requestIds);
            while (describeReplyCount < requestIds.size()) {
                long requestId = requestIds.get(describeReplyCount);
                describeReplyCount++;
                appendInbound(knownSchemaReply(requestId, describeReplyCount));
            }
        }

        private void appendInbound(byte[] bytes) {
            int unread = inbound.length - inboundPosition;
            byte[] merged = new byte[unread + bytes.length];
            System.arraycopy(inbound, inboundPosition, merged, 0, unread);
            System.arraycopy(bytes, 0, merged, unread, bytes.length);
            inbound = merged;
            inboundPosition = 0;
        }

        /**
         * Waits until the frames this socket received read {@code expected}, for
         * at most 5 s, and returns what they read last.
         */
        private synchronized String awaitWire(String expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            String actual = wire();
            while (!actual.equals(expected)) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
                actual = wire();
            }
            return actual;
        }

        /**
         * The client's wait for the socket to become writable, which it calls
         * only after a send that wrote nothing. Blocks until the test releases it,
         * then ends as the OS wait would: the socket drained when the wait's
         * timeout covers the stall, else the wait timed out first. A broken
         * socket ends the wait, and the next send fails.
         */
        private boolean awaitWritable(int timeoutMillis) {
            blockedWriter = Thread.currentThread();
            writeBlocked.countDown();
            try {
                if (!releaseWrite.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("the stalled write was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            if (isBroken) {
                return true;
            }
            if (timeoutMillis < stallMillis) {
                return false;
            }
            isStalled = false;
            return true;
        }

        /**
         * Waits until a send meets the stall and returns the thread that waits
         * for the socket.
         */
        private Thread awaitWriteBlocked() throws InterruptedException {
            Assert.assertTrue("no send met the stalled socket", writeBlocked.await(5, TimeUnit.SECONDS));
            return blockedWriter;
        }

        private void breakConnection() {
            isBroken = true;
        }

        /**
         * Decodes the masked client frames this socket received into labels:
         * {@code data:<n>} for an 8-byte test frame, {@code describe:<table>} for
         * a DESCRIBE, whose request id goes to {@code describeRequestIds}.
         */
        private void decode(List<String> labels, List<Long> describeRequestIds) {
            byte[] b = wire.toByteArray();
            int p = 0;
            while (p + 2 <= b.length) {
                int opcode = b[p] & 0x0F;
                int len = b[p + 1] & 0x7F;
                boolean isMasked = (b[p + 1] & 0x80) != 0;
                int header = 2;
                if (len == 126) {
                    len = ((b[p + 2] & 0xff) << 8) | (b[p + 3] & 0xff);
                    header = 4;
                } else if (len == 127) {
                    throw new AssertionError("unexpected 64-bit frame length");
                }
                int maskOffset = p + header;
                int payloadOffset = maskOffset + (isMasked ? 4 : 0);
                if (payloadOffset + len > b.length) {
                    break;
                }
                byte[] payload = new byte[len];
                for (int i = 0; i < len; i++) {
                    payload[i] = (byte) (b[payloadOffset + i] ^ (isMasked ? b[maskOffset + (i & 3)] : 0));
                }
                p = payloadOffset + len;
                ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
                if (opcode == 2 && len == PAYLOAD_LEN) {
                    labels.add("data:" + buffer.getLong(0));
                } else if (opcode == 2 && len > QwpConstants.HEADER_SIZE
                        && buffer.getInt(0) == QwpConstants.MAGIC_MESSAGE
                        && payload[QwpConstants.HEADER_OFFSET_FLAGS] == QwpSchemaProtocol.FLAG_CONTROL
                        && payload[QwpConstants.HEADER_SIZE] == QwpSchemaProtocol.KIND_DESCRIBE) {
                    describeRequestIds.add(buffer.getLong(QwpConstants.HEADER_SIZE + 1));
                    int nameLength = buffer.getShort(QwpConstants.HEADER_SIZE + 9) & 0xffff;
                    labels.add("describe:" + new String(
                            payload, QwpConstants.HEADER_SIZE + 11, nameLength, StandardCharsets.UTF_8));
                } else if (opcode == 8) {
                    labels.add("close");
                } else {
                    labels.add("opcode:" + opcode + "/" + len);
                }
            }
        }

        private synchronized List<Long> describeRequestIds() {
            List<Long> requestIds = new ArrayList<>();
            decode(new ArrayList<>(), requestIds);
            return requestIds;
        }

        private synchronized List<String> frames() {
            List<String> labels = new ArrayList<>();
            decode(labels, new ArrayList<>());
            return labels;
        }

        private void release() {
            releaseWrite.countDown();
        }

        private void stall(int millis) {
            stallMillis = millis;
            isStalled = true;
        }

        private synchronized String wire() {
            return frames().toString();
        }
    }
}
