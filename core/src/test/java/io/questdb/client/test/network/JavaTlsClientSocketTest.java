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

package io.questdb.client.test.network;

import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.network.IOOperation;
import io.questdb.client.network.JavaTlsClientSocket;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.network.SocketReadinessWaiter;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

public class JavaTlsClientSocketTest {

    private static final String TLS_BUFFER_SIZE_PROP = "questdb.experimental.tls.buffersize";

    @Test
    public void testRecvGrowsTlsOutputBufferAndDrainsRemainder() throws Exception {
        String previous = System.getProperty(TLS_BUFFER_SIZE_PROP);
        try {
            System.setProperty(TLS_BUFFER_SIZE_PROP, "8");
            TestUtils.assertMemoryLeak(() -> {
                try (JavaTlsClientSocket socket = newSocket()) {
                    invoke(socket, "prepareInternalBuffers");
                    setField(socket, "sslEngine", new OverflowThenPayloadSslEngine("abcdef".getBytes()));
                    setIntField(socket, "state", 2);

                    ByteBuffer unwrapInputBuffer = getField(socket, "unwrapInputBuffer");
                    long unwrapInputBufferPtr = getLongField(socket, "unwrapInputBufferPtr");
                    for (int i = 0; i < 6; i++) {
                        Unsafe.getUnsafe().putByte(unwrapInputBufferPtr + i, (byte) ('0' + i));
                    }
                    unwrapInputBuffer.position(0);
                    unwrapInputBuffer.limit(6);

                    long out1 = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                    long out2 = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                    try {
                        int n1 = socket.recv(out1, 4);
                        assertEquals(4, n1);
                        assertBytes("abcd", out1, n1);

                        int n2 = socket.recv(out2, 4);
                        assertEquals(2, n2);
                        assertBytes("ef", out2, n2);

                        ByteBuffer unwrapOutputBuffer = getField(socket, "unwrapOutputBuffer");
                        assertEquals(0, unwrapOutputBuffer.position());
                    } finally {
                        Unsafe.free(out2, 4, MemoryTag.NATIVE_DEFAULT);
                        Unsafe.free(out1, 4, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            });
        } finally {
            if (previous == null) {
                System.clearProperty(TLS_BUFFER_SIZE_PROP);
            } else {
                System.setProperty(TLS_BUFFER_SIZE_PROP, previous);
            }
        }
    }

    @Test
    public void testRecvSpillsAndGrowsInternalBufferForOversizedRecord() throws Exception {
        String previous = System.getProperty(TLS_BUFFER_SIZE_PROP);
        try {
            System.setProperty(TLS_BUFFER_SIZE_PROP, "4");
            TestUtils.assertMemoryLeak(() -> {
                try (JavaTlsClientSocket socket = newSocket()) {
                    invoke(socket, "prepareInternalBuffers");
                    setField(socket, "sslEngine", new RealisticOverflowSslEngine("012345".getBytes()));
                    setIntField(socket, "state", 2);

                    ByteBuffer unwrapInputBuffer = getField(socket, "unwrapInputBuffer");
                    long unwrapInputBufferPtr = getLongField(socket, "unwrapInputBufferPtr");
                    for (int i = 0; i < 4; i++) {
                        Unsafe.getUnsafe().putByte(unwrapInputBufferPtr + i, (byte) ('a' + i));
                    }
                    unwrapInputBuffer.position(0);
                    unwrapInputBuffer.limit(4);

                    long out1 = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                    long out2 = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                    try {
                        int n1 = socket.recv(out1, 4);
                        assertEquals(4, n1);
                        assertBytes("0123", out1, n1);

                        // Caller's 4-byte buffer forced a spill into the internal buffer; the internal
                        // buffer was 4 bytes too, so the spill path had to grow it (4 -> 8) before the
                        // SSLEngine could write the 6-byte payload.
                        ByteBuffer unwrapOutputBuffer = getField(socket, "unwrapOutputBuffer");
                        assertEquals(8, unwrapOutputBuffer.capacity());

                        int n2 = socket.recv(out2, 4);
                        assertEquals(2, n2);
                        assertBytes("45", out2, n2);
                    } finally {
                        Unsafe.free(out2, 4, MemoryTag.NATIVE_DEFAULT);
                        Unsafe.free(out1, 4, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            });
        } finally {
            if (previous == null) {
                System.clearProperty(TLS_BUFFER_SIZE_PROP);
            } else {
                System.setProperty(TLS_BUFFER_SIZE_PROP, previous);
            }
        }
    }

    @Test
    public void testRecvProcessesBufferedRecordAfterEmptyOkUnwrap() throws Exception {
        String previous = System.getProperty(TLS_BUFFER_SIZE_PROP);
        try {
            System.setProperty(TLS_BUFFER_SIZE_PROP, "32");
            TestUtils.assertMemoryLeak(() -> {
                try (JavaTlsClientSocket socket = newSocket()) {
                    invoke(socket, "prepareInternalBuffers");
                    setField(socket, "sslEngine", new TicketThenDataSslEngine("DATA12".getBytes()));
                    setIntField(socket, "state", 2);

                    ByteBuffer unwrapInputBuffer = getField(socket, "unwrapInputBuffer");
                    long unwrapInputBufferPtr = getLongField(socket, "unwrapInputBufferPtr");
                    // 12 bytes: first 6 simulate a post-handshake control record (e.g. TLS 1.3 NewSessionTicket)
                    // that consumes input but produces no plaintext, the next 6 bytes are a real data record.
                    for (int i = 0; i < 12; i++) {
                        Unsafe.getUnsafe().putByte(unwrapInputBufferPtr + i, (byte) ('a' + i));
                    }
                    unwrapInputBuffer.position(0);
                    unwrapInputBuffer.limit(12);

                    long out = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                    try {
                        int n = socket.recv(out, 16);
                        // recv must keep unwrapping after an OK-with-zero-output result and deliver the
                        // plaintext from the buffered data record.
                        assertEquals(6, n);
                        assertBytes("DATA12", out, n);
                    } finally {
                        Unsafe.free(out, 16, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            });
        } finally {
            if (previous == null) {
                System.clearProperty(TLS_BUFFER_SIZE_PROP);
            } else {
                System.setProperty(TLS_BUFFER_SIZE_PROP, previous);
            }
        }
    }

    /**
     * Regression test for the TLS handshake busy-spin / unbounded handshake.
     * On a non-blocking socket, a peer that completes TCP but stalls before
     * sending its half of the handshake leaves the engine in NEED_UNWRAP with
     * the socket returning "would block" (recv == 0). The handshake must hand
     * control to the readiness waiter -- which in production parks on the event
     * loop bounded by the connect deadline -- instead of re-reading in a tight
     * loop. Here the waiter stands in for that deadline: it records the wait
     * and then throws, exactly as the bounded ioWait() does once the budget is
     * spent. The method-level timeout fails the test if the handshake ever
     * busy-spins past the waiter (i.e. if the deadline-aware wait is removed).
     */
    @Test(timeout = 30_000)
    public void testHandshakeWaitsForReadabilityInsteadOfBusySpinning() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (JavaTlsClientSocket socket = newSocket()) {
                invoke(socket, "prepareInternalBuffers");
                setField(socket, "sslEngine", new StallingUnwrapSslEngine());
                // Mark the session as TLS so try-with-resources close() frees the internal buffers
                // allocated above. Without this the socket stays STATE_EMPTY and close() returns early,
                // leaking the 3x256KB NATIVE_TLS_RSS buffers.
                setIntField(socket, "state", 2);

                Method runHandshake = JavaTlsClientSocket.class.getDeclaredMethod(
                        "runHandshake", SocketReadinessWaiter.class);
                runHandshake.setAccessible(true);

                AtomicInteger readWaits = new AtomicInteger();
                AtomicInteger writeWaits = new AtomicInteger();
                SocketReadinessWaiter waiter = op -> {
                    if (op == IOOperation.READ) {
                        readWaits.incrementAndGet();
                    } else {
                        writeWaits.incrementAndGet();
                    }
                    // Stand in for the connect deadline firing inside ioWait().
                    throw new DeadlineReached();
                };

                try {
                    runHandshake.invoke(socket, waiter);
                    Assert.fail("runHandshake must not complete the handshake against a stalled peer");
                } catch (InvocationTargetException e) {
                    Assert.assertTrue(
                            "handshake must surface the readiness waiter's deadline, was: " + e.getCause(),
                            e.getCause() instanceof DeadlineReached);
                }

                Assert.assertEquals(
                        "handshake must wait for the socket to become readable instead of busy-spinning",
                        1, readWaits.get());
                Assert.assertEquals(
                        "a NEED_UNWRAP stall must not trigger a write wait", 0, writeWaits.get());
            }
        });
    }

    /**
     * Happy-path guard for the refactor: when the engine makes progress (a
     * complete record is available, unwrap returns OK and the handshake
     * finishes), runHandshake must complete without ever parking on socket
     * readiness. The would-block waits only fire on recv/send == 0, so a
     * responsive peer never triggers them.
     */
    @Test(timeout = 30_000)
    public void testHandshakeCompletesWithoutWaitingWhenEngineMakesProgress() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (JavaTlsClientSocket socket = newSocket()) {
                invoke(socket, "prepareInternalBuffers");
                setField(socket, "sslEngine", new ProgressingUnwrapSslEngine());
                // Mark the session as TLS so try-with-resources close() frees the internal buffers
                // allocated above. Without this the socket stays STATE_EMPTY and close() returns early,
                // leaking the 3x256KB NATIVE_TLS_RSS buffers.
                setIntField(socket, "state", 2);

                Method runHandshake = JavaTlsClientSocket.class.getDeclaredMethod(
                        "runHandshake", SocketReadinessWaiter.class);
                runHandshake.setAccessible(true);

                AtomicInteger waits = new AtomicInteger();
                SocketReadinessWaiter waiter = op -> waits.incrementAndGet();

                runHandshake.invoke(socket, waiter); // must return normally (handshake finished)

                Assert.assertEquals(
                        "a handshake that makes progress must not wait on socket readiness",
                        0, waits.get());
            }
        });
    }

    /**
     * Regression guard for the NOT_HANDSHAKING loop exit. Per the JSSE
     * contract, {@code getHandshakeStatus()} never returns FINISHED -- once a
     * delegated task is the TERMINAL handshake step, the re-polled status is
     * NOT_HANDSHAKING. runHandshake must treat that as completion; without
     * the explicit NOT_HANDSHAKING exit clause the status matches no switch
     * case and the loop busy-spins forever with no deadline escape (this
     * method's timeout is the tripwire).
     */
    @Test(timeout = 30_000)
    public void testHandshakeExitsOnNotHandshakingAfterTerminalDelegatedTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (JavaTlsClientSocket socket = newSocket()) {
                invoke(socket, "prepareInternalBuffers");
                TerminalDelegatedTaskSslEngine engine = new TerminalDelegatedTaskSslEngine();
                setField(socket, "sslEngine", engine);
                // Mark the session as TLS so try-with-resources close() frees the internal buffers
                // allocated above. Without this the socket stays STATE_EMPTY and close() returns early,
                // leaking the 3x256KB NATIVE_TLS_RSS buffers.
                setIntField(socket, "state", 2);

                Method runHandshake = JavaTlsClientSocket.class.getDeclaredMethod(
                        "runHandshake", SocketReadinessWaiter.class);
                runHandshake.setAccessible(true);

                AtomicInteger waits = new AtomicInteger();
                SocketReadinessWaiter waiter = op -> waits.incrementAndGet();

                runHandshake.invoke(socket, waiter); // must return: NOT_HANDSHAKING == done

                Assert.assertEquals("the terminal delegated task must run exactly once",
                        1, engine.tasksRun.get());
                Assert.assertEquals(
                        "completion via NOT_HANDSHAKING must not park on socket readiness",
                        0, waits.get());
            }
        });
    }

    private static void assertBytes(String expected, long ptr, int len) {
        Assert.assertEquals(expected.length(), len);
        for (int i = 0; i < len; i++) {
            assertEquals((byte) expected.charAt(i), Unsafe.getUnsafe().getByte(ptr + i));
        }
    }

    private static void invoke(Object obj, String methodName) throws Exception {
        Method method = obj.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        method.invoke(obj);
    }

    private static JavaTlsClientSocket newSocket() throws Exception {
        java.lang.reflect.Constructor<JavaTlsClientSocket> constructor = JavaTlsClientSocket.class.getDeclaredConstructor(
                NetworkFacade.class,
                org.slf4j.Logger.class,
                ClientTlsConfiguration.class
        );
        constructor.setAccessible(true);
        return constructor.newInstance(
                new NoOpNetworkFacade(),
                LoggerFactory.getLogger(JavaTlsClientSocketTest.class),
                ClientTlsConfiguration.INSECURE_NO_VALIDATION
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object obj, String fieldName) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
    }

    private static long getLongField(Object obj, String fieldName) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getLong(obj);
    }

    private static void setField(Object obj, String fieldName, Object value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    private static void setIntField(Object obj, String fieldName, int value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setInt(obj, value);
    }

    private static final class NoOpNetworkFacade extends NetworkFacadeImpl {
        @Override
        public int recvRaw(int fd, long buffer, int bufferLen) {
            return 0;
        }

        @Override
        public int sendRaw(int fd, long buffer, int bufferLen) {
            return 0;
        }
    }

    private static final class OverflowThenPayloadSslEngine extends StubSslEngine {
        private final byte[] payload;
        private int unwrapCalls;

        private OverflowThenPayloadSslEngine(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
            if (length == 0) {
                throw new IllegalArgumentException("no destination buffers");
            }
            unwrapCalls++;
            ByteBuffer dst = dsts[offset];
            if (unwrapCalls == 1) {
                return new SSLEngineResult(
                        SSLEngineResult.Status.BUFFER_OVERFLOW,
                        SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                        0,
                        0
                );
            }
            if (unwrapCalls > 2) {
                throw new IllegalStateException("unexpected extra unwrap call");
            }
            if (dst.remaining() < payload.length) {
                throw new IllegalStateException("destination should have been grown");
            }
            for (byte b : payload) {
                dst.put(b);
            }
            src.position(src.position() + payload.length);
            return new SSLEngineResult(
                    SSLEngineResult.Status.OK,
                    SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                    payload.length,
                    payload.length
            );
        }
    }

    private static final class RealisticOverflowSslEngine extends StubSslEngine {
        private final byte[] payload;
        private boolean payloadWritten;

        private RealisticOverflowSslEngine(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
            ByteBuffer dst = dsts[offset];
            if (dst.remaining() < payload.length) {
                return new SSLEngineResult(
                        SSLEngineResult.Status.BUFFER_OVERFLOW,
                        SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                        0,
                        0
                );
            }
            if (payloadWritten) {
                throw new IllegalStateException("payload already written");
            }
            int consumed = src.remaining();
            src.position(src.position() + consumed);
            payloadWritten = true;
            for (byte b : payload) {
                dst.put(b);
            }
            return new SSLEngineResult(
                    SSLEngineResult.Status.OK,
                    SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                    consumed,
                    payload.length
            );
        }
    }

    private static final class DeadlineReached extends RuntimeException {
    }

    private static final class ProgressingUnwrapSslEngine extends StubSslEngine {
        @Override
        public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
            return SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
            // A complete record was available: consume it and finish the
            // handshake, so the loop exits without waiting.
            return new SSLEngineResult(
                    SSLEngineResult.Status.OK,
                    SSLEngineResult.HandshakeStatus.FINISHED,
                    0,
                    0
            );
        }
    }

    private static final class StallingUnwrapSslEngine extends StubSslEngine {
        @Override
        public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
            return SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
            // No complete TLS record buffered yet: ask for more bytes from the
            // socket. The stalled peer never sends them, so the handshake must
            // wait on readability rather than spin.
            return new SSLEngineResult(
                    SSLEngineResult.Status.BUFFER_UNDERFLOW,
                    SSLEngineResult.HandshakeStatus.NEED_UNWRAP,
                    0,
                    0
            );
        }
    }

    /**
     * Models the JSSE terminal-delegated-task shape: NEED_TASK until the
     * handed-out task has run, then NOT_HANDSHAKING (never FINISHED --
     * getHandshakeStatus() cannot return it per the JSSE contract).
     */
    private static final class TerminalDelegatedTaskSslEngine extends StubSslEngine {
        final AtomicInteger tasksRun = new AtomicInteger();
        private boolean taskHandedOut;

        @Override
        public Runnable getDelegatedTask() {
            if (taskHandedOut) {
                return null;
            }
            taskHandedOut = true;
            return tasksRun::incrementAndGet;
        }

        @Override
        public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
            return tasksRun.get() == 0
                    ? SSLEngineResult.HandshakeStatus.NEED_TASK
                    : SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
            throw new IllegalStateException(
                    "NEED_TASK -> NOT_HANDSHAKING completion must not unwrap");
        }
    }

    private static abstract class StubSslEngine extends SSLEngine {
        @Override
        public void beginHandshake() {
        }

        @Override
        public void closeInbound() {
        }

        @Override
        public void closeOutbound() {
        }

        @Override
        public String getApplicationProtocol() {
            return null;
        }

        @Override
        public Runnable getDelegatedTask() {
            return null;
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return new String[0];
        }

        @Override
        public String[] getEnabledProtocols() {
            return new String[0];
        }

        @Override
        public boolean getEnableSessionCreation() {
            return false;
        }

        @Override
        public String getHandshakeApplicationProtocol() {
            return null;
        }

        @Override
        public BiFunction<SSLEngine, List<String>, String> getHandshakeApplicationProtocolSelector() {
            return null;
        }

        @Override
        public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
            return SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
        }

        @Override
        public boolean getNeedClientAuth() {
            return false;
        }

        @Override
        public SSLParameters getSSLParameters() {
            return new SSLParameters();
        }

        @Override
        public SSLSession getSession() {
            return null;
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return new String[0];
        }

        @Override
        public String[] getSupportedProtocols() {
            return new String[0];
        }

        @Override
        public boolean getUseClientMode() {
            return true;
        }

        @Override
        public boolean getWantClientAuth() {
            return false;
        }

        @Override
        public boolean isInboundDone() {
            return false;
        }

        @Override
        public boolean isOutboundDone() {
            return false;
        }

        @Override
        public void setEnableSessionCreation(boolean flag) {
        }

        @Override
        public void setEnabledCipherSuites(String[] suites) {
        }

        @Override
        public void setEnabledProtocols(String[] protocols) {
        }

        @Override
        public void setHandshakeApplicationProtocolSelector(BiFunction<SSLEngine, List<String>, String> selector) {
        }

        @Override
        public void setNeedClientAuth(boolean need) {
        }

        @Override
        public void setSSLParameters(SSLParameters params) {
        }

        @Override
        public void setUseClientMode(boolean mode) {
        }

        @Override
        public void setWantClientAuth(boolean want) {
        }

        @Override
        public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) {
            return new SSLEngineResult(
                    SSLEngineResult.Status.OK,
                    SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                    0,
                    0
            );
        }
    }

    private static final class TicketThenDataSslEngine extends StubSslEngine {
        private final byte[] payload;
        private int unwrapCalls;

        private TicketThenDataSslEngine(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
            unwrapCalls++;
            if (unwrapCalls == 1) {
                // Simulate a post-handshake control record (e.g. NewSessionTicket): consume input,
                // produce no plaintext.
                src.position(src.position() + 6);
                return new SSLEngineResult(
                        SSLEngineResult.Status.OK,
                        SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                        6,
                        0
                );
            }
            if (unwrapCalls > 2) {
                throw new IllegalStateException("unexpected extra unwrap call");
            }
            ByteBuffer dst = dsts[offset];
            for (byte b : payload) {
                dst.put(b);
            }
            src.position(src.position() + payload.length);
            return new SSLEngineResult(
                    SSLEngineResult.Status.OK,
                    SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                    payload.length,
                    payload.length
            );
        }
    }
}
