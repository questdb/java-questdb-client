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
import io.questdb.client.network.JavaTlsClientSocket;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
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

                        ByteBuffer unwrapOutputBuffer = getField(socket, "unwrapOutputBuffer");
                        assertEquals(16, unwrapOutputBuffer.capacity());

                        int n2 = socket.recv(out2, 4);
                        assertEquals(2, n2);
                        assertBytes("ef", out2, n2);

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
        var constructor = JavaTlsClientSocket.class.getDeclaredConstructor(
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

    private static final class OverflowThenPayloadSslEngine extends SSLEngine {
        private final byte[] payload;
        private int unwrapCalls;

        private OverflowThenPayloadSslEngine(byte[] payload) {
            this.payload = payload;
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

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) throws SSLException {
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

        @Override
        public Runnable getDelegatedTask() {
            return null;
        }

        @Override
        public void closeInbound() {
        }

        @Override
        public boolean isInboundDone() {
            return false;
        }

        @Override
        public void closeOutbound() {
        }

        @Override
        public boolean isOutboundDone() {
            return false;
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return new String[0];
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return new String[0];
        }

        @Override
        public void setEnabledCipherSuites(String[] suites) {
        }

        @Override
        public String[] getSupportedProtocols() {
            return new String[0];
        }

        @Override
        public String[] getEnabledProtocols() {
            return new String[0];
        }

        @Override
        public void setEnabledProtocols(String[] protocols) {
        }

        @Override
        public SSLSession getSession() {
            return null;
        }

        @Override
        public void beginHandshake() {
        }

        @Override
        public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
            return SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
        }

        @Override
        public void setUseClientMode(boolean mode) {
        }

        @Override
        public boolean getUseClientMode() {
            return true;
        }

        @Override
        public void setNeedClientAuth(boolean need) {
        }

        @Override
        public boolean getNeedClientAuth() {
            return false;
        }

        @Override
        public void setWantClientAuth(boolean want) {
        }

        @Override
        public boolean getWantClientAuth() {
            return false;
        }

        @Override
        public void setEnableSessionCreation(boolean flag) {
        }

        @Override
        public boolean getEnableSessionCreation() {
            return false;
        }

        @Override
        public SSLParameters getSSLParameters() {
            return new SSLParameters();
        }

        @Override
        public void setSSLParameters(SSLParameters params) {
        }

        @Override
        public String getApplicationProtocol() {
            return null;
        }

        @Override
        public String getHandshakeApplicationProtocol() {
            return null;
        }

        @Override
        public void setHandshakeApplicationProtocolSelector(BiFunction<SSLEngine, List<String>, String> selector) {
        }

        @Override
        public BiFunction<SSLEngine, List<String>, String> getHandshakeApplicationProtocolSelector() {
            return null;
        }
    }
}
