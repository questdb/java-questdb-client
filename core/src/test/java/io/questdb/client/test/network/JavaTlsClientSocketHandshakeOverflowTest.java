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
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class JavaTlsClientSocketHandshakeOverflowTest {

    private static final String PROVIDER_NAME = "HandshakeOverflowTestProvider";

    /*
     * Demonstrates that startTlsSession() spins forever in the NEED_WRAP branch when
     * SSLEngine.wrap() returns BUFFER_OVERFLOW with wrapOutputBuffer.position() > 0
     * and handshakeStatus stays NEED_WRAP. The new code path
     *
     *     case BUFFER_OVERFLOW:
     *         if (wrapOutputBuffer.position() == 0) { growWrapOutputBuffer(); }
     *         break;
     *
     * does not drain or grow when position > 0, so the outer while-loop re-enters
     * NEED_WRAP with identical state and never makes progress. The original code
     * threw an AssertionError here, which at least failed loudly.
     */
    @Test
    public void testHandshakeWrapOverflowWithNonEmptyBufferShouldNotLoopForever() throws Exception {
        Provider provider = new HandshakeOverflowProvider();
        Security.insertProviderAt(provider, 1);
        Thread t = null;
        try {
            try (JavaTlsClientSocket socket = newSocket()) {
                socket.of(0); // -> STATE_PLAINTEXT

                CountDownLatch done = new CountDownLatch(1);
                t = new Thread(() -> {
                    try {
                        socket.startTlsSession("test.host", op -> {
                        });
                    } catch (Throwable ignored) {
                        // Expected: a healthy handshake loop should fail loudly here,
                        // not spin forever. Any exception (AssertionError, SSLException,
                        // TlsSessionInitFailedException) counts as "did not hang".
                    } finally {
                        done.countDown();
                    }
                });
                t.setDaemon(true);
                t.start();

                boolean completed = done.await(2, TimeUnit.SECONDS);
                Assert.assertTrue(
                        "startTlsSession looped without making progress — handshake BUFFER_OVERFLOW " +
                                "with wrapOutputBuffer.position() > 0 has no break-out path",
                        completed
                );
            }
        } finally {
            Security.removeProvider(PROVIDER_NAME);
            if (t != null && t.isAlive()) {
                t.interrupt();
            }
        }
    }

    private static JavaTlsClientSocket newSocket() throws Exception {
        Constructor<JavaTlsClientSocket> ctor = JavaTlsClientSocket.class.getDeclaredConstructor(
                NetworkFacade.class,
                org.slf4j.Logger.class,
                ClientTlsConfiguration.class
        );
        ctor.setAccessible(true);
        return ctor.newInstance(
                new NoOpFacade(),
                LoggerFactory.getLogger(JavaTlsClientSocketHandshakeOverflowTest.class),
                ClientTlsConfiguration.INSECURE_NO_VALIDATION
        );
    }

    public static final class HandshakeOverflowProvider extends Provider {
        public HandshakeOverflowProvider() {
            super(PROVIDER_NAME, 1.0, "test-only");
            put("SSLContext.TLS", HandshakeOverflowSslContextSpi.class.getName());
        }
    }

    public static final class HandshakeOverflowSslContextSpi extends SSLContextSpi {
        public HandshakeOverflowSslContextSpi() {
        }

        @Override
        protected SSLEngine engineCreateSSLEngine() {
            return new HandshakeOverflowEngine();
        }

        @Override
        protected SSLEngine engineCreateSSLEngine(String host, int port) {
            return new HandshakeOverflowEngine();
        }

        @Override
        protected SSLSessionContext engineGetClientSessionContext() {
            return null;
        }

        @Override
        protected SSLServerSocketFactory engineGetServerSocketFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected SSLSessionContext engineGetServerSessionContext() {
            return null;
        }

        @Override
        protected SSLSocketFactory engineGetSocketFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void engineInit(KeyManager[] km, TrustManager[] tm, SecureRandom sr) {
        }
    }

    private static final class HandshakeOverflowEngine extends SSLEngine {
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
        public Runnable getDelegatedTask() {
            return null;
        }

        @Override
        public boolean getEnableSessionCreation() {
            return false;
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
        public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
            return SSLEngineResult.HandshakeStatus.NEED_WRAP;
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
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
            // Not used — handshake stays in NEED_WRAP.
            return new SSLEngineResult(
                    SSLEngineResult.Status.OK,
                    SSLEngineResult.HandshakeStatus.NEED_WRAP,
                    0,
                    0
            );
        }

        @Override
        public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) {
            // Write one byte to dst to make wrapOutputBuffer.position() > 0 on the
            // very first call. From then on we always return BUFFER_OVERFLOW with
            // NEED_WRAP, modelling a (contrived but spec-permitted) engine where
            // wrap() advances dst.position but signals overflow.
            if (dst.remaining() > 0) {
                dst.put((byte) 0x42);
            }
            return new SSLEngineResult(
                    SSLEngineResult.Status.BUFFER_OVERFLOW,
                    SSLEngineResult.HandshakeStatus.NEED_WRAP,
                    0,
                    1
            );
        }

        // Java 9+ overrides — stubbed so the test compiles on JDK 17.
        @Override
        public String getApplicationProtocol() {
            return null;
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
        public void setHandshakeApplicationProtocolSelector(BiFunction<SSLEngine, List<String>, String> selector) {
        }
    }

    private static final class NoOpFacade extends NetworkFacadeImpl {
        @Override
        public int recvRaw(int fd, long buffer, int bufferLen) {
            return 0;
        }

        @Override
        public int sendRaw(int fd, long buffer, int bufferLen) {
            return bufferLen; // Pretend the bytes were sent so the OK send-loop terminates if reached.
        }
    }
}
