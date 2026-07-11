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

import io.questdb.client.network.JavaTlsClientSocketFactory;
import io.questdb.client.network.Kqueue;
import io.questdb.client.network.KqueueFacade;
import io.questdb.client.network.KqueueFacadeImpl;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.network.PlainSocket;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketReadinessWaiter;
import io.questdb.client.network.TlsSessionInitFailedException;
import io.questdb.client.std.Os;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SocketTrafficShutdownTest {
    private static final NetworkFacade NF = NetworkFacadeImpl.INSTANCE;

    private static class CompatibilityNetworkFacade implements NetworkFacade {
        private final AtomicInteger closeCount;

        private CompatibilityNetworkFacade(AtomicInteger closeCount) {
            this.closeCount = closeCount;
        }

        @Override
        public int close(int fd) {
            closeCount.incrementAndGet();
            return 0;
        }

        @Override
        public void close(int fd, org.slf4j.Logger logger) {
            close(fd);
        }

        @Override
        public void configureKeepAlive(int fd) {
        }

        @Override
        public int configureNonBlocking(int fd) {
            return 0;
        }

        @Override
        public int connect(int fd, long pSockaddr) {
            return 0;
        }

        @Override
        public int connectAddrInfo(int fd, long pAddrInfo) {
            return 0;
        }

        @Override
        public int connectAddrInfoTimeout(int fd, long pAddrInfo, int timeoutMillis) {
            return 0;
        }

        @Override
        public int errno() {
            return 0;
        }

        @Override
        public void freeAddrInfo(long pAddrInfo) {
        }

        @Override
        public void freeSockAddr(long pSockaddr) {
        }

        @Override
        public long getAddrInfo(CharSequence host, int port) {
            return 0;
        }

        @Override
        public int getSndBuf(int fd) {
            return 0;
        }

        @Override
        public int recvRaw(int fd, long buffer, int bufferLen) {
            return 0;
        }

        @Override
        public int sendRaw(int fd, long buffer, int bufferLen) {
            return 0;
        }

        @Override
        public int sendToRaw(int fd, long lo, int len, long socketAddress) {
            return 0;
        }

        @Override
        public int sendToRawScatter(int fd, long segmentsPtr, int segmentCount, long socketAddress) {
            return 0;
        }

        @Override
        public int setMulticastInterface(int fd, int ipv4Address) {
            return 0;
        }

        @Override
        public int setMulticastTtl(int fd, int ttl) {
            return 0;
        }

        @Override
        public boolean setSndBuf(int fd, int size) {
            return false;
        }

        @Override
        public int setTcpNoDelay(int fd, boolean noDelay) {
            return 0;
        }

        @Override
        public long sockaddr(int address, int port) {
            return 0;
        }

        @Override
        public int socketTcp(boolean blocking) {
            return 0;
        }

        @Override
        public int socketUdp() {
            return 0;
        }

        @Override
        public boolean testConnection(int fd, long buffer, int bufferSize) {
            return false;
        }
    }

    private static class CompatibilitySocket implements Socket {
        private final AtomicInteger closeCount;

        private CompatibilitySocket(AtomicInteger closeCount) {
            this.closeCount = closeCount;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        @Override
        public int getFd() {
            return -1;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void of(int fd) {
        }

        @Override
        public int recv(long bufferPtr, int bufferLen) {
            return -1;
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            return -1;
        }

        @Override
        public void startTlsSession(CharSequence peerName, SocketReadinessWaiter waiter) throws TlsSessionInitFailedException {
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
    }

    @Test
    public void testCompatibilityDefaultsDoNotBypassCustomTransportOwnership() {
        AtomicInteger facadeCloseCount = new AtomicInteger();
        NetworkFacade customFacade = new CompatibilityNetworkFacade(facadeCloseCount);

        PlainSocket plainSocket = new PlainSocket(customFacade, LoggerFactory.getLogger(SocketTrafficShutdownTest.class));
        plainSocket.of(42);
        assertUnsupported(plainSocket::closeTraffic);
        Assert.assertEquals("facade compatibility fallback must not release a synthetic descriptor",
                0, facadeCloseCount.get());
        Assert.assertEquals(42, plainSocket.getFd());
        plainSocket.close();
        Assert.assertEquals(1, facadeCloseCount.get());

        AtomicInteger socketCloseCount = new AtomicInteger();
        Socket customSocket = new CompatibilitySocket(socketCloseCount);
        assertUnsupported(customSocket::closeTraffic);
        Assert.assertEquals("socket compatibility fallback must not run destructive close",
                0, socketCloseCount.get());
    }

    @Test(timeout = 30_000L)
    public void testPlainSocketShutdownWakesMacOsKqueueAndRetainsFd() throws Exception {
        assertShutdownWakesMacOsKqueue(new PlainSocket(NF, LoggerFactory.getLogger(SocketTrafficShutdownTest.class)));
    }

    @Test
    public void testTlsSocketTrafficGatePreservesTlsStateUntilFullClose() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        AtomicInteger shutdownCount = new AtomicInteger();
        NetworkFacade facade = (NetworkFacade) Proxy.newProxyInstance(
                NetworkFacade.class.getClassLoader(),
                new Class<?>[]{NetworkFacade.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closeCount.incrementAndGet();
                        return method.getReturnType() == int.class ? 0 : null;
                    }
                    if ("shutdown".equals(method.getName())) {
                        shutdownCount.incrementAndGet();
                        return 0;
                    }
                    if (method.getReturnType() == boolean.class) {
                        return false;
                    }
                    if (method.getReturnType() == int.class) {
                        return 0;
                    }
                    if (method.getReturnType() == long.class) {
                        return 0L;
                    }
                    return null;
                }
        );
        Socket socket = JavaTlsClientSocketFactory.INSECURE_NO_VALIDATION.newInstance(
                facade,
                LoggerFactory.getLogger(SocketTrafficShutdownTest.class)
        );
        socket.of(42);
        Field sslEngineField = socket.getClass().getDeclaredField("sslEngine");
        Field stateField = socket.getClass().getDeclaredField("state");
        sslEngineField.setAccessible(true);
        stateField.setAccessible(true);
        sslEngineField.set(socket, SSLContext.getDefault().createSSLEngine());
        stateField.setInt(socket, 2); // JavaTlsClientSocket.STATE_TLS
        Object[] tlsState = snapshotTlsState(socket);

        try {
            socket.closeTraffic();

            Object[] stateAfterTrafficClose = snapshotTlsState(socket);
            for (int i = 0; i < tlsState.length; i++) {
                if (i == 2) {
                    Assert.assertEquals("traffic cancellation must preserve TLS state", tlsState[i], stateAfterTrafficClose[i]);
                } else {
                    Assert.assertSame("traffic cancellation must preserve SSLEngine/buffer references",
                            tlsState[i], stateAfterTrafficClose[i]);
                }
            }
            Assert.assertEquals(1, shutdownCount.get());
            Assert.assertEquals("traffic cancellation must not release the delegate fd", 0, closeCount.get());
            Assert.assertEquals(42, socket.getFd());
            Assert.assertFalse(socket.isClosed());
        } finally {
            // Restore a valid plaintext state so full close does not attempt a
            // synthetic TLS close_notify with uninitialised session buffers.
            sslEngineField.set(socket, null);
            stateField.setInt(socket, 1); // JavaTlsClientSocket.STATE_PLAINTEXT
            socket.close();
        }
        Assert.assertEquals(1, closeCount.get());
        Assert.assertTrue(socket.isClosed());
    }

    @Test(timeout = 30_000L)
    public void testTlsSocketTrafficGateUsesDelegateShutdownAndRetainsFd() throws Exception {
        assertShutdownWakesMacOsKqueue(JavaTlsClientSocketFactory.INSECURE_NO_VALIDATION.newInstance(
                NF,
                LoggerFactory.getLogger(SocketTrafficShutdownTest.class)
        ));
    }

    private static void assertShutdownWakesMacOsKqueue(Socket socket) throws Exception {
        Assume.assumeTrue("real kqueue cancellation coverage runs on macOS", Os.type == Os.DARWIN);

        AtomicBoolean pollDone = new AtomicBoolean();
        AtomicInteger pollResult = new AtomicInteger(Integer.MIN_VALUE);
        AtomicReference<Throwable> pollFailure = new AtomicReference<>();
        CountDownLatch pollEntered = new CountDownLatch(1);
        KqueueFacade facade = new KqueueFacade() {
            private final KqueueFacade delegate = KqueueFacadeImpl.INSTANCE;

            @Override
            public NetworkFacade getNetworkFacade() {
                return delegate.getNetworkFacade();
            }

            @Override
            public int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents, int timeout) {
                if (eventList != 0 && nEvents > 0) {
                    pollEntered.countDown();
                }
                return delegate.kevent(kq, changeList, nChanges, eventList, nEvents, timeout);
            }

            @Override
            public int kqueue() {
                return delegate.kqueue();
            }
        };

        int fd = -1;
        Thread waiter = null;
        try (ServerSocket listener = new ServerSocket()) {
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            long addrInfo = NF.getAddrInfo("127.0.0.1", listener.getLocalPort());
            Assert.assertNotEquals(-1L, addrInfo);
            try {
                fd = NF.socketTcp(true);
                Assert.assertTrue("could not allocate client socket", fd >= 0);
                Assert.assertEquals(0, NF.connectAddrInfoTimeout(fd, addrInfo, 5_000));
            } finally {
                NF.freeAddrInfo(addrInfo);
            }

            try (java.net.Socket peer = listener.accept(); Kqueue kqueue = new Kqueue(facade, 1)) {
                try {
                    Assert.assertEquals(0, NF.configureNonBlocking(fd));
                    socket.of(fd);
                    int retainedFd = fd;
                    fd = -1;

                    kqueue.setWriteOffset(0);
                    kqueue.readFD(retainedFd, 0);
                    Assert.assertEquals(0, kqueue.register(1));

                    waiter = new Thread(() -> {
                        try {
                            pollResult.set(kqueue.poll(10_000));
                        } catch (Throwable t) {
                            pollFailure.set(t);
                        } finally {
                            pollDone.set(true);
                        }
                    }, "socket-traffic-kqueue-waiter");
                    waiter.start();

                    Assert.assertTrue("waiter did not enter the native kqueue wait",
                            pollEntered.await(5, TimeUnit.SECONDS));
                    Assert.assertFalse("peer unexpectedly made the read wait ready", pollDone.get());

                    socket.closeTraffic();

                    waiter.join(TimeUnit.SECONDS.toMillis(5));
                    Assert.assertFalse("shutdown did not wake the native kqueue wait", waiter.isAlive());
                    Assert.assertNull("native kqueue wait failed", pollFailure.get());
                    Assert.assertTrue("shutdown must produce a readiness event", pollResult.get() > 0);
                    Assert.assertEquals("traffic cancellation must retain fd ownership", retainedFd, socket.getFd());
                    Assert.assertFalse("traffic cancellation must not perform full close", socket.isClosed());

                    socket.close();
                    Assert.assertTrue("full close must release the retained fd", socket.isClosed());
                } finally {
                    socket.closeTraffic();
                    if (waiter != null) {
                        waiter.join(TimeUnit.SECONDS.toMillis(5));
                    }
                    socket.close();
                }
            }
        } finally {
            if (fd != -1) {
                NF.close(fd);
            }
        }
    }

    private static void assertUnsupported(Runnable operation) {
        try {
            operation.run();
            Assert.fail("expected unsupported traffic shutdown");
        } catch (UnsupportedOperationException expected) {
            // Expected compatibility behavior: no native or destructive fallback.
        }
    }

    private static Object[] snapshotTlsState(Socket socket) throws Exception {
        String[] fieldNames = {
                "callerOutputBuffer",
                "sslEngine",
                "state",
                "unwrapInputBuffer",
                "unwrapOutputBuffer",
                "wrapInputBuffer",
                "wrapOutputBuffer"
        };
        Object[] state = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            Field field = socket.getClass().getDeclaredField(fieldNames[i]);
            field.setAccessible(true);
            state[i] = field.get(socket);
        }
        return state;
    }
}
