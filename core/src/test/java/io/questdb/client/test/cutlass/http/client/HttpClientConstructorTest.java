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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.cutlass.http.client.HttpClientLinux;
import io.questdb.client.cutlass.http.client.HttpClientOsx;
import io.questdb.client.cutlass.http.client.HttpClientWindows;
import io.questdb.client.network.EpollFacade;
import io.questdb.client.network.KqueueFacade;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.PlainSocket;
import io.questdb.client.network.SelectFacade;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketFactory;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Covers the rollback the HTTP client constructors run when a later acquisition throws. A
 * half-built client never reaches the caller, so nothing will ever close it and the constructor
 * itself has to release what it already took. The enclosing {@code assertMemoryLeak} observes the
 * native blocks, and the close counter on the injected socket observes the socket.
 */
public class HttpClientConstructorTest {

    @Test
    public void testBaseConstructorFailureAfterRequestBufferReleasesEverything() throws Exception {
        // A negative response-buffer size makes the second malloc throw with the socket and the
        // request buffer already taken. sun.misc.Unsafe.allocateMemory rejects a negative size with
        // IllegalArgumentException on every supported JDK, which makes this the one fault the
        // client can inject deterministically: it has no RSS-limit seam the way the server does.
        final HttpClientConfiguration configuration = new DefaultHttpClientConfiguration() {
            @Override
            public int getInitialRequestBufferSize() {
                return 1024;
            }

            @Override
            public int getResponseBufferSize() {
                return -1;
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            final CountingSocketFactory socketFactory = new CountingSocketFactory();
            try {
                buildAndClose(() -> HttpClientFactory.newInstance(configuration, socketFactory));
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
                // the response-buffer malloc rejected the negative size
            }
            Assert.assertEquals("the constructor must close the socket it took", 1, socketFactory.closeCount);
        });
    }

    @Test
    public void testBaseConstructorFailureAfterSocketClosesSocket() throws Exception {
        // getTimeout() is the first configuration read after the socket, so this fails with the
        // socket taken and no native block allocated yet.
        final HttpClientConfiguration configuration = new DefaultHttpClientConfiguration() {
            @Override
            public int getTimeout() {
                throw new InjectedFailure();
            }
        };

        assertInjectedFailureRollback(socketFactory -> HttpClientFactory.newInstance(configuration, socketFactory));
    }

    @Test
    public void testLinuxConstructorFailureClosesBaseClient() throws Exception {
        // super() has completed by the time the subclass runs, so the socket, both buffers and the
        // response parser are all live and the subclass catch has to hand them all back. The facade
        // getter throws before new Epoll(...) is invoked, so the test never touches epoll and is
        // host-independent.
        final HttpClientConfiguration configuration = new DefaultHttpClientConfiguration() {
            @Override
            public EpollFacade getEpollFacade() {
                throw new InjectedFailure();
            }
        };

        assertInjectedFailureRollback(socketFactory -> new HttpClientLinux(configuration, socketFactory));
    }

    @Test
    public void testOsxConstructorFailureClosesBaseClient() throws Exception {
        final HttpClientConfiguration configuration = new DefaultHttpClientConfiguration() {
            @Override
            public KqueueFacade getKQueueFacade() {
                throw new InjectedFailure();
            }
        };

        assertInjectedFailureRollback(socketFactory -> new HttpClientOsx(configuration, socketFactory));
    }

    @Test
    public void testWindowsConstructorFailureClosesBaseClient() throws Exception {
        // The Windows constructor reads the select facade before it takes the FD set, so this
        // failure lands with only the base class holding resources - and, unlike a failing
        // new FDSet(...), it never initialises the Windows-only SelectAccessor natives.
        final HttpClientConfiguration configuration = new DefaultHttpClientConfiguration() {
            @Override
            public SelectFacade getSelectFacade() {
                throw new InjectedFailure();
            }
        };

        assertInjectedFailureRollback(socketFactory -> new HttpClientWindows(configuration, socketFactory));
    }

    private static void assertInjectedFailureRollback(ClientFactory clientFactory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountingSocketFactory socketFactory = new CountingSocketFactory();
            try {
                buildAndClose(() -> clientFactory.newInstance(socketFactory));
                Assert.fail("expected InjectedFailure");
            } catch (InjectedFailure ignore) {
                // the injected configuration read threw
            }
            Assert.assertEquals("the constructor must close the socket it took", 1, socketFactory.closeCount);
        });
    }

    private static void buildAndClose(ClientSupplier supplier) {
        // Closing here keeps the leak check honest on the path where the constructor unexpectedly
        // succeeds: dropping a built client would leak on top of the failure the caller asserts and
        // bury it.
        HttpClient client = supplier.get();
        client.close();
    }

    @FunctionalInterface
    private interface ClientFactory {
        HttpClient newInstance(SocketFactory socketFactory);
    }

    @FunctionalInterface
    private interface ClientSupplier {
        HttpClient get();
    }

    private static class CountingSocketFactory implements SocketFactory {
        int closeCount;

        @Override
        public Socket newInstance(NetworkFacade nf, Logger log) {
            return new PlainSocket(nf, log) {
                @Override
                public synchronized void close() {
                    closeCount++;
                    super.close();
                }
            };
        }
    }

    private static class InjectedFailure extends RuntimeException {
        InjectedFailure() {
            super("injected constructor failure", null, false, false);
        }
    }
}
