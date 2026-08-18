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
import io.questdb.client.network.EpollFacade;
import io.questdb.client.network.EpollFacadeImpl;
import io.questdb.client.network.KqueueFacade;
import io.questdb.client.network.KqueueFacadeImpl;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.std.Os;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * A constructor that fails partway leaves an object nobody can close. It never reaches the caller, so no
 * {@code finally}, no try-with-resources and no {@code close()} ever runs on it, and whatever it had already
 * taken is lost for the life of the process.
 * <p>
 * {@link HttpClient}'s base constructor takes a socket and two native buffers, then each platform subclass
 * builds its poller. A poller that fails to initialise therefore stranded all of that. What makes it worth
 * guarding is the trigger: {@code epoll_create}/{@code kqueue} fail on fd exhaustion, and the two mallocs
 * fail under memory pressure, so the failure arrives exactly when resources are already scarce - and a
 * caller that retries compounds the loss each time. OIDC discovery newly exposes it by building a client per
 * fetch.
 * <p>
 * One test covers the base constructor's own staging and runs everywhere; the other three cover the poller,
 * and only the one matching the running platform executes. All assert through {@code assertMemoryLeak} that
 * nothing survives the throw. The injection differs because the clean failure point does: epoll and kqueue
 * are reached through a facade, so a facade returning a negative descriptor mimics fd exhaustion without
 * touching real descriptors, while FDSet and the base buffers have no facade and take a failing size
 * instead. Removing the rollback leaks 131072 bytes on the poller path and 65536 on the base path.
 * <p>
 * Only the base test and the platform test for the developing machine can be run locally; the other two
 * platforms' tests are exercised by CI.
 */
public class HttpClientConstructorLeakTest {

    @Test
    public void testEpollCreateFailureLeaksNothing() throws Exception {
        Assume.assumeTrue("epoll is the Linux poller", Os.type == Os.LINUX);
        assertMemoryLeak(() -> assertConstructionFailureLeaksNothing(new DefaultHttpClientConfiguration() {
            @Override
            public EpollFacade getEpollFacade() {
                return new EpollFacade() {
                    @Override
                    public int epollCreate() {
                        return -1; // as on fd exhaustion
                    }

                    @Override
                    public int epollCtl(int epfd, int op, int fd, long eventPtr) {
                        return EpollFacadeImpl.INSTANCE.epollCtl(epfd, op, fd, eventPtr);
                    }

                    @Override
                    public int epollWait(int epfd, long eventPtr, int eventCount, int timeout) {
                        return EpollFacadeImpl.INSTANCE.epollWait(epfd, eventPtr, eventCount, timeout);
                    }

                    @Override
                    public int errno() {
                        return 24; // EMFILE
                    }

                    @Override
                    public NetworkFacade getNetworkFacade() {
                        return EpollFacadeImpl.INSTANCE.getNetworkFacade();
                    }
                };
            }
        }));
    }

    @Test
    public void testKqueueCreateFailureLeaksNothing() throws Exception {
        Assume.assumeTrue("kqueue is the BSD/macOS poller", Os.type == Os.DARWIN || Os.type == Os.FREEBSD);
        assertMemoryLeak(() -> assertConstructionFailureLeaksNothing(new DefaultHttpClientConfiguration() {
            @Override
            public KqueueFacade getKQueueFacade() {
                return new KqueueFacade() {
                    @Override
                    public NetworkFacade getNetworkFacade() {
                        return KqueueFacadeImpl.INSTANCE.getNetworkFacade();
                    }

                    @Override
                    public int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents, int timeout) {
                        return KqueueFacadeImpl.INSTANCE.kevent(kq, changeList, nChanges, eventList, nEvents, timeout);
                    }

                    @Override
                    public int kqueue() {
                        return -1; // as on fd exhaustion
                    }
                };
            }
        }));
    }

    @Test
    public void testBaseConstructorFailureLeaksNothing() throws Exception {
        // The base constructor's OWN staging, independent of any platform poller: it takes a socket, then the
        // request buffer, then the response-parser buffer, then hands the last one to ResponseHeaders. A
        // failure at any step must not strand the earlier ones. A negative response-buffer size makes the
        // second malloc fail while the first has already succeeded - the shape a real allocation failure
        // takes under memory pressure - and it needs no platform-specific injection point.
        assertMemoryLeak(() -> assertConstructionFailureLeaksNothing(new DefaultHttpClientConfiguration() {
            @Override
            public int getResponseBufferSize() {
                return -1;
            }
        }));
    }

    @Test
    public void testSelectFdSetFailureLeaksNothing() throws Exception {
        Assume.assumeTrue("select/FDSet is the Windows poller", Os.type == Os.WINDOWS);
        // FDSet reaches no facade, so the injection is its size instead: the constructor computes
        // ARRAY_OFFSET + 8 * capacity in int arithmetic, which a large capacity overflows negative, and
        // allocateMemory rejects a negative size. An allocation that simply fails is exactly the shape a
        // real one takes under memory pressure.
        assertMemoryLeak(() -> assertConstructionFailureLeaksNothing(new DefaultHttpClientConfiguration() {
            @Override
            public int getWaitQueueCapacity() {
                return Integer.MAX_VALUE;
            }
        }));
    }

    private static void assertConstructionFailureLeaksNothing(HttpClientConfiguration configuration) {
        HttpClient client = null;
        try {
            client = HttpClientFactory.newPlainTextInstance(configuration);
            Assert.fail("expected the poller's initialisation failure to abort construction");
        } catch (Throwable expected) {
            // the point of the test is what assertMemoryLeak checks around it: the socket and the two native
            // buffers the base constructor took must not survive a throw from the subclass
        } finally {
            // defensive: if construction unexpectedly succeeded, do not leak it out of the test
            if (client != null) {
                client.close();
            }
        }
    }
}
