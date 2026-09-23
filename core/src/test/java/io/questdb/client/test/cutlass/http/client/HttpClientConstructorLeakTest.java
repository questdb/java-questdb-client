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
import io.questdb.client.network.SelectFacade;
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
    public void testSelectFacadeFailureLeaksNothingIncludingTheFdSet() throws Exception {
        Assume.assumeTrue("select/FDSet is the Windows poller", Os.type == Os.WINDOWS);
        // The OTHER arm of the Windows guard, and the one the sibling above cannot reach: here FDSet is
        // constructed successfully and the throw lands on the next statement, so the guard has to free the
        // FDSet as well as everything the base constructor took. getSelectFacade() is a caller-supplied
        // extension point evaluated inside the try for exactly this reason, and until now nothing drove it.
        // Deterministic, with no arithmetic to rot.
        assertMemoryLeak(() -> assertConstructionFailureLeaksNothing(new DefaultHttpClientConfiguration() {
            @Override
            public SelectFacade getSelectFacade() {
                throw new IllegalStateException("injected select facade failure");
            }
        }));
    }

    @Test
    public void testSelectFdSetFailureLeaksNothing() throws Exception {
        Assume.assumeTrue("select/FDSet is the Windows poller", Os.type == Os.WINDOWS);
        // FDSet reaches no facade, so the injection is its size instead: the constructor computes
        // ARRAY_OFFSET + 8 * capacity in INT arithmetic, and a capacity that overflows it negative makes
        // allocateMemory reject the size. An allocation that simply fails is the shape a real one takes
        // under memory pressure, and FDSet throwing rather than the statement after it is what exercises
        // the guard's null-tolerant Misc.free(fdSet).
        //
        // The capacity has to overflow to a LARGE negative, not merely a negative. Integer.MAX_VALUE - the
        // obvious choice, and what this used - makes 8 * capacity exactly -8, so the size works out to
        // ARRAY_OFFSET - 8: negative only where ARRAY_OFFSET is 0 or 4. On Windows fd_set is
        // { u_int fd_count; SOCKET fd_array[]; } with an 8-byte SOCKET, so arrayOffset() reports 8, the
        // size lands on exactly 0, and allocateMemory(0) succeeds and hands back a null pointer instead of
        // failing - construction completed and the test asserted nothing. 1 << 28 makes 8 * capacity
        // overflow to exactly Integer.MIN_VALUE, so the size is negative whatever arrayOffset() reports.
        assertMemoryLeak(() -> assertConstructionFailureLeaksNothing(new DefaultHttpClientConfiguration() {
            @Override
            public int getWaitQueueCapacity() {
                return 1 << 28;
            }
        }));
    }

    private static void assertConstructionFailureLeaksNothing(HttpClientConfiguration configuration) {
        HttpClient client = null;
        // The "it threw" assertion CANNOT be an Assert.fail() inside the try: fail() throws AssertionError,
        // which the catch below swallows, so an injection that stopped failing would report a green test
        // having injected nothing - and all four tests share this helper, so all four would go green at once.
        // The catch has to stay this broad, which is why the flag is needed rather than a narrower catch: the
        // four injections share no supertype below Throwable. Epoll and Kqueue throw NetworkError, which
        // extends Error, while the two failing allocations throw IllegalArgumentException out of
        // Unsafe.malloc.
        boolean threw = false;
        try {
            client = HttpClientFactory.newPlainTextInstance(configuration);
        } catch (Throwable expected) {
            // the point of the test is what assertMemoryLeak checks around it: the socket and the two native
            // buffers the base constructor took must not survive a throw from the subclass
            threw = true;
        } finally {
            // defensive: if construction unexpectedly succeeded, do not leak it out of the test
            if (client != null) {
                client.close();
            }
        }
        Assert.assertTrue(
                "construction succeeded, so this test's injected failure no longer fires and it proved nothing",
                threw
        );
    }
}
