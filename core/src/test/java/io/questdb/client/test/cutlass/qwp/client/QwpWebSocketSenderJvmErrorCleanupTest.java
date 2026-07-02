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

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpHostHealthTracker;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Regression coverage (M10): {@code buildAndConnect}'s connect/upgrade try
 * used to catch only {@code HttpClientException} and {@code Exception}, so a
 * JVM {@link java.lang.Error} (OOM, LinkageError, StackOverflowError) thrown
 * mid-connect escaped with the half-built {@code WebSocketClient} open -- fd
 * plus native buffers, unreachable by GC, freed only in {@code close()}. The
 * fix adds a {@code catch (Error)} arm that closes the client quietly (a
 * close failure under memory pressure must not mask the original Error) and
 * rethrows without recording endpoint-health penalties: a JVM failure is not
 * endpoint health data.
 * <p>
 * Uses the same bare-instance pattern as
 * {@code CursorWebSocketSendLoopJvmErrorTest}: {@code Unsafe.allocateInstance}
 * plus reflective wiring of the fields the connect walk dereferences, with the
 * {@code clientFactoryOverride} test seam substituting a stub client whose
 * {@code connect()} throws.
 */
public class QwpWebSocketSenderJvmErrorCleanupTest {

    @Test
    public void testErrorDuringConnectClosesClientAndStopsWalk() throws Exception {
        // Two endpoints: an Error on the FIRST connect attempt must close that
        // attempt's client and propagate immediately -- no walk to endpoint 2,
        // no health penalty. Contrast with the Exception path (below) which
        // closes, records a transport error and keeps walking.
        QwpWebSocketSender sender = newBareSender();
        QwpHostHealthTracker tracker = wireEndpoints(sender, 2);
        List<StubClient> built = new ArrayList<>();
        OutOfMemoryError oom = new OutOfMemoryError("simulated allocation failure");
        installFactory(sender, () -> {
            StubClient c = newStubClient();
            c.connectError = oom;
            built.add(c);
            return c;
        });

        try {
            invokeBuildAndConnect(sender);
            Assert.fail("a JVM Error must propagate out of buildAndConnect");
        } catch (InvocationTargetException ite) {
            Assert.assertSame("the original Error must surface", oom, ite.getCause());
        }
        Assert.assertEquals("Error must stop the walk on the first attempt", 1, built.size());
        Assert.assertEquals("half-built client must be closed exactly once",
                1, built.get(0).closeCalls);
        Assert.assertEquals("a JVM failure is not endpoint health data",
                QwpHostHealthTracker.HostState.UNKNOWN, tracker.getState(0));
        Assert.assertEquals("unattempted endpoint must stay untouched",
                QwpHostHealthTracker.HostState.UNKNOWN, tracker.getState(1));
    }

    @Test
    public void testCloseFailureDoesNotMaskOriginalError() throws Exception {
        // Under OOM, close() itself can throw. The cleanup must be
        // best-effort: the ORIGINAL Error surfaces, not the close failure.
        QwpWebSocketSender sender = newBareSender();
        wireEndpoints(sender, 1);
        OutOfMemoryError oom = new OutOfMemoryError("simulated allocation failure");
        StubClient stub = newStubClient();
        stub.connectError = oom;
        stub.throwOnClose = true;
        installFactory(sender, () -> stub);

        try {
            invokeBuildAndConnect(sender);
            Assert.fail("a JVM Error must propagate out of buildAndConnect");
        } catch (InvocationTargetException ite) {
            Assert.assertSame("close() failure must not mask the original Error",
                    oom, ite.getCause());
        }
        Assert.assertEquals("close must have been attempted", 1, stub.closeCalls);
    }

    @Test
    public void testExceptionPathStillClosesAndWalksAllEndpoints() throws Exception {
        // Seam sanity + behavioral contrast: a plain RuntimeException stays on
        // the existing path -- close, record a transport penalty, walk the
        // next endpoint, and surface LineSenderException once the round is
        // exhausted.
        QwpWebSocketSender sender = newBareSender();
        QwpHostHealthTracker tracker = wireEndpoints(sender, 2);
        List<StubClient> built = new ArrayList<>();
        installFactory(sender, () -> {
            StubClient c = newStubClient();
            c.connectRuntimeError = new IllegalStateException("simulated transport failure");
            built.add(c);
            return c;
        });

        try {
            invokeBuildAndConnect(sender);
            Assert.fail("an exhausted round must surface LineSenderException");
        } catch (InvocationTargetException ite) {
            Assert.assertTrue("expected LineSenderException, got " + ite.getCause(),
                    ite.getCause() instanceof LineSenderException);
        }
        Assert.assertEquals("an Exception must keep the walk going", 2, built.size());
        for (StubClient c : built) {
            Assert.assertEquals("every attempt's client must be closed", 1, c.closeCalls);
        }
        Assert.assertEquals("Exception path records the transport penalty",
                QwpHostHealthTracker.HostState.TRANSPORT_ERROR, tracker.getState(0));
        Assert.assertEquals("Exception path records the transport penalty",
                QwpHostHealthTracker.HostState.TRANSPORT_ERROR, tracker.getState(1));
    }

    /**
     * Bypasses the real constructor -- no wire client, engine or dispatcher
     * needed. The connect walk dereferences only the fields wired below plus
     * primitives whose zero-defaults are valid here (field initializers do
     * not run under {@code Unsafe.allocateInstance}).
     */
    private static QwpWebSocketSender newBareSender() throws Exception {
        return (QwpWebSocketSender) Unsafe.getUnsafe()
                .allocateInstance(QwpWebSocketSender.class);
    }

    private static QwpHostHealthTracker wireEndpoints(QwpWebSocketSender sender,
                                                      int count) throws Exception {
        QwpWebSocketSender.Endpoint[] eps = new QwpWebSocketSender.Endpoint[count];
        for (int i = 0; i < count; i++) {
            eps[i] = new QwpWebSocketSender.Endpoint("localhost", 9000 + i);
        }
        setField(sender, "endpoints", Arrays.asList(eps));
        QwpHostHealthTracker tracker = new QwpHostHealthTracker(count);
        setField(sender, "hostTracker", tracker);
        return tracker;
    }

    private static void installFactory(QwpWebSocketSender sender,
                                       Supplier<WebSocketClient> factory) throws Exception {
        setField(sender, "clientFactoryOverride", factory);
    }

    /**
     * Drives the private connect walk through its private foreground
     * {@code ReconnectSupplier} (no-arg: abortCheck null means foreground;
     * the bare sender's null {@code cursorSendLoop} and false {@code closed}
     * make {@code isAborted()} false).
     */
    private static void invokeBuildAndConnect(QwpWebSocketSender sender) throws Exception {
        Class<?> supplierClass = Class.forName(
                "io.questdb.client.cutlass.qwp.client.QwpWebSocketSender$ReconnectSupplier");
        Constructor<?> ctor = supplierClass.getDeclaredConstructor(QwpWebSocketSender.class);
        ctor.setAccessible(true);
        Object ctx = ctor.newInstance(sender);
        Method m = QwpWebSocketSender.class.getDeclaredMethod("buildAndConnect", supplierClass);
        m.setAccessible(true);
        m.invoke(sender, ctx);
    }

    private static StubClient newStubClient() {
        try {
            return (StubClient) Unsafe.getUnsafe().allocateInstance(StubClient.class);
        } catch (InstantiationException e) {
            throw new AssertionError(e);
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = QwpWebSocketSender.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    /**
     * Minimal stub: every method the connect walk touches is overridden so no
     * base-class state (native buffers, socket) is ever dereferenced --
     * instances come from {@code Unsafe.allocateInstance}, so the base
     * constructor never ran. Fields rely on zero-defaults; tests assign them
     * post-allocation.
     */
    private static final class StubClient extends WebSocketClient {
        int closeCalls;
        Error connectError;
        RuntimeException connectRuntimeError;
        boolean throwOnClose;

        private StubClient() {
            // Never invoked -- instances come from Unsafe.allocateInstance.
            super(null, null);
        }

        @Override
        public void close() {
            closeCalls++;
            if (throwOnClose) {
                throw new IllegalStateException("simulated close failure under memory pressure");
            }
        }

        @Override
        public void connect(CharSequence host, int port) {
            if (connectError != null) {
                throw connectError;
            }
            if (connectRuntimeError != null) {
                throw connectRuntimeError;
            }
        }

        @Override
        public void setConnectTimeout(int connectTimeoutMillis) {
        }

        @Override
        public void setQwpClientId(String clientId) {
        }

        @Override
        public void setQwpMaxVersion(int maxVersion) {
        }

        @Override
        public void setQwpRequestDurableAck(boolean enabled) {
        }

        @Override
        public void upgrade(CharSequence path, int timeout, CharSequence authorizationHeader) {
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }
}
