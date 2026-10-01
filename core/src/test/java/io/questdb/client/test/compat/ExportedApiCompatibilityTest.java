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

package io.questdb.client.test.compat;

import io.questdb.client.cutlass.http.client.Fragment;
import io.questdb.client.cutlass.http.client.Response;
import io.questdb.client.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Pins the public signatures this branch had to restore after replacing them in place.
 * <p>
 * Four exported methods were changed rather than added to - {@code Response.recv(int)} arrived as an
 * abstract interface method, two {@code QwpWebSocketSender.connect(..., String, ...)} overloads were retyped
 * to {@code Supplier<String>}, the multi-host {@code AbstractLineHttpSender.createLineSender} gained a
 * parameter in place, and the twenty-three-arg {@code QwpWebSocketSender.connectWithCredentialSupplier}
 * overload gained the symbol-dictionary recycle knobs in place instead of through a new overload. All four
 * sit in packages {@code module-info.java} exports and that ship a javadoc jar, so a caller compiled against
 * an earlier release - or, for the credential-supplier overload, against main before this branch - would
 * have failed with {@code NoSuchMethodError}, and an external {@code Response} implementation with
 * {@code AbstractMethodError}. Nothing in this repository, in questdb, or in questdb-enterprise calls them,
 * which is why the break was latent rather than observed - and why nothing would have caught it coming back.
 * <p>
 * There is no japicmp or revapi gate on this build, so this test is the gate. The expected signatures below
 * are the ones present at this branch's merge base ({@code 981bdb02}); they are written out literally rather
 * than derived from the current classes, because a pin computed from the thing it pins proves nothing.
 * Adding an overload is fine and this test stays green; retyping or removing one turns it red.
 * <p>
 * Not every pin guards a signature that shipped. The {@code connect}, {@code createLineSender} and
 * {@code Response.recv} pins, and the seven {@link #RELEASED_CONSTRUCTOR_SIGNATURES} constructor pins, all
 * guard signatures present in a published jar - 1.3.9 has every one of them. The two
 * {@code connectWithCredentialSupplier} pins are different: that signature is on {@code main} since the OIDC
 * device-flow change ({@code 0b9b5766}), after 1.3.9, and has not shipped in a release, so the pin protects
 * a caller building against {@code main}, not against a published jar.
 * <p>
 * {@link #RELEASED_CONSTRUCTOR_SIGNATURES} pins every public {@code CursorWebSocketSendLoop} constructor
 * present in the published {@code org.questdb:questdb-client:1.3.9} jar, copied from {@code javap} of that
 * jar for the same reason: the policy-aware constructor was widened in place on this branch and the
 * released 13-argument form had to be restored as an overload.
 */
public class ExportedApiCompatibilityTest {

    /**
     * Every {@code QwpWebSocketSender.connect}, {@code QwpWebSocketSender.connectWithCredentialSupplier} and
     * {@code AbstractLineHttpSender.createLineSender} signature that existed at the merge base, as
     * {@code name(paramType,...)returnType} over erased type names.
     */
    private static final String[] PRE_BRANCH_SIGNATURES = {
            // ---- QwpWebSocketSender.connect ----
            "connect(java.lang.String,int)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.lang.String,int,io.questdb.client.ClientTlsConfiguration)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.lang.String,int,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.lang.String,int,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.lang.String,int,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.lang.String,int,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long,io.questdb.client.Sender$InitialConnectMode)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.lang.String,int,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long,io.questdb.client.Sender$InitialConnectMode,io.questdb.client.SenderErrorHandler,int)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.lang.String,int,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long,io.questdb.client.Sender$InitialConnectMode,io.questdb.client.SenderErrorHandler,int,long)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.util.List,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long,io.questdb.client.Sender$InitialConnectMode,io.questdb.client.SenderErrorHandler,int,long,long)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.util.List,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long,io.questdb.client.Sender$InitialConnectMode,io.questdb.client.SenderErrorHandler,int,long,long,int,io.questdb.client.SenderConnectionListener,int)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connect(java.util.List,io.questdb.client.ClientTlsConfiguration,int,int,long,java.lang.String,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long,io.questdb.client.Sender$InitialConnectMode,io.questdb.client.SenderErrorHandler,int,long,long,int,io.questdb.client.SenderConnectionListener,int,int,long,long)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            // ---- QwpWebSocketSender.connectWithCredentialSupplier ----
            "connectWithCredentialSupplier(java.util.List,io.questdb.client.ClientTlsConfiguration,int,int,long,java.util.function.Supplier,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long,io.questdb.client.Sender$InitialConnectMode,io.questdb.client.SenderErrorHandler,int,long,long,int,io.questdb.client.SenderConnectionListener,int)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            "connectWithCredentialSupplier(java.util.List,io.questdb.client.ClientTlsConfiguration,int,int,long,java.util.function.Supplier,boolean,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,long,long,io.questdb.client.Sender$InitialConnectMode,io.questdb.client.SenderErrorHandler,int,long,long,int,io.questdb.client.SenderConnectionListener,int,int,long,long)io.questdb.client.cutlass.qwp.client.QwpWebSocketSender",
            // ---- AbstractLineHttpSender.createLineSender ----
            "createLineSender(java.lang.String,int,java.lang.String,io.questdb.client.HttpClientConfiguration,io.questdb.client.ClientTlsConfiguration,int,java.lang.String,java.lang.String,java.lang.String,int,long,int,long,long,int)io.questdb.client.cutlass.line.http.AbstractLineHttpSender",
            "createLineSender(io.questdb.client.std.ObjList,io.questdb.client.std.IntList,java.lang.String,io.questdb.client.HttpClientConfiguration,io.questdb.client.ClientTlsConfiguration,int,java.lang.String,java.lang.String,java.lang.String,int,long,int,long,long,int)io.questdb.client.cutlass.line.http.AbstractLineHttpSender",
    };

    /**
     * Every public {@code CursorWebSocketSendLoop} constructor in the 1.3.9 jar, as {@code (paramType,...)}
     * over erased type names, in the order {@code javap} printed them.
     */
    private static final String[] RELEASED_CONSTRUCTOR_SIGNATURES = {
            "(io.questdb.client.cutlass.http.client.WebSocketClient,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop$ReconnectFactory,long,long)",
            "(io.questdb.client.cutlass.http.client.WebSocketClient,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop$ReconnectFactory,long,long,boolean)",
            "(io.questdb.client.cutlass.http.client.WebSocketClient,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop$ReconnectFactory,long,long,boolean,long)",
            "(io.questdb.client.cutlass.http.client.WebSocketClient,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop$ReconnectFactory,long,long,boolean,long,int)",
            "(io.questdb.client.cutlass.http.client.WebSocketClient,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop$ReconnectFactory,long,long,boolean,long,int,long)",
            "(io.questdb.client.cutlass.http.client.WebSocketClient,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop$ReconnectFactory,long,long,boolean,long,int,long,long)",
            "(io.questdb.client.cutlass.http.client.WebSocketClient,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine,long,long,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop$ReconnectFactory,long,long,boolean,long,int,long,long,io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop$ReconnectPolicy)",
    };

    @Test
    public void testPreBranchCreateLineSenderOverloadsStillLink() {
        assertSignaturesPresent(AbstractLineHttpSender.class, "createLineSender");
    }

    @Test
    public void testPreBranchQwpWebSocketSenderConnectOverloadsStillLink() {
        assertSignaturesPresent(QwpWebSocketSender.class, "connect");
    }

    @Test
    public void testPreBranchQwpWebSocketSenderConnectWithCredentialSupplierOverloadsStillLink() {
        assertSignaturesPresent(QwpWebSocketSender.class, "connectWithCredentialSupplier");
    }

    @Test
    public void testReleasedCursorWebSocketSendLoopConstructorsStillLink() {
        assertConstructorsPresent(CursorWebSocketSendLoop.class, RELEASED_CONSTRUCTOR_SIGNATURES);
    }

    @Test
    public void testResponseRecvIntIsDefaultNotAbstract() throws Exception {
        // The defect was recv(int) arriving as an abstract interface method: an external implementation
        // written against the earlier Response compiles fine and then fails at run time with
        // AbstractMethodError, which is exactly the failure a unit test of this library never sees.
        Method recvWithTimeout = Response.class.getMethod("recv", int.class);
        Assert.assertFalse("Response.recv(int) must stay a default method - an implementation written "
                        + "before the overload existed has no override for it",
                Modifier.isAbstract(recvWithTimeout.getModifiers()));
        Assert.assertEquals(Fragment.class, recvWithTimeout.getReturnType());

        Method recv = Response.class.getMethod("recv");
        Assert.assertTrue("recv() is the one method an implementation must supply",
                Modifier.isAbstract(recv.getModifiers()));
        Assert.assertEquals(Fragment.class, recv.getReturnType());
    }

    @Test
    public void testResponseImplementorOverridingOnlyRecvStillWorks() {
        // LegacyResponse below is the compile-time half: it implements Response and overrides recv() ONLY,
        // exactly as an implementation predating the overload does. If recv(int) went back to being
        // abstract, this class stops compiling and the whole test module goes with it - which is the point.
        LegacyResponse legacy = new LegacyResponse();
        Fragment first = legacy.recv();
        Assert.assertNotNull(first);
        Assert.assertEquals(1, legacy.calls);

        // and the default must keep the PREVIOUS behaviour, not merely link: it ignores the bound and
        // defers to recv(), which is what such an implementation did before the overload existed
        Fragment bounded = legacy.recv(5_000);
        Assert.assertSame("the default must delegate to recv()", legacy.fragment, bounded);
        Assert.assertEquals("and must not read anything of its own", 2, legacy.calls);

        Fragment unbounded = legacy.recv(0);
        Assert.assertSame("a non-positive timeout is the legacy unbounded path, same delegation",
                legacy.fragment, unbounded);
        Assert.assertEquals(3, legacy.calls);
    }

    private static void assertConstructorsPresent(Class<?> type, String[] expectedSignatures) {
        final Set<String> actual = new TreeSet<>();
        for (Constructor<?> c : type.getConstructors()) {
            actual.add(signatureOf(c));
        }
        final Set<String> missing = new LinkedHashSet<>();
        for (String signature : expectedSignatures) {
            if (!actual.contains(signature)) {
                missing.add(signature);
            }
        }
        Assert.assertTrue(
                "these public " + type.getSimpleName() + " constructors are in the released jar and no longer "
                        + "exist, so a caller compiled against that release breaks with NoSuchMethodError. "
                        + "Add an overload instead of widening one in place.\n  missing:\n    "
                        + String.join("\n    ", missing) + "\n  present:\n    "
                        + String.join("\n    ", actual),
                missing.isEmpty());
    }

    private static void assertSignaturesPresent(Class<?> type, String methodName) {
        final Set<String> actual = new TreeSet<>();
        for (Method m : type.getMethods()) {
            if (m.getName().equals(methodName)) {
                actual.add(signatureOf(m));
            }
        }
        final Set<String> missing = new LinkedHashSet<>();
        int expected = 0;
        for (String signature : PRE_BRANCH_SIGNATURES) {
            if (!signature.startsWith(methodName + "(")) {
                continue;
            }
            expected++;
            if (!actual.contains(signature)) {
                missing.add(signature);
            }
        }
        Assert.assertTrue("expected at least one pinned signature for " + methodName, expected > 0);
        Assert.assertTrue(
                "these " + type.getSimpleName() + '.' + methodName + " signatures existed at the merge base "
                        + "and no longer do, so a caller compiled against an earlier release breaks with "
                        + "NoSuchMethodError. Add an overload instead of retyping one.\n  missing:\n    "
                        + String.join("\n    ", missing) + "\n  present:\n    "
                        + String.join("\n    ", actual),
                missing.isEmpty());
    }

    private static String signatureOf(Constructor<?> c) {
        final StringBuilder sb = new StringBuilder("(");
        final Class<?>[] params = c.getParameterTypes();
        for (int i = 0; i < params.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(params[i].getName());
        }
        return sb.append(')').toString();
    }

    private static String signatureOf(Method m) {
        final StringBuilder sb = new StringBuilder(m.getName()).append('(');
        final Class<?>[] params = m.getParameterTypes();
        for (int i = 0; i < params.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(params[i].getName());
        }
        return sb.append(')').append(m.getReturnType().getName()).toString();
    }

    /**
     * A {@link Response} written before {@code recv(int)} existed: it overrides {@code recv()} and nothing
     * else. Its value is mostly at compile time - it does not compile against an abstract {@code recv(int)}.
     */
    private static final class LegacyResponse implements Response {
        private final Fragment fragment = new Fragment() {
            @Override
            public long hi() {
                return 128L;
            }

            @Override
            public long lo() {
                return 64L;
            }
        };
        private int calls;

        @Override
        public Fragment recv() {
            calls++;
            return fragment;
        }
    }
}
