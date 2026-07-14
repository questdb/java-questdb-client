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

import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Pinpointed tests for the latched-error contract on {@link CursorWebSocketSendLoop}:
 * {@code recordFatal} → {@link CursorWebSocketSendLoop#getTerminalError} +
 * {@link CursorWebSocketSendLoop#getLastTerminalServerError} +
 * {@link CursorWebSocketSendLoop#checkError}. Bypasses the constructor entirely
 * via {@code Unsafe.allocateInstance} to avoid the live wire/engine dependencies
 * — the latch is a self-contained piece of state.
 */
public class CursorWebSocketSendLoopErrorLatchTest {

    @Test
    public void testCheckErrorRethrowsLineSenderException() throws Exception {
        // checkError must rethrow the SAME LineSenderException instance, not
        // a wrapper. Producers depend on this so getServerError() works on
        // typed throws.
        CursorWebSocketSendLoop loop = newBareLoop();
        SenderError err = newSenderError();
        LineSenderServerException original = new LineSenderServerException(err);
        setField(loop, "terminalError", original);
        Assert.assertNull(loop.getSynchronouslySurfacedError());

        try {
            loop.checkError();
            Assert.fail("expected throw");
        } catch (LineSenderException thrown) {
            Assert.assertSame("checkError must rethrow LineSenderException unchanged",
                    original, thrown);
            Assert.assertSame(err,
                    ((LineSenderServerException) thrown).getServerError());
        }
        Assert.assertSame("checkError must mark the exact latched throwable as user-owned",
                original, loop.getSynchronouslySurfacedError());
        // a synchronously surfaced latch is owned -- the close() safety net
        // must stay silent now
        loop.checkUnsurfacedError();
    }

    @Test
    public void testRecordFatalWrapsNonLineSenderThrowableOnce() throws Exception {
        // Non-LineSenderException causes (NPE, IOException, etc.) are wrapped
        // in a LineSenderException ONCE, at latch time -- so producers always
        // see one exception type AND every rethrow delivers the same instance,
        // which close() relies on to suppress double-signals by identity.
        CursorWebSocketSendLoop loop = newBareLoop();
        Throwable raw = new RuntimeException("oh no");
        invokeRecordFatal(loop, raw);
        Assert.assertNull(loop.getSynchronouslySurfacedError());

        LineSenderException first = null;
        try {
            loop.checkError();
            Assert.fail("expected throw");
        } catch (LineSenderException thrown) {
            Assert.assertNotSame(raw, thrown);
            Assert.assertSame(raw, thrown.getCause());
            Assert.assertTrue(thrown.getMessage().contains("oh no"));
            first = thrown;
        }
        Assert.assertSame("the latch must hold the wrapper, not the raw cause",
                first, loop.getTerminalError());
        Assert.assertSame("ownership tracks the latched wrapper",
                first, loop.getSynchronouslySurfacedError());
        loop.checkUnsurfacedError(); // owned -> silent

        try {
            loop.checkError();
            Assert.fail("expected throw");
        } catch (LineSenderException thrownAgain) {
            Assert.assertSame("every rethrow must deliver the same instance",
                    first, thrownAgain);
        }
    }

    @Test
    public void testCheckErrorIsNoopWhenNoLatch() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        Assert.assertNull(loop.getTerminalError());
        loop.checkError(); // must not throw
    }

    @Test
    public void testCheckUnsurfacedErrorThrowsOnceThenStaysSilent() throws Exception {
        // The close() safety net: an unowned latch must rethrow exactly like
        // checkError; once any synchronous caller has owned the error, it
        // must stay silent -- unlike checkError, which keeps throwing.
        CursorWebSocketSendLoop loop = newBareLoop();
        loop.checkUnsurfacedError(); // no latch -> silent

        LineSenderException e = new LineSenderException("wire fail");
        setField(loop, "terminalError", e);
        try {
            loop.checkUnsurfacedError();
            Assert.fail("an unowned latch must rethrow from the safety net");
        } catch (LineSenderException thrown) {
            Assert.assertSame(e, thrown);
        }

        loop.checkUnsurfacedError(); // the throw above made the caller owner -> silent

        try {
            loop.checkError();
            Assert.fail("checkError must keep rethrowing a latched error");
        } catch (LineSenderException thrown) {
            Assert.assertSame(e, thrown);
        }
    }

    @Test
    public void testGetTerminalErrorReturnsLatchedThrowable() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        Throwable e = new LineSenderException("boom");
        setField(loop, "terminalError", e);
        Assert.assertSame(e, loop.getTerminalError());
    }

    @Test
    public void testGetTerminalErrorIsNullBeforeAnyFailure() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        Assert.assertNull("loops with no latched error must report null",
                loop.getTerminalError());
    }

    @Test
    public void testRecordFatalLatchesThrowableOnly() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        // running must be true initially so we can verify recordFatal flips it.
        setField(loop, "running", true);
        Throwable e = new LineSenderException("wire fail");

        invokeRecordFatal(loop, e);

        Assert.assertSame(e, loop.getTerminalError());
        Assert.assertNull("typed payload must be null for a wire-level fatal",
                loop.getLastTerminalServerError());
        Assert.assertFalse("recordFatal must stop the loop",
                (Boolean) getField(loop, "running"));
    }

    @Test
    public void testRecordFatalLatchesBothThrowableAndSenderError() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        setField(loop, "running", true);
        SenderError err = newSenderError();
        LineSenderServerException ex = new LineSenderServerException(err);

        invokeRecordFatal(loop, ex);

        Assert.assertSame(ex, loop.getTerminalError());
        Assert.assertSame("typed payload is derived from the latched LineSenderServerException",
                err, loop.getLastTerminalServerError());
        Assert.assertFalse((Boolean) getField(loop, "running"));
    }

    @Test
    public void testRecordFatalIsIdempotent() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        setField(loop, "running", true);
        SenderError firstErr = newSenderError();
        SenderError secondErr = newSenderError();
        LineSenderServerException first = new LineSenderServerException(firstErr);
        LineSenderServerException second = new LineSenderServerException(secondErr);

        invokeRecordFatal(loop, first);
        invokeRecordFatal(loop, second);

        // Only the first failure latches — subsequent calls must not
        // overwrite, otherwise a follow-on cascade would mask the original
        // root cause.
        Assert.assertSame("first throwable must remain latched",
                first, loop.getTerminalError());
        Assert.assertSame("first SenderError must remain latched",
                firstErr, loop.getLastTerminalServerError());
    }

    private static SenderError newSenderError() {
        return new SenderError(
                SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.TERMINAL,
                0x03,
                "test-msg",
                7L,
                100L, 100L,
                "tbl",
                System.nanoTime()
        );
    }

    private static CursorWebSocketSendLoop newBareLoop() throws Exception {
        // Bypass the real constructor — we don't need a wire client or engine
        // to test the latched-error contract.
        return (CursorWebSocketSendLoop) Unsafe.getUnsafe()
                .allocateInstance(CursorWebSocketSendLoop.class);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    private static void invokeRecordFatal(CursorWebSocketSendLoop loop, Throwable t)
            throws Exception {
        Method m = CursorWebSocketSendLoop.class.getDeclaredMethod(
                "recordFatal", Throwable.class);
        m.setAccessible(true);
        m.invoke(loop, t);
    }
}
