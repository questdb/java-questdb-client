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
 * {@code recordFatal} → {@link CursorWebSocketSendLoop#getLastError} +
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
        setField(loop, "lastError", original);

        try {
            loop.checkError();
            Assert.fail("expected throw");
        } catch (LineSenderException thrown) {
            Assert.assertSame("checkError must rethrow LineSenderException unchanged",
                    original, thrown);
            Assert.assertSame(err,
                    ((LineSenderServerException) thrown).getServerError());
        }
    }

    @Test
    public void testCheckErrorWrapsNonLineSenderThrowable() throws Exception {
        // For non-LineSenderException throwables (NPE, IOException, etc.),
        // checkError wraps in a fresh LineSenderException with the original
        // as cause so producers always see one exception type.
        CursorWebSocketSendLoop loop = newBareLoop();
        Throwable raw = new RuntimeException("oh no");
        setField(loop, "lastError", raw);

        try {
            loop.checkError();
            Assert.fail("expected throw");
        } catch (LineSenderException thrown) {
            Assert.assertNotSame(raw, thrown);
            Assert.assertEquals(raw, thrown.getCause());
            Assert.assertTrue(thrown.getMessage().contains("oh no"));
        }
    }

    @Test
    public void testCheckErrorIsNoopWhenNoLatch() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        Assert.assertNull(loop.getLastError());
        loop.checkError(); // must not throw
    }

    @Test
    public void testGetLastErrorReturnsLatchedThrowable() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        Throwable e = new LineSenderException("boom");
        setField(loop, "lastError", e);
        Assert.assertSame(e, loop.getLastError());
    }

    @Test
    public void testGetLastErrorIsNullBeforeAnyFailure() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        Assert.assertNull("loops with no latched error must report null",
                loop.getLastError());
    }

    @Test
    public void testRecordFatalLatchesThrowableOnly() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        // running must be true initially so we can verify recordFatal flips it.
        setField(loop, "running", true);
        Throwable e = new LineSenderException("wire fail");

        invokeRecordFatal(loop, e, null);

        Assert.assertSame(e, loop.getLastError());
        Assert.assertNull("typed payload must be null when recordFatal called without one",
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

        invokeRecordFatal(loop, ex, err);

        Assert.assertSame(ex, loop.getLastError());
        Assert.assertSame(err, loop.getLastTerminalServerError());
        Assert.assertFalse((Boolean) getField(loop, "running"));
    }

    @Test
    public void testRecordFatalIsIdempotent() throws Exception {
        CursorWebSocketSendLoop loop = newBareLoop();
        setField(loop, "running", true);
        Throwable first = new LineSenderException("first");
        Throwable second = new LineSenderException("second");
        SenderError firstErr = newSenderError();
        SenderError secondErr = newSenderError();

        invokeRecordFatal(loop, first, firstErr);
        invokeRecordFatal(loop, second, secondErr);

        // Only the first failure latches — subsequent calls must not
        // overwrite, otherwise a follow-on cascade would mask the original
        // root cause.
        Assert.assertSame("first throwable must remain latched",
                first, loop.getLastError());
        Assert.assertSame("first SenderError must remain latched",
                firstErr, loop.getLastTerminalServerError());
    }

    private static SenderError newSenderError() {
        return new SenderError(
                SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.HALT,
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

    private static void invokeRecordFatal(CursorWebSocketSendLoop loop, Throwable t, SenderError err)
            throws Exception {
        Method m = CursorWebSocketSendLoop.class.getDeclaredMethod(
                "recordFatal", Throwable.class, SenderError.class);
        m.setAccessible(true);
        m.invoke(loop, t, err);
    }
}
