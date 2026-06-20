/*******************************************************************************
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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Pins the {@link QwpWebSocketSender#isSlotLockReleased()} contract that the
 * {@link io.questdb.client.impl.SenderPool} leaked-slot accounting depends on:
 * <ul>
 *   <li><b>Happy path</b> — a clean {@code close()} that winds the I/O thread
 *       down reports {@code isSlotLockReleased() == true}.</li>
 *   <li><b>Leak path</b> — a {@code close()} that early-returns because the I/O
 *       thread refused to stop ({@code ioThreadStopped == false}) reports
 *       {@code isSlotLockReleased() == false}.</li>
 * </ul>
 * The pool's {@code flockReleased(s)} treats {@code false} as "flock still held,
 * retire the slot permanently". Before this test that contract was driven only
 * by tests that <em>forged</em> the {@code slotLockReleased} field directly, so
 * the real production write path (and the direction of the getter) was never
 * exercised: an inverted getter or a misplaced {@code slotLockReleased = true}
 * would have gone unnoticed. These tests drive the real {@code close()} paths.
 */
public class SlotLockReleasedContractTest {

    /**
     * Happy path: a live sender has not released its slot lock; a clean
     * {@code close()} (I/O thread stops normally) flips it to released.
     */
    @Test
    public void testSlotLockReleasedAfterCleanClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";close_flush_timeout_millis=2000;";
                QwpWebSocketSender wss = (QwpWebSocketSender) Sender.fromConfig(cfg);
                // A live, never-closed sender holds its slot: must report
                // NOT-released (guards against the flag defaulting/initialising
                // to true).
                Assert.assertFalse(
                        "a live sender must not report its slot lock released",
                        wss.isSlotLockReleased());

                wss.table("t").longColumn("v", 1L).atNow();
                wss.flush();
                wss.close();

                // Clean close => I/O thread stopped => slot lock released.
                Assert.assertTrue(
                        "a clean close() (I/O thread stopped) must release the slot lock",
                        wss.isSlotLockReleased());
            }
        });
    }

    /**
     * Leak path: when {@code close()} cannot wind the I/O loop down it bails
     * out via the {@code !ioThreadStopped} early-return and must leave the slot
     * lock reported as NOT released.
     * <p>
     * We can't make a real {@link CursorWebSocketSendLoop#close()} throw
     * (the loop is {@code final}, its latch is {@code final}, and every throw
     * inside is caught), so we forge the exact production precondition the catch
     * at {@code QwpWebSocketSender.close()} reacts to: a loop whose
     * {@code close()} throws while the I/O thread is still alive. After cleanly
     * stopping the real I/O thread (so it doesn't leak), we null the loop's
     * shutdown latch and point its {@code ioThread} at a live thread, so
     * {@code shutdownLatch.await()} NPEs — exactly the "Error closing cursor
     * send loop" path that sets {@code ioThreadStopped = false}.
     */
    @Test
    public void testSlotLockNotReleasedWhenIoThreadRefusesToStop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // close_flush_timeout_millis=0 => close() skips the drain step;
                // we write no rows, so the drain block has nothing to do.
                String cfg = "ws::addr=localhost:" + port + ";close_flush_timeout_millis=0;";
                QwpWebSocketSender wss = (QwpWebSocketSender) Sender.fromConfig(cfg);
                Assert.assertFalse("precondition: live sender, lock not released",
                        wss.isSlotLockReleased());

                CursorWebSocketSendLoop loop = readField(wss, "cursorSendLoop", CursorWebSocketSendLoop.class);
                Assert.assertNotNull("connected sender must have an I/O loop", loop);

                try {
                    // 1) Cleanly stop the REAL I/O thread (so it does not leak);
                    //    this also counts its shutdown latch down to zero.
                    Thread realIo = readField(loop, "ioThread", Thread.class);
                    setField(loop, "running", Boolean.FALSE);
                    if (realIo != null) {
                        LockSupport.unpark(realIo);
                        realIo.join(TimeUnit.SECONDS.toMillis(5));
                        Assert.assertFalse("real I/O thread must have stopped", realIo.isAlive());
                    }

                    // 2) Forge "the I/O thread refuses to stop": with the latch
                    //    nulled and ioThread pointing at a live thread,
                    //    loop.close()'s shutdownLatch.await() throws NPE -- the
                    //    uncaught throwable production catches as
                    //    ioThreadStopped=false.
                    setField(loop, "shutdownLatch", null);
                    setField(loop, "ioThread", Thread.currentThread());

                    // 3) Drive the real close() early-return. It rethrows the
                    //    swallowed teardown error after the guard.
                    try {
                        wss.close();
                        Assert.fail("close() must surface the loop-teardown failure");
                    } catch (Throwable expected) {
                        // expected: rethrowTerminal() surfaces the captured NPE.
                    }

                    // 4) Contract under test: the I/O thread refused to stop, so
                    //    the slot lock must NOT be reported released.
                    Assert.assertFalse(
                            "isSlotLockReleased() must be false when close() early-returns "
                                    + "with the I/O thread still running",
                            wss.isSlotLockReleased());
                } finally {
                    // close()'s early-return deliberately leaks the resources the
                    // still-running I/O thread might touch (to avoid SIGSEGV).
                    // Free them here -- the same set the post-guard close() tail
                    // would have freed -- so assertMemoryLeak passes.
                    freeFieldQuietly(wss, "buffer0");
                    freeFieldQuietly(wss, "buffer1");
                    freeFieldQuietly(wss, "client");
                    if (Boolean.TRUE.equals(readField(wss, "ownsCursorEngine", Boolean.class))) {
                        freeFieldQuietly(wss, "cursorEngine");
                    }
                    freeFieldQuietly(wss, "errorDispatcher");
                    freeFieldQuietly(wss, "progressDispatcher");
                    freeFieldQuietly(wss, "connectionDispatcher");
                }
            }
        });
    }

    // ------------------------------------------------------------------ utils

    private static void freeFieldQuietly(Object target, String name) {
        try {
            Object v = readField(target, name, Object.class);
            if (v != null) {
                v.getClass().getMethod("close").invoke(v);
            }
        } catch (Throwable ignored) {
            // Best-effort cleanup of a leaked resource: a double-close (or an
            // -ea AssertionError) must not mask the assertions above.
        }
    }

    private static <T> T readField(Object target, String name, Class<T> type) throws Exception {
        Class<?> cls = target.getClass();
        while (cls != null) {
            try {
                Field f = cls.getDeclaredField(name);
                f.setAccessible(true);
                return type.cast(f.get(target));
            } catch (NoSuchFieldException e) {
                cls = cls.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Class<?> cls = target.getClass();
        while (cls != null) {
            try {
                Field f = cls.getDeclaredField(name);
                f.setAccessible(true);
                f.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                cls = cls.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    /** ACKs every binary frame with a running sequence so flush/close drain cleanly. */
    private static final class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
