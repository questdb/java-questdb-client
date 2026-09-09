/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.RejectedMiniSlotArchive;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SchemaRejectionState;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class CursorWebSocketSendLoopSchemaPreservationCloseTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test(timeout = 30_000L)
    public void testBlockedCopyRetainsEngineUntilIoThreadCleanup() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String directory = temp.newFolder("slot").getAbsolutePath();
            BlockingArchiveFacade ff = new BlockingArchiveFacade();
            CursorSendEngine engine = new CursorSendEngine(directory, 4096);
            WebSocketClient client = new WebSocketClient(
                    DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE) {
                @Override
                protected void ioWait(int timeout, int operation) {
                }

                @Override
                protected void setupIoWait() {
                }

                @Override
                public void closeTraffic() {
                    // No real socket; closing network traffic cannot cancel disk I/O.
                }
            };
            CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(client, engine, 0,
                    CursorWebSocketSendLoop.DEFAULT_PARK_NANOS, null, 1000, 5000, false);
            CountDownLatch cleaned = new CountDownLatch(1);
            try (SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(error -> { })) {
                long frame = Unsafe.malloc(QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(frame, QwpConstants.HEADER_SIZE, (byte) 0);
                    Unsafe.getUnsafe().putInt(frame, QwpConstants.MAGIC_MESSAGE);
                    engine.appendBlocking(frame, QwpConstants.HEADER_SIZE);
                } finally {
                    Unsafe.free(frame, QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
                SchemaRejectionState state = new SchemaRejectionState();
                state.setEngine(engine);
                state.beginLease(1, 0, false);
                assertTrue(state.reject(0, 0, new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                        SenderError.Policy.REJECT_AND_CONTINUE, 3, "mismatch", 0,
                        0, 0, "tab", System.nanoTime())));
                loop.setSchemaRejectionState(state);
                loop.setRejectionArchive(new RejectedMiniSlotArchive(ff, directory));
                loop.setErrorDispatcher(dispatcher);
                loop.setShutdownAwaitTimeoutMillis(25);
                loop.start();
                assertTrue("I/O thread did not begin preservation", ff.entered.await(5, TimeUnit.SECONDS));
                assertEquals("qdb-cursor-ws-io", ff.copyThread.get().getName());
                assertEquals(-1, engine.ackedFsn());
                try {
                    loop.close();
                    fail("blocked disk I/O must exhaust the shutdown budget");
                } catch (LineSenderException expected) {
                    assertTrue(expected.getMessage().contains("timed out"));
                }
                assertTrue(loop.delegateClose(() -> {
                    engine.close();
                    cleaned.countDown();
                }));
                assertEquals(1, cleaned.getCount());
                try (SlotLock ignored = SlotLock.acquire(directory)) {
                    fail("engine lock was released while the copy still uses its memory");
                } catch (IllegalStateException expected) {
                    // The I/O thread retains engine ownership until disk access ends.
                }
                ff.release.countDown();
                assertTrue("deferred engine cleanup did not finish", cleaned.await(5, TimeUnit.SECONDS));
                try (SlotLock ignored = SlotLock.acquire(directory)) {
                    // Rebuild can now acquire the same queue.
                }
            } finally {
                ff.release.countDown();
                loop.close();
                engine.close();
                client.close();
            }
        });
    }

    private static final class BlockingArchiveFacade extends DelegatingFilesFacade {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<Thread> copyThread = new AtomicReference<>();

        @Override
        public int mkdir(String path, int mode) {
            if (path.contains("/rejected/.tmp-")) {
                copyThread.set(Thread.currentThread());
                entered.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        release.await();
                        break;
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
                if (interrupted) Thread.currentThread().interrupt();
            }
            return super.mkdir(path, mode);
        }
    }
}
