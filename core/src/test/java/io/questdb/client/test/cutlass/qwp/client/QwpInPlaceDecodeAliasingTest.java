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

import io.questdb.client.cutlass.qwp.client.QueryEvent;
import io.questdb.client.cutlass.qwp.client.QwpBatchBuffer;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpEgressIoThread;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression for issue #3: close()-during-onBatch must not corrupt the
 * user-visible column bytes.
 * <p>
 * In-place RESULT_BATCH decoding leaves the {@link QwpColumnBatch} view
 * pointing into the same native bytes that the WebSocket recv buffer owns
 * (see {@code QwpResultBatchDecoder.parseStringColumn}). The I/O thread
 * parks on {@code pendingRelease.take()} after publishing the
 * {@code KIND_BATCH} event, so the user thread can read those bytes
 * safely. Before the fix, an {@code InterruptedException} on that take()
 * (delivered by {@code close()}'s {@code Thread.interrupt}) returned
 * early; control unwound through {@code WebSocketClient.tryParseFrame}
 * which advanced {@code recvReadPos} and ran {@code compactRecvBuffer}
 * (a {@code Vect.memmove}) over bytes the user thread was still reading
 * via aliased column pointers.
 * <p>
 * The fix makes the park uninterruptible: an interrupt sets a
 * thread-local sticky flag and re-enters {@code take()}. The I/O thread
 * does not return from {@code handleResultBatch} until the user thread
 * actually releases the buffer. {@code close()} may still time out and
 * abandon the I/O thread (the documented leak path), but the abandoned
 * thread is parked on {@code take()}, not unwinding through the recv
 * buffer.
 * <p>
 * This test pins that contract: under interrupt, the I/O thread stays
 * parked; only release lets it proceed.
 */
public class QwpInPlaceDecodeAliasingTest {

    @Test
    public void testInterruptDuringOnBatchKeepsIoThreadParkedUntilRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int payloadCap = 256;
            long staging = Unsafe.malloc(payloadCap, MemoryTag.NATIVE_DEFAULT);
            QwpEgressIoThread io = new QwpEgressIoThread(null, /*bufferPoolSize=*/ 2,
                    (status, message) -> {
                        // Terminal failures during this test would mean the
                        // decode itself rejected our frame -- fail loudly.
                        throw new AssertionError("unexpected terminal failure during decode: " + message);
                    });
            CountDownLatch onBatchReturned = new CountDownLatch(1);
            AtomicBoolean workerInterruptedFlagOnReturn = new AtomicBoolean();

            try {
                int len = writeSingleVarcharBatch(staging, "ALPHA");

                // Worker plays the I/O thread role: it decodes the frame,
                // publishes the batch event, and (with the fix) parks on
                // pendingRelease.take() until the main thread releases the
                // buffer.
                Thread worker = new Thread(() -> {
                    try {
                        io.onBinaryMessage(staging, len);
                    } finally {
                        workerInterruptedFlagOnReturn.set(Thread.currentThread().isInterrupted());
                        onBatchReturned.countDown();
                    }
                }, "qwp-egress-io-test");
                worker.setDaemon(true);
                worker.start();

                // Drain the KIND_BATCH event the worker published. Use a
                // bounded retry loop so a regression that fails to publish
                // surfaces as a test timeout rather than a hang.
                QueryEvent batchEvent = null;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (batchEvent == null && System.nanoTime() < deadline) {
                    QueryEvent ev = io.takeEvent();
                    Assert.assertEquals("expected KIND_BATCH from a single RESULT_BATCH frame",
                            QueryEvent.KIND_BATCH, ev.kind);
                    batchEvent = ev;
                }
                Assert.assertNotNull("worker must publish a KIND_BATCH event", batchEvent);
                QwpBatchBuffer batchBuf = batchEvent.buffer;
                Assert.assertNotNull(batchBuf);

                QwpColumnBatch batch = batchOf(batchBuf);
                DirectUtf8Sequence cell = batch.getStrA(0, 0);
                Assert.assertNotNull(cell);
                Assert.assertEquals("ALPHA", cell.toString());

                // Simulate close()'s interrupt. The fix MUST keep the worker
                // parked on pendingRelease.take(); without the fix, the
                // worker returns from handleResultBatch / onBinaryMessage
                // and onBatchReturned counts down before we release.
                worker.interrupt();
                Assert.assertFalse(
                        "worker thread must stay parked on pendingRelease.take() while "
                                + "the user is mid-onBatch -- otherwise tryParseFrame's "
                                + "compactRecvBuffer would run over the user's aliased "
                                + "column pointers.",
                        onBatchReturned.await(500, TimeUnit.MILLISECONDS));
                Assert.assertTrue("worker must still be alive (parked on take())", worker.isAlive());

                // Re-read the cell after the interrupt. With the fix, no
                // recv-buf compaction has run, so the column view still
                // resolves to the original payload bytes.
                DirectUtf8Sequence cellAfterInterrupt = batch.getStrA(0, 0);
                Assert.assertEquals(
                        "user-visible column bytes must remain stable while the worker "
                                + "is parked under interrupt",
                        "ALPHA", cellAfterInterrupt.toString());

                // Release: the worker can now finish handleResultBatch and
                // onBinaryMessage returns. The interrupt status set by
                // worker.interrupt() must be preserved on the way out so
                // any caller higher up the stack can still observe it.
                releaseBuffer(io, batchBuf);
                Assert.assertTrue("worker must complete after release",
                        onBatchReturned.await(5, TimeUnit.SECONDS));
                worker.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse("worker must terminate", worker.isAlive());
                Assert.assertTrue(
                        "interrupt status must be preserved across the uninterruptible park "
                                + "so the run loop's outer shutdown check still observes it",
                        workerInterruptedFlagOnReturn.get());
            } finally {
                Unsafe.free(staging, payloadCap, MemoryTag.NATIVE_DEFAULT);
                closePool(io);
            }
        });
    }

    private static QwpColumnBatch batchOf(QwpBatchBuffer buffer) {
        try {
            Field f = QwpBatchBuffer.class.getDeclaredField("batch");
            f.setAccessible(true);
            return (QwpColumnBatch) f.get(buffer);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not access QwpBatchBuffer.batch via reflection", e);
        }
    }

    private static void closePool(QwpEgressIoThread io) {
        try {
            Method m = QwpEgressIoThread.class.getDeclaredMethod("closePool");
            m.setAccessible(true);
            m.invoke(io);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not invoke QwpEgressIoThread.closePool via reflection", e);
        }
    }

    private static long putByte(long p, byte v) {
        Unsafe.getUnsafe().putByte(p, v);
        return p + 1;
    }

    private static long putInt(long p, int v) {
        Unsafe.getUnsafe().putInt(p, v);
        return p + 4;
    }

    private static long putLong(long p, long v) {
        Unsafe.getUnsafe().putLong(p, v);
        return p + 8;
    }

    private static long putVarint(long p, long value) {
        while ((value & ~0x7FL) != 0) {
            Unsafe.getUnsafe().putByte(p++, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        Unsafe.getUnsafe().putByte(p++, (byte) (value & 0x7F));
        return p;
    }

    private static void releaseBuffer(QwpEgressIoThread io, QwpBatchBuffer buffer) {
        io.releaseBuffer(buffer);
    }

    /**
     * Crafts a RESULT_BATCH carrying one VARCHAR row whose value is
     * {@code content}. String bytes sit at the tail of the frame so the
     * cell view's {@code stringBytesAddr} points there directly.
     */
    private static int writeSingleVarcharBatch(long buf, String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);                     // flags
        p = putByte(p, (byte) 1);                     // table_count
        p = putInt(p, 0);                             // payload_length placeholder
        p = putByte(p, (byte) 0x11);                  // msg_kind = RESULT_BATCH
        p = putLong(p, 7L);                           // request_id
        p = putVarint(p, 0L);                         // batch_seq
        p = putVarint(p, 0L);                         // table_name_len
        p = putVarint(p, 1L);                         // row_count = 1
        p = putVarint(p, 1L);                         // column_count = 1
        p = putVarint(p, 1L);                         // column name length
        p = putByte(p, (byte) 's');
        p = putByte(p, QwpConstants.TYPE_VARCHAR);
        p = putByte(p, (byte) 0);                     // null_flag
        p = putInt(p, 0);                             // offset[0]
        p = putInt(p, bytes.length);                  // offset[1] = totalBytes
        for (byte b : bytes) p = putByte(p, b);
        return (int) (p - buf);
    }
}
