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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.DurableAckTiers;
import io.questdb.client.test.tools.TestWebSocketClient;
import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentBudget;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SfManifest;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression tests for segment handoffs. The live-rotation test covers the race fixed in
 * {@code CursorWebSocketSendLoop.trySendBatch()} (PR #41): the loop could observe
 * a segment rotation without observing the final frame published into that
 * segment, and advance past it — silently dropping the frame.
 * <p>
 * A producer appends fsn-stamped frames while the real I/O loop ships them to an
 * in-memory recording transport; we then assert the wire saw every fsn exactly
 * once, in order. A dropped tail frame shows up as a gap. The window is two
 * volatile reads wide, so this can't be hit deterministically — it's a stress
 * test that runs on every PR and only ever fails on the buggy code (the fix
 * never drops a frame, so no false failures). It reproduces a fair fraction of
 * runs; one miss is fine, a reintroduced bug is caught within a few PRs.
 * <p>
 * The one non-obvious knob: segments hold enough frames that the producer never
 * out-runs the hot-spare manager. With tiny segments it parks ~50&nbsp;µs on
 * every rotation, and that gap lets the consumer observe the tail frame before
 * the rotation — masking the race. Larger segments, with the manager woken on
 * every rotation, keep a spare ready so the seal follows the last publish
 * back-to-back, which is the only arrangement that opens the window.
 */
public class CursorWebSocketSendLoopRotationRaceTest {

    private static final int PAYLOAD_LEN = 8; // payload is the 8-byte little-endian fsn
    private static final int FRAMES_PER_SEGMENT = 64;
    private static final long SEGMENT_BYTES = MmapSegment.HEADER_SIZE
            + (long) FRAMES_PER_SEGMENT * (MmapSegment.FRAME_HEADER_SIZE + PAYLOAD_LEN);
    // Modest on purpose: segments are never trimmed here, and the consumer
    // rescans the sealed list (~FRAMES/FRAMES_PER_SEGMENT entries) on every
    // rotation — keep the product small enough that scan stays cheap.
    private static final int FRAMES = 50_000;
    // Upper bound on the drain wait. A healthy run exits the instant the last
    // frame lands, so this only caps how long a *failing* (frame-dropped) run
    // takes — set high enough that a stalled, overloaded CI box is never
    // mistaken for a dropped frame.
    private static final long DRAIN_TIMEOUT_NANOS = 30_000_000_000L; // 30s

    @Test
    public void tailFrameSurvivesSegmentRotation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int n = FRAMES;

            SegmentManager manager = new SegmentManager(
                    SEGMENT_BYTES, new SegmentBudget(SegmentManager.UNLIMITED_TOTAL_BYTES));
            manager.start();
            // sfDir == null => memory-only segments (no files/fds); same trySendBatch() path.
            CursorSendEngine engine = new CursorSendEngine(null, SEGMENT_BYTES, manager);
            RecordingClient client = new RecordingClient(n);
            CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    client, engine, 0L,
                    () -> {
                        throw new UnsupportedOperationException("no reconnect in this test");
                    },
                    100L, 5_000L, DurableAckTiers.NONE);

            long buf = Unsafe.malloc(PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
            try {
                loop.start(); // the consumer runs on the loop's own I/O thread
                // Produce on this thread, concurrently with the consumer.
                for (long seq = 0; seq < n; seq++) {
                    Unsafe.getUnsafe().putLong(buf, seq);
                    Assert.assertEquals(seq, engine.appendBlocking(buf, PAYLOAD_LEN));
                }
                // Wait for the consumer to ship every frame. A dropped frame
                // leaves it permanently short, so cap the wait — only a failing
                // run reaches the cap.
                long deadline = System.nanoTime() + DRAIN_TIMEOUT_NANOS;
                while (client.sentCount < n && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
            } finally {
                Unsafe.free(buf, PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
                loop.close();   // joins the I/O thread before we free the segments
                engine.close();
                manager.close();
            }

            // The I/O thread is joined, so its writes to recorded[] are visible here.
            assertContiguous(client, n);
            Assert.assertEquals(n, loop.getTotalFramesSent());
        });
    }

    @Test
    public void testRecoveredSealedSegmentsFlushBeforeEmptyActive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int sealedSegments = 3;
            final int framesPerSegment = 20;
            final int frames = sealedSegments * framesPerSegment;
            String sfDir = TestUtils.createTmpDir("qdb-recovered-batch-");
            try {
                writeRecoveredSegments(sfDir, sealedSegments, framesPerSegment);
                try (CursorSendEngine engine = new CursorSendEngine(sfDir, SEGMENT_BYTES);
                     RecordingClient client = new RecordingClient(frames);
                     CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                             client, engine, 0L, null, 0L, 0L)) {
                    Assert.assertTrue(engine.wasRecoveredFromDisk());
                    Assert.assertEquals(frames - 1, engine.publishedFsn());
                    Assert.assertEquals(frames, engine.activeSegment().baseSeq());
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, engine.activeSegment().publishedOffset());
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();

                    // Each sealed segment needs a send pass and an advance pass.
                    // No producer publishes into the empty active to trigger a later flush.
                    for (int i = 0; i < 2 * sealedSegments + 1; i++) {
                        loop.trySendBatchForTest();
                        loop.checkError();
                    }

                    assertContiguous(client, frames);
                    Assert.assertEquals(frames, loop.getTotalFramesSent());
                    Assert.assertFalse(loop.trySendBatchForTest());
                }
            } finally {
                TestUtils.removeTmpDirRec(sfDir);
            }
        });
    }

    private static void assertContiguous(RecordingClient client, int expected) {
        int count = client.recordedCount;
        for (int i = 0; i < count; i++) {
            if (client.recorded[i] != i) {
                Assert.fail("send loop reordered/dropped a frame — "
                        + "fsn " + i + " on the wire was " + client.recorded[i]);
            }
        }
        if (count != expected) {
            Assert.fail("send loop dropped a frame — expected " + expected
                    + " frames on the wire, got " + count);
        }
    }

    private static void writeRecoveredSegments(String sfDir, int sealedSegments, int framesPerSegment) {
        long payload = Unsafe.malloc(PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int s = 0; s <= sealedSegments; s++) {
                String name = s == 0 ? "sf-initial.sfa" : String.format("sf-%016x.sfa", s);
                long baseFsn = (long) s * framesPerSegment;
                try (MmapSegment segment = MmapSegment.create(sfDir + "/" + name, baseFsn, SEGMENT_BYTES)) {
                    // The last segment is the empty active left by a crash after rotation.
                    if (s < sealedSegments) {
                        for (int i = 0; i < framesPerSegment; i++) {
                            Unsafe.getUnsafe().putLong(payload, baseFsn + i);
                            Assert.assertTrue(segment.tryAppend(payload, PAYLOAD_LEN) >= 0);
                        }
                    }
                }
            }
            SfManifest.create(FilesFacade.INSTANCE, sfDir, 0, (long) sealedSegments * framesPerSegment).close();
        } finally {
            Unsafe.free(payload, PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * In-memory transport: {@code batchAppendBinary} buffers each fsn and
     * {@code batchFlush} publishes the delivered count. The other callbacks are inert — this
     * test never receives ACKs and never reconnects. Only {@code sentCount} is
     * read cross-thread (by the test, for progress) and so is volatile.
     */
    private static final class RecordingClient extends TestWebSocketClient {
        final long[] recorded;
        volatile long sentCount;
        private int appendedCount;
        private int recordedCount;

        RecordingClient(int capacity) {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
            this.recorded = new long[capacity];
        }

        @Override
        public void batchAppendBinary(long dataPtr, int length) {
            Assert.assertTrue("more frames than expected", appendedCount < recorded.length);
            recorded[appendedCount++] = Unsafe.getUnsafe().getLong(dataPtr);
        }

        @Override
        public void batchFlush() {
            recordedCount = appendedCount;
            sentCount = recordedCount;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            return false;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }
}
