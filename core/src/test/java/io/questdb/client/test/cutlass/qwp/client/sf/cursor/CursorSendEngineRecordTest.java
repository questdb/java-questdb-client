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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class CursorSendEngineRecordTest {
    // room for the active segment, one spare and the symbol-dictionary side file
    private static final long CAP = 2 * CursorSendEngineRecordTest.SEGMENT_SIZE + 4096;
    // one frame of this length leaves less room than a record reservation needs
    private static final int FILL = 200_000;
    private static final int MIB = 1024 * 1024;
    private static final int RESERVATION = 64 * 1024;
    private static final long SEGMENT_SIZE = 256 * 1024;
    private static final long TS_BASE = 1_700_000_000_000_000L;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-engine-rec-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDirRec(tmpDir);
    }

    @Test
    public void testRecordAccessorsDescribeTheReservation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                Assert.assertEquals(SEGMENT_SIZE - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE,
                        engine.segmentPayloadCapacity());
                Assert.assertFalse(engine.hasReservedRecord());
                Assert.assertEquals(0, engine.reservedRecordCapacity());

                long off = engine.reserveRecordBlocking(RESERVATION);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, off);
                Assert.assertTrue(engine.hasReservedRecord());
                Assert.assertEquals(RESERVATION, engine.reservedRecordCapacity());
                Assert.assertEquals(engine.activeSegment().address() + off + MmapSegment.FRAME_HEADER_SIZE,
                        engine.recordPayloadAddress(off));
                Assert.assertEquals(-1L, engine.publishedFsn());

                engine.abandonRecord(off);
                Assert.assertFalse(engine.hasReservedRecord());
                Assert.assertEquals(0, engine.reservedRecordCapacity());
                Assert.assertEquals("the abandoned space is reserved again",
                        off, engine.reserveRecordOrFsn(RESERVATION, System.nanoTime()));
                engine.abandonRecord(off);
                Assert.assertEquals(-1L, engine.publishedFsn());
            }
        });
    }

    @Test
    public void testReserveRecordBlockingTimesOutWithTheBackpressureMessage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long deadlineNanos = TimeUnit.MILLISECONDS.toNanos(200);
            long buf = Unsafe.malloc(FILL, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE, CAP, deadlineNanos)) {
                fillSecondSegment(engine, buf);
                long stalls = engine.getTotalBackpressureStalls();
                long published = engine.publishedFsn();
                long t0 = System.nanoTime();
                try {
                    engine.reserveRecordBlocking(MIB);
                    Assert.fail("the reservation needs a spare the cap does not allow");
                } catch (LineSenderException expected) {
                    long elapsed = System.nanoTime() - t0;
                    Assert.assertTrue("threw too early: " + elapsed + "ns", elapsed >= deadlineNanos);
                    TestUtils.assertContains(expected.getMessage(), "cursor ring backpressured");
                }
                Assert.assertEquals(stalls + 1, engine.getTotalBackpressureStalls());
                Assert.assertFalse(engine.hasReservedRecord());
                Assert.assertEquals(published, engine.publishedFsn());
            } finally {
                Unsafe.free(buf, FILL, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReserveRecordOrFsnReturnsSentinelOnBusyRing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(FILL, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE, CAP,
                    CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS)) {
                fillSecondSegment(engine, buf);
                long stalls = engine.getTotalBackpressureStalls();
                long published = engine.publishedFsn();
                long t0 = System.nanoTime();
                Assert.assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE,
                        engine.reserveRecordOrFsn(MIB, t0 + TimeUnit.MILLISECONDS.toNanos(20)));
                Assert.assertTrue("the spin must last until its deadline",
                        System.nanoTime() - t0 >= TimeUnit.MILLISECONDS.toNanos(20));
                Assert.assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, engine.reserveRecordOrFsn(MIB, t0));
                Assert.assertEquals("the spin path counts no stall", stalls, engine.getTotalBackpressureStalls());
                Assert.assertFalse(engine.hasReservedRecord());
                Assert.assertEquals(published, engine.publishedFsn());
            } finally {
                Unsafe.free(buf, FILL, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSealRecordThenRecoveryAnalysisSeesACommitBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long fsn;
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                long off = engine.reserveRecordBlocking(RESERVATION);
                writer.open(engine.recordPayloadAddress(off), engine.reservedRecordCapacity(),
                        QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
                for (int r = 0; r < 3; r++) {
                    appendRow(writer, tb, r);
                }
                writer.publish();
                fsn = engine.sealRecord(off, writer.seal(false));
                Assert.assertEquals(0L, fsn);
                Assert.assertEquals(fsn, engine.publishedFsn());
                Assert.assertFalse(engine.hasReservedRecord());
            }
            try (CursorSendEngine reopened = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                Assert.assertTrue(reopened.wasRecoveredFromDisk());
                Assert.assertEquals(fsn, reopened.publishedFsn());
                Assert.assertEquals(fsn, reopened.recoveredCommitBoundaryFsn());
                Assert.assertEquals(-1L, reopened.recoveredOrphanTipFsn());
            }
        });
    }

    private static void appendRow(QwoRecordWriter writer, QwpTableBuffer tb, int row) {
        tb.getOrCreateColumn("price", QwpConstants.TYPE_DOUBLE, true).addDouble(1.5 * row);
        tb.getOrCreateColumn("qty", QwpConstants.TYPE_LONG, true).addLong(100L + row);
        tb.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + row);
        tb.nextRow();
        Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(tb, new GlobalSymbolDictionary(), -1));
        tb.clearStagedRow();
    }

    private static void awaitHotSpare(CursorSendEngine engine) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (engine.getRingForTesting().getHotSpareForTesting() == null) {
            if (System.nanoTime() > deadline) {
                Assert.fail("the segment manager did not install a spare within 10s");
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    /**
     * Leaves the engine in its second segment with too little room for a
     * record, so the next reservation needs a second spare.
     */
    private static void fillSecondSegment(CursorSendEngine engine, long buf) {
        Assert.assertEquals(0L, engine.appendBlocking(buf, FILL));
        awaitHotSpare(engine);
        MmapSegment first = engine.activeSegment();
        long off = engine.reserveRecordBlocking(MIB);
        Assert.assertNotSame("too little room left: the reservation rotates", first, engine.activeSegment());
        Assert.assertEquals(MmapSegment.HEADER_SIZE, off);
        engine.abandonRecord(off);
        Assert.assertEquals(1L, engine.appendBlocking(buf, FILL));
    }
}
