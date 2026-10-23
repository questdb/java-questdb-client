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

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SfManifest;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SfRecoveryException;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordReader;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordWriter;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoValidator;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class SegmentRingRecordTest {
    // after one frame of this length the room left is MIN_RESERVE - 1
    private static final int FILL_BELOW_MINIMUM = (int) (SegmentRingRecordTest.PAYLOAD_CAPACITY
            - MmapSegment.FRAME_HEADER_SIZE - (SegmentRingRecordTest.MIN_RESERVE - 1));
    private static final int MIB = 1024 * 1024;
    private static final int MIN_RESERVE = (int) (SegmentRingRecordTest.PAYLOAD_CAPACITY / 4 < QwoFormat.MIN_RECORD_RESERVATION
            ? SegmentRingRecordTest.PAYLOAD_CAPACITY / 4
            : QwoFormat.MIN_RECORD_RESERVATION);
    private static final long PAYLOAD_CAPACITY = SegmentRingRecordTest.SEGMENT_SIZE
            - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE;
    private static final int RESERVATION = 64 * 1024;
    private static final long SEGMENT_SIZE = 256 * 1024;
    private static final long TS_BASE = 1_700_000_000_000_000L;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-ring-rec-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDirRec(tmpDir);
    }

    @Test
    public void testAbandonThenAppendReusesTheSpace() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 SegmentRing ring = new SegmentRing(MmapSegment.create(tmpDir + "/0.sfa", 0L, SEGMENT_SIZE), SEGMENT_SIZE)) {
                fillPattern(frame, 16, 1);
                MmapSegment seg = ring.getActive();
                Assert.assertEquals(0L, ring.appendOrFsn(frame, 16));
                long remaining = seg.capacityRemaining();
                long off = ring.reserveRecord(RESERVATION);
                // the sender abandons only a record without committed rows
                writeRows(ring, off, writer, tb, 2, false);
                writer.abandon();
                ring.abandonRecord(off);
                Assert.assertFalse(ring.hasReservedRecord());
                Assert.assertEquals(0, ring.reservedCapacity());
                Assert.assertEquals(remaining, seg.capacityRemaining());
                Assert.assertEquals(0L, ring.publishedFsn());
                assertZero(seg.address(), off, SEGMENT_SIZE);

                Assert.assertEquals(1L, ring.appendOrFsn(frame, 16));
                Assert.assertEquals("the frame lands where the record was", 16,
                        Unsafe.getUnsafe().getInt(seg.address() + off + 4));
                Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + 16, seg.publishedOffset());
                Assert.assertEquals(2L, seg.frameCount());
                Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + 16, ring.reserveRecord(RESERVATION));
                ring.abandonRecord(seg.reservedOffset());
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAbandonThenRotateStillFlushesTheAbandonedRange() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long intervalNanos = 100L;
            AtomicLong ticks = new AtomicLong();
            MsyncRecordingFacade ff = new MsyncRecordingFacade();
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long big = 0L;
            int bigLen = 0;
            SegmentManager manager = null;
            SegmentRing ring = null;
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                fillPattern(frame, 16, 2);
                MmapSegment active = MmapSegment.create(ff, tmpDir + "/active.sfa", 0L, SEGMENT_SIZE);
                ring = new SegmentRing(active, SEGMENT_SIZE);
                MmapSegment spare = MmapSegment.create(ff, tmpDir + "/spare.sfa", 1L, SEGMENT_SIZE);
                ring.installHotSpare(spare);
                manager = new SegmentManager(
                        SEGMENT_SIZE,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        SEGMENT_SIZE * 4L,
                        ff,
                        ticks::get);
                manager.register(ring, tmpDir, null, intervalNanos);

                Assert.assertEquals(0L, ring.appendOrFsn(frame, 16));
                manager.serviceRingForTesting(ring);
                Assert.assertTrue(active.isDurable());

                long off = ring.reserveRecord(RESERVATION);
                long reservedEnd = off + MmapSegment.FRAME_HEADER_SIZE + RESERVATION;
                writeRows(ring, off, writer, tb, 1, false);
                ticks.set(intervalNanos);
                manager.serviceRingForTesting(ring);
                Assert.assertTrue("the barrier wrote the open record's bytes to disk",
                        ff.lastLenFor(active.address()) >= reservedEnd);
                Assert.assertFalse("a reserved record is never durable", active.isDurable());

                // the zeroes replacing those bytes are not durable, though nothing was published
                writer.abandon();
                ring.abandonRecord(off);
                Assert.assertTrue(active.isPublishedDurable());
                Assert.assertFalse(active.isDurable());
                int msyncsAtAbandon = ff.msyncCalls;

                bigLen = (int) (active.capacityRemaining() + 1);
                big = Unsafe.malloc(bigLen, MemoryTag.NATIVE_DEFAULT);
                fillPattern(big, bigLen, 3);
                Assert.assertEquals("rotation must wait for the abandoned range to be flushed",
                        SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(big, bigLen));
                Assert.assertSame(active, ring.getActive());

                // the deadline is not due: this pass runs only because the rotation asked for it
                manager.serviceRingForTesting(ring);
                Assert.assertTrue(active.isDurable());
                Assert.assertTrue("an msync after the abandon must cover the abandoned extent",
                        ff.msyncCalls > msyncsAtAbandon && ff.lastLenFor(active.address()) >= reservedEnd);

                Assert.assertEquals(1L, ring.appendOrFsn(big, bigLen));
                Assert.assertSame(spare, ring.getActive());
                Assert.assertEquals(1, ring.getSealedSegments().size());
                Assert.assertSame(active, ring.getSealedSegments().get(0));
            } finally {
                if (manager != null && ring != null) {
                    manager.deregister(ring);
                }
                if (ring != null) {
                    ring.close();
                }
                if (manager != null) {
                    manager.close();
                }
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
                if (big != 0L) {
                    Unsafe.free(big, bigLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testAbandonWithCommittedRowsIsRefused() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 SegmentRing ring = new SegmentRing(MmapSegment.create(tmpDir + "/0.sfa", 0L, SEGMENT_SIZE), SEGMENT_SIZE)) {
                MmapSegment seg = ring.getActive();
                long off = ring.reserveRecord(RESERVATION);
                writeRows(ring, off, writer, tb, 2, true);
                byte[] envelope = copyOf(seg.address() + off, MmapSegment.FRAME_HEADER_SIZE + RESERVATION);
                try {
                    ring.abandonRecord(off);
                    Assert.fail("a record with committed rows must not be abandoned");
                } catch (AssertionError expected) {
                    TestUtils.assertContains(expected.getMessage(), "committed rows");
                }
                Assert.assertTrue(ring.hasReservedRecord());
                Assert.assertEquals(off, seg.reservedOffset());
                assertMemory(seg.address() + off, envelope);

                Assert.assertEquals(0L, ring.sealRecord(off, writer.seal(false)));
                assertSealed(seg, off, 2);
            }
        });
    }

    @Test
    public void testAppendOrFsnWhileReservedThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.create(tmpDir + "/0.sfa", 0L, SEGMENT_SIZE), SEGMENT_SIZE)) {
                fillPattern(frame, 16, 4);
                MmapSegment seg = ring.getActive();
                Assert.assertEquals(0L, ring.appendOrFsn(frame, 16));
                long published = seg.publishedOffset();
                // leave less room than any reservation needs, so a second reserve would rotate
                long off = ring.reserveRecord((int) (seg.capacityRemaining() - 100));
                Assert.assertTrue(seg.capacityRemaining() < MIN_RESERVE);
                MmapSegment spare = MmapSegment.create(tmpDir + "/1.sfa", 1L, SEGMENT_SIZE);
                ring.installHotSpare(spare);
                try {
                    ring.appendOrFsn(frame, 16);
                    Assert.fail("a frame must not be appended while a record is reserved");
                } catch (IllegalStateException expected) {
                    TestUtils.assertContains(expected.getMessage(), "record reserved");
                }
                try {
                    ring.reserveRecord(MIB);
                    Assert.fail("a second record must not be reserved");
                } catch (IllegalStateException expected) {
                    TestUtils.assertContains(expected.getMessage(), "record reserved");
                }
                Assert.assertSame("nothing may rotate while a record is reserved", seg, ring.getActive());
                Assert.assertSame(spare, ring.getHotSpareForTesting());
                Assert.assertEquals(0L, ring.publishedFsn());
                Assert.assertEquals(1L, seg.frameCount());
                Assert.assertEquals(published, seg.publishedOffset());
                Assert.assertEquals(off, seg.reservedOffset());
                ring.abandonRecord(off);
                Assert.assertEquals(1L, ring.appendOrFsn(frame, 16));
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOpenRecordInASealedMemberFailsClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                fillPattern(frame, 16, 5);
                for (int pass = 0; pass < 2; pass++) {
                    // pass 0: a recoverable open record; pass 1: one that fails validation
                    boolean isCorrupt = pass == 1;
                    String dir = tmpDir + "/pass" + pass;
                    Assert.assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
                    String sealedPath = dir + "/s0.sfa";
                    String activePath = dir + "/s1.sfa";
                    try (MmapSegment s0 = MmapSegment.create(sealedPath, 0L, SEGMENT_SIZE)) {
                        Assert.assertEquals(MmapSegment.HEADER_SIZE, s0.tryAppend(frame, 16));
                        long off = s0.reserveRecord(RESERVATION);
                        writeRows(s0, off, writer, tb, 3, true);
                        if (isCorrupt) {
                            flipCommittedValueByte(s0.payloadAddress(off), RESERVATION);
                        }
                        writer.abandon();
                    }
                    try (MmapSegment s1 = MmapSegment.create(activePath, 1L, SEGMENT_SIZE)) {
                        Assert.assertEquals(MmapSegment.HEADER_SIZE, s1.tryAppend(frame, 16));
                    }
                    // the frame accounting validates: the sealed member ends at 1, the active starts there
                    SfManifest.create(FilesFacade.INSTANCE, dir, 0L, 1L).close();
                    byte[] sealedBytes = readFile(sealedPath);
                    byte[] activeBytes = readFile(activePath);
                    for (int attempt = 0; attempt < 2; attempt++) {
                        try {
                            SegmentRing ring = SegmentRing.openExisting(dir, SEGMENT_SIZE);
                            if (ring != null) {
                                ring.close();
                            }
                            Assert.fail("an open record in a sealed member must fail recovery");
                        } catch (SfRecoveryException expected) {
                            TestUtils.assertContains(expected.getMessage(), "open record");
                            TestUtils.assertContains(expected.getMessage(), sealedPath);
                        }
                        Assert.assertArrayEquals("the sealed member's bytes must stay intact",
                                sealedBytes, readFile(sealedPath));
                        Assert.assertArrayEquals(activeBytes, readFile(activePath));
                    }
                }
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRecoveryAfterCrashWithOpenRecordReplaysItAsOneFrame() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                fillPattern(frame, 16, 6);
                long previous;
                long off;
                try (SegmentRing ring = new SegmentRing(MmapSegment.create(tmpDir + "/0.sfa", 0L, SEGMENT_SIZE), SEGMENT_SIZE)) {
                    Assert.assertEquals(0L, ring.appendOrFsn(frame, 16));
                    previous = ring.appendOrFsn(frame, 16);
                    off = ring.reserveRecord(RESERVATION);
                    writeRows(ring, off, writer, tb, 3, true);
                    // an uncommitted row the crash loses
                    appendRow(writer, tb, 3);
                    writer.abandon();
                    // the process dies here: the ring closes without sealing the record
                }
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, SEGMENT_SIZE)) {
                    Assert.assertNotNull(ring);
                    MmapSegment active = ring.getActive();
                    Assert.assertEquals(previous + 1, ring.publishedFsn());
                    Assert.assertEquals(previous + 2, ring.nextSeqHint());
                    Assert.assertSame(active, ring.findSegmentContaining(previous + 1));
                    Assert.assertEquals(previous + 2, active.frameCount());
                    Assert.assertEquals(0L, active.tornTailBytes());
                    Assert.assertFalse(active.hasUnsanitizedTornTail());
                    Assert.assertFalse(active.hasRecoverableOpenRecord());
                    assertSealed(active, off, 3);
                    int used = Unsafe.getUnsafe().getInt(active.address() + off + 4);
                    Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + used, active.publishedOffset());
                    assertZero(active.address(), active.publishedOffset(), SEGMENT_SIZE);
                    Assert.assertEquals(previous + 2, ring.appendOrFsn(frame, 16));
                }
                // the next recovery finds a sealed record: nothing left to seal
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, SEGMENT_SIZE)) {
                    Assert.assertNotNull(ring);
                    Assert.assertEquals(previous + 2, ring.publishedFsn());
                    Assert.assertEquals(previous + 3, ring.getActive().frameCount());
                    Assert.assertEquals(0L, ring.getActive().tornTailBytes());
                }
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRecoveryOfAnActiveHoldingOnlyAnOpenRecord() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                fillPattern(frame, 16, 7);

                // a rotation committed, then the crash came before the new active's first seal
                String rotated = tmpDir + "/rotated";
                Assert.assertEquals(0, Files.mkdir(rotated, Files.DIR_MODE_DEFAULT));
                try (MmapSegment s0 = MmapSegment.create(rotated + "/s0.sfa", 0L, SEGMENT_SIZE)) {
                    s0.tryAppend(frame, 16);
                    s0.tryAppend(frame, 16);
                }
                try (MmapSegment s1 = MmapSegment.create(rotated + "/s1.sfa", 2L, SEGMENT_SIZE)) {
                    long off = s1.reserveRecord(RESERVATION);
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, off);
                    writeRows(s1, off, writer, tb, 3, true);
                    writer.abandon();
                }
                // a clean spare carrying the same provisional base
                String sparePath = rotated + "/spare.sfa";
                MmapSegment.create(sparePath, 2L, SEGMENT_SIZE).close();
                SfManifest.create(FilesFacade.INSTANCE, rotated, 0L, 2L).close();
                try (SegmentRing ring = SegmentRing.openExisting(rotated, SEGMENT_SIZE)) {
                    Assert.assertNotNull(ring);
                    MmapSegment active = ring.getActive();
                    Assert.assertTrue(active.path().endsWith("/s1.sfa"));
                    Assert.assertEquals(2L, active.baseSeq());
                    Assert.assertEquals(1L, active.frameCount());
                    Assert.assertEquals(2L, ring.publishedFsn());
                    Assert.assertSame(active, ring.findSegmentContaining(2L));
                    Assert.assertEquals(1, ring.getSealedSegments().size());
                    Assert.assertEquals(0L, active.tornTailBytes());
                    assertSealed(active, MmapSegment.HEADER_SIZE, 3);
                    Assert.assertFalse("the spare is discarded", Files.exists(sparePath));
                    Assert.assertEquals(3L, ring.appendOrFsn(frame, 16));
                }

                // a slot without a manifest whose only segment holds only an open record
                String sole = tmpDir + "/sole";
                Assert.assertEquals(0, Files.mkdir(sole, Files.DIR_MODE_DEFAULT));
                try (MmapSegment seg = MmapSegment.create(sole + "/sf-initial.sfa", 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    writeRows(seg, off, writer, tb, 2, true);
                    writer.abandon();
                }
                try (SegmentRing ring = SegmentRing.openExisting(sole, SEGMENT_SIZE)) {
                    Assert.assertNotNull("the open record's committed rows make the slot non-empty", ring);
                    MmapSegment active = ring.getActive();
                    Assert.assertEquals(0L, active.baseSeq());
                    Assert.assertEquals(1L, active.frameCount());
                    Assert.assertEquals(0L, ring.publishedFsn());
                    Assert.assertSame(active, ring.findSegmentContaining(0L));
                    Assert.assertEquals(0L, active.tornTailBytes());
                    assertSealed(active, MmapSegment.HEADER_SIZE, 2);
                }
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReserveReturnsBackpressureWithoutSpare() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long big = Unsafe.malloc(FILL_BELOW_MINIMUM, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.create(tmpDir + "/0.sfa", 0L, SEGMENT_SIZE), SEGMENT_SIZE)) {
                fillPattern(big, FILL_BELOW_MINIMUM, 8);
                MmapSegment seg = ring.getActive();
                Assert.assertEquals(0L, ring.appendOrFsn(big, FILL_BELOW_MINIMUM));
                long published = seg.publishedOffset();
                for (int i = 0; i < 2; i++) {
                    Assert.assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, ring.reserveRecord(MIB));
                    Assert.assertFalse(ring.hasReservedRecord());
                    Assert.assertSame(seg, ring.getActive());
                    Assert.assertEquals(MIN_RESERVE - 1, seg.capacityRemaining());
                    Assert.assertEquals(published, seg.publishedOffset());
                    Assert.assertEquals(0L, ring.publishedFsn());
                    Assert.assertTrue(ring.needsHotSpare());
                }
            } finally {
                Unsafe.free(big, FILL_BELOW_MINIMUM, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReserveReturnsPayloadTooLargeWhenAFreshSegmentCannotHoldTheMinimum() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long big = Unsafe.malloc(FILL_BELOW_MINIMUM, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.create(tmpDir + "/0.sfa", 0L, SEGMENT_SIZE), SEGMENT_SIZE)) {
                try {
                    ring.reserveRecord(QwoFormat.HEADER_SIZE - 1);
                    Assert.fail("a record smaller than its header must be refused");
                } catch (IllegalArgumentException expected) {
                    TestUtils.assertContains(expected.getMessage(), "record capacity");
                }
                fillPattern(big, FILL_BELOW_MINIMUM, 9);
                Assert.assertEquals(0L, ring.appendOrFsn(big, FILL_BELOW_MINIMUM));
                // a spare too small to hold the minimum reservation
                MmapSegment small = MmapSegment.create(tmpDir + "/1.sfa", 1L,
                        MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + MIN_RESERVE - 1);
                ring.installHotSpare(small);
                Assert.assertEquals(SegmentRing.PAYLOAD_TOO_LARGE, ring.reserveRecord(MIB));
                Assert.assertFalse(ring.hasReservedRecord());
                Assert.assertSame("the rotation stands, as for an oversized frame", small, ring.getActive());
                Assert.assertEquals(0L, ring.publishedFsn());
            } finally {
                Unsafe.free(big, FILL_BELOW_MINIMUM, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReserveRotatesWhenRemainingBelowMinimum() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long big = Unsafe.malloc(FILL_BELOW_MINIMUM, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 SegmentRing ring = new SegmentRing(MmapSegment.create(tmpDir + "/0.sfa", 0L, SEGMENT_SIZE), SEGMENT_SIZE)) {
                fillPattern(big, FILL_BELOW_MINIMUM, 10);
                MmapSegment seg0 = ring.getActive();
                Assert.assertEquals(0L, ring.appendOrFsn(big, FILL_BELOW_MINIMUM));
                Assert.assertEquals(MIN_RESERVE - 1, seg0.capacityRemaining());

                // a request no larger than the room left fits without rotating
                long off = ring.reserveRecord(MIN_RESERVE - 1);
                Assert.assertEquals(seg0.publishedOffset(), off);
                Assert.assertSame(seg0, ring.getActive());
                Assert.assertEquals(MIN_RESERVE - 1, ring.reservedCapacity());
                ring.abandonRecord(off);

                MmapSegment spare = MmapSegment.create(tmpDir + "/1.sfa", 7L, SEGMENT_SIZE);
                ring.installHotSpare(spare);
                off = ring.reserveRecord(MIB);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, off);
                Assert.assertSame(spare, ring.getActive());
                Assert.assertEquals("rotation rebases the spare", 1L, spare.baseSeq());
                Assert.assertEquals(PAYLOAD_CAPACITY, ring.reservedCapacity());
                Assert.assertEquals(1, ring.getSealedSegments().size());
                Assert.assertSame(seg0, ring.getSealedSegments().get(0));
                Assert.assertTrue(ring.needsHotSpare());
                Assert.assertEquals("a reservation consumes no FSN", 0L, ring.publishedFsn());
                assertZero(seg0.address(), seg0.publishedOffset(), SEGMENT_SIZE);

                writeRows(ring, off, writer, tb, 2, true);
                Assert.assertEquals(1L, ring.sealRecord(off, writer.seal(false)));
                Assert.assertSame(spare, ring.findSegmentContaining(1L));
                assertSealed(spare, off, 2);
            } finally {
                Unsafe.free(big, FILL_BELOW_MINIMUM, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReserveSealAssignsNextFsnAndPublishes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 SegmentRing ring = new SegmentRing(MmapSegment.create(tmpDir + "/0.sfa", 0L, SEGMENT_SIZE), SEGMENT_SIZE)) {
                fillPattern(frame, 16, 11);
                MmapSegment seg = ring.getActive();
                Assert.assertEquals(0L, ring.appendOrFsn(frame, 16));
                long remaining = seg.capacityRemaining();
                long published = seg.publishedOffset();
                Assert.assertFalse(ring.hasReservedRecord());

                long off = ring.reserveRecord(MIB);
                Assert.assertEquals(published, off);
                Assert.assertTrue(ring.hasReservedRecord());
                Assert.assertEquals("the record takes the room left", remaining, ring.reservedCapacity());
                writeRows(ring, off, writer, tb, 3, true);
                Assert.assertEquals("an open record is not visible to the consumer", published, seg.publishedOffset());
                Assert.assertEquals(0L, ring.publishedFsn());
                Assert.assertEquals(1L, seg.frameCount());

                int used = writer.seal(false);
                Assert.assertEquals(1L, ring.sealRecord(off, used));
                Assert.assertEquals(1L, ring.publishedFsn());
                Assert.assertEquals(2L, ring.nextSeqHint());
                Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + used, seg.publishedOffset());
                Assert.assertEquals(2L, seg.frameCount());
                Assert.assertFalse(ring.hasReservedRecord());
                Assert.assertEquals(0, ring.reservedCapacity());
                Assert.assertSame(seg, ring.findSegmentContaining(1L));
                assertSealed(seg, off, 3);
                Assert.assertEquals(2L, ring.appendOrFsn(frame, 16));
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
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

    private static void assertMemory(long addr, byte[] expected) {
        for (int p = 0; p < expected.length; p++) {
            if (Unsafe.getUnsafe().getByte(addr + p) != expected[p]) {
                Assert.fail("memory changed at offset " + p);
            }
        }
    }

    private static void assertSealed(MmapSegment seg, long off, int rows) {
        Assert.assertEquals(QwoFormat.STATE_SEALED, Unsafe.getUnsafe().getInt(seg.address() + off));
        int len = Unsafe.getUnsafe().getInt(seg.address() + off + 4);
        QwoValidator validator = new QwoValidator();
        Assert.assertTrue(validator.detail(), validator.validateSealed(seg.payloadAddress(off), len));
        Assert.assertEquals(rows, validator.committedRows());
    }

    private static void assertZero(long base, long from, long to) {
        for (long p = from; p < to; p++) {
            if (Unsafe.getUnsafe().getByte(base + p) != 0) {
                Assert.fail("non-zero byte at offset " + p);
            }
        }
    }

    private static byte[] copyOf(long addr, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return bytes;
    }

    private static void fillPattern(long addr, int len, int seed) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(addr + i, (byte) (seed * 31 + i + 17));
        }
    }

    /**
     * Flips a byte of the first committed value of the record's first column,
     * so the row CRC no longer matches.
     */
    private static void flipCommittedValueByte(long payload, int len) {
        QwoRecordReader reader = new QwoRecordReader();
        reader.of(payload, len);
        int def = reader.columnDef(reader.firstBlock(), 0);
        long value = reader.chunkValuesAddr(reader.firstChunk(def), QwpConstants.TYPE_DOUBLE,
                reader.columnUsesBitmap(def));
        Unsafe.getUnsafe().putByte(value, (byte) (Unsafe.getUnsafe().getByte(value) ^ 0x40));
    }

    private static byte[] readFile(String path) throws Exception {
        return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path));
    }

    /**
     * Opens a record in the reservation at {@code off} of the ring's active
     * segment and appends {@code rows} rows, publishing them when asked.
     */
    private static void writeRows(SegmentRing ring, long off, QwoRecordWriter writer, QwpTableBuffer tb, int rows, boolean publish) {
        writeRows(ring.getActive().payloadAddress(off), ring.reservedCapacity(), writer, tb, rows, publish);
    }

    private static void writeRows(MmapSegment seg, long off, QwoRecordWriter writer, QwpTableBuffer tb, int rows, boolean publish) {
        writeRows(seg.payloadAddress(off), seg.reservedCapacity(), writer, tb, rows, publish);
    }

    private static void writeRows(long payload, int capacity, QwoRecordWriter writer, QwpTableBuffer tb, int rows, boolean publish) {
        writer.open(payload, capacity, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
        for (int r = 0; r < rows; r++) {
            appendRow(writer, tb, r);
        }
        if (publish) {
            writer.publish();
        }
    }

    private static final class MsyncRecordingFacade extends DelegatingFilesFacade {
        private long[] addrs = new long[64];
        private long[] lens = new long[64];
        private int msyncCalls;

        @Override
        public synchronized int msync(long addr, long len, boolean async) {
            if (msyncCalls == addrs.length) {
                addrs = java.util.Arrays.copyOf(addrs, msyncCalls * 2);
                lens = java.util.Arrays.copyOf(lens, msyncCalls * 2);
            }
            addrs[msyncCalls] = addr;
            lens[msyncCalls] = len;
            msyncCalls++;
            return super.msync(addr, len, async);
        }

        /**
         * Length of the last msync that started at {@code addr}; -1 when none did.
         */
        synchronized long lastLenFor(long addr) {
            for (int i = msyncCalls - 1; i >= 0; i--) {
                if (addrs[i] == addr) {
                    return lens[i];
                }
            }
            return -1L;
        }
    }
}
