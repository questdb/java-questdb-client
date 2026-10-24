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
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentCorruptionException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentException;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordReader;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordWriter;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoValidator;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.MsyncRecordingFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.RandomAccessFile;

public class MmapSegmentRecordTest {
    private static final byte FLAGS_OPEN = (byte) (QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT);
    private static final byte POISON = 0x5A;
    private static final int RESERVATION = 64 * 1024;
    private static final long SEGMENT_SIZE = 1024 * 1024;
    private static final long TS_BASE = 1_700_000_000_000_000L;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-mmap-rec-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test
    public void testAbandonLeavesTheReservationAboveTheHighWaterMarkUntouched() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/abandon-mark.sfa";
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                long off = seg.reserveRecord(RESERVATION);
                long payload = seg.payloadAddress(off);
                writeRows(seg, off, writer, tb, 2, false);
                int mark = writer.maxUsedBytes();
                Assert.assertTrue(mark < RESERVATION);
                // the zero-fill invariant keeps these bytes zero; poisoned, they show what the abandon touches
                Unsafe.getUnsafe().setMemory(payload + mark, RESERVATION - mark, POISON);
                writer.abandon();
                seg.abandonReserved(off, writer.maxUsedBytes());
                Assert.assertEquals(-1L, seg.reservedOffset());
                assertZero(seg.address(), off, off + MmapSegment.FRAME_HEADER_SIZE + mark);
                assertFilled(payload + mark, RESERVATION - mark, POISON);
            }
        });
    }

    @Test
    public void testAbandonRefusesADirtyExtentOutsideTheReservation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/abandon-bound.sfa";
            try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                long off = seg.reserveRecord(RESERVATION);
                for (int dirty : new int[]{-1, RESERVATION + 1}) {
                    try {
                        seg.abandonReserved(off, dirty);
                        Assert.fail("dirtyBytes " + dirty + " must be refused");
                    } catch (IllegalArgumentException expected) {
                        TestUtils.assertContains(expected.getMessage(), "dirtyBytes " + dirty + " outside [0, " + RESERVATION + ']');
                    }
                    Assert.assertEquals("the record stays reserved", off, seg.reservedOffset());
                    Assert.assertEquals(QwoFormat.STATE_OPEN, Unsafe.getUnsafe().getInt(seg.address() + off));
                }
                seg.abandonReserved(off, RESERVATION);
                Assert.assertEquals(-1L, seg.reservedOffset());
                assertZero(seg.address(), MmapSegment.HEADER_SIZE, SEGMENT_SIZE);
            }
        });
    }

    @Test
    public void testAbandonZeroesWhatRolledBackAppendsDirtied() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/abandon-rollback.sfa";
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    writeRows(seg, off, writer, tb, 3, false);
                    int mark = writer.usedBytes();
                    // the sender abandons a record whose rows were all rolled back
                    writer.rollback();
                    Assert.assertFalse(writer.hasUncommittedRows());
                    Assert.assertTrue(writer.usedBytes() < mark);
                    Assert.assertEquals(mark, writer.maxUsedBytes());
                    writer.abandon();
                    seg.abandonReserved(off, writer.maxUsedBytes());
                    assertZero(seg.address(), MmapSegment.HEADER_SIZE, SEGMENT_SIZE);
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(0L, seg.frameCount());
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    Assert.assertEquals(-1L, seg.recoveredOpenRecordOffset());
                }
            }
        });
    }

    @Test
    public void testAbandonedRecordLeavesZerosAndRecoversClean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/abandon.sfa";
            long frame = Unsafe.malloc(40, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                fillPattern(frame, 40, 3);
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    // the sender abandons only a record without committed rows
                    writeRows(seg, off, writer, tb, 2, false);
                    seg.abandonReserved(off, writer.maxUsedBytes());
                    writer.abandon();
                    Assert.assertEquals(-1L, seg.reservedOffset());
                    assertZero(seg.address(), MmapSegment.HEADER_SIZE, SEGMENT_SIZE);
                    Assert.assertEquals(SEGMENT_SIZE - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE,
                            seg.capacityRemaining());
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.tryAppend(frame, 40));
                    Assert.assertEquals(1L, seg.frameCount());
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 40, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    assertZero(seg.address(), seg.publishedOffset(), SEGMENT_SIZE);
                }
            } finally {
                Unsafe.free(frame, 40, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testBarrierCoversAnAbandonedReservation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/barrier-abandon.sfa";
            MsyncRecordingFacade ff = new MsyncRecordingFacade();
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 MmapSegment seg = MmapSegment.create(ff, path, 0L, SEGMENT_SIZE)) {
                long off = seg.reserveRecord(RESERVATION);
                long reservedEnd = off + MmapSegment.FRAME_HEADER_SIZE + RESERVATION;
                // the sender abandons only a record without committed rows
                writeRows(seg, off, writer, tb, 1, false);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(1, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);

                // that barrier wrote the record's bytes to disk: the zeroes replacing them must follow
                writer.abandon();
                seg.abandonReserved(off, writer.maxUsedBytes());
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(2, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals("nothing changed since the last barrier", 2, ff.calls());

                // the same without a barrier while the record was reserved
                Assert.assertEquals(off, seg.reserveRecord(RESERVATION));
                seg.abandonReserved(off, 0);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(3, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(3, ff.calls());
            }
        });
    }

    @Test
    public void testBarrierCoversTheTailZeroedBySeal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/barrier-seal.sfa";
            MsyncRecordingFacade ff = new MsyncRecordingFacade();
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 MmapSegment seg = MmapSegment.create(ff, path, 0L, SEGMENT_SIZE)) {
                fillPattern(frame, 16, 7);
                long off = seg.reserveRecord(RESERVATION);
                long reservedEnd = off + MmapSegment.FRAME_HEADER_SIZE + RESERVATION;
                writeRows(seg, off, writer, tb, 3, true);
                int committedUsed = writer.usedBytes();
                // an uncommitted row in a new chunk past usedBytes, which the next barrier writes to disk
                appendRow(writer, tb, 3);
                Assert.assertTrue(writer.usedBytes() > committedUsed);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(1, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);

                // the seal zeroes that row with the rest of the tail, and does not publish
                int used = writer.seal(false);
                Assert.assertEquals(committedUsed, used);
                seg.sealReserved(off, used);
                Assert.assertTrue(off + MmapSegment.FRAME_HEADER_SIZE + used < reservedEnd);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(2, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals("nothing changed since the last barrier", 2, ff.calls());

                // publishing the record (with a frame behind it) needs a barrier again
                long frameOffset = seg.tryAppend(frame, 16);
                Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + used, frameOffset);
                Assert.assertFalse(seg.isPublishedDurable());
                Assert.assertEquals(seg.publishedOffset(), seg.syncPublished());
                Assert.assertTrue(seg.isPublishedDurable());
                Assert.assertEquals(3, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCorruptSealedRecordTruncatesLikeATornTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/corrupt-sealed.sfa";
            long frame = Unsafe.malloc(40, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                fillPattern(frame, 40, 5);
                long recordOffset = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 40;
                try (MmapSegment seg = MmapSegment.create(path, 10L, SEGMENT_SIZE)) {
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.tryAppend(frame, 40));
                    long off = seg.reserveRecord(RESERVATION);
                    Assert.assertEquals(recordOffset, off);
                    writeRows(seg, off, writer, tb, 3, true);
                    seg.sealReserved(off, writer.seal(false));
                    // publishes the sealed record together with the frame behind it
                    Assert.assertTrue(seg.tryAppend(frame, 40) > off);
                    Assert.assertEquals(3L, seg.frameCount());
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals("an intact sealed record and the frame after it are recovered",
                            3L, seg.frameCount());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    flipCommittedValueByte(seg.payloadAddress(recordOffset),
                            Unsafe.getUnsafe().getInt(seg.address() + recordOffset + 4));
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals("recovery stops at the corrupt record", 1L, seg.frameCount());
                    Assert.assertEquals(recordOffset, seg.publishedOffset());
                    Assert.assertEquals(SEGMENT_SIZE - recordOffset, seg.tornTailBytes());
                    Assert.assertTrue(seg.hasUnsanitizedTornTail());
                    // the bytes are left on disk, exactly as for a torn frame
                    Assert.assertEquals(QwoFormat.STATE_SEALED, Unsafe.getUnsafe().getInt(seg.address() + recordOffset));
                }
            } finally {
                Unsafe.free(frame, 40, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testInvalidOpenTailIsReportedNotZeroed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/open-invalid.sfa";
            long off = MmapSegment.HEADER_SIZE;
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    Assert.assertEquals(off, seg.reserveRecord(RESERVATION));
                    writeRows(seg, off, writer, tb, 3, true);
                    writer.abandon();
                    // flip a committed value byte: the row CRC no longer matches
                    flipCommittedValueByte(seg.payloadAddress(off), RESERVATION);
                }
            }
            byte[] onDisk = readFile(path);
            // openExisting only observes, so a second open sees exactly the same
            for (int pass = 0; pass < 2; pass++) {
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertTrue(seg.hasInvalidOpenRecord());
                    Assert.assertFalse(seg.hasRecoverableOpenRecord());
                    Assert.assertEquals(off, seg.recoveredOpenRecordOffset());
                    Assert.assertEquals(0, seg.recoveredOpenRecordRows());
                    TestUtils.assertContains(seg.recoveredOpenRecordDetail(), "block 0 (table 'trades') row crc mismatch");
                    Assert.assertEquals(0L, seg.frameCount());
                    Assert.assertEquals(off, seg.publishedOffset());
                    Assert.assertEquals(SEGMENT_SIZE - off, seg.tornTailBytes());
                    assertSealRefused(seg);
                    assertMapping(seg.address(), onDisk);
                }
                Assert.assertArrayEquals(onDisk, readFile(path));
            }
            // ring recovery zeroes the resumed active's residue; the record stays reported
            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                seg.sanitizeTornTail();
                assertZero(seg.address(), off, SEGMENT_SIZE);
                Assert.assertTrue(seg.hasInvalidOpenRecord());
                Assert.assertNotNull(seg.recoveredOpenRecordDetail());
            }
            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                Assert.assertFalse(seg.hasInvalidOpenRecord());
                Assert.assertEquals(-1L, seg.recoveredOpenRecordOffset());
                Assert.assertEquals(0L, seg.tornTailBytes());
            }
        });
    }

    @Test
    public void testNewSegmentsAreVersion2AndVersion1StillOpens() throws Exception {
        Assert.assertEquals(2, MmapSegment.VERSION);
        Assert.assertEquals(1, MmapSegment.MIN_VERSION);
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/version.sfa";
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                fillPattern(frame, 16, 9);
                try (MmapSegment seg = MmapSegment.create(path, 4L, SEGMENT_SIZE)) {
                    Assert.assertEquals(2, Unsafe.getUnsafe().getByte(seg.address() + 4));
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.tryAppend(frame, 16));
                }
                try (MmapSegment mem = MmapSegment.createInMemory(4L, SEGMENT_SIZE)) {
                    Assert.assertEquals(2, Unsafe.getUnsafe().getByte(mem.address() + 4));
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(1L, seg.frameCount());
                }

                patchVersion(path, 1);
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(4L, seg.baseSeq());
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                }

                for (int version : new int[]{3, 0}) {
                    patchVersion(path, version);
                    try {
                        MmapSegment.openExisting(path).close();
                        Assert.fail("version " + version + " must be refused");
                    } catch (MmapSegmentCorruptionException e) {
                        Assert.fail("an unsupported version is not corruption: " + e.getMessage());
                    } catch (MmapSegmentException expected) {
                        TestUtils.assertContains(expected.getMessage(), "unsupported version");
                    }
                }
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOpenTailUncommittedRowsAreDropped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/open-uncommitted.sfa";
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                int committedUsed;
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    writeRows(seg, off, writer, tb, 2, true);
                    committedUsed = writer.usedBytes();
                    // row 2 lands in the free slot of a published chunk, row 3 in a new chunk past usedBytes
                    appendRow(writer, tb, 2);
                    Assert.assertEquals(committedUsed, writer.usedBytes());
                    appendRow(writer, tb, 3);
                    Assert.assertTrue(writer.usedBytes() > committedUsed);
                    Assert.assertEquals(4, writer.rowCount());
                    Assert.assertEquals(2, writer.committedRowCount());
                    writer.abandon();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    long off = MmapSegment.HEADER_SIZE;
                    Assert.assertTrue(seg.hasRecoverableOpenRecord());
                    Assert.assertEquals(2, seg.recoveredOpenRecordRows());
                    Assert.assertEquals(0L, seg.frameCount());
                    Assert.assertEquals(QwoFormat.STATE_OPEN, Unsafe.getUnsafe().getInt(seg.address() + off));

                    Assert.assertEquals(off, seg.sealRecoveredOpenRecord());
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(committedUsed, Unsafe.getUnsafe().getInt(seg.address() + off + 4));
                    assertSealed(seg, off, 2);
                    Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + committedUsed, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    // the uncommitted rows' bytes past usedBytes are zeroed with the rest of the reservation
                    assertZero(seg.address(), seg.publishedOffset(), SEGMENT_SIZE);
                }
            }
        });
    }

    @Test
    public void testOpenTailWithCommittedRowsIsSealedOnRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/open-committed.sfa";
            long off = MmapSegment.HEADER_SIZE;
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                int used;
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    Assert.assertEquals(off, seg.reserveRecord(RESERVATION));
                    writeRows(seg, off, writer, tb, 3, true);
                    used = writer.usedBytes();
                    // a seal(true) cut right after its flags store leaves DEFER_COMMIT behind
                    long payload = seg.payloadAddress(off);
                    Unsafe.getUnsafe().putByte(payload + QwoFormat.OFF_FLAGS,
                            (byte) (FLAGS_OPEN | QwpConstants.FLAG_DEFER_COMMIT));
                    writer.abandon();
                    Assert.assertEquals(QwoFormat.STATE_OPEN, Unsafe.getUnsafe().getInt(seg.address() + off));
                    Assert.assertEquals(RESERVATION, Unsafe.getUnsafe().getInt(seg.address() + off + 4));
                    Assert.assertEquals(0L, seg.frameCount());
                }
                byte[] onDisk = readFile(path);
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    // openExisting reports the record and leaves it open, as torn tail
                    Assert.assertTrue(seg.hasRecoverableOpenRecord());
                    Assert.assertFalse(seg.hasInvalidOpenRecord());
                    Assert.assertEquals(off, seg.recoveredOpenRecordOffset());
                    Assert.assertEquals(3, seg.recoveredOpenRecordRows());
                    Assert.assertNull(seg.recoveredOpenRecordDetail());
                    Assert.assertEquals(0L, seg.frameCount());
                    Assert.assertEquals(off, seg.publishedOffset());
                    Assert.assertEquals(SEGMENT_SIZE - off, seg.tornTailBytes());
                    assertMapping(seg.address(), onDisk);

                    Assert.assertEquals(off, seg.sealRecoveredOpenRecord());
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(QwoFormat.STATE_SEALED, Unsafe.getUnsafe().getInt(seg.address() + off));
                    Assert.assertEquals(used, Unsafe.getUnsafe().getInt(seg.address() + off + 4));
                    long payload = seg.payloadAddress(off);
                    Assert.assertEquals(FLAGS_OPEN, Unsafe.getUnsafe().getByte(payload + QwoFormat.OFF_FLAGS));
                    Assert.assertEquals(used, Unsafe.getUnsafe().getInt(payload + QwoFormat.OFF_USED_BYTES));
                    assertSealed(seg, off, 3);
                    Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + used, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    Assert.assertFalse(seg.hasUnsanitizedTornTail());
                    Assert.assertFalse(seg.hasRecoverableOpenRecord());
                    Assert.assertEquals(-1L, seg.recoveredOpenRecordOffset());
                    assertZero(seg.address(), seg.publishedOffset(), SEGMENT_SIZE);
                    assertSealRefused(seg);
                }
                // the recovery seal is a fixed point: reopening changes nothing
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + used, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    Assert.assertFalse(seg.hasRecoverableOpenRecord());
                    Assert.assertEquals(-1L, seg.recoveredOpenRecordOffset());
                }
            }
        });
    }

    @Test
    public void testOpenTailWithoutCommittedRowsIsReportedAsTornTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/open-empty.sfa";
            long off = MmapSegment.HEADER_SIZE;
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    Assert.assertEquals(off, seg.reserveRecord(RESERVATION));
                    writeRows(seg, off, writer, tb, 1, false);
                    Assert.assertTrue(writer.hasUncommittedRows());
                    Assert.assertFalse(writer.hasCommittedRows());
                    writer.abandon();
                }
            }
            byte[] onDisk = readFile(path);
            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                Assert.assertEquals(0L, seg.frameCount());
                Assert.assertFalse(seg.hasRecoverableOpenRecord());
                Assert.assertFalse(seg.hasInvalidOpenRecord());
                Assert.assertEquals(off, seg.recoveredOpenRecordOffset());
                Assert.assertEquals(0, seg.recoveredOpenRecordRows());
                Assert.assertNull(seg.recoveredOpenRecordDetail());
                // the residue runs from the open record to the file end
                Assert.assertEquals(SEGMENT_SIZE - off, seg.tornTailBytes());
                Assert.assertEquals(off, seg.publishedOffset());
                Assert.assertEquals(SEGMENT_SIZE - off - MmapSegment.FRAME_HEADER_SIZE, seg.capacityRemaining());
                assertMapping(seg.address(), onDisk);
                assertSealRefused(seg);

                // ring recovery zeroes the resumed active's residue by policy
                seg.sanitizeTornTail();
                assertZero(seg.address(), off, SEGMENT_SIZE);
            }
            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                Assert.assertEquals(0L, seg.frameCount());
                Assert.assertEquals(0L, seg.tornTailBytes());
                Assert.assertEquals(-1L, seg.recoveredOpenRecordOffset());
            }
        });
    }

    @Test
    public void testReserveRefusedWhenNoRoom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/no-room.sfa";
            try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                Assert.assertEquals(-1L, seg.reserveRecord((int) SEGMENT_SIZE));
                int fits = (int) (SEGMENT_SIZE - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE);
                Assert.assertEquals(-1L, seg.reserveRecord(fits + 1));
                Assert.assertEquals(-1L, seg.reservedOffset());
                assertZero(seg.address(), MmapSegment.HEADER_SIZE, SEGMENT_SIZE);

                long off = seg.reserveRecord(fits);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, off);
                Assert.assertEquals(off, seg.reservedOffset());
                Assert.assertEquals(fits, seg.reservedCapacity());
                Assert.assertEquals(seg.address() + off + MmapSegment.FRAME_HEADER_SIZE, seg.payloadAddress(off));
                Assert.assertEquals(QwoFormat.STATE_OPEN, Unsafe.getUnsafe().getInt(seg.address() + off));
                Assert.assertEquals(fits, Unsafe.getUnsafe().getInt(seg.address() + off + 4));
                Assert.assertEquals(0L, seg.capacityRemaining());
                Assert.assertTrue(seg.isFull());
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.publishedOffset());
                Assert.assertEquals(0L, seg.frameCount());
                try {
                    seg.reserveRecord(QwoFormat.HEADER_SIZE);
                    Assert.fail("a second reservation must be refused");
                } catch (IllegalStateException expected) {
                    TestUtils.assertContains(expected.getMessage(), "record reserved");
                }
                seg.abandonReserved(off, 0);
                Assert.assertEquals(0, seg.reservedCapacity());
                assertZero(seg.address(), MmapSegment.HEADER_SIZE, SEGMENT_SIZE);
            }
        });
    }

    @Test
    public void testReserveSealReopenCountsOneFrame() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seal.sfa";
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                int used;
                try (MmapSegment seg = MmapSegment.create(path, 5L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, off);
                    Assert.assertEquals(RESERVATION, seg.reservedCapacity());
                    Assert.assertEquals(SEGMENT_SIZE - off - 2 * MmapSegment.FRAME_HEADER_SIZE - RESERVATION,
                            seg.capacityRemaining());
                    writeRows(seg, off, writer, tb, 3, true);
                    used = writer.seal(false);
                    seg.sealReserved(off, used);
                    Assert.assertEquals(-1L, seg.reservedOffset());
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(QwoFormat.STATE_SEALED, Unsafe.getUnsafe().getInt(seg.address() + off));
                    Assert.assertEquals(used, Unsafe.getUnsafe().getInt(seg.address() + off + 4));
                    long end = off + MmapSegment.FRAME_HEADER_SIZE + used;
                    Assert.assertEquals(SEGMENT_SIZE - end - MmapSegment.FRAME_HEADER_SIZE, seg.capacityRemaining());
                    assertZero(seg.address(), end, SEGMENT_SIZE);
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(5L, seg.baseSeq());
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + used, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    assertZero(seg.address(), seg.publishedOffset(), SEGMENT_SIZE);
                    assertSealed(seg, MmapSegment.HEADER_SIZE, 3);
                }
            }
        });
    }

    @Test
    public void testSealLeavesTheReservationAboveTheHighWaterMarkUntouched() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seal-mark.sfa";
            long off = MmapSegment.HEADER_SIZE;
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    Assert.assertEquals(off, seg.reserveRecord(RESERVATION));
                    long payload = seg.payloadAddress(off);
                    writeRows(seg, off, writer, tb, 3, true);
                    int committedUsed = writer.usedBytes();
                    // an uncommitted row in a new chunk past usedBytes, which the seal rolls back
                    appendRow(writer, tb, 3);
                    int mark = writer.maxUsedBytes();
                    Assert.assertTrue(mark > committedUsed);
                    // the zero-fill invariant keeps these bytes zero; poisoned, they show what the seal touches
                    Unsafe.getUnsafe().setMemory(payload + mark, RESERVATION - mark, POISON);

                    int used = writer.seal(false);
                    Assert.assertEquals(committedUsed, used);
                    seg.sealReserved(off, used);
                    // the writer zeroed the bytes its appends dirtied; neither it nor the segment touched the rest
                    assertZero(payload, used, mark);
                    assertFilled(payload + mark, RESERVATION - mark, POISON);
                    assertSealed(seg, off, 3);
                    // what the invariant guarantees in production, so the segment reopens clean
                    Unsafe.getUnsafe().setMemory(payload + mark, RESERVATION - mark, (byte) 0);
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    assertSealed(seg, off, 3);
                }
            }
        });
    }

    @Test
    public void testSealRecoveredOpenRecordWithoutOneThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/no-open.sfa";
            String openPath = tmpDir + "/open-recoverable.sfa";
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                fillPattern(frame, 16, 2);
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    Assert.assertEquals(-1L, seg.recoveredOpenRecordOffset());
                    assertSealRefused(seg);
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.tryAppend(frame, 16));
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertFalse(seg.hasRecoverableOpenRecord());
                    assertSealRefused(seg);
                    Assert.assertEquals(1L, seg.frameCount());
                }

                try (MmapSegment seg = MmapSegment.create(openPath, 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    writeRows(seg, off, writer, tb, 2, true);
                    writer.abandon();
                }
                // sanitizing the tail zeroes the record, which is then no longer recoverable
                try (MmapSegment seg = MmapSegment.openExisting(openPath)) {
                    Assert.assertTrue(seg.hasRecoverableOpenRecord());
                    seg.sanitizeTornTail();
                    Assert.assertFalse(seg.hasRecoverableOpenRecord());
                    assertSealRefused(seg);
                }
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSyncPublishedCoversReservation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/sync.sfa";
            MsyncRecordingFacade ff = new MsyncRecordingFacade();
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades");
                 MmapSegment seg = MmapSegment.create(ff, path, 0L, SEGMENT_SIZE)) {
                long off = seg.reserveRecord(RESERVATION);
                long reservedEnd = off + MmapSegment.FRAME_HEADER_SIZE + RESERVATION;
                writeRows(seg, off, writer, tb, 2, true);

                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(1, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);

                appendRow(writer, tb, 2);
                writer.publish();
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(2, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);

                // the open record changes in place, so every barrier covers it again while it is reserved
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals(3, ff.calls());
                assertMsyncReaches(ff, seg, reservedEnd);
                Assert.assertTrue(seg.isPublishedDurable());
            }
        });
    }

    @Test
    public void testTryWriteWhileReservedThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/write-reserved.sfa";
            long frame = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                fillPattern(frame, 16, 1);
                long off = seg.reserveRecord(RESERVATION);
                long remaining = seg.capacityRemaining();
                try {
                    seg.tryAppend(frame, 16);
                    Assert.fail("a frame must not be written over a reservation");
                } catch (IllegalStateException expected) {
                    TestUtils.assertContains(expected.getMessage(), "record reserved");
                }
                Assert.assertEquals(0L, seg.frameCount());
                Assert.assertEquals(remaining, seg.capacityRemaining());
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.publishedOffset());
                seg.abandonReserved(off, 0);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.tryAppend(frame, 16));
                Assert.assertEquals(1L, seg.frameCount());
            } finally {
                Unsafe.free(frame, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testValidOpenRecordTailLogsNoTornTailWarning() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger)
                    org.slf4j.LoggerFactory.getLogger(MmapSegment.class);
            ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                    new ch.qos.logback.core.read.ListAppender<>();
            appender.start();
            ch.qos.logback.classic.Level savedLevel = logger.getLevel();
            logger.setLevel(ch.qos.logback.classic.Level.ALL);
            logger.addAppender(appender);
            long off = MmapSegment.HEADER_SIZE;
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                // 0: a recoverable open record and nothing past its reservation;
                // 1: the same with a stray byte past the reservation;
                // 2: an open record that fails validation;
                // 3: a valid open record without committed rows (a crash after a seal, before
                //    the next publish) and nothing past its reservation
                for (int tail = 0; tail < 4; tail++) {
                    String path = tmpDir + "/warn" + tail + ".sfa";
                    try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                        Assert.assertEquals(off, seg.reserveRecord(RESERVATION));
                        writeRows(seg, off, writer, tb, 3, tail != 3);
                        writer.abandon();
                        if (tail == 1) {
                            Unsafe.getUnsafe().putByte(seg.address() + off + MmapSegment.FRAME_HEADER_SIZE + RESERVATION + 100, (byte) 1);
                        } else if (tail == 2) {
                            flipCommittedValueByte(seg.payloadAddress(off), RESERVATION);
                        }
                    }
                    appender.list.clear();
                    try (MmapSegment seg = MmapSegment.openExisting(path)) {
                        Assert.assertEquals("tail " + tail, tail == 0 || tail == 1, seg.hasRecoverableOpenRecord());
                        Assert.assertEquals("tail " + tail, tail == 2, seg.hasInvalidOpenRecord());
                        Assert.assertTrue(seg.tornTailBytes() > 0);
                    }
                    boolean isWarned = false;
                    for (ch.qos.logback.classic.spi.ILoggingEvent e : appender.list) {
                        if (e.getLevel() == ch.qos.logback.classic.Level.WARN
                                && e.getFormattedMessage().contains("torn tail")) {
                            isWarned = true;
                            break;
                        }
                    }
                    Assert.assertEquals("only a torn tail that is not exactly a valid open record warns [tail="
                            + tail + ", events=" + appender.list + ']', tail == 1 || tail == 2, isWarned);
                }
            } finally {
                logger.detachAppender(appender);
                logger.setLevel(savedLevel);
                appender.stop();
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

    private static void assertFilled(long addr, long len, byte value) {
        for (long p = 0; p < len; p++) {
            byte b = Unsafe.getUnsafe().getByte(addr + p);
            if (b != value) {
                Assert.fail("byte " + b + " at offset " + p + ", expected " + value);
            }
        }
    }

    private static void assertMapping(long base, byte[] expected) {
        for (int p = 0; p < expected.length; p++) {
            if (Unsafe.getUnsafe().getByte(base + p) != expected[p]) {
                Assert.fail("mapping differs from the file at offset " + p);
            }
        }
    }

    private static void assertMsyncReaches(MsyncRecordingFacade ff, MmapSegment seg, long end) {
        Assert.assertEquals(seg.address(), ff.lastAddr());
        Assert.assertTrue("msync ends at " + ff.lastLen() + ", below " + end, ff.lastLen() >= end);
    }

    private static void assertSealRefused(MmapSegment seg) {
        try {
            seg.sealRecoveredOpenRecord();
            Assert.fail("there is no recoverable open record to seal");
        } catch (IllegalStateException expected) {
            TestUtils.assertContains(expected.getMessage(), "no recoverable open record");
        }
    }

    private static void assertSealed(MmapSegment seg, long off, int rows) {
        Assert.assertEquals(QwoFormat.STATE_SEALED, Unsafe.getUnsafe().getInt(seg.address() + off));
        int len = Unsafe.getUnsafe().getInt(seg.address() + off + 4);
        QwoValidator validator = new QwoValidator();
        Assert.assertTrue(validator.detail(), validator.validateSealed(seg.payloadAddress(off), len));
        Assert.assertEquals(rows, validator.committedRows());
        Assert.assertEquals(len, validator.usedBytes());
    }

    private static void assertZero(long base, long from, long to) {
        for (long p = from; p < to; p++) {
            if (Unsafe.getUnsafe().getByte(base + p) != 0) {
                Assert.fail("non-zero byte at offset " + p);
            }
        }
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

    private static void patchVersion(String path, int version) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
            file.seek(4L);
            file.writeByte(version);
        }
    }

    private static byte[] readFile(String path) throws Exception {
        return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path));
    }

    /**
     * Opens a record in the reservation at {@code off} and appends {@code rows}
     * rows, publishing them when asked.
     */
    private static void writeRows(MmapSegment seg, long off, QwoRecordWriter writer, QwpTableBuffer tb, int rows, boolean publish) {
        writer.open(seg.payloadAddress(off), seg.reservedCapacity(), QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
        for (int r = 0; r < rows; r++) {
            appendRow(writer, tb, r);
        }
        if (publish) {
            writer.publish();
        }
    }
}
