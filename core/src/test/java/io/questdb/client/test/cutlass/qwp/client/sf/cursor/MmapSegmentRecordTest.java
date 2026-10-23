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
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.RandomAccessFile;

public class MmapSegmentRecordTest {
    private static final byte FLAGS_OPEN = (byte) (QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT);
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
    public void testAbandonedRecordLeavesZerosAndRecoversClean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/abandon.sfa";
            long frame = Unsafe.malloc(40, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                fillPattern(frame, 40, 3);
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    writeRows(seg, off, writer, tb, 2, true);
                    seg.abandonReserved(off);
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
                    // flip a committed value byte: the row CRC no longer matches
                    long payload = seg.payloadAddress(recordOffset);
                    QwoRecordReader reader = new QwoRecordReader();
                    reader.of(payload, Unsafe.getUnsafe().getInt(seg.address() + recordOffset + 4));
                    int def = reader.columnDef(reader.firstBlock(), 0);
                    long value = reader.chunkValuesAddr(reader.firstChunk(def), QwpConstants.TYPE_DOUBLE,
                            reader.columnUsesBitmap(def));
                    Unsafe.getUnsafe().putByte(value, (byte) (Unsafe.getUnsafe().getByte(value) ^ 0x40));
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
                    Assert.assertEquals(1L, seg.frameCount());
                    long off = MmapSegment.HEADER_SIZE;
                    Assert.assertEquals(committedUsed, Unsafe.getUnsafe().getInt(seg.address() + off + 4));
                    assertSealed(seg, off, 2);
                    Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + committedUsed, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    // the uncommitted row's bytes past usedBytes are zeroed with the rest of the reservation
                    assertZero(seg.address(), seg.publishedOffset(), SEGMENT_SIZE);
                }
            }
        });
    }

    @Test
    public void testOpenTailWithCommittedRowsIsSealedOnRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/open-committed.sfa";
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                int used;
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
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
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    long off = MmapSegment.HEADER_SIZE;
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(QwoFormat.STATE_SEALED, Unsafe.getUnsafe().getInt(seg.address() + off));
                    Assert.assertEquals(used, Unsafe.getUnsafe().getInt(seg.address() + off + 4));
                    long payload = seg.payloadAddress(off);
                    Assert.assertEquals(FLAGS_OPEN, Unsafe.getUnsafe().getByte(payload + QwoFormat.OFF_FLAGS));
                    Assert.assertEquals(used, Unsafe.getUnsafe().getInt(payload + QwoFormat.OFF_USED_BYTES));
                    assertSealed(seg, off, 3);
                    Assert.assertEquals(off + MmapSegment.FRAME_HEADER_SIZE + used, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    assertZero(seg.address(), seg.publishedOffset(), SEGMENT_SIZE);
                }
                // the recovery seal is a fixed point: reopening changes nothing
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(1L, seg.frameCount());
                    Assert.assertEquals(MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + used, seg.publishedOffset());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                }
            }
        });
    }

    @Test
    public void testOpenTailWithoutCommittedRowsIsZeroed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/open-empty.sfa";
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                try (MmapSegment seg = MmapSegment.create(path, 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    writeRows(seg, off, writer, tb, 1, false);
                    Assert.assertTrue(writer.hasUncommittedRows());
                    Assert.assertFalse(writer.hasCommittedRows());
                    writer.abandon();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    Assert.assertEquals(0L, seg.frameCount());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.publishedOffset());
                    Assert.assertEquals(SEGMENT_SIZE - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE,
                            seg.capacityRemaining());
                    assertZero(seg.address(), MmapSegment.HEADER_SIZE, SEGMENT_SIZE);
                }
            }

            // an open record that fails validation has nothing recoverable either: it is zeroed the same way
            String invalidPath = tmpDir + "/open-invalid.sfa";
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                try (MmapSegment seg = MmapSegment.create(invalidPath, 0L, SEGMENT_SIZE)) {
                    long off = seg.reserveRecord(RESERVATION);
                    writeRows(seg, off, writer, tb, 3, true);
                    writer.abandon();
                    long payload = seg.payloadAddress(off);
                    Unsafe.getUnsafe().putInt(payload + QwoFormat.OFF_CAPACITY, RESERVATION - 1);
                }
                try (MmapSegment seg = MmapSegment.openExisting(invalidPath)) {
                    Assert.assertEquals(0L, seg.frameCount());
                    Assert.assertEquals(0L, seg.tornTailBytes());
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.publishedOffset());
                    assertZero(seg.address(), MmapSegment.HEADER_SIZE, SEGMENT_SIZE);
                }
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
                seg.abandonReserved(off);
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
                Assert.assertEquals(1, ff.msyncCalls);
                Assert.assertEquals(seg.address(), ff.lastMsyncAddr);
                Assert.assertTrue("msync must reach the end of the reservation",
                        ff.lastMsyncAddr + ff.lastMsyncLen >= seg.address() + reservedEnd);

                // the open record keeps changing in place, so every barrier covers it again
                appendRow(writer, tb, 2);
                writer.publish();
                seg.syncPublished();
                Assert.assertEquals(2, ff.msyncCalls);
                Assert.assertTrue(ff.lastMsyncAddr + ff.lastMsyncLen >= seg.address() + reservedEnd);

                writer.abandon();
                seg.abandonReserved(off);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.syncPublished());
                Assert.assertEquals("nothing reserved or published: the barrier is a no-op", 2, ff.msyncCalls);
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
                seg.abandonReserved(off);
                Assert.assertEquals(MmapSegment.HEADER_SIZE, seg.tryAppend(frame, 16));
                Assert.assertEquals(1L, seg.frameCount());
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

    private static void patchVersion(String path, int version) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
            file.seek(4L);
            file.writeByte(version);
        }
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

    private static final class MsyncRecordingFacade extends DelegatingFilesFacade {
        long lastMsyncAddr;
        long lastMsyncLen;
        int msyncCalls;

        @Override
        public int msync(long addr, long len, boolean async) {
            msyncCalls++;
            lastMsyncAddr = addr;
            lastMsyncLen = len;
            return super.msync(addr, len, async);
        }
    }
}
