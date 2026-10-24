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

import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.client.QwpSfTestUtils.FrameLog;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.FullDiskDictFacade;
import io.questdb.client.test.tools.MsyncRecordingFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.cutlass.qwp.client.QwpOpenRecordSenderTest.dataFrame;
import static io.questdb.client.test.cutlass.qwp.client.QwpOpenRecordSenderTest.longRow;
import static io.questdb.client.test.cutlass.qwp.client.QwpOpenRecordSenderTest.symbolRow;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The open record with a store-and-forward directory: rows published into the
 * open record survive a process crash and ship once a new sender recovers the
 * slot; a sealed record's new symbols reach {@code .symbol-dict} before its
 * seal, and a failed write there switches the sender to full-dictionary frames
 * after the seal; a corrupt open tail is reported as DATA_LOSS; a version 1
 * active segment is rotated before it would take a record; the periodic
 * barrier covers the reservation. Every data frame is compared byte for byte
 * with the frame the legacy encoder builds for the same rows.
 */
public class QwpOpenRecordRecoveryTest {
    private static final String SENDER_ID = "s1";
    private static final long TS_BASE = 1_700_000_000_000_000L;
    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();
    private String sfDir;

    @Before
    public void setUp() {
        sfDir = temporaryFolder.getRoot().toPath().resolve("sf").toString();
    }

    @Test
    public void testAbandonedRecordLeavesTheSegmentZeroPastItsOffset() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle();
                 TestWebSocketServer server = startServer(log);
                 QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 5_000))) {
                QwpTableBuffer t = oracle.block("t");
                // rows that write blocks and chunks past the record's header and dictionary reserve
                sender.table("t").longColumn("v", 1).at(TS_BASE + 1, ChronoUnit.MICROS);
                sender.table("u").stringColumn("s", "abc").longColumn("w", 2).at(TS_BASE + 2, ChronoUnit.MICROS);
                MmapSegment active = sender.getCursorEngineForTesting().activeSegment();
                long recordOffset = active.reservedOffset();
                Assert.assertTrue(recordOffset >= 0);
                // reset() rolls them back, so the flush gives the empty record back to the ring
                sender.reset();
                Assert.assertEquals(0, sender.pendingRowCount());
                sender.flush();
                Assert.assertEquals(-1L, active.reservedOffset());
                for (long p = recordOffset, end = active.sizeBytes(); p < end; p++) {
                    if (Unsafe.getUnsafe().getByte(active.address() + p) != 0) {
                        Assert.fail("non-zero byte at offset " + p + " past the abandoned record at " + recordOffset);
                    }
                }
                Assert.assertEquals("nothing shipped", 0, log.dataFrames());

                // the next record takes the space back
                longRow(sender, t, "v", 3);
                Assert.assertEquals(recordOffset, active.reservedOffset());
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testCloseSealsTheOpenRecordWhenAConnectionErrorIsLatched() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle()) {
                QwpTableBuffer t = oracle.block("t");
                try (TestWebSocketServer server = startServer(new SilentHandler())) {
                    QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 0));
                    longRow(sender, t, "v", 1);
                    longRow(sender, t, "v", 2);
                    sender.publishPending();
                    // appended, not published: only a close that seals the record ships it
                    longRow(sender, t, "v", 3);
                    long recordOffset = sender.getCursorEngineForTesting().getRingForTesting().getActive().reservedOffset();
                    Assert.assertTrue(recordOffset >= 0);
                    latchConnectionError(sender);
                    // close() skips its flush, commit and drain; its safety net still seals the record
                    sender.close();
                    Assert.assertEquals("the record is sealed on disk", QwoFormat.STATE_SEALED,
                            QwpSfTestUtils.readInt(activeSegmentPath(), recordOffset));
                }
                FrameLog log = new FrameLog();
                try (TestWebSocketServer server = startServer(log);
                     Sender ignored = Sender.fromConfig(config(server.getPort(), 5_000))) {
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals("all three rows, the unpublished one too", oracle.frame(-1, false), dataFrame(log, 0));
                }
                Assert.assertEquals(1, log.dataFrames());
            }
        });
    }

    @Test
    public void testCorruptOpenTailIsReportedAsDataLoss() throws Exception {
        assertMemoryLeak(() -> {
            String segmentPath = activeSegmentPath();
            long recordOffset = crashWithACorruptOpenRecord();

            List<SenderError> errors = new ArrayList<>();
            FrameLog log = new FrameLog();
            try (QwpOpenRecordSenderTest.Oracle later = new QwpOpenRecordSenderTest.Oracle();
                 TestWebSocketServer server = startServer(log)) {
                QwpTableBuffer t = later.block("t");
                SenderErrorHandler handler = error -> {
                    synchronized (errors) {
                        errors.add(error);
                    }
                };
                try (Sender sender = Sender.builder(config(server.getPort(), 5_000)).errorHandler(handler).build()) {
                    synchronized (errors) {
                        Assert.assertEquals("one report, at build time: " + errors, 1, errors.size());
                        SenderError loss = errors.get(0);
                        Assert.assertEquals(SenderError.Category.DATA_LOSS, loss.getCategory());
                        Assert.assertEquals(slotPath(), loss.getQuarantinedPath());
                        TestUtils.assertContains(loss.getServerMessage(), segmentPath);
                        TestUtils.assertContains(loss.getServerMessage(), "offset " + recordOffset);
                        TestUtils.assertContains(loss.getServerMessage(), "row crc mismatch");
                    }
                    // the corrupt record's rows are gone; later rows flow
                    longRow(sender, t, "v", 4);
                    sender.flush();
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(later.frame(-1, false), dataFrame(log, 0));
                }
                Assert.assertEquals(1, log.dataFrames());
                synchronized (errors) {
                    Assert.assertEquals("reported once: " + errors, 1, errors.size());
                }
            }
        });
    }

    @Test
    public void testCorruptOpenTailOfAnOrphanSlotIsReportedByItsDrainer() throws Exception {
        assertMemoryLeak(() -> {
            long recordOffset = crashWithACorruptOpenRecord();
            List<SenderError> errors = Collections.synchronizedList(new ArrayList<SenderError>());
            BackgroundDrainer drainer = new BackgroundDrainer(slotPath(), 4L * 1024 * 1024, 1L << 30,
                    () -> {
                        throw new AssertionError("the corrupt record was the slot's only content, nothing is left to send");
                    },
                    5_000L, 1L, 5L, false, 200L);
            drainer.setErrorSink(errors::add);
            drainer.run();
            Assert.assertEquals(BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
            Assert.assertEquals("one report: " + errors, 1, errors.size());
            SenderError loss = errors.get(0);
            Assert.assertEquals(SenderError.Category.DATA_LOSS, loss.getCategory());
            Assert.assertEquals(slotPath(), loss.getQuarantinedPath());
            TestUtils.assertContains(loss.getServerMessage(), "offset " + recordOffset);
            TestUtils.assertContains(loss.getServerMessage(), "row crc mismatch");
        });
    }

    @Test
    public void testCrashKeepsAFullyAcknowledgedSlot() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle();
                 TestWebSocketServer server = startServer(log)) {
                QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 0));
                longRow(sender, oracle.block("t"), "v", 1);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                CursorSendEngine engine = sender.getCursorEngineForTesting();
                Assert.assertFalse("nothing is reserved after the seal", engine.hasReservedRecord());
                Assert.assertEquals("everything published is acknowledged", engine.publishedFsn(), engine.ackedFsn());
                // a close would count the slot drained and unlink its segments; a crash does not
                sender.crashForTest();
                Assert.assertTrue("the crash keeps the slot", segmentFileCount() > 0);
                try (QwpWebSocketSender recovered = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 5_000))) {
                    Assert.assertTrue(recovered.getCursorEngineForTesting().wasRecoveredFromDisk());
                }
            }
        });
    }

    @Test
    public void testPeriodicDurabilityCoversTheReservation() throws Exception {
        assertMemoryLeak(() -> {
            final long segmentBytes = 256 * 1024;
            MsyncRecordingFacade ff = new MsyncRecordingFacade();
            FrameLog log = new FrameLog();
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle();
                 TestWebSocketServer server = startServer(log, 32 * 1024);
                 SegmentManager manager = new SegmentManager(segmentBytes, SegmentManager.DEFAULT_POLL_NANOS,
                         SegmentManager.UNLIMITED_TOTAL_BYTES, ff)) {
                manager.start();
                Assert.assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
                CursorSendEngine engine = new CursorSendEngine(slotPath(), segmentBytes, manager,
                        TimeUnit.MILLISECONDS.toNanos(10));
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", server.getPort(), null, 0, 0, 0L, null, false, engine)) {
                    QwpTableBuffer t = oracle.block("t");
                    int msyncsAtReserve = ff.calls();
                    for (int i = 1; i <= 3; i++) {
                        longRow(sender, t, "v", i);
                    }
                    MmapSegment active = engine.getRingForTesting().getActive();
                    long recordOffset = active.reservedOffset();
                    Assert.assertTrue("the rows sit in an open record", recordOffset >= 0);
                    // let a barrier flush the reservation before the rows are published, so the
                    // one the test then waits for is a later barrier, not the reservation's own
                    long reservationEnd = recordOffset + MmapSegment.FRAME_HEADER_SIZE + active.reservedCapacity();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                    while (ff.maxLenSince(active.address(), msyncsAtReserve) < reservationEnd && System.nanoTime() < deadline) {
                        Thread.sleep(5);
                    }
                    Assert.assertTrue("setup: a barrier flushed the reservation",
                            ff.maxLenSince(active.address(), msyncsAtReserve) >= reservationEnd);
                    sender.publishPending();
                    // only a barrier issued after the publish counts: the earlier one flushed the
                    // reservation before the rows were committed
                    int msyncsAtPublish = ff.calls();
                    long committedEnd = recordOffset + MmapSegment.FRAME_HEADER_SIZE + sender.recordUsedBytesForTest();
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                    while (ff.maxLenSince(active.address(), msyncsAtPublish) < committedEnd && System.nanoTime() < deadline) {
                        Thread.sleep(5);
                    }
                    Assert.assertTrue("a periodic barrier after the publish covers the committed rows [msync="
                                    + ff.maxLenSince(active.address(), msyncsAtPublish) + ", committedEnd=" + committedEnd + ']',
                            ff.maxLenSince(active.address(), msyncsAtPublish) >= committedEnd);
                    Assert.assertEquals("the barrier ships nothing", 0, log.dataFrames());
                    sender.flush();
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
                }
            }
        });
    }

    @Test
    public void testPersistFailureAtARecordFullSealFailsAnOverCapRefusedRowAlone() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            String pad = TestUtils.repeat("x", 3000);
            try (QwpOpenRecordSenderTest.Oracle first = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle second = new QwpOpenRecordSenderTest.Oracle();
                 TestWebSocketServer server = startServer(log, 8 * 1024)) {
                FullDiskDictFacade ff = new FullDiskDictFacade();
                QwpWebSocketSender sender = connectOnDictFacade(server.getPort(), ff);
                try {
                    ff.isArmed = true;
                    padRow(sender, first, first.block("t1"), "a", pad, 1);
                    padRow(sender, first, first.block("t2"), "b", pad, 2);
                    // a row over the 8 KiB cap: the record refuses it, its seal persists a and b,
                    // which fails, and the sender switches; the row then meets the frame path's cap
                    // guard and fails alone, as it would without the switch
                    try {
                        sender.table("t3").symbol("s", "c").stringColumn("p", TestUtils.repeat("x", 9000))
                                .at(TS_BASE + 3, ChronoUnit.MICROS);
                        Assert.fail("a row over the cap must fail on its at()");
                    } catch (LineSenderException expected) {
                        TestUtils.assertContains(expected.getMessage(), "row too large for server batch cap");
                    }
                    Assert.assertFalse("setup: the failed persist switched the sender", sender.isDeltaDictEnabledForTest());
                    Assert.assertEquals("nothing of the failed row is buffered", 0, sender.pendingRowCount());
                    Assert.assertFalse(sender.hasRowInProgress());
                    sender.checkError(); // nothing is retained
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(first.frame(-1, false), dataFrame(log, 0));

                    // the sender stays usable: a fitting row ships on the frame path. The failed
                    // row minted c, which full-dictionary mode keeps, so the frame carries it.
                    for (String s : new String[]{"a", "b", "c"}) {
                        second.dict.getOrAddSymbol(s);
                    }
                    padRow(sender, second, second.block("t4"), "d", "y", 4);
                    sender.flush();
                    log.awaitDataFrames(2, 5_000);
                    Assert.assertArrayEquals("a full-dictionary frame", second.frame(-1, false), dataFrame(log, 1));
                    sender.checkError();
                } finally {
                    sender.close();
                }
                Assert.assertEquals(2, log.dataFrames());
            }
        });
    }

    @Test
    public void testPersistFailureAtARecordFullSealMovesTheRefusedRowToTheFullDictionaryPath() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            String pad = TestUtils.repeat("x", 3000);
            try (QwpOpenRecordSenderTest.Oracle first = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle second = new QwpOpenRecordSenderTest.Oracle();
                 TestWebSocketServer server = startServer(log, 8 * 1024)) {
                FullDiskDictFacade ff = new FullDiskDictFacade();
                QwpWebSocketSender sender = connectOnDictFacade(server.getPort(), ff);
                try {
                    ff.isArmed = true;
                    // one table per row, so each row costs one block of one-row chunks
                    padRow(sender, first, first.block("t1"), "a", pad, 1);
                    padRow(sender, first, first.block("t2"), "b", pad, 2);
                    Assert.assertEquals(1, sender.recordOpensForTest());
                    // the record cannot take a third row: its seal persists a and b, which fails,
                    // and the sender switches; the refused row stays buffered for the frame path
                    second.dict.getOrAddSymbol("a");
                    second.dict.getOrAddSymbol("b");
                    padRow(sender, second, second.block("t3"), "c", pad, 3);
                    Assert.assertEquals("no record opened after the switch", 1, sender.recordOpensForTest());
                    Assert.assertEquals("the refused row is the frame path's pending row", 1, sender.pendingRowCount());
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(first.frame(-1, false), dataFrame(log, 0));
                    sender.flush();
                    log.awaitDataFrames(2, 5_000);
                    Assert.assertArrayEquals("a full-dictionary frame", second.frame(-1, false), dataFrame(log, 1));
                    Assert.assertEquals(0, sender.getPersistedSymbolsForTest().size());
                } finally {
                    sender.close();
                }
                Assert.assertEquals(2, log.dataFrames());
            }
        });
    }

    @Test
    public void testPersistFailureSealsThenSwitchesToFullDictionaryMode() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (QwpOpenRecordSenderTest.Oracle first = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle second = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle third = new QwpOpenRecordSenderTest.Oracle();
                 TestWebSocketServer server = startServer(log)) {
                FullDiskDictFacade ff = new FullDiskDictFacade();
                QwpWebSocketSender sender = connectOnDictFacade(server.getPort(), ff);
                try {
                    // the first growth of the .symbol-dict append window is refused
                    ff.isArmed = true;
                    QwpTableBuffer t1 = first.block("t");
                    symbolRow(sender, first, t1, "a", 1);
                    symbolRow(sender, first, t1, "b", 2);
                    // the persist fails; the seal completes and the record's frame carries its entries
                    sender.flush();
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(first.frame(-1, false), dataFrame(log, 0));
                    Assert.assertEquals("nothing reached the side file", 0, sender.getPersistedSymbolsForTest().size());
                    ff.isArmed = false;

                    // later rows go through the frame path, each frame with the whole dictionary
                    second.dict.getOrAddSymbol("a");
                    second.dict.getOrAddSymbol("b");
                    QwpTableBuffer t2 = second.block("t");
                    symbolRow(sender, second, t2, "c", 3);
                    symbolRow(sender, second, t2, "a", 4);
                    sender.flush();
                    log.awaitDataFrames(2, 5_000);
                    Assert.assertArrayEquals(second.frame(-1, false), dataFrame(log, 1));
                    QwpTableBuffer t3 = third.block("t");
                    symbolRow(sender, third, t3, "a", 5);
                    symbolRow(sender, third, t3, "b", 6);
                    sender.flush();
                    log.awaitDataFrames(3, 5_000);
                    Assert.assertArrayEquals(third.frame(-1, false), dataFrame(log, 2));
                    Assert.assertEquals("no record after the switch", 1, sender.recordOpensForTest());
                    Assert.assertEquals(0, sender.getPersistedSymbolsForTest().size());
                } finally {
                    sender.close();
                }
                Assert.assertEquals(3, log.dataFrames());
            }
        });
    }

    @Test
    public void testPersistFailureThenFullDictionaryFramesRecoverEveryRowAndSymbolOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpOpenRecordSenderTest.Oracle first = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle second = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle third = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle fourth = new QwpOpenRecordSenderTest.Oracle()) {
                // Phase 1, nothing acknowledged: the first record's persist fails at its seal, the
                // sender switches, and two frame-path flushes mint new ids inside their own full
                // dictionaries. Then the process crashes.
                try (TestWebSocketServer server = startServer(new SilentHandler())) {
                    FullDiskDictFacade ff = new FullDiskDictFacade();
                    QwpWebSocketSender sender = connectOnDictFacade(server.getPort(), ff);
                    ff.isArmed = true;
                    QwpTableBuffer t1 = first.block("t");
                    symbolRow(sender, first, t1, "a", 1);
                    symbolRow(sender, first, t1, "b", 2);
                    sender.flush();
                    ff.isArmed = false;
                    Assert.assertFalse("setup: the failed persist switched the sender", sender.isDeltaDictEnabledForTest());
                    Assert.assertEquals("setup: the side file holds none of the record's entries",
                            0, sender.getPersistedSymbolsForTest().size());
                    for (String s : new String[]{"a", "b"}) {
                        second.dict.getOrAddSymbol(s);
                        third.dict.getOrAddSymbol(s);
                    }
                    QwpTableBuffer t2 = second.block("t");
                    symbolRow(sender, second, t2, "c", 3);
                    symbolRow(sender, second, t2, "d", 4);
                    sender.flush();
                    for (String s : new String[]{"c", "d"}) {
                        third.dict.getOrAddSymbol(s);
                    }
                    symbolRow(sender, third, third.block("t"), "e", 5);
                    sender.flush();
                    Assert.assertEquals("setup: one record and two full-dictionary frames on the ring",
                            2L, sender.getCursorEngineForTesting().publishedFsn());
                    sender.crashForTest();
                }

                // Phase 2: recovery replays the record and both full-dictionary frames. A
                // full-dictionary frame carries the whole dictionary in a delta section from id 0, so
                // the recovery fold sees every id it defines: the slot keeps its side file (it holds
                // a record), the heal writes all five ids back, and a symbol the recovered sender
                // mints takes the id after them instead of colliding with c.
                FrameLog log = new FrameLog();
                try (TestWebSocketServer server = startServer(log);
                     QwpWebSocketSender recovered = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 5_000))) {
                    log.awaitDataFrames(3, 5_000);
                    Assert.assertArrayEquals("the record", first.frame(-1, false), dataFrame(log, 0));
                    Assert.assertArrayEquals("the first full-dictionary frame", second.frame(-1, false), dataFrame(log, 1));
                    Assert.assertArrayEquals("the second full-dictionary frame", third.frame(-1, false), dataFrame(log, 2));
                    Assert.assertTrue("the slot holds a record, so it recovers in delta mode",
                            recovered.isDeltaDictEnabledForTest());
                    Assert.assertEquals("the recovered side file holds every id the replayed frames define",
                            Arrays.asList("a", "b", "c", "d", "e"), toList(recovered.getPersistedSymbolsForTest()));
                    for (String s : new String[]{"a", "b", "c", "d", "e"}) {
                        fourth.dict.getOrAddSymbol(s);
                    }
                    symbolRow(recovered, fourth, fourth.block("t"), "f", 6);
                    recovered.flush();
                    log.awaitDataFrames(4, 5_000);
                    Assert.assertArrayEquals("f takes id 5, after every id the replayed frames define",
                            fourth.frame(4, false), dataFrame(log, 3));
                    List<String> dictionary = new ArrayList<>();
                    for (byte[] frame : log.frames()) {
                        QwpWireTestUtils.accumulateDeltaDictionary(frame, dictionary);
                    }
                    Assert.assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f"), dictionary);
                }
                Assert.assertEquals("every row ships exactly once", 4, log.dataFrames());
            }
        });
    }

    @Test
    public void testPersistSkipsTheEntriesTheSideFileAlreadyHolds() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle();
                 TestWebSocketServer server = startServer(log);
                 QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 5_000))) {
                QwpTableBuffer t = oracle.block("t");
                symbolRow(sender, oracle, t, "a", 1);
                symbolRow(sender, oracle, t, "b", 2);
                symbolRow(sender, oracle, t, "c", 3);
                // the side file already holds the record's first entry, as after a crash between a
                // persist and the seal it preceded
                PersistedSymbolDict pd = sender.getCursorEngineForTesting().getPersistedSymbolDict();
                pd.appendSymbol("a");
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
                ObjList<String> persisted = sender.getPersistedSymbolsForTest();
                Assert.assertEquals("a, b, c once each", 3, persisted.size());
                Assert.assertEquals("a", persisted.getQuick(0));
                Assert.assertEquals("b", persisted.getQuick(1));
                Assert.assertEquals("c", persisted.getQuick(2));
            }
        });
    }

    @Test
    public void testRecordSealFailureLeavesTheRecordForRecovery() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle()) {
                QwpTableBuffer t = oracle.block("t");
                try (TestWebSocketServer server = startServer(new SilentHandler())) {
                    QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 5_000));
                    try {
                        longRow(sender, t, "v", 1);
                        longRow(sender, t, "v", 2);
                        sender.publishPending();
                        sender.setRecordSealFaultForTesting(() -> {
                            throw new IllegalStateException("injected seal fault");
                        });
                        try {
                            sender.flush();
                            Assert.fail("the seal must fail");
                        } catch (LineSenderException expected) {
                            TestUtils.assertContains(expected.getMessage(), "seal");
                        }
                        Assert.assertTrue("the ring keeps the reservation", sender.getCursorEngineForTesting().hasReservedRecord());
                    } finally {
                        long start = System.nanoTime();
                        sender.close();
                        Assert.assertTrue("close() does not wait out the drain timeout",
                                System.nanoTime() - start < TimeUnit.SECONDS.toNanos(4));
                    }
                    Assert.assertTrue("close() keeps the slot", segmentFileCount() > 0);
                }
                FrameLog log = new FrameLog();
                try (TestWebSocketServer server = startServer(log);
                     Sender ignored = Sender.fromConfig(config(server.getPort(), 5_000))) {
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
                }
                Assert.assertEquals(1, log.dataFrames());
            }
        });
    }

    @Test
    public void testRowsPublishedAtReleaseSurviveACrash() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle()) {
                QwpTableBuffer t = oracle.block("t");
                SilentHandler silent = new SilentHandler();
                try (TestWebSocketServer server = startServer(silent)) {
                    QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 0));
                    for (int i = 1; i <= 3; i++) {
                        longRow(sender, t, "v", i);
                    }
                    sender.publishPending();
                    CursorSendEngine engine = sender.getCursorEngineForTesting();
                    Assert.assertTrue("the rows sit in an open record", engine.hasReservedRecord());
                    Assert.assertEquals("a publish puts no frame on the ring", -1L, engine.publishedFsn());
                    sender.crashForTest();
                    assertClosed(sender);
                }
                Assert.assertEquals("nothing reached the wire before the crash", 0, silent.messages());
                FrameLog log = new FrameLog();
                try (TestWebSocketServer server = startServer(log);
                     Sender ignored = Sender.fromConfig(config(server.getPort(), 5_000))) {
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
                }
                Assert.assertEquals("the rows arrive once", 1, log.dataFrames());
            }
        });
    }

    @Test
    public void testSymbolsOfARecoveredOpenTailResolve() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle next = new QwpOpenRecordSenderTest.Oracle()) {
                QwpTableBuffer t = oracle.block("t");
                try (TestWebSocketServer server = startServer(new SilentHandler())) {
                    QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 0));
                    symbolRow(sender, oracle, t, "a", 1);
                    symbolRow(sender, oracle, t, "b", 2);
                    symbolRow(sender, oracle, t, "a", 3);
                    symbolRow(sender, oracle, t, "c", 4);
                    sender.publishPending();
                    Assert.assertEquals("the entries live only in the record until its seal",
                            0, sender.getPersistedSymbolsForTest().size());
                    sender.crashForTest();
                }
                FrameLog log = new FrameLog();
                try (TestWebSocketServer server = startServer(log);
                     QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 5_000))) {
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals("the recovered frame's delta carries the record's entries",
                            oracle.frame(-1, false), dataFrame(log, 0));
                    Assert.assertEquals("recovery wrote the record's entries to the side file",
                            Arrays.asList("a", "b", "c"), toList(sender.getPersistedSymbolsForTest()));
                    // the next symbol the recovered sender mints takes the id after the record's last
                    next.dict.getOrAddSymbol("a");
                    next.dict.getOrAddSymbol("b");
                    next.dict.getOrAddSymbol("c");
                    QwpTableBuffer t2 = next.block("t");
                    symbolRow(sender, next, t2, "d", 5);
                    symbolRow(sender, next, t2, "b", 6);
                    sender.flush();
                    log.awaitDataFrames(2, 5_000);
                    Assert.assertArrayEquals("d takes id 3, after the record's dictEndId 2",
                            next.frame(2, false), dataFrame(log, 1));
                    List<String> dictionary = new ArrayList<>();
                    for (byte[] frame : log.frames()) {
                        QwpWireTestUtils.accumulateDeltaDictionary(frame, dictionary);
                    }
                    Assert.assertEquals(Arrays.asList("a", "b", "c", "d"), dictionary);
                }
            }
        });
    }

    @Test
    public void testUnpublishedRowsDoNotSurviveACrash() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle()) {
                QwpTableBuffer t = oracle.block("t");
                try (TestWebSocketServer server = startServer(new SilentHandler())) {
                    QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 0));
                    longRow(sender, t, "v", 1);
                    longRow(sender, t, "v", 2);
                    sender.publishPending();
                    // not in the oracle: the crash loses it
                    sender.table("t").longColumn("v", 3).at(TS_BASE + 3, ChronoUnit.MICROS);
                    Assert.assertEquals(3, sender.pendingRowCount());
                    sender.crashForTest();
                }
                FrameLog log = new FrameLog();
                try (TestWebSocketServer server = startServer(log);
                     Sender ignored = Sender.fromConfig(config(server.getPort(), 5_000))) {
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
                }
                Assert.assertEquals(1, log.dataFrames());
            }
        });
    }

    @Test
    public void testVersion1DirectoryIsRotatedBeforeTheFirstRecord() throws Exception {
        assertMemoryLeak(() -> {
            final long segmentBytes = 1024 * 1024;
            String initialPath = slotPath() + "/sf-initial.sfa";
            try (QwpOpenRecordSenderTest.Oracle older = new QwpOpenRecordSenderTest.Oracle();
                 QwpOpenRecordSenderTest.Oracle newer = new QwpOpenRecordSenderTest.Oracle()) {
                // a version 1 slot as an older client leaves it: one frame, an empty side file
                QwpTableBuffer t0 = older.block("t");
                t0.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(0);
                t0.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE);
                t0.nextRow();
                byte[] olderFrame = older.frame(-1, false);
                java.nio.file.Files.createDirectories(java.nio.file.Paths.get(slotPath()));
                writeVersion1Segment(initialPath, segmentBytes, olderFrame);
                PersistedSymbolDict.openClean(slotPath()).close();

                FrameLog log = new FrameLog();
                try (TestWebSocketServer server = startServer(log);
                     QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 5_000)
                             + "sf_max_segment_bytes=" + segmentBytes + ';')) {
                    CursorSendEngine engine = sender.getCursorEngineForTesting();
                    Assert.assertEquals("the recovered active is the version 1 segment",
                            initialPath, engine.getRingForTesting().getActive().path());
                    Assert.assertEquals(1, engine.getRingForTesting().getActive().version());
                    longRow(sender, newer.block("t"), "v", 1);
                    MmapSegment active = engine.getRingForTesting().getActive();
                    Assert.assertNotEquals("the record went to a fresh segment", initialPath, active.path());
                    Assert.assertEquals("the fresh segment continues the FSNs", 1L, active.baseSeq());
                    Assert.assertTrue(active.reservedOffset() >= 0);
                    sender.flush();
                    log.awaitDataFrames(2, 5_000);
                    Assert.assertArrayEquals(olderFrame, dataFrame(log, 0));
                    Assert.assertArrayEquals(newer.frame(-1, false), dataFrame(log, 1));
                    Assert.assertEquals("the record's segment is version 2", MmapSegment.VERSION, readByte(active.path(), 4));
                }
            }
        });
    }

    @Test
    public void testVersion1EmptyActiveIsRotatedBeforeTheFirstRecord() throws Exception {
        assertMemoryLeak(() -> {
            final long segmentBytes = 1024 * 1024;
            String initialPath = slotPath() + "/sf-initial.sfa";
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(slotPath()));
            writeVersion1Segment(initialPath, segmentBytes, null);
            PersistedSymbolDict.openClean(slotPath()).close();
            try (QwpOpenRecordSenderTest.Oracle oracle = new QwpOpenRecordSenderTest.Oracle()) {
                QwpTableBuffer t = oracle.block("t");
                try (TestWebSocketServer server = startServer(new SilentHandler())) {
                    QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 0)
                            + "sf_max_segment_bytes=" + segmentBytes + ';');
                    longRow(sender, t, "v", 1);
                    MmapSegment active = sender.getCursorEngineForTesting().getRingForTesting().getActive();
                    Assert.assertNotEquals("the record went to a fresh segment", initialPath, active.path());
                    Assert.assertEquals(0L, active.baseSeq());
                    sender.publishPending();
                    sender.crashForTest();
                }
                // the rotated, empty version 1 segment does not stand in the record's way on recovery
                FrameLog log = new FrameLog();
                try (TestWebSocketServer server = startServer(log);
                     Sender ignored = Sender.fromConfig(config(server.getPort(), 5_000)
                             + "sf_max_segment_bytes=" + segmentBytes + ';')) {
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
                }
                Assert.assertEquals(1, log.dataFrames());
            }
        });
    }

    private static void assertClosed(Sender sender) {
        try {
            sender.table("t");
            Assert.fail("a crashed sender is closed");
        } catch (LineSenderException expected) {
            TestUtils.assertContains(expected.getMessage(), "closed");
        }
    }

    @SuppressWarnings("unchecked")
    private static void latchConnectionError(QwpWebSocketSender sender) throws Exception {
        Field f = QwpWebSocketSender.class.getDeclaredField("connectionError");
        f.setAccessible(true);
        ((AtomicReference<LineSenderException>) f.get(sender)).set(new LineSenderException("injected connection error"));
    }

    /**
     * Appends {@code table(t).symbol("s", symbol).stringColumn("p", pad).at(TS_BASE + value)}
     * to the sender and the same row to the oracle's block {@code t}.
     */
    private static void padRow(Sender sender, QwpOpenRecordSenderTest.Oracle oracle, QwpTableBuffer t, String symbol, String pad, long value) {
        sender.table(t.getTableName()).symbol("s", symbol).stringColumn("p", pad).at(TS_BASE + value, ChronoUnit.MICROS);
        t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId(symbol, oracle.dict.getOrAddSymbol(symbol));
        t.getOrCreateColumn("p", QwpConstants.TYPE_VARCHAR, true).addString(pad);
        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + value);
        t.nextRow();
    }

    private static int readByte(String path, long offset) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
            file.seek(offset);
            return file.readByte();
        }
    }

    private static TestWebSocketServer startServer(TestWebSocketServer.WebSocketServerHandler handler) throws Exception {
        return startServer(handler, 0);
    }

    private static TestWebSocketServer startServer(TestWebSocketServer.WebSocketServerHandler handler, int maxBatchSize) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.setAdvertisedMaxBatchSize(maxBatchSize);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static List<String> toList(ObjList<String> symbols) {
        List<String> list = new ArrayList<>();
        for (int i = 0, n = symbols.size(); i < n; i++) {
            list.add(symbols.getQuick(i));
        }
        return list;
    }

    /**
     * Writes a version 1 segment at {@code path} holding {@code frame}, or no
     * frame when it is null.
     */
    private static void writeVersion1Segment(String path, long segmentBytes, byte[] frame) throws Exception {
        try (MmapSegment segment = MmapSegment.create(path, 0L, segmentBytes)) {
            if (frame != null) {
                long buf = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < frame.length; i++) {
                        Unsafe.getUnsafe().putByte(buf + i, frame[i]);
                    }
                    Assert.assertEquals(MmapSegment.HEADER_SIZE, segment.tryAppend(buf, frame.length));
                } finally {
                    Unsafe.free(buf, frame.length, MemoryTag.NATIVE_DEFAULT);
                }
            }
            segment.msync();
        }
        try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
            file.seek(4L);
            file.writeByte(1);
        }
    }

    /**
     * The path of the slot's segment file holding the active segment's first
     * byte: the slot's only segment in these tests.
     */
    private String activeSegmentPath() {
        return slotPath() + "/sf-initial.sfa";
    }

    private String config(int port, long closeFlushTimeoutMillis) {
        return "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";sender_id=" + SENDER_ID
                + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000"
                + ";close_flush_timeout_millis=" + closeFlushTimeoutMillis + ';';
    }

    private QwpWebSocketSender connectOnDictFacade(int port, FullDiskDictFacade ff) {
        Assert.assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
        CursorSendEngine engine = new CursorSendEngine(slotPath(), 4L * 1024 * 1024,
                128L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff);
        return QwpWebSocketSender.connect("localhost", port, null, 0, 0, 0L, null, false, engine);
    }

    /**
     * Publishes three rows into an open record, crashes the sender and flips a
     * committed value byte of the record on disk.
     *
     * @return the record's envelope offset in {@link #activeSegmentPath()}
     */
    private long crashWithACorruptOpenRecord() throws Exception {
        long recordOffset;
        try (QwpOpenRecordSenderTest.Oracle lost = new QwpOpenRecordSenderTest.Oracle();
             TestWebSocketServer server = startServer(new SilentHandler())) {
            QwpTableBuffer t = lost.block("t");
            QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config(server.getPort(), 0));
            for (int i = 1; i <= 3; i++) {
                longRow(sender, t, "v", i);
            }
            sender.publishPending();
            recordOffset = sender.getCursorEngineForTesting().getRingForTesting().getActive().reservedOffset();
            Assert.assertTrue(recordOffset >= 0);
            sender.crashForTest();
        }
        QwpSfTestUtils.flipCommittedValueByte(activeSegmentPath(), recordOffset);
        return recordOffset;
    }

    private int segmentFileCount() {
        java.io.File[] files = new java.io.File(slotPath()).listFiles((dir, name) -> name.endsWith(".sfa"));
        return files == null ? 0 : files.length;
    }

    private String slotPath() {
        return sfDir + "/" + SENDER_ID;
    }

    /**
     * Counts the WebSocket messages it receives and acknowledges none.
     */
    private static final class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        private int messages;

        synchronized int messages() {
            return messages;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            messages++;
        }
    }
}
