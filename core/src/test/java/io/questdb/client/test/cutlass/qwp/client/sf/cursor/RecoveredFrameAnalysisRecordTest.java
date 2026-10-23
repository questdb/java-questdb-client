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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
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

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * The recovery fold reads a sealed QWO1 record like a QWP frame: its flags
 * decide whether it is a commit boundary, and its dictionary range and
 * entries extend the recovered symbol suffix.
 */
public class RecoveredFrameAnalysisRecordTest {
    private static final int RESERVATION = 64 * 1024;
    private static final long SEGMENT_SIZE = 256 * 1024;
    private static final long TS_BASE = 1_700_000_000_000_000L;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-analysis-rec-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDirRec(tmpDir);
    }

    @Test
    public void testDeferredRecordDoesNotMoveTheBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                Assert.assertEquals(0L, appendPlainFrame(engine, dict, false, "a"));
                Assert.assertEquals(1L, sealRecord(engine, writer, dict, true, "b", "a", "c"));
            }
            try (CursorSendEngine reopened = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                Assert.assertEquals(1L, reopened.publishedFsn());
                Assert.assertEquals("a deferred record is not a commit boundary",
                        0L, reopened.recoveredCommitBoundaryFsn());
                Assert.assertEquals("the deferred record is the orphan tail",
                        1L, reopened.recoveredOrphanTipFsn());
                Assert.assertEquals("the orphan record's ids stay out of the committed result",
                        0L, reopened.recoveredMaxSymbolId());
                assertRecoveredSymbols(reopened, "a");
                Assert.assertEquals("the fold still walked the orphan record's entries",
                        3L, reopened.recoverySymbolEntriesVisited());
            }
        });
    }

    @Test
    public void testRecordDictionaryEntriesAreFolded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                Assert.assertEquals(0L, appendPlainFrame(engine, dict, false, "a", "b"));
                Assert.assertEquals(1L, sealRecord(engine, writer, dict, false, "c", "a", "d", "e", "d"));
            }
            try (CursorSendEngine reopened = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                Assert.assertEquals(1L, reopened.recoveredCommitBoundaryFsn());
                Assert.assertEquals(-1L, reopened.recoveredOrphanTipFsn());
                Assert.assertEquals("maxDeltaEnd covers the record's range", 4L, reopened.recoveredMaxSymbolId());
                Assert.assertEquals("the record's dictStartId is the largest delta start",
                        2L, reopened.recoveredMaxSymbolDeltaStart());
                assertRecoveredSymbols(reopened, "a", "b", "c", "d", "e");
                Assert.assertEquals(5L, reopened.recoverySymbolEntriesVisited());
            }
        });
    }

    @Test
    public void testRecordWithoutDeferFlagIsACommitBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String sealedDir = subDir("sealed");
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 CursorSendEngine engine = new CursorSendEngine(sealedDir, SEGMENT_SIZE)) {
                Assert.assertEquals(0L, appendPlainFrame(engine, dict, true, "a"));
                Assert.assertEquals(1L, sealRecord(engine, writer, dict, false, "b", "a"));
            }
            try (CursorSendEngine reopened = new CursorSendEngine(sealedDir, SEGMENT_SIZE)) {
                Assert.assertEquals("the record commits the deferred frame before it",
                        1L, reopened.recoveredCommitBoundaryFsn());
                Assert.assertEquals(-1L, reopened.recoveredOrphanTipFsn());
                Assert.assertEquals(1L, reopened.recoveredMaxSymbolId());
                assertRecoveredSymbols(reopened, "a", "b");
            }

            // A record the producer never sealed: the recovery seal clears DEFER,
            // so the recovered open tail commits the deferred frame before it.
            String openDir = subDir("open");
            dict = new GlobalSymbolDictionary();
            try (QwoRecordWriter writer = new QwoRecordWriter();
                 CursorSendEngine engine = new CursorSendEngine(openDir, SEGMENT_SIZE)) {
                Assert.assertEquals(0L, appendPlainFrame(engine, dict, true, "a"));
                reserveAndWrite(engine, writer, dict, "b", "c");
                Assert.assertTrue(engine.hasReservedRecord());
            }
            try (CursorSendEngine reopened = new CursorSendEngine(openDir, SEGMENT_SIZE)) {
                Assert.assertEquals("recovery seals the open record as the next frame", 1L, reopened.publishedFsn());
                Assert.assertEquals("a recovered open record is a commit boundary",
                        1L, reopened.recoveredCommitBoundaryFsn());
                Assert.assertEquals(-1L, reopened.recoveredOrphanTipFsn());
                Assert.assertEquals(2L, reopened.recoveredMaxSymbolId());
                assertRecoveredSymbols(reopened, "a", "b", "c");
            }
        });
    }

    /**
     * Appends a hand-built QWP frame that carries only a delta dictionary
     * section registering {@code symbols} after the ones {@code dict} holds.
     */
    private static long appendPlainFrame(CursorSendEngine engine, GlobalSymbolDictionary dict,
                                         boolean deferCommit, String... symbols) {
        int deltaStart = dict.size();
        byte[] entries = entries(symbols);
        int sectionLen = varintSize(deltaStart) + varintSize(symbols.length) + entries.length;
        int len = QwpConstants.HEADER_SIZE + sectionLen;
        long frame = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(frame, QwpConstants.MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(frame + 4, QwpConstants.VERSION);
            byte flags = (byte) (QwpConstants.FLAG_DELTA_SYMBOL_DICT
                    | (deferCommit ? QwpConstants.FLAG_DEFER_COMMIT : 0));
            Unsafe.getUnsafe().putByte(frame + QwpConstants.HEADER_OFFSET_FLAGS, flags);
            Unsafe.getUnsafe().putShort(frame + 6, (short) 0);
            Unsafe.getUnsafe().putInt(frame + 8, sectionLen);
            long p = putVarint(frame + QwpConstants.HEADER_SIZE, deltaStart);
            p = putVarint(p, symbols.length);
            for (int i = 0; i < entries.length; i++) {
                Unsafe.getUnsafe().putByte(p + i, entries[i]);
            }
            for (String s : symbols) {
                dict.getOrAddSymbol(s);
            }
            return engine.appendBlocking(frame, len);
        } finally {
            Unsafe.free(frame, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertRecoveredSymbols(CursorSendEngine engine, String... expected) {
        GlobalSymbolDictionary recovered = new GlobalSymbolDictionary();
        Assert.assertEquals("recovered coverage", expected.length, engine.addRecoveredSymbolsTo(0, recovered));
        Assert.assertEquals(expected.length, recovered.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("symbol " + i, expected[i], recovered.getSymbol(i));
        }
    }

    private static byte[] entries(String... symbols) {
        int len = 0;
        for (String s : symbols) {
            int n = s.getBytes(StandardCharsets.UTF_8).length;
            len += varintSize(n) + n;
        }
        byte[] out = new byte[len];
        int pos = 0;
        for (String s : symbols) {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            long v = utf8.length;
            while (v > 0x7F) {
                out[pos++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            out[pos++] = (byte) v;
            System.arraycopy(utf8, 0, out, pos, utf8.length);
            pos += utf8.length;
        }
        return out;
    }

    private static long putVarint(long p, long value) {
        while (value > 0x7F) {
            Unsafe.getUnsafe().putByte(p++, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        Unsafe.getUnsafe().putByte(p++, (byte) value);
        return p;
    }

    /**
     * Reserves a record that continues {@code dict}, and writes and publishes
     * one row per symbol; returns the envelope offset.
     */
    private static long reserveAndWrite(CursorSendEngine engine, QwoRecordWriter writer,
                                        GlobalSymbolDictionary dict, String... symbols) {
        long off = engine.reserveRecordBlocking(RESERVATION);
        writer.open(engine.recordPayloadAddress(off), engine.reservedRecordCapacity(),
                QwpConstants.VERSION, dict.size(), QwoFormat.DEFAULT_DICT_RESERVE);
        try (QwpTableBuffer table = new QwpTableBuffer("trades")) {
            for (int r = 0; r < symbols.length; r++) {
                table.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true)
                        .addSymbolWithGlobalId(symbols[r], dict.getOrAddSymbol(symbols[r]));
                table.getOrCreateColumn("qty", QwpConstants.TYPE_LONG, true).addLong(r);
                table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + r);
                table.nextRow();
                Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(table, dict, dict.size() - 1));
                table.clearStagedRow();
            }
        }
        writer.publish();
        return off;
    }

    private static long sealRecord(CursorSendEngine engine, QwoRecordWriter writer, GlobalSymbolDictionary dict,
                                   boolean deferCommit, String... symbols) {
        long off = reserveAndWrite(engine, writer, dict, symbols);
        return engine.sealRecord(off, writer.seal(deferCommit));
    }

    private static int varintSize(long value) {
        int n = 1;
        while (value > 0x7F) {
            value >>>= 7;
            n++;
        }
        return n;
    }

    private String subDir(String name) {
        File dir = new File(tmpDir, name);
        Assert.assertTrue(dir.mkdirs());
        return dir.getAbsolutePath();
    }
}
