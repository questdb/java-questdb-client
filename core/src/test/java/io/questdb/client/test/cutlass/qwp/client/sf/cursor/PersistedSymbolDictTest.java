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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.cutlass.qwp.client.sf.cursor.UnreplayableSlotException;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class PersistedSymbolDictTest {

    private static final int APPEND_MAP_CAPACITY = 4 * 1024 * 1024;
    // On-disk geometry, mirrored from PersistedSymbolDict so the layout-derived
    // corruption tests below read as arithmetic rather than magic numbers.
    private static final int CRC_SIZE = 4;
    private static final int HEADER_SIZE = 8;

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testAppendAllocateFailureClosesDescriptorWithoutNativeLeak() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict-allocate-failure");
            IoFailureFacade ff = new IoFailureFacade(IoFailure.ALLOCATE);
            try (PersistedSymbolDict dict = PersistedSymbolDict.openClean(ff, dir.toString())) {
                Assert.assertNotNull(dict);
                try {
                    dict.appendSymbol("A");
                    Assert.fail("expected append allocation failure");
                } catch (IllegalStateException expected) {
                    Assert.assertEquals(
                            "could not grow mmap append region for .symbol-dict"
                                    + " [required=" + (HEADER_SIZE + 8) + ", fileSize=" + APPEND_MAP_CAPACITY + ']',
                            expected.getMessage());
                }
                Assert.assertEquals("failed append must not advance the dictionary", 0, dict.size());
            }
            ff.assertAllOpenedDescriptorsClosed();
            Assert.assertEquals(1, ff.allocateCalls);
            Assert.assertEquals(0, ff.mmapCalls);
        });
    }

    @Test
    public void testAppendMmapFailureClosesDescriptorWithoutNativeLeak() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict-mmap-append-failure");
            IoFailureFacade ff = new IoFailureFacade(IoFailure.MMAP);
            try (PersistedSymbolDict dict = PersistedSymbolDict.openClean(ff, dir.toString())) {
                Assert.assertNotNull(dict);
                try {
                    dict.appendSymbol("A");
                    Assert.fail("expected append mmap failure");
                } catch (IllegalStateException expected) {
                    Assert.assertEquals(
                            "could not mmap append region for .symbol-dict"
                                    + " [offset=0, capacity=" + APPEND_MAP_CAPACITY + ']',
                            expected.getMessage());
                }
                Assert.assertEquals("failed append must not advance the dictionary", 0, dict.size());
            }
            ff.assertAllOpenedDescriptorsClosed();
            Assert.assertEquals(1, ff.allocateCalls);
            Assert.assertEquals(1, ff.mmapCalls);
        });
    }

    @Test
    public void testAppendPersistsAcrossReopen() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(d);
                Assert.assertEquals(0, d.size());
                d.appendSymbol("AAPL");
                d.appendSymbol("GOOG");
                d.appendSymbol("MSFT");
                Assert.assertEquals(3, d.size());
            }

            // Reopen: entries recovered in id order.
            try (PersistedSymbolDict reopened = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(reopened);
                Assert.assertEquals(3, reopened.size());
                ObjList<String> symbols = reopened.readLoadedSymbols();
                Assert.assertEquals(3, symbols.size());
                Assert.assertEquals("AAPL", symbols.getQuick(0));
                Assert.assertEquals("GOOG", symbols.getQuick(1));
                Assert.assertEquals("MSFT", symbols.getQuick(2));
                Assert.assertTrue(reopened.loadedEntriesLen() > 0);
                Assert.assertTrue("production recovery must parse through mmap instead of a heap file copy",
                        reopened.usedMappedRecoveryInput());

                GlobalSymbolDictionary recovered = new GlobalSymbolDictionary();
                reopened.addLoadedSymbolsTo(recovered);
                Assert.assertEquals("direct recovery decode must preserve dense ids", 3, recovered.size());
                Assert.assertEquals("GOOG", recovered.getSymbol(1));

                // Appending after recovery continues from the recovered tip.
                reopened.appendSymbol("TSLA");
                Assert.assertEquals(4, reopened.size());
            }

            try (PersistedSymbolDict third = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertEquals(4, third.size());
                Assert.assertEquals("TSLA", third.readLoadedSymbols().getQuick(3));
            }
        });
    }

    @Test
    public void testAppendRawEntriesMatchesAppendSymbols() throws Exception {
        // M1: the producer persists the frame's already-encoded delta bytes via
        // appendRawEntries instead of re-encoding the symbols. Those bytes are the
        // same [len][utf8]... layout appendSymbols writes, so both must produce an
        // identical, recoverable dictionary. Encode a range with appendSymbols,
        // reopen to grab its on-disk entry bytes, replay them through
        // appendRawEntries into a fresh dict, and assert the recovered symbols
        // match -- including an empty entry mid-range.
        assertMemoryLeak(() -> {
            Path src = newFolder("qwp-symdict-src");
            Path dst = newFolder("qwp-symdict-dst");
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            dict.getOrAddSymbol("AAPL"); // id 0
            dict.getOrAddSymbol("");     // id 1 -- empty entry mid-range
            dict.getOrAddSymbol("MSFT"); // id 2

            try (PersistedSymbolDict encoded = PersistedSymbolDict.open(src.toString())) {
                encoded.appendSymbols(dict, 0, 2);
            }

            // Reopen to obtain the on-disk entry region [len][utf8]... verbatim,
            // then replay it byte-for-byte into a fresh dict via appendRawEntries.
            try (PersistedSymbolDict reopened = PersistedSymbolDict.open(src.toString());
                 PersistedSymbolDict raw = PersistedSymbolDict.open(dst.toString())) {
                raw.appendRawEntries(reopened.loadedEntriesAddr(),
                        reopened.loadedEntriesLen(), reopened.size());
                Assert.assertEquals(3, raw.size());
            }

            // The raw-appended dict must recover the same dense symbols.
            try (PersistedSymbolDict recovered = PersistedSymbolDict.open(dst.toString())) {
                Assert.assertEquals(3, recovered.size());
                ObjList<String> symbols = recovered.readLoadedSymbols();
                Assert.assertEquals("AAPL", symbols.getQuick(0));
                Assert.assertEquals("", symbols.getQuick(1));
                Assert.assertEquals("MSFT", symbols.getQuick(2));
            }
        });
    }

    @Test
    public void testAppendRawEntriesRejectsInconsistentTripleUnderAssertions() throws Exception {
        // appendRawEntries validates the (addr,len,count) triple before writing: an
        // inconsistent triple would record a chunk whose stored entryCount disagreed
        // with its entries and shift the dense id->symbol map on recovery, so it must
        // fail loudly rather than corrupt the file. That check is an assert -- gated on
        // -ea to keep the per-entry walk off the per-flush production path (client apps
        // run without -ea) -- so observe it under the test suite's -ea, and skip if
        // assertions happen to be disabled.
        boolean assertionsEnabled = false;
        assert assertionsEnabled = true;
        Assume.assumeTrue("the guard is assert-gated; only observable under -ea", assertionsEnabled);
        assertMemoryLeak(() -> {
            Path src = newFolder("qwp-symdict-src");
            Path dst = newFolder("qwp-symdict-dst");
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            dict.getOrAddSymbol("AAPL"); // id 0
            dict.getOrAddSymbol("MSFT"); // id 1
            dict.getOrAddSymbol("GOOG"); // id 2
            try (PersistedSymbolDict encoded = PersistedSymbolDict.open(src.toString())) {
                encoded.appendSymbols(dict, 0, 2);
            }

            // Grab the on-disk entry region (3 entries) verbatim, then feed it back
            // with a count of 1: validateRawEntries stops one entry in with
            // src < srcLimit and throws "under-filled", before any bytes are written.
            try (PersistedSymbolDict reopened = PersistedSymbolDict.open(src.toString());
                 PersistedSymbolDict raw = PersistedSymbolDict.open(dst.toString())) {
                try {
                    raw.appendRawEntries(reopened.loadedEntriesAddr(), reopened.loadedEntriesLen(), 1);
                    Assert.fail("an inconsistent (len,count) triple must be rejected");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue("message names the under-filled buffer: " + expected.getMessage(),
                            expected.getMessage().contains("under-filled"));
                    Assert.assertEquals("a rejected triple must write nothing", 0, raw.size());
                }
            }
        });
    }

    @Test
    public void testAppendRawEntriesShortWriteThrowsWithoutAdvancingIsIdempotentOnRetry() throws Exception {
        // The producer's FAST path persists a frame's already-encoded delta bytes
        // via appendRawEntries. A short write (disk full / quota) mid-persist must
        // throw WITHOUT advancing size()/appendOffset, so a retry keyed off size()
        // re-writes the identical bytes at the same offset and recovers a gap-free,
        // duplicate-free dictionary. The failed-PUBLISH regressions
        // (DeltaDictRecoveryTest) exercise a persist that SUCCEEDED; only this pins
        // the persist-FAILURE trigger, whose idempotency the write-ahead
        // (persistNewSymbolsBeforePublish, resuming from pd.size()) relies on.
        assertMemoryLeak(() -> {
            Path src = newFolder("qwp-symdict-src");
            Path dst = newFolder("qwp-symdict-dst");
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            dict.getOrAddSymbol("AAPL"); // id 0
            dict.getOrAddSymbol("MSFT"); // id 1

            // Encode the range once to obtain its on-disk [len][utf8]... bytes.
            try (PersistedSymbolDict encoded = PersistedSymbolDict.open(src.toString())) {
                encoded.appendSymbols(dict, 0, 1);
            }

            try (PersistedSymbolDict source = PersistedSymbolDict.open(src.toString())) {
                long addr = source.loadedEntriesAddr();
                int rawLen = source.loadedEntriesLen();
                int count = source.size();

                ShortWriteOnceFacade ff = new ShortWriteOnceFacade();
                try (PersistedSymbolDict d = PersistedSymbolDict.open(ff, dst.toString())) {
                    Assert.assertNotNull(d);
                    ff.armed = true; // the next entry append lands short
                    try {
                        d.appendRawEntries(addr, rawLen, count);
                        Assert.fail("a short write must throw");
                    } catch (IllegalStateException expected) {
                        Assert.assertTrue("short-write error: " + expected.getMessage(),
                                expected.getMessage().contains("short write"));
                    }
                    // The throw preceded the size/offset advance: nothing persisted.
                    Assert.assertEquals("short write must NOT advance size", 0, d.size());

                    // Retry the SAME bytes (the facade auto-disarmed): the write
                    // lands at the unchanged offset, overwriting the torn prefix.
                    d.appendRawEntries(addr, rawLen, count);
                    Assert.assertEquals(2, d.size());
                }
            }

            // Recovery sees a gap-free, duplicate-free dictionary (size 2, not 3).
            try (PersistedSymbolDict reopened = PersistedSymbolDict.open(dst.toString())) {
                Assert.assertEquals("retry must not duplicate or gap the dictionary", 2, reopened.size());
                ObjList<String> symbols = reopened.readLoadedSymbols();
                Assert.assertEquals("AAPL", symbols.getQuick(0));
                Assert.assertEquals("MSFT", symbols.getQuick(1));
            }
        });
    }

    @Test
    public void testAppendSymbolsBatchWritesDenseRange() throws Exception {
        // appendSymbols persists a whole id range in one write (the hot-path
        // syscall reduction). It must produce the same dense, id-ordered file a
        // per-symbol loop would, including an empty symbol mid-range, and a second
        // batched call keyed off size() must continue densely (the resume-from-
        // durable-size contract the producer relies on).
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            dict.getOrAddSymbol("AAPL"); // id 0
            dict.getOrAddSymbol("");     // id 1 -- empty symbol mid-range
            dict.getOrAddSymbol("MSFT"); // id 2
            dict.getOrAddSymbol("TSLA"); // id 3

            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbols(dict, 0, 3); // one write for all four ids
                Assert.assertEquals(4, d.size());
                d.appendSymbols(dict, 4, 3); // empty range (to < from) is a no-op
                Assert.assertEquals(4, d.size());
            }

            try (PersistedSymbolDict reopened = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertEquals(4, reopened.size());
                ObjList<String> symbols = reopened.readLoadedSymbols();
                Assert.assertEquals(4, symbols.size());
                Assert.assertEquals("AAPL", symbols.getQuick(0));
                Assert.assertEquals("", symbols.getQuick(1));
                Assert.assertEquals("MSFT", symbols.getQuick(2));
                Assert.assertEquals("TSLA", symbols.getQuick(3));

                // A follow-on batch keyed off the recovered size continues
                // the dense sequence without a gap or duplicate.
                dict.getOrAddSymbol("NVDA"); // id 4
                reopened.appendSymbols(dict, reopened.size(), 4);
                Assert.assertEquals(5, reopened.size());
            }

            try (PersistedSymbolDict third = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertEquals(5, third.size());
                Assert.assertEquals("NVDA", third.readLoadedSymbols().getQuick(4));
            }
        });
    }

    @Test
    public void testAppendSymbolsShortWriteThrowsWithoutAdvancingIsIdempotentOnRetry() throws Exception {
        // The producer's SLOW path (re-encode the [pd.size()..batchMax] suffix after
        // a prior partial persist) writes via appendSymbols. A short write (disk full
        // / quota) must throw WITHOUT advancing size()/appendOffset, so a retry keyed
        // off size() re-persists the SAME range at the SAME offset and recovers a
        // gap-free, duplicate-free dictionary. A regression that advanced size before
        // the written==len check would strand a torn/duplicated dictionary here.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            dict.getOrAddSymbol("AAPL"); // id 0
            dict.getOrAddSymbol("GOOG"); // id 1

            ShortWriteOnceFacade ff = new ShortWriteOnceFacade();
            try (PersistedSymbolDict d = PersistedSymbolDict.open(ff, dir.toString())) {
                Assert.assertNotNull(d);
                ff.armed = true; // the next entry append lands short
                try {
                    d.appendSymbols(dict, 0, 1);
                    Assert.fail("a short write must throw");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue("short-write error: " + expected.getMessage(),
                            expected.getMessage().contains("short write"));
                }
                // The throw preceded the size/offset advance: nothing persisted.
                Assert.assertEquals("short write must NOT advance size", 0, d.size());

                // Retry the SAME range (the facade auto-disarmed): re-writes at
                // the unchanged offset, overwriting the torn bytes.
                d.appendSymbols(dict, 0, 1);
                Assert.assertEquals(2, d.size());
            }

            // Recovery sees a gap-free, duplicate-free dictionary (size 2, not 3).
            try (PersistedSymbolDict reopened = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertEquals("retry must not duplicate or gap the dictionary", 2, reopened.size());
                ObjList<String> symbols = reopened.readLoadedSymbols();
                Assert.assertEquals("AAPL", symbols.getQuick(0));
                Assert.assertEquals("GOOG", symbols.getQuick(1));
            }
        });
    }

    @Test
    public void testBatchAppendWritesOneChunkWithOneChecksum() throws Exception {
        // A batch append must write ONE chunk -- one [entryCount][entryBytes] header and
        // ONE trailing CRC-32C for the whole range -- not one checksummed record per
        // symbol.
        //
        // This is the load-bearing property of the v3 layout, and it is a producer-thread
        // cost, not a cosmetic one: Crc32c.update is a NATIVE call, so a per-entry
        // checksum put one JNI transition (plus one sub-cache-line copy and one redundant
        // varint decode) on the flush path for every new symbol. On the high-cardinality
        // batch this whole feature exists to serve -- one new symbol per row -- that is a
        // thousand native calls per flush where the chunk needs one.
        //
        // Asserted through the file size, which is exact and cheap: N single-byte ASCII
        // symbols cost N * ([len varint] + 1 utf8 byte) of entries, wrapped in one chunk.
        // Per-entry CRCs would add 4 * N bytes instead of 4.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            final int n = 26; // 'a'..'z' -- distinct, and one UTF-8 byte each
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            for (int i = 0; i < n; i++) {
                dict.getOrAddSymbol(String.valueOf((char) ('a' + i)));
            }
            Assert.assertEquals(n, dict.size());
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(d);
                d.appendSymbols(dict, 0, n - 1);
                Assert.assertEquals(n, d.size());
            }

            int entryBytes = n * 2;                       // [len=1 varint][1 utf8 byte]
            int chunkHeader = 1 + varintSize(entryBytes); // entryCount=50 fits one byte
            long expected = HEADER_SIZE + chunkHeader + entryBytes + CRC_SIZE;
            Assert.assertEquals(
                    "a batch append must cost ONE chunk checksum, not one per symbol",
                    expected, Files.size(dir.resolve(".symbol-dict")));

            // And it must still read back symbol-for-symbol.
            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(re);
                Assert.assertEquals(n, re.size());
                ObjList<String> got = re.readLoadedSymbols();
                for (int i = 0; i < n; i++) {
                    Assert.assertEquals(dict.getSymbol(i), got.getQuick(i));
                }
            }
        });
    }

    @Test
    public void testMappedAppendUsesMultiMegabyteWindowsWithoutPositionedWrites() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            Path file = dir.resolve(PersistedSymbolDict.FILE_NAME);
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(d);
                for (int i = 0; i < 10_000; i++) {
                    d.appendSymbol("sym-" + i);
                }
                Assert.assertEquals(10_000, d.size());
                Assert.assertEquals("production appends must write directly into the mmap",
                        0L, d.appendWriteCount());
                Assert.assertEquals("ten thousand flushes must share one 4 MiB mapped window",
                        1, d.appendMapGrowthCount());
                Assert.assertEquals("the active append window must reserve one bounded segment",
                        APPEND_MAP_CAPACITY, Files.size(file));
            }
            Assert.assertTrue("close must truncate the unused mmap reserve",
                    Files.size(file) < APPEND_MAP_CAPACITY);

            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(re);
                Assert.assertEquals(10_000, re.size());
                Assert.assertEquals("sym-9999", re.readLoadedSymbols().getQuick(9_999));
            }
        });
    }

    @Test
    public void testMappedAppendSpansMultipleWindowsAndRecoversDenseIds() throws Exception {
        // A dictionary larger than one 4 MiB append window must remap the window
        // mid-append (appendMapGrowthCount >= 2) and still recover every symbol in
        // dense id order across the boundary. Guards two paths the single-window
        // happy path leaves uncovered: the remap arithmetic in ensureAppendMap, and
        // the single-pass appendSymbols encode as its chunks straddle the boundary.
        assertMemoryLeak(() -> {
            final int n = 640;
            final String pad = TestUtils.repeat("x", 8192); // ~8 KiB per symbol -> >4 MiB total
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            for (int i = 0; i < n; i++) {
                dict.getOrAddSymbol("sym-" + i + "-" + pad); // distinct, id == i
            }
            Assert.assertEquals(n, dict.size());

            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(d);
                // Multi-symbol batches so the appendSymbols encode loop is exercised,
                // not just the single-symbol path.
                for (int from = 0; from < n; from += 8) {
                    int to = Math.min(from + 8, n) - 1;
                    d.appendSymbols(dict, from, to);
                }
                Assert.assertEquals(n, d.size());
                Assert.assertEquals("production mmap appends must not use positioned writes",
                        0L, d.appendWriteCount());
                Assert.assertTrue(
                        "a dictionary larger than the 4 MiB window must remap at least once (saw "
                                + d.appendMapGrowthCount() + ")",
                        d.appendMapGrowthCount() >= 2);
            }

            // Fresh open: every id must resolve to its own symbol, including the ids
            // whose chunks straddled the 4 MiB window boundary.
            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(re);
                Assert.assertEquals(n, re.size());
                ObjList<String> got = re.readLoadedSymbols();
                for (int i = 0; i < n; i++) {
                    Assert.assertEquals("symbol at id " + i + " must survive the window boundary",
                            dict.getSymbol(i), got.getQuick(i));
                }
            }
        });
    }

    /**
     * A chunk whose declared entryCount disagrees with the entries its entryBytes region
     * actually holds must END THE TRUSTED PREFIX, exactly as a CRC failure does.
     * <p>
     * The CRC proves the bytes are what was WRITTEN; it says nothing about whether the
     * header triple is self-consistent, and the only write-side guard
     * ({@code validateRawEntries}) sits behind an assert -- which this library, shipping
     * embedded in user applications, runs without. The scan used to accept such a chunk
     * and count its declared 3 entries, leaving decodeLoadedSymbols to discover the
     * problem later and throw two layers up: a quarantine of the whole slot instead of
     * salvaging its intact prefix.
     * <p>
     * Validating during the scan is also what makes decodeLoadedSymbols' own throws
     * unreachable through {@code open()}. Their type change -- IllegalStateException to
     * UnreplayableSlotException, so Sender.build() can set a slot aside rather than
     * rethrow forever -- stays pinned on its reachable sibling by
     * CursorSendEngineTest#testRecoveryBaselineMismatchIsQuarantinableNotAPermanentBrick.
     */
    @Test
    public void testChunkWhoseCountDisagreesWithItsEntriesEndsTheTrustedPrefix() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            Path f = dir.resolve(".symbol-dict");

            // [entryCount=3][entryBytes=2][len=1]['a'] -- 3 entries claimed, 1 supplied.
            byte[] body = new byte[]{0x03, 0x02, 0x01, (byte) 'a'};
            int crc;
            long scratch = Unsafe.malloc(body.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < body.length; i++) {
                    Unsafe.getUnsafe().putByte(scratch + i, body[i]);
                }
                crc = Crc32c.update(Crc32c.INIT, scratch, body.length);
            } finally {
                Unsafe.free(scratch, body.length, MemoryTag.NATIVE_DEFAULT);
            }

            byte[] file = new byte[HEADER_SIZE + body.length + 4];
            file[0] = 'S';
            file[1] = 'Y';
            file[2] = 'D';
            file[3] = '1';
            file[4] = 1; // VERSION
            System.arraycopy(body, 0, file, HEADER_SIZE, body.length);
            int crcAt = HEADER_SIZE + body.length;
            file[crcAt] = (byte) crc;
            file[crcAt + 1] = (byte) (crc >>> 8);
            file[crcAt + 2] = (byte) (crc >>> 16);
            file[crcAt + 3] = (byte) (crc >>> 24);
            Files.write(f, file);

            try (PersistedSymbolDict dict = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull("a valid CRC must not destroy the file", dict);
                Assert.assertEquals("3 entries claimed inside a region holding 1 must end the "
                        + "trusted prefix, not be counted", 0, dict.size());
                // Nothing was trusted, so nothing decodes -- and no throw escapes.
                dict.addLoadedSymbolsTo(new GlobalSymbolDictionary());
            }
            Assert.assertEquals("the untrusted tail must be truncated away",
                    (long) HEADER_SIZE, Files.size(f));
        });
    }

    @Test
    public void testChunkClaimingEntriesInAZeroByteRegionEndsTheTrustedPrefix() throws Exception {
        // The CRC proves the bytes are what was WRITTEN, never that the chunk header is
        // self-consistent. Every entry costs at least its own length varint, so entryCount
        // > 0 inside a zero-byte region is impossible -- yet the scan used to accept it and
        // leave decodeLoadedSymbols to discover the problem later, as a throw two layers up
        // instead of a trimmed trusted prefix. Validating it during the scan gives such a
        // chunk the same treatment a CRC failure already gets.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            Path f = dir.resolve(".symbol-dict");

            byte[] body = new byte[]{0x01, 0x00}; // entryCount=1, entryBytes=0
            int crc;
            long scratch = Unsafe.malloc(body.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < body.length; i++) {
                    Unsafe.getUnsafe().putByte(scratch + i, body[i]);
                }
                crc = Crc32c.update(Crc32c.INIT, scratch, body.length);
            } finally {
                Unsafe.free(scratch, body.length, MemoryTag.NATIVE_DEFAULT);
            }

            byte[] file = new byte[HEADER_SIZE + body.length + 4];
            file[0] = 'S';
            file[1] = 'Y';
            file[2] = 'D';
            file[3] = '1';
            file[4] = 1; // VERSION
            System.arraycopy(body, 0, file, HEADER_SIZE, body.length);
            int crcAt = HEADER_SIZE + body.length;
            file[crcAt] = (byte) crc;
            file[crcAt + 1] = (byte) (crc >>> 8);
            file[crcAt + 2] = (byte) (crc >>> 16);
            file[crcAt + 3] = (byte) (crc >>> 24);
            Files.write(f, file);

            try (PersistedSymbolDict dict = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(dict);
                Assert.assertEquals("an inconsistent chunk must end the trusted prefix, "
                        + "not be counted", 0, dict.size());
            }
            Assert.assertEquals("the untrusted tail must be truncated away",
                    (long) HEADER_SIZE, Files.size(f));
        });
    }

    private static int varintSize(int v) {
        int n = 1;
        while ((v >>>= 7) != 0) {
            n++;
        }
        return n;
    }

    @Test
    public void testBadMagicDegradesWithoutDestroyingTheFile() throws Exception {
        // An unparseable existing file must degrade to null -- the sender falls back
        // to full self-sufficient frames -- and must NOT be recreated. open() used to
        // fall through to openFresh(), which is O_TRUNC: a single unreadable byte
        // destroyed the only copy of load-bearing state.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            Path f = dir.resolve(".symbol-dict");
            // At least HEADER_SIZE bytes so open() takes the parseable-but-bad-magic
            // path (openExisting) rather than treating this as a sub-header stub with
            // nothing to lose.
            byte[] original = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
            Files.write(f, original);
            Assert.assertNull("an unparseable existing dict must degrade to null",
                    PersistedSymbolDict.open(dir.toString()));
            Assert.assertArrayEquals("open() must NOT destroy an existing dictionary",
                    original, Files.readAllBytes(f));
        });
    }

    @Test
    public void testBadVersionDegradesWithoutDestroyingTheFile() throws Exception {
        // A file with correct 'SYD1' magic and a VALID chunk but an unknown version
        // byte belongs to a foreign/future format and must not be parsed as v3.
        // Covers the version sub-condition specifically (testBadMagicDegrades... covers
        // the magic one).
        //
        // The file must SURVIVE. This is the client-rollback trap: a newer client
        // writes v4, ops roll back to this build, and if open() recreated the file the
        // v4 dictionary would be gone for good -- rolling forward again could not
        // recover it, and every surviving delta frame would be permanently
        // unreplayable. Degrading to null instead leaves the bytes for the client that
        // does understand them.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            Path f = dir.resolve(".symbol-dict");
            try (PersistedSymbolDict seed = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(seed);
                seed.appendSymbol("a");
            }
            // Corrupt ONLY the version byte (offset 4) to an unknown value.
            byte[] bytes = Files.readAllBytes(f);
            bytes[4] = (byte) 99;
            Files.write(f, bytes);

            Assert.assertNull("an unknown-version dict must degrade to null",
                    PersistedSymbolDict.open(dir.toString()));
            Assert.assertArrayEquals("open() must NOT destroy a future-version dictionary",
                    bytes, Files.readAllBytes(f));
        });
    }

    @Test
    public void testCloseNullsLoadedEntries() throws Exception {
        // close() must null loadedEntriesAddr/Len after freeing them (like
        // scratchAddr), so an accidental post-close read of the getters cannot
        // dereference freed native memory. Pre-fix the pointer survived close()
        // non-zero.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol("AAPL");
            }

            // Reopen so recovery loads the entries into native memory. This test
            // closes explicitly because the post-close state is the assertion target.
            PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
            Assert.assertTrue("recovery must load entries into native memory",
                    re.loadedEntriesAddr() != 0L && re.loadedEntriesLen() > 0);
            re.close();
            Assert.assertEquals("close() must null loadedEntriesAddr", 0L, re.loadedEntriesAddr());
            Assert.assertEquals("close() must null loadedEntriesLen", 0, re.loadedEntriesLen());
        });
    }

    @Test
    public void testEmptySymbolRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol("");
                d.appendSymbol("nonempty");
            }
            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertEquals(2, re.size());
                ObjList<String> s = re.readLoadedSymbols();
                Assert.assertEquals("", s.getQuick(0));
                Assert.assertEquals("nonempty", s.getQuick(1));
            }
        });
    }

    /**
     * Fails if {@code symbol} is pure ASCII, i.e. if its UTF-8 encoding is one
     * byte per char. Guards the multi-byte tests against silently degrading into
     * ASCII ones should the source file's encoding ever be lost.
     */
    private static void assertMultiByte(String symbol) {
        Assert.assertTrue("expected a multi-byte UTF-8 symbol, got pure ASCII: " + symbol,
                symbol.getBytes(StandardCharsets.UTF_8).length > symbol.length());
    }

    @Test
    public void testMultiByteUtf8SymbolsRoundTripAcrossReopen() throws Exception {
        // Every other symbol in these suites is ASCII, where a symbol's UTF-8
        // byte length and its String.length() are equal -- so a length confusion
        // between the two is invisible to all of them. The side-file's entries
        // are [len varint][utf8] and recovery walks them by that length, so
        // sizing any of it from String.length() would desynchronize the walk at
        // the first multi-byte symbol and shift every id after it.
        //
        // Mixes 2-, 3- and 4-byte sequences, and puts a plain ASCII symbol AFTER
        // them so a shift introduced earlier shows up as a wrong value here
        // rather than only as a truncated tail.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            String twoByte = "températures";   // e-acute, 2 bytes
            String threeByte = "東京";      // Tokyo, 3 bytes each
            String fourByte = "sensor🔥"; // fire emoji, 4 bytes
            // Self-check: prove these literals really are multi-byte at runtime.
            // Without it the test degrades silently if the source ever loses its
            // encoding -- the literals collapse to ASCII '?', every assertion below
            // still passes, and the coverage this test exists for is gone. That is
            // not hypothetical: the pre-existing lone-surrogate test in
            // DeltaDictRecoveryTest encodes to a single-byte '?' for exactly this
            // reason, which is why none of the suites covered multi-byte at all.
            assertMultiByte(twoByte);
            assertMultiByte(threeByte);
            assertMultiByte(fourByte);
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol(twoByte);
                d.appendSymbol(threeByte);
                d.appendSymbol(fourByte);
                d.appendSymbol("ascii_after_multibyte");
            }
            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertEquals(4, re.size());
                ObjList<String> s = re.readLoadedSymbols();
                Assert.assertEquals(twoByte, s.getQuick(0));
                Assert.assertEquals(threeByte, s.getQuick(1));
                Assert.assertEquals(fourByte, s.getQuick(2));
                Assert.assertEquals("ascii_after_multibyte", s.getQuick(3));
            }
        });
    }

    @Test
    public void testInteriorCorruptionIsCaughtNotSilentlyMisattributed() throws Exception {
        // A host-crash interior tear (a lost page reading back as zeroes) or a
        // stale chunk left past the end by a failed truncate can change the bytes
        // of a NON-trailing chunk. Without the per-chunk CRC the parse would
        // accept those bytes, shifting the dense id->symbol map and silently
        // misattributing symbol-column values on replay. With the CRC the corrupt
        // chunk fails verification and the parse stops there, so recovery trusts
        // only the intact prefix (fail-clean: the send loop's torn-dict guard then
        // forces a resend of the rest).
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol("s0");
                d.appendSymbol("s1");
                d.appendSymbol("s2");
                d.appendSymbol("s3");
                d.appendSymbol("s4");
                Assert.assertEquals(5, d.size());
            }

            // Corrupt one byte inside the 3rd chunk's entry region (id 2). Each
            // appendSymbol writes one single-entry chunk
            // ([entryCount][entryBytes][len][utf8][crc32c]), and all five symbols
            // are the same width, so the five chunks are equal-sized -- derive the
            // stride from the file rather than hard-coding it. The byte flipped is
            // the last one before the chunk's trailing CRC, so that CRC goes stale.
            Path f = dir.resolve(".symbol-dict");
            byte[] bytes = Files.readAllBytes(f);
            int chunkLen = (bytes.length - HEADER_SIZE) / 5;
            int chunk2 = HEADER_SIZE + 2 * chunkLen;
            bytes[chunk2 + chunkLen - CRC_SIZE - 1] ^= 0x7F;
            Files.write(f, bytes);

            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(re);
                // Only the intact prefix [s0, s1] is trusted; the corrupt chunk
                // and everything after it are dropped. No recovered symbol is
                // the corrupted string -- the tear is DETECTED, never silently
                // misattributed.
                Assert.assertEquals("parse must stop at the corrupt interior chunk", 2, re.size());
                ObjList<String> s = re.readLoadedSymbols();
                Assert.assertEquals("s0", s.getQuick(0));
                Assert.assertEquals("s1", s.getQuick(1));
            }
        });
    }

    @Test
    public void testLargeSymbolRoundTripsAcrossReopen() throws Exception {
        // C1 regression: the write path caps nothing, so a symbol larger than the
        // old fixed 1 MB read ceiling must still recover intact. Before the fix,
        // appendSymbol wrote the oversized entry but openExisting rejected it as
        // "oversized", truncated the dictionary at that id (dropping it and every
        // higher id), and a normal process-crash recovery then hard-failed with a
        // spurious "host crash / resend required" terminal -- defeating store-and-
        // forward's process-crash durability for large symbols. The file length is
        // now the only bound, so the write and read paths agree.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            // Just over the old 1 << 20 (1 MB) ceiling.
            String big = TestUtils.repeat("x", (1 << 20) + 17);
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol("before");
                d.appendSymbol(big);
                d.appendSymbol("after");
                Assert.assertEquals(3, d.size());
            }

            // Recovery must load ALL three; pre-fix the reopen truncated at the
            // big entry and came back with size 1 (only "before" survived).
            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertEquals("large entry must survive recovery, not be truncated",
                        3, re.size());
                ObjList<String> s = re.readLoadedSymbols();
                Assert.assertEquals("before", s.getQuick(0));
                Assert.assertEquals(big, s.getQuick(1));
                Assert.assertEquals("after", s.getQuick(2));
            }
        });
    }

    @Test
    public void testOpenCleanDiscardsSurvivingDictionary() throws Exception {
        // A fresh start must NOT inherit a dictionary left by a prior lifecycle:
        // openClean() truncates any survivor to empty, where open() would recover
        // (and TRUST) it. Trusting a survivor whose segments are gone -- the
        // fresh-start producer is not seeded from it -- shifts the dense id->symbol
        // mapping and misattributes symbols on the next reconnect.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict-clean");
            try (PersistedSymbolDict stale = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(stale);
                stale.appendSymbol("staleX");
                stale.appendSymbol("staleY");
                Assert.assertEquals(2, stale.size());
            }

            // Fresh start: openClean yields an EMPTY dictionary regardless of
            // the survivor, and appends from id 0 again.
            try (PersistedSymbolDict fresh = PersistedSymbolDict.openClean(dir.toString())) {
                Assert.assertNotNull(fresh);
                Assert.assertEquals(0, fresh.size());
                Assert.assertEquals(0, fresh.readLoadedSymbols().size());
                fresh.appendSymbol("freshA");
                Assert.assertEquals(1, fresh.size());
            }

            // The survivor's bytes are physically gone, not just hidden: a
            // subsequent recovery open() sees only the post-clean content.
            try (PersistedSymbolDict reopened = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(reopened);
                Assert.assertEquals(1, reopened.size());
                Assert.assertEquals("freshA", reopened.readLoadedSymbols().getQuick(0));
            }
        });
    }

    @Test
    public void testOpenCleanRefusesWhenAnExistingFileCannotBeTruncated() throws Exception {
        // openFresh returning null WITHOUT truncating leaves a previous generation's
        // dictionary on disk while this session runs full-dict from id 0. The next
        // recovery reads a side-file whose ids describe a different id space from the
        // surviving frames, and no existing guard can tell: no gap, valid CRC, both
        // bounds checks pass, and the catch-up registers the wrong strings.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict-clean-refuse");
            try (PersistedSymbolDict first = PersistedSymbolDict.openClean(dir.toString())) {
                first.appendSymbol("a");
                first.appendSymbol("b");
            }
            DelegatingFilesFacade ff = new DelegatingFilesFacade() {
                @Override
                public int openCleanRW(String path) {
                    return -1;
                }
            };
            try {
                PersistedSymbolDict.openClean(ff, dir.toString());
                Assert.fail("expected openClean to refuse rather than leave a stale dictionary");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("cannot be truncated"));
            }
            // Refusing is not deleting: the bytes stay for an operator.
            Assert.assertTrue(Files.exists(dir.resolve(PersistedSymbolDict.FILE_NAME)));
        });
    }

    @Test
    public void testOpenFailuresDoNotCreateOrDestroyDictionaryFiles() throws Exception {
        assertMemoryLeak(() -> {
            Path freshDir = newFolder("qwp-symdict-fresh-open-failure");
            IoFailureFacade freshFf = new IoFailureFacade(IoFailure.OPEN_CLEAN);
            Assert.assertNull(PersistedSymbolDict.openClean(freshFf, freshDir.toString()));
            Assert.assertEquals(1, freshFf.openCleanAttempts);
            freshFf.assertAllOpenedDescriptorsClosed();
            Assert.assertFalse(Files.exists(freshDir.resolve(PersistedSymbolDict.FILE_NAME)));

            Path existingDir = newFolder("qwp-symdict-existing-open-failure");
            try (PersistedSymbolDict seed = PersistedSymbolDict.open(existingDir.toString())) {
                Assert.assertNotNull(seed);
                seed.appendSymbol("AAPL");
            }
            Path file = existingDir.resolve(PersistedSymbolDict.FILE_NAME);
            byte[] before = Files.readAllBytes(file);
            IoFailureFacade existingFf = new IoFailureFacade(IoFailure.OPEN_EXISTING);
            Assert.assertNull(PersistedSymbolDict.open(existingFf, existingDir.toString()));
            Assert.assertEquals(1, existingFf.openRwAttempts);
            existingFf.assertAllOpenedDescriptorsClosed();
            Assert.assertArrayEquals("failed recovery open must preserve the load-bearing file",
                    before, Files.readAllBytes(file));
        });
    }

    @Test
    public void testOpenToleratesAnAbsentDictionary() throws Exception {
        // The recovery entry point must keep its existing tolerance: with no file there
        // is nothing stale to inherit, so degrading to full-dict frames is safe.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict-empty-slot");
            DelegatingFilesFacade ff = new DelegatingFilesFacade() {
                @Override
                public int openCleanRW(String path) {
                    return -1;
                }
            };
            Assert.assertNull(PersistedSymbolDict.open(ff, dir.toString()));
        });
    }

    @Test
    public void testRecoveryMmapFailureClosesDescriptorAndPreservesFile() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict-recovery-mmap-failure");
            try (PersistedSymbolDict seed = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(seed);
                seed.appendSymbol("AAPL");
            }
            Path file = dir.resolve(PersistedSymbolDict.FILE_NAME);
            byte[] before = Files.readAllBytes(file);

            IoFailureFacade ff = new IoFailureFacade(IoFailure.MMAP);
            Assert.assertNull(PersistedSymbolDict.open(ff, dir.toString()));
            ff.assertAllOpenedDescriptorsClosed();
            Assert.assertEquals(1, ff.mmapCalls);
            Assert.assertArrayEquals("failed recovery mmap must preserve the load-bearing file",
                    before, Files.readAllBytes(file));
        });
    }

    @Test
    public void testRemoveOrphanDeletesFile() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol("A");
            }
            Path f = dir.resolve(".symbol-dict");
            Assert.assertTrue(Files.exists(f));
            PersistedSymbolDict.removeOrphan(dir.toString());
            Assert.assertFalse(Files.exists(f));
        });
    }

    @Test
    public void testShortHeaderWriteClosesDescriptorAndRemovesStub() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict-short-header");
            IoFailureFacade ff = new IoFailureFacade(IoFailure.HEADER_WRITE);
            Assert.assertNull(PersistedSymbolDict.openClean(ff, dir.toString()));
            ff.assertAllOpenedDescriptorsClosed();
            Assert.assertFalse("headerless stub must be removed",
                    Files.exists(dir.resolve(PersistedSymbolDict.FILE_NAME)));
        });
    }

    @Test
    public void testShortRecoveryReadClosesDescriptorAndPreservesFile() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict-short-read");
            try (PersistedSymbolDict seed = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(seed);
                seed.appendSymbol("AAPL");
            }
            Path file = dir.resolve(PersistedSymbolDict.FILE_NAME);
            byte[] before = Files.readAllBytes(file);

            IoFailureFacade ff = new IoFailureFacade(IoFailure.READ);
            Assert.assertNull(PersistedSymbolDict.open(ff, dir.toString()));
            ff.assertAllOpenedDescriptorsClosed();
            Assert.assertArrayEquals("short recovery read must preserve the load-bearing file",
                    before, Files.readAllBytes(file));
        });
    }

    @Test
    public void testTooLargeToReopenDegradesWithoutOpeningTheFile() throws Exception {
        // A dictionary at or past Integer.MAX_VALUE cannot be read into one int-sized
        // buffer, so open() must short-circuit to null BEFORE touching the file.
        //
        // The facade reports 2^32 + 100 -- deliberately NOT Integer.MAX_VALUE + 1.
        // That value casts to Integer.MIN_VALUE, which Unsafe.malloc rejects anyway,
        // so openExisting's catch would produce the same null and the test could not
        // tell the guard from its absence (it was exactly that vacuous before). A
        // length of 2^32 + k instead casts to a SMALL POSITIVE prefix -- the branch
        // the guard's own javadoc calls out -- under which openExisting would happily
        // malloc, read a truncated prefix, and parse it.
        //
        // The assertion that pins the guard is therefore NOT the null (both paths give
        // null) but that the file is never even OPENED: with the guard, open() returns
        // before openRW; without it, openExisting opens the file and parses garbage.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict seed = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(seed);
                for (int i = 0; i < 40; i++) {
                    seed.appendSymbol("sym" + i);
                }
            }
            Path f = dir.resolve(".symbol-dict");
            byte[] before = Files.readAllBytes(f);

            HugeLengthFacade ff = new HugeLengthFacade();
            Assert.assertNull("a >=2GB dictionary must degrade to null",
                    PersistedSymbolDict.open(ff, dir.toString()));
            Assert.assertEquals("the guard must short-circuit BEFORE opening the file",
                    0, ff.openRwCalls);
            Assert.assertArrayEquals("open() must NOT destroy or trim an oversized dictionary",
                    before, Files.readAllBytes(f));
        });
    }

    @Test
    public void testTornTrailingEntrySelfHeals() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            // Write two complete entries, then a torn trailing record: a
            // length prefix of 5 followed by only 2 bytes (crash mid-append).
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol("one");
                d.appendSymbol("two");
            }

            Path f = dir.resolve(".symbol-dict");
            long cleanLen = Files.size(f); // header + "one" + "two", no tail
            Files.write(f, new byte[]{(byte) 5, (byte) 'x', (byte) 'y'},
                    StandardOpenOption.APPEND);
            Assert.assertEquals("torn tail present before reopen", cleanLen + 3, Files.size(f));

            // Reopen: the torn tail is ignored; only the two complete entries load.
            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                // open() physically truncates the torn tail: the file returns
                // to its clean length, so a later SHORTER append can never
                // leave residue past its end that a future recovery mis-parses
                // as a ghost symbol.
                Assert.assertEquals("torn tail physically dropped by open", cleanLen, Files.size(f));
                Assert.assertEquals(2, re.size());
                ObjList<String> s = re.readLoadedSymbols();
                Assert.assertEquals("one", s.getQuick(0));
                Assert.assertEquals("two", s.getQuick(1));
                // The next append continues from the truncated tail cleanly.
                re.appendSymbol("three");
                Assert.assertEquals(3, re.size());
            }

            try (PersistedSymbolDict re2 = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertEquals(3, re2.size());
                Assert.assertEquals("three", re2.readLoadedSymbols().getQuick(2));
            }
        });
    }

    @Test
    public void testTransientLengthFaultDegradesWithoutDestroyingTheFile() throws Exception {
        // open() routes on ff.length(): a value < HEADER_SIZE falls through to the
        // truncating openFresh(). A genuine sub-header stub reports a length in
        // [0, HEADER_SIZE), but a TRANSIENT stat failure (an EIO on a flaky disk)
        // reports the -1 error sentinel for a fully populated file -- and routing that
        // to openFresh would O_TRUNC the only copy of load-bearing state, the exact
        // destruction the "Never recreate over an existing file" contract forbids. A
        // negative length is distinguishable from a stub, so open() must degrade to
        // null (fall back to full self-sufficient frames) and leave every byte on disk
        // for a later attempt, once the transient clears.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol("one");
                d.appendSymbol("two");
            }

            Path f = dir.resolve(".symbol-dict");
            byte[] before = Files.readAllBytes(f);
            Assert.assertTrue("a populated dict must exceed the header", before.length > HEADER_SIZE);

            // Reopen through a facade whose length() reports the -1 stat-error
            // sentinel for the (present) file -- the branch under test.
            PersistedSymbolDict reopened = PersistedSymbolDict.open(new StatFailsLengthFacade(), dir.toString());
            if (reopened != null) {
                // Pre-fix: open() fell through to openFresh() and handed back a fresh
                // empty dict over the now-truncated file. Close its fd so only the
                // assertion below reports the failure, not a leaked descriptor.
                reopened.close();
            }
            Assert.assertNull("a populated dict whose length cannot be stat'd must degrade to null, not truncate",
                    reopened);
            Assert.assertArrayEquals("a transient length-stat fault must NOT destroy the dictionary",
                    before, Files.readAllBytes(f));

            // Once the filesystem recovers, the SAME file reopens its intact content.
            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(re);
                Assert.assertEquals("the intact content survives the transient", 2, re.size());
                ObjList<String> s = re.readLoadedSymbols();
                Assert.assertEquals("one", s.getQuick(0));
                Assert.assertEquals("two", s.getQuick(1));
            }
        });
    }

    @Test
    public void testExactHeaderSizeFileIsCompleteWhileOneByteShorterIsARecreatedStub() throws Exception {
        // HEADER_SIZE shrank from 16 to 8 when the lineage stamp was removed (see
        // PersistedSymbolDict's class javadoc): a length that used to fall inside the
        // sub-header-stub range [0, 16) -- and so got silently recreated by the
        // truncating openFresh() -- can now be a COMPLETE, zero-chunk dictionary that
        // open() must load as-is instead of discarding. This pins the new boundary
        // from both sides: exactly HEADER_SIZE bytes is a complete empty dict and
        // must NOT be recreated; HEADER_SIZE - 1 bytes is still the crash-left stub
        // it always was and must still be recreated. A file-size assertion alone
        // cannot tell "loaded" from "recreated" here, since openFresh writes the
        // very same magic/version/reserved bytes the fixture hand-writes --
        // usedMappedRecoveryInput() is what actually distinguishes the two paths,
        // being true only through openExisting's mmap recovery and always false
        // from openFresh.
        assertMemoryLeak(() -> {
            // Side 1: exactly HEADER_SIZE bytes -- magic, version, three reserved
            // zero bytes, no chunks -- is complete and must load, not recreate.
            Path completeDir = newFolder("qwp-symdict-exact-header");
            Path completeFile = completeDir.resolve(".symbol-dict");
            byte[] complete = new byte[HEADER_SIZE];
            complete[0] = 'S';
            complete[1] = 'Y';
            complete[2] = 'D';
            complete[3] = '1';
            complete[4] = 1; // VERSION
            Files.write(completeFile, complete);

            try (PersistedSymbolDict d = PersistedSymbolDict.open(completeDir.toString())) {
                Assert.assertNotNull("an exactly-HEADER_SIZE file must load, not be refused", d);
                Assert.assertTrue("must recover through openExisting's mmap path, not a silent recreate",
                        d.usedMappedRecoveryInput());
                Assert.assertEquals("a zero-chunk file must recover empty", 0, d.size());
                Assert.assertEquals("open() must NOT rewrite an already-complete header",
                        (long) HEADER_SIZE, Files.size(completeFile));

                // Prove the returned instance is genuinely usable, not just non-null.
                d.appendSymbol("a");
            }
            try (PersistedSymbolDict reopened = PersistedSymbolDict.open(completeDir.toString())) {
                Assert.assertEquals("the append made after loading the boundary file must survive reopen",
                        1, reopened.size());
                Assert.assertEquals("a", reopened.readLoadedSymbols().getQuick(0));
            }

            // Side 2: one byte short of HEADER_SIZE is still the crash-left
            // sub-header stub it always was, and open() must still recreate it with
            // a full header rather than try to load it.
            Path stubDir = newFolder("qwp-symdict-sub-header-stub");
            Path stubFile = stubDir.resolve(".symbol-dict");
            byte[] stub = new byte[HEADER_SIZE - 1];
            stub[0] = 'S';
            stub[1] = 'Y';
            stub[2] = 'D';
            stub[3] = '1';
            stub[4] = 1; // VERSION
            Files.write(stubFile, stub);

            try (PersistedSymbolDict d = PersistedSymbolDict.open(stubDir.toString())) {
                Assert.assertNotNull("a sub-header stub must still be recreated, not refused", d);
                Assert.assertFalse("a sub-header stub must be recreated by openFresh, not loaded",
                        d.usedMappedRecoveryInput());
                Assert.assertEquals("a recreated dict must start empty", 0, d.size());
                Assert.assertEquals("openFresh must have written a full header over the stub",
                        (long) HEADER_SIZE, Files.size(stubFile));
            }
        });
    }

    @Test
    public void testTruncateFailureDegradesWithoutDestroyingTheFile() throws Exception {
        // A host crash can leave a torn/stale tail past the last complete chunk.
        // open() drops it with a truncate; if that truncate FAILS (a read-only
        // remount, a Windows share lock), the file still exposes the stale bytes,
        // whose self-consistent chunk CRC a later shifted parse could accept as a real
        // symbol. So a failed truncate must make the file UNTRUSTED -- open() degrades
        // to null (the sender falls back to full self-sufficient frames) rather than
        // returning a dict laid over stale bytes.
        //
        // It must NOT recreate the file, which is what it used to do: a read-only
        // remount is transient, and destroying the [one, two] prefix on the way past it
        // makes every surviving delta frame permanently unreplayable. Degrading leaves
        // the bytes for the next attempt, once the mount is writable again.
        assertMemoryLeak(() -> {
            Path dir = newFolder("qwp-symdict");
            try (PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString())) {
                d.appendSymbol("one");
                d.appendSymbol("two");
            }

            // Append a torn trailing record so the reopen parses [one, two], then
            // finds validLen < len and tries to truncate the tail -- the branch
            // under test.
            Path f = dir.resolve(".symbol-dict");
            long cleanLen = Files.size(f); // header + "one" + "two", no tail
            Files.write(f, new byte[]{(byte) 5, (byte) 'x', (byte) 'y'}, StandardOpenOption.APPEND);
            Assert.assertEquals("torn tail present before reopen", cleanLen + 3, Files.size(f));
            byte[] before = Files.readAllBytes(f);

            // Reopen through a facade whose truncate() fails.
            Assert.assertNull("a dict whose torn tail cannot be trimmed must degrade to null",
                    PersistedSymbolDict.open(new FailingTruncateFacade(), dir.toString()));
            Assert.assertArrayEquals("a failed truncate must NOT destroy the dictionary",
                    before, Files.readAllBytes(f));

            // And once the filesystem is writable again, the SAME file recovers its
            // intact prefix in full -- which is the whole point of not destroying it.
            try (PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString())) {
                Assert.assertNotNull(re);
                Assert.assertEquals("the intact prefix survives the transient", 2, re.size());
                ObjList<String> s = re.readLoadedSymbols();
                Assert.assertEquals("one", s.getQuick(0));
                Assert.assertEquals("two", s.getQuick(1));
                Assert.assertEquals("the torn tail is trimmed once truncate works", cleanLen, Files.size(f));
            }
        });
    }

    private Path newFolder(String name) throws IOException {
        return temporaryFolder.newFolder(name).toPath();
    }

    private enum IoFailure {
        ALLOCATE,
        HEADER_WRITE,
        MMAP,
        OPEN_CLEAN,
        OPEN_EXISTING,
        READ
    }

    private static final class IoFailureFacade extends DelegatingFilesFacade {
        private final IoFailure failure;
        private int allocateCalls;
        private int closeCalls;
        private int mmapCalls;
        private int openCleanAttempts;
        private int openRwAttempts;
        private int openedDescriptors;

        private IoFailureFacade(IoFailure failure) {
            this.failure = failure;
        }

        @Override
        public boolean allocate(int fd, long size) {
            allocateCalls++;
            return failure != IoFailure.ALLOCATE && super.allocate(fd, size);
        }

        @Override
        public int close(int fd) {
            closeCalls++;
            return super.close(fd);
        }

        @Override
        public boolean isMmapAllowed() {
            return failure == IoFailure.ALLOCATE || failure == IoFailure.MMAP;
        }

        @Override
        public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
            mmapCalls++;
            if (failure == IoFailure.MMAP) {
                return io.questdb.client.std.Files.FAILED_MMAP_ADDRESS;
            }
            return io.questdb.client.std.Files.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public int openCleanRW(String path) {
            openCleanAttempts++;
            if (failure == IoFailure.OPEN_CLEAN) {
                return -1;
            }
            int fd = super.openCleanRW(path);
            if (fd >= 0) {
                openedDescriptors++;
            }
            return fd;
        }

        @Override
        public int openRW(String path) {
            openRwAttempts++;
            if (failure == IoFailure.OPEN_EXISTING) {
                return -1;
            }
            int fd = super.openRW(path);
            if (fd >= 0) {
                openedDescriptors++;
            }
            return fd;
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            if (failure == IoFailure.READ && len > 0) {
                return super.read(fd, addr, len - 1, offset);
            }
            return super.read(fd, addr, len, offset);
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            if (failure == IoFailure.HEADER_WRITE && offset == 0 && len == HEADER_SIZE) {
                return super.write(fd, addr, len - 1, offset);
            }
            return super.write(fd, addr, len, offset);
        }

        private void assertAllOpenedDescriptorsClosed() {
            Assert.assertEquals("every successfully opened fd must be closed",
                    openedDescriptors, closeCalls);
        }
    }

    /**
     * Fails every {@link #truncate(int, long)} -- reproducing a host where the
     * torn/stale-tail truncate cannot succeed (read-only remount, Windows share
     * lock) so {@code open()}'s fail-clean recreate path runs.
     */
    private static final class FailingTruncateFacade extends DelegatingFilesFacade {
        @Override
        public boolean truncate(int fd, long size) {
            return false;
        }
    }

    /**
     * Reports a dictionary length past {@link Integer#MAX_VALUE} -- reproducing a
     * dictionary that legitimately grew beyond 2GB, which {@code open()} cannot read
     * into one int-sized buffer and must refuse before touching the file.
     * <p>
     * The length is {@code 2^32 + 100}, NOT {@code Integer.MAX_VALUE + 1}. The latter
     * casts to {@code Integer.MIN_VALUE}, which {@code Unsafe.malloc} rejects on its
     * own, so {@code openExisting}'s catch would produce the same {@code null} with or
     * without the guard -- which is precisely what made the old version of this test
     * vacuous. {@code 2^32 + k} casts to a small POSITIVE prefix instead, so without
     * the guard {@code openExisting} really would open the file and parse a truncated
     * prefix of it. {@link #openRwCalls} is what catches that.
     */
    private static final class HugeLengthFacade extends DelegatingFilesFacade {
        int openRwCalls;

        @Override
        public long length(String path) {
            return (1L << 32) + 100L;
        }

        @Override
        public int openRW(String path) {
            openRwCalls++;
            return super.openRW(path);
        }
    }

    /**
     * Lands ONE armed entry append short -- writes {@code len-1} of the {@code len}
     * requested bytes and reports {@code len-1} -- reproducing a disk-full / quota
     * short write mid-persist. Fires only on an entry append (offset past the
     * 16-byte header), never the header write, and disarms after firing so the retry
     * writes cleanly.
     */
    private static final class ShortWriteOnceFacade extends DelegatingFilesFacade {
        boolean armed;

        @Override
        public long write(int fd, long addr, long len, long offset) {
            if (armed && offset > 0 && len > 1) {
                armed = false;
                return INSTANCE.write(fd, addr, len - 1, offset);
            }
            return INSTANCE.write(fd, addr, len, offset);
        }
    }

    /**
     * Reports a length of -1 -- the stat-error sentinel -- for the dictionary file,
     * reproducing a transient stat failure (an EIO on a flaky disk) where the file is
     * present but its size cannot be read. open() must treat this as "present but
     * unreadable" and degrade to null, NOT route it to the truncating fresh-open path.
     */
    private static final class StatFailsLengthFacade extends DelegatingFilesFacade {
        @Override
        public long length(String path) {
            return -1L;
        }
    }
}
