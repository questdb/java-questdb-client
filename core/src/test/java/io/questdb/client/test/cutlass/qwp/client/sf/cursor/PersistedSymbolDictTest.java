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

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.ObjList;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class PersistedSymbolDictTest {

    // On-disk geometry, mirrored from PersistedSymbolDict so the layout-derived
    // corruption tests below read as arithmetic rather than magic numbers.
    private static final int CRC_SIZE = 4;
    private static final int HEADER_SIZE = 8;

    @Test
    public void testAppendPersistsAcrossReopen() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(d);
                try {
                    Assert.assertEquals(0, d.size());
                    d.appendSymbol("AAPL");
                    d.appendSymbol("GOOG");
                    d.appendSymbol("MSFT");
                    Assert.assertEquals(3, d.size());
                } finally {
                    d.close();
                }

                // Reopen: entries recovered in id order.
                PersistedSymbolDict reopened = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(reopened);
                try {
                    Assert.assertEquals(3, reopened.size());
                    ObjList<String> symbols = reopened.readLoadedSymbols();
                    Assert.assertEquals(3, symbols.size());
                    Assert.assertEquals("AAPL", symbols.getQuick(0));
                    Assert.assertEquals("GOOG", symbols.getQuick(1));
                    Assert.assertEquals("MSFT", symbols.getQuick(2));
                    Assert.assertTrue(reopened.loadedEntriesLen() > 0);

                    // Appending after recovery continues from the recovered tip.
                    reopened.appendSymbol("TSLA");
                    Assert.assertEquals(4, reopened.size());
                } finally {
                    reopened.close();
                }

                PersistedSymbolDict third = PersistedSymbolDict.open(dir.toString());
                try {
                    Assert.assertEquals(4, third.size());
                    Assert.assertEquals("TSLA", third.readLoadedSymbols().getQuick(3));
                } finally {
                    third.close();
                }
            } finally {
                rmDir(dir);
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
            Path src = Files.createTempDirectory("qwp-symdict-src");
            Path dst = Files.createTempDirectory("qwp-symdict-dst");
            try {
                GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
                dict.getOrAddSymbol("AAPL"); // id 0
                dict.getOrAddSymbol("");     // id 1 -- empty entry mid-range
                dict.getOrAddSymbol("MSFT"); // id 2

                PersistedSymbolDict encoded = PersistedSymbolDict.open(src.toString());
                encoded.appendSymbols(dict, 0, 2);
                encoded.close();

                // Reopen to obtain the on-disk entry region [len][utf8]... verbatim,
                // then replay it byte-for-byte into a fresh dict via appendRawEntries.
                PersistedSymbolDict reopened = PersistedSymbolDict.open(src.toString());
                try {
                    PersistedSymbolDict raw = PersistedSymbolDict.open(dst.toString());
                    try {
                        raw.appendRawEntries(reopened.loadedEntriesAddr(),
                                reopened.loadedEntriesLen(), reopened.size());
                        Assert.assertEquals(3, raw.size());
                    } finally {
                        raw.close();
                    }
                } finally {
                    reopened.close();
                }

                // The raw-appended dict must recover the same dense symbols.
                PersistedSymbolDict recovered = PersistedSymbolDict.open(dst.toString());
                try {
                    Assert.assertEquals(3, recovered.size());
                    ObjList<String> symbols = recovered.readLoadedSymbols();
                    Assert.assertEquals("AAPL", symbols.getQuick(0));
                    Assert.assertEquals("", symbols.getQuick(1));
                    Assert.assertEquals("MSFT", symbols.getQuick(2));
                } finally {
                    recovered.close();
                }
            } finally {
                rmDir(src);
                rmDir(dst);
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
            Path src = Files.createTempDirectory("qwp-symdict-src");
            Path dst = Files.createTempDirectory("qwp-symdict-dst");
            try {
                GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
                dict.getOrAddSymbol("AAPL"); // id 0
                dict.getOrAddSymbol("MSFT"); // id 1

                // Encode the range once to obtain its on-disk [len][utf8]... bytes.
                PersistedSymbolDict encoded = PersistedSymbolDict.open(src.toString());
                encoded.appendSymbols(dict, 0, 1);
                encoded.close();

                PersistedSymbolDict source = PersistedSymbolDict.open(src.toString());
                try {
                    long addr = source.loadedEntriesAddr();
                    int rawLen = source.loadedEntriesLen();
                    int count = source.size();

                    ShortWriteOnceFacade ff = new ShortWriteOnceFacade();
                    PersistedSymbolDict d = PersistedSymbolDict.open(ff, dst.toString());
                    Assert.assertNotNull(d);
                    try {
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
                    } finally {
                        d.close();
                    }
                } finally {
                    source.close();
                }

                // Recovery sees a gap-free, duplicate-free dictionary (size 2, not 3).
                PersistedSymbolDict reopened = PersistedSymbolDict.open(dst.toString());
                try {
                    Assert.assertEquals("retry must not duplicate or gap the dictionary", 2, reopened.size());
                    ObjList<String> symbols = reopened.readLoadedSymbols();
                    Assert.assertEquals("AAPL", symbols.getQuick(0));
                    Assert.assertEquals("MSFT", symbols.getQuick(1));
                } finally {
                    reopened.close();
                }
            } finally {
                rmDir(src);
                rmDir(dst);
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
                dict.getOrAddSymbol("AAPL"); // id 0
                dict.getOrAddSymbol("");     // id 1 -- empty symbol mid-range
                dict.getOrAddSymbol("MSFT"); // id 2
                dict.getOrAddSymbol("TSLA"); // id 3

                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                try {
                    d.appendSymbols(dict, 0, 3); // one write for all four ids
                    Assert.assertEquals(4, d.size());
                    d.appendSymbols(dict, 4, 3); // empty range (to < from) is a no-op
                    Assert.assertEquals(4, d.size());
                } finally {
                    d.close();
                }

                PersistedSymbolDict reopened = PersistedSymbolDict.open(dir.toString());
                try {
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
                } finally {
                    reopened.close();
                }

                PersistedSymbolDict third = PersistedSymbolDict.open(dir.toString());
                try {
                    Assert.assertEquals(5, third.size());
                    Assert.assertEquals("NVDA", third.readLoadedSymbols().getQuick(4));
                } finally {
                    third.close();
                }
            } finally {
                rmDir(dir);
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
                dict.getOrAddSymbol("AAPL"); // id 0
                dict.getOrAddSymbol("GOOG"); // id 1

                ShortWriteOnceFacade ff = new ShortWriteOnceFacade();
                PersistedSymbolDict d = PersistedSymbolDict.open(ff, dir.toString());
                Assert.assertNotNull(d);
                try {
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
                } finally {
                    d.close();
                }

                // Recovery sees a gap-free, duplicate-free dictionary (size 2, not 3).
                PersistedSymbolDict reopened = PersistedSymbolDict.open(dir.toString());
                try {
                    Assert.assertEquals("retry must not duplicate or gap the dictionary", 2, reopened.size());
                    ObjList<String> symbols = reopened.readLoadedSymbols();
                    Assert.assertEquals("AAPL", symbols.getQuick(0));
                    Assert.assertEquals("GOOG", symbols.getQuick(1));
                } finally {
                    reopened.close();
                }
            } finally {
                rmDir(dir);
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                final int n = 26; // 'a'..'z' -- distinct, and one UTF-8 byte each
                GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
                for (int i = 0; i < n; i++) {
                    dict.getOrAddSymbol(String.valueOf((char) ('a' + i)));
                }
                Assert.assertEquals(n, dict.size());
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(d);
                try {
                    d.appendSymbols(dict, 0, n - 1);
                    Assert.assertEquals(n, d.size());
                } finally {
                    d.close();
                }

                int entryBytes = n * 2;                       // [len=1 varint][1 utf8 byte]
                int chunkHeader = 1 + varintSize(entryBytes); // entryCount=50 fits one byte
                long expected = HEADER_SIZE + chunkHeader + entryBytes + CRC_SIZE;
                Assert.assertEquals(
                        "a batch append must cost ONE chunk checksum, not one per symbol",
                        expected, Files.size(dir.resolve(".symbol-dict")));

                // And it must still read back symbol-for-symbol.
                PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(re);
                try {
                    Assert.assertEquals(n, re.size());
                    ObjList<String> got = re.readLoadedSymbols();
                    for (int i = 0; i < n; i++) {
                        Assert.assertEquals(dict.getSymbol(i), got.getQuick(i));
                    }
                } finally {
                    re.close();
                }
            } finally {
                rmDir(dir);
            }
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                Path f = dir.resolve(".symbol-dict");
                byte[] original = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
                Files.write(f, original);
                Assert.assertNull("an unparseable existing dict must degrade to null",
                        PersistedSymbolDict.open(dir.toString()));
                Assert.assertArrayEquals("open() must NOT destroy an existing dictionary",
                        original, Files.readAllBytes(f));
            } finally {
                rmDir(dir);
            }
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                Path f = dir.resolve(".symbol-dict");
                PersistedSymbolDict seed = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(seed);
                seed.appendSymbol("a");
                seed.close();
                // Corrupt ONLY the version byte (offset 4) to an unknown value.
                byte[] bytes = Files.readAllBytes(f);
                bytes[4] = (byte) 99;
                Files.write(f, bytes);

                Assert.assertNull("an unknown-version dict must degrade to null",
                        PersistedSymbolDict.open(dir.toString()));
                Assert.assertArrayEquals("open() must NOT destroy a future-version dictionary",
                        bytes, Files.readAllBytes(f));
            } finally {
                rmDir(dir);
            }
        });
    }

    @Test
    public void testCloseNullsLoadedEntries() throws Exception {
        // close() must null loadedEntriesAddr/Len after freeing them (like
        // scratchAddr), so an accidental post-close read of the getters cannot
        // dereference freed native memory. Pre-fix the pointer survived close()
        // non-zero.
        assertMemoryLeak(() -> {
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                d.appendSymbol("AAPL");
                d.close();

                // Reopen so recovery loads the entries into native memory.
                PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
                Assert.assertTrue("recovery must load entries into native memory",
                        re.loadedEntriesAddr() != 0L && re.loadedEntriesLen() > 0);
                re.close();
                Assert.assertEquals("close() must null loadedEntriesAddr", 0L, re.loadedEntriesAddr());
                Assert.assertEquals("close() must null loadedEntriesLen", 0, re.loadedEntriesLen());
            } finally {
                rmDir(dir);
            }
        });
    }

    @Test
    public void testEmptySymbolRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                try {
                    d.appendSymbol("");
                    d.appendSymbol("nonempty");
                } finally {
                    d.close();
                }
                PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
                try {
                    Assert.assertEquals(2, re.size());
                    ObjList<String> s = re.readLoadedSymbols();
                    Assert.assertEquals("", s.getQuick(0));
                    Assert.assertEquals("nonempty", s.getQuick(1));
                } finally {
                    re.close();
                }
            } finally {
                rmDir(dir);
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                try {
                    d.appendSymbol("s0");
                    d.appendSymbol("s1");
                    d.appendSymbol("s2");
                    d.appendSymbol("s3");
                    d.appendSymbol("s4");
                    Assert.assertEquals(5, d.size());
                } finally {
                    d.close();
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

                PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(re);
                try {
                    // Only the intact prefix [s0, s1] is trusted; the corrupt chunk
                    // and everything after it are dropped. No recovered symbol is
                    // the corrupted string -- the tear is DETECTED, never silently
                    // misattributed.
                    Assert.assertEquals("parse must stop at the corrupt interior chunk", 2, re.size());
                    ObjList<String> s = re.readLoadedSymbols();
                    Assert.assertEquals("s0", s.getQuick(0));
                    Assert.assertEquals("s1", s.getQuick(1));
                } finally {
                    re.close();
                }
            } finally {
                rmDir(dir);
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                // Just over the old 1 << 20 (1 MB) ceiling.
                String big = TestUtils.repeat("x", (1 << 20) + 17);
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                try {
                    d.appendSymbol("before");
                    d.appendSymbol(big);
                    d.appendSymbol("after");
                    Assert.assertEquals(3, d.size());
                } finally {
                    d.close();
                }

                // Recovery must load ALL three; pre-fix the reopen truncated at the
                // big entry and came back with size 1 (only "before" survived).
                PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
                try {
                    Assert.assertEquals("large entry must survive recovery, not be truncated",
                            3, re.size());
                    ObjList<String> s = re.readLoadedSymbols();
                    Assert.assertEquals("before", s.getQuick(0));
                    Assert.assertEquals(big, s.getQuick(1));
                    Assert.assertEquals("after", s.getQuick(2));
                } finally {
                    re.close();
                }
            } finally {
                rmDir(dir);
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
            Path dir = Files.createTempDirectory("qwp-symdict-clean");
            try {
                PersistedSymbolDict stale = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(stale);
                try {
                    stale.appendSymbol("staleX");
                    stale.appendSymbol("staleY");
                    Assert.assertEquals(2, stale.size());
                } finally {
                    stale.close();
                }

                // Fresh start: openClean yields an EMPTY dictionary regardless of
                // the survivor, and appends from id 0 again.
                PersistedSymbolDict fresh = PersistedSymbolDict.openClean(dir.toString());
                Assert.assertNotNull(fresh);
                try {
                    Assert.assertEquals(0, fresh.size());
                    Assert.assertEquals(0, fresh.readLoadedSymbols().size());
                    fresh.appendSymbol("freshA");
                    Assert.assertEquals(1, fresh.size());
                } finally {
                    fresh.close();
                }

                // The survivor's bytes are physically gone, not just hidden: a
                // subsequent recovery open() sees only the post-clean content.
                PersistedSymbolDict reopened = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(reopened);
                try {
                    Assert.assertEquals(1, reopened.size());
                    Assert.assertEquals("freshA", reopened.readLoadedSymbols().getQuick(0));
                } finally {
                    reopened.close();
                }
            } finally {
                rmDir(dir);
            }
        });
    }

    @Test
    public void testRemoveOrphanDeletesFile() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                d.appendSymbol("A");
                d.close();
                Path f = dir.resolve(".symbol-dict");
                Assert.assertTrue(Files.exists(f));
                PersistedSymbolDict.removeOrphan(dir.toString());
                Assert.assertFalse(Files.exists(f));
            } finally {
                rmDir(dir);
            }
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                PersistedSymbolDict seed = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(seed);
                for (int i = 0; i < 40; i++) {
                    seed.appendSymbol("sym" + i);
                }
                seed.close();
                Path f = dir.resolve(".symbol-dict");
                byte[] before = Files.readAllBytes(f);

                HugeLengthFacade ff = new HugeLengthFacade();
                Assert.assertNull("a >=2GB dictionary must degrade to null",
                        PersistedSymbolDict.open(ff, dir.toString()));
                Assert.assertEquals("the guard must short-circuit BEFORE opening the file",
                        0, ff.openRwCalls);
                Assert.assertArrayEquals("open() must NOT destroy or trim an oversized dictionary",
                        before, Files.readAllBytes(f));
            } finally {
                rmDir(dir);
            }
        });
    }

    @Test
    public void testTornTrailingEntrySelfHeals() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                // Write two complete entries, then a torn trailing record: a
                // length prefix of 5 followed by only 2 bytes (crash mid-append).
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                d.appendSymbol("one");
                d.appendSymbol("two");
                d.close();

                Path f = dir.resolve(".symbol-dict");
                long cleanLen = Files.size(f); // header + "one" + "two", no tail
                Files.write(f, new byte[]{(byte) 5, (byte) 'x', (byte) 'y'},
                        StandardOpenOption.APPEND);
                Assert.assertEquals("torn tail present before reopen", cleanLen + 3, Files.size(f));

                // Reopen: the torn tail is ignored; only the two complete entries load.
                PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
                try {
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
                } finally {
                    re.close();
                }

                PersistedSymbolDict re2 = PersistedSymbolDict.open(dir.toString());
                try {
                    Assert.assertEquals(3, re2.size());
                    Assert.assertEquals("three", re2.readLoadedSymbols().getQuick(2));
                } finally {
                    re2.close();
                }
            } finally {
                rmDir(dir);
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
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                d.appendSymbol("one");
                d.appendSymbol("two");
                d.close();

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
                PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(re);
                try {
                    Assert.assertEquals("the intact prefix survives the transient", 2, re.size());
                    ObjList<String> s = re.readLoadedSymbols();
                    Assert.assertEquals("one", s.getQuick(0));
                    Assert.assertEquals("two", s.getQuick(1));
                    Assert.assertEquals("the torn tail is trimmed once truncate works", cleanLen, Files.size(f));
                } finally {
                    re.close();
                }
            } finally {
                rmDir(dir);
            }
        });
    }

    private static void rmDir(Path dir) {
        TestUtils.removeTmpDirRec(dir == null ? null : dir.toString());
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
     * 8-byte header), never the header write, and disarms after firing so the retry
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
}
