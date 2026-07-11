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
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Stream;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class PersistedSymbolDictTest {

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
    public void testBadMagicIsRecreatedEmpty() throws Exception {
        assertMemoryLeak(() -> {
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                // A file with the right size but garbage content (bad magic).
                Path f = dir.resolve(".symbol-dict");
                Files.write(f, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(d);
                try {
                    Assert.assertEquals("bad-magic file recreated empty", 0, d.size());
                    d.appendSymbol("X");
                    Assert.assertEquals(1, d.size());
                } finally {
                    d.close();
                }
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
        // stale entry left past the end by a failed truncate can change the bytes
        // of a NON-trailing entry. Without the per-entry CRC the parse would
        // accept those bytes, shifting the dense id->symbol map and silently
        // misattributing symbol-column values on replay. With the CRC the corrupt
        // entry fails verification and the parse stops there, so recovery trusts
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

                // Corrupt one byte inside the 3rd entry's UTF-8 (id 2). On-disk
                // entry layout is [len varint][utf8][crc32c u32]; a 2-byte ASCII
                // symbol is 1 + 2 + 4 = 7 bytes, after the 8-byte header:
                //   header[0,8) e0[8,15) e1[15,22) e2[22,29) ...
                // Offset 23 is "s2"'s first UTF-8 byte; flipping it leaves e2's
                // stored CRC stale.
                Path f = dir.resolve(".symbol-dict");
                byte[] bytes = Files.readAllBytes(f);
                bytes[23] ^= 0x7F;
                Files.write(f, bytes);

                PersistedSymbolDict re = PersistedSymbolDict.open(dir.toString());
                Assert.assertNotNull(re);
                try {
                    // Only the intact prefix [s0, s1] is trusted; the corrupt e2
                    // and everything after it are dropped. No recovered symbol is
                    // the corrupted string -- the tear is DETECTED, never silently
                    // misattributed.
                    Assert.assertEquals("parse must stop at the corrupt interior entry", 2, re.size());
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
    public void testTruncateFailureRecreatesEmpty() throws Exception {
        // A host crash can leave a torn/stale tail past the last complete entry.
        // open() drops it with a truncate; if that truncate FAILS (a read-only
        // remount, a Windows share lock), the file still exposes the stale bytes,
        // whose self-consistent per-entry CRC a later shifted parse could accept as
        // a real symbol. So a failed truncate must make the file UNTRUSTED --
        // open() recreates it empty (fail-clean) rather than returning a dict laid
        // over stale bytes. Drive the truncate failure with a facade and assert the
        // reopened dictionary is empty, not the [one, two] prefix.
        assertMemoryLeak(() -> {
            Path dir = Files.createTempDirectory("qwp-symdict");
            try {
                PersistedSymbolDict d = PersistedSymbolDict.open(dir.toString());
                d.appendSymbol("one");
                d.appendSymbol("two");
                d.close();

                // Append a torn trailing record (length prefix 5, only 2 bytes) so
                // the reopen parses [one, two], then finds validLen < len and tries
                // to truncate the tail -- the branch under test.
                Path f = dir.resolve(".symbol-dict");
                long cleanLen = Files.size(f); // header + "one" + "two", no tail
                Files.write(f, new byte[]{(byte) 5, (byte) 'x', (byte) 'y'}, StandardOpenOption.APPEND);
                Assert.assertEquals("torn tail present before reopen", cleanLen + 3, Files.size(f));

                // Reopen through a facade whose truncate() fails.
                PersistedSymbolDict re = PersistedSymbolDict.open(new FailingTruncateFacade(), dir.toString());
                Assert.assertNotNull(re);
                try {
                    // The failed truncate made the file untrusted, so open() recreated
                    // it empty rather than trusting the [one, two] prefix over a stale
                    // tail: size()==0, not 2, and no recovered symbols.
                    Assert.assertEquals("failed truncate must recreate the dictionary empty", 0, re.size());
                    Assert.assertEquals(0, re.readLoadedSymbols().size());
                    // openFresh rewrote a bare 8-byte header (magic + version), so both
                    // the two entries and the torn tail are gone.
                    Assert.assertEquals("recreated file is a bare header", 8L, Files.size(f));
                } finally {
                    re.close();
                }
            } finally {
                rmDir(dir);
            }
        });
    }

    private static void rmDir(Path dir) {
        try {
            if (dir == null || !Files.exists(dir)) {
                return;
            }
            // try-with-resources: Files.walk returns a Stream backed by an open
            // directory handle that must be closed, or each rmDir leaks a descriptor.
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException ignored) {
                            }
                        });
            }
        } catch (IOException ignored) {
        }
    }

    /**
     * A {@link FilesFacade} that delegates every call to {@link FilesFacade#INSTANCE}
     * except {@link #truncate(int, long)}, which always fails -- reproducing a
     * host where the torn/stale-tail truncate cannot succeed (read-only remount,
     * Windows share lock) so {@code open()}'s fail-clean recreate path runs.
     */
    private static final class FailingTruncateFacade implements FilesFacade {
        @Override
        public long allocNativePath(String path) {
            return INSTANCE.allocNativePath(path);
        }

        @Override
        public boolean allocate(int fd, long size) {
            return INSTANCE.allocate(fd, size);
        }

        @Override
        public int close(int fd) {
            return INSTANCE.close(fd);
        }

        @Override
        public boolean exists(String path) {
            return INSTANCE.exists(path);
        }

        @Override
        public void findClose(long findPtr) {
            INSTANCE.findClose(findPtr);
        }

        @Override
        public long findFirst(String dir) {
            return INSTANCE.findFirst(dir);
        }

        @Override
        public long findName(long findPtr) {
            return INSTANCE.findName(findPtr);
        }

        @Override
        public int findNext(long findPtr) {
            return INSTANCE.findNext(findPtr);
        }

        @Override
        public int findType(long findPtr) {
            return INSTANCE.findType(findPtr);
        }

        @Override
        public void freeNativePath(long pathPtr) {
            INSTANCE.freeNativePath(pathPtr);
        }

        @Override
        public int fsync(int fd) {
            return INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            return INSTANCE.length(fd);
        }

        @Override
        public long length(String path) {
            return INSTANCE.length(path);
        }

        @Override
        public long length(long pathPtr) {
            return INSTANCE.length(pathPtr);
        }

        @Override
        public int lock(int fd) {
            return INSTANCE.lock(fd);
        }

        @Override
        public int mkdir(String path, int mode) {
            return INSTANCE.mkdir(path, mode);
        }

        @Override
        public int openCleanRW(String path) {
            return INSTANCE.openCleanRW(path);
        }

        @Override
        public int openCleanRW(long pathPtr) {
            return INSTANCE.openCleanRW(pathPtr);
        }

        @Override
        public int openRW(String path) {
            return INSTANCE.openRW(path);
        }

        @Override
        public int openRW(long pathPtr) {
            return INSTANCE.openRW(pathPtr);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            return INSTANCE.read(fd, addr, len, offset);
        }

        @Override
        public boolean remove(String path) {
            return INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
            return INSTANCE.remove(pathPtr);
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return INSTANCE.rename(oldPath, newPath);
        }

        @Override
        public boolean truncate(int fd, long size) {
            return false; // the fault under test
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            return INSTANCE.write(fd, addr, len, offset);
        }
    }
}
