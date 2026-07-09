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
import io.questdb.client.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

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

    private static void rmDir(Path dir) {
        try {
            if (dir == null || !Files.exists(dir)) {
                return;
            }
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException ignored) {
        }
    }
}
