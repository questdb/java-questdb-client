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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import org.junit.Test;

import static org.junit.Assert.*;

public class GlobalSymbolDictionaryTest {

    @Test
    public void testAddRecoveredSymbol_appendsWithoutDeduplicating() {
        // Recovery replays persisted entries in id order. Distinct source strings
        // that decode to the same characters -- lone UTF-16 surrogates both
        // UTF-8-encode to '?', so they read back as the string "?" -- must keep
        // DISTINCT ids, so the producer id space matches the persisted entry count.
        // getOrAddSymbol de-dups them; addRecoveredSymbol must not.
        GlobalSymbolDictionary dedup = new GlobalSymbolDictionary();
        dedup.getOrAddSymbol("?");
        dedup.getOrAddSymbol("?");
        assertEquals("getOrAddSymbol de-dups colliding strings", 1, dedup.size());

        GlobalSymbolDictionary recovered = new GlobalSymbolDictionary();
        assertEquals(0, recovered.addRecoveredSymbol("?"));
        assertEquals(1, recovered.addRecoveredSymbol("?"));
        assertEquals(2, recovered.addRecoveredSymbol("nvda"));
        assertEquals("addRecoveredSymbol keeps colliding entries distinct", 3, recovered.size());

        // Dense id -> symbol mapping is preserved position-for-position.
        assertEquals("?", recovered.getSymbol(0));
        assertEquals("?", recovered.getSymbol(1));
        assertEquals("nvda", recovered.getSymbol(2));

        // A later ingest of a colliding string reuses the highest recovered id
        // (harmless -- both encode to identical bytes), and a genuinely new symbol
        // continues past the recovered tip.
        assertEquals(1, recovered.getOrAddSymbol("?"));
        assertEquals(3, recovered.getOrAddSymbol("brand-new"));
    }

    @Test
    public void testAddRecoveredSymbol_rejectsNullWithoutMutation() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        assertEquals(0, dict.addRecoveredSymbol("AAPL"));

        try {
            dict.addRecoveredSymbol(null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertEquals("symbol cannot be null", expected.getMessage());
        }

        assertEquals(1, dict.size());
        assertEquals("AAPL", dict.getSymbol(0));
        assertEquals(0, dict.getId("AAPL"));
    }

    @Test
    public void testAddSymbol_assignsSequentialIds() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        assertEquals(0, dict.getOrAddSymbol("AAPL"));
        assertEquals(1, dict.getOrAddSymbol("GOOG"));
        assertEquals(2, dict.getOrAddSymbol("MSFT"));
        assertEquals(3, dict.getOrAddSymbol("TSLA"));

        assertEquals(4, dict.size());
    }

    @Test
    public void testAddSymbol_deduplicatesSameSymbol() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        int id1 = dict.getOrAddSymbol("AAPL");
        int id2 = dict.getOrAddSymbol("AAPL");
        int id3 = dict.getOrAddSymbol("AAPL");

        assertEquals(id1, id2);
        assertEquals(id2, id3);
        assertEquals(0, id1);
        assertEquals(1, dict.size());
    }

    @Test
    public void testClear() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");
        assertEquals(2, dict.size());

        dict.clear();

        assertTrue(dict.isEmpty());
        assertEquals(0, dict.size());
        assertFalse(dict.contains("AAPL"));
    }

    @Test
    public void testClear_thenAddRestartsFromZero() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");
        dict.clear();

        // New IDs should start from 0
        assertEquals(0, dict.getOrAddSymbol("MSFT"));
        assertEquals(1, dict.getOrAddSymbol("TSLA"));
    }

    @Test
    public void testContains() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        assertFalse(dict.contains("AAPL"));

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");

        assertTrue(dict.contains("AAPL"));
        assertTrue(dict.contains("GOOG"));
        assertFalse(dict.contains("MSFT"));
        assertFalse(dict.contains(null));
    }

    @Test
    public void testCustomInitialCapacity() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary(1024);

        // Should work normally
        for (int i = 0; i < 100; i++) {
            assertEquals(i, dict.getOrAddSymbol("SYM_" + i));
        }
        assertEquals(100, dict.size());
    }

    @Test
    public void testGetId_returnsCorrectId() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");
        dict.getOrAddSymbol("MSFT");

        assertEquals(0, dict.getId("AAPL"));
        assertEquals(1, dict.getId("GOOG"));
        assertEquals(2, dict.getId("MSFT"));
    }

    @Test
    public void testGetId_returnsMinusOneForNull() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        assertEquals(-1, dict.getId(null));
    }

    @Test
    public void testGetId_returnsMinusOneForUnknown() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        dict.getOrAddSymbol("AAPL");

        assertEquals(-1, dict.getId("GOOG"));
        assertEquals(-1, dict.getId("UNKNOWN"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetOrAddSymbol_throwsForNull() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        dict.getOrAddSymbol(null);
    }

    @Test
    public void testGetSymbol_returnsCorrectSymbol() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");
        dict.getOrAddSymbol("MSFT");

        assertEquals("AAPL", dict.getSymbol(0));
        assertEquals("GOOG", dict.getSymbol(1));
        assertEquals("MSFT", dict.getSymbol(2));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetSymbol_throwsForInvalidId() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        dict.getOrAddSymbol("AAPL");
        dict.getSymbol(1); // Only id 0 exists
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetSymbol_throwsForNegativeId() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        dict.getOrAddSymbol("AAPL");
        dict.getSymbol(-1);
    }

    @Test
    public void testIsEmpty() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        assertTrue(dict.isEmpty());

        dict.getOrAddSymbol("AAPL");
        assertFalse(dict.isEmpty());
    }

    @Test
    public void testLargeNumberOfSymbols() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        // Add 10000 symbols
        for (int i = 0; i < 10000; i++) {
            assertEquals(i, dict.getOrAddSymbol("SYMBOL_" + i));
        }

        assertEquals(10000, dict.size());

        // Verify retrieval
        for (int i = 0; i < 10000; i++) {
            assertEquals("SYMBOL_" + i, dict.getSymbol(i));
            assertEquals(i, dict.getId("SYMBOL_" + i));
        }
    }

    @Test
    public void testMixedSymbolsAcrossTables() {
        // Simulates symbols from multiple tables sharing the dictionary
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        // Table "trades": exchange column
        int nyse = dict.getOrAddSymbol("NYSE");      // 0
        int nasdaq = dict.getOrAddSymbol("NASDAQ");  // 1

        // Table "prices": currency column
        int usd = dict.getOrAddSymbol("USD");        // 2
        int eur = dict.getOrAddSymbol("EUR");        // 3

        // Table "orders": exchange column (reuses)
        int nyse2 = dict.getOrAddSymbol("NYSE");     // Still 0

        assertEquals(nyse, nyse2);
        assertEquals(4, dict.size());

        // All symbols accessible
        assertEquals("NYSE", dict.getSymbol(nyse));
        assertEquals("NASDAQ", dict.getSymbol(nasdaq));
        assertEquals("USD", dict.getSymbol(usd));
        assertEquals("EUR", dict.getSymbol(eur));
    }

    @Test
    public void testSpecialCharactersInSymbols() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("");           // Empty string
        dict.getOrAddSymbol(" ");          // Space
        dict.getOrAddSymbol("a b c");      // With spaces
        dict.getOrAddSymbol("AAPL\u0000"); // With null char
        dict.getOrAddSymbol("é");     // Unicode
        dict.getOrAddSymbol("\uD83D\uDE00"); // Emoji

        assertEquals(6, dict.size());

        assertEquals("", dict.getSymbol(0));
        assertEquals(" ", dict.getSymbol(1));
        assertEquals("a b c", dict.getSymbol(2));
        assertEquals("AAPL\u0000", dict.getSymbol(3));
        assertEquals("é", dict.getSymbol(4));
        assertEquals("\uD83D\uDE00", dict.getSymbol(5));
    }

    @Test
    public void testGetOrAddSymbol_refusesGrowthPastProtocolCap() {
        // Pre-sized so the 2M fill does not rehash its way through the test budget.
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary(1 << 22);
        for (int i = 0; i < QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE; i++) {
            assertEquals(i, dict.getOrAddSymbol("f" + i));
        }
        // Boundary: the 2,000,000th distinct symbol (id 1_999_999) was ACCEPTED above --
        // the guard must refuse growth PAST the cap, not growth TO it, because the
        // server accepts a catch-up of exactly deltaStart + deltaCount == 2_000_000.
        assertEquals(QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE, dict.size());

        try {
            dict.getOrAddSymbol("one-too-many");
            fail("expected LineSenderException past the dictionary cap");
        } catch (LineSenderException expected) {
            assertTrue("message names the limit: " + expected.getMessage(),
                    expected.getMessage().contains("2000000"));
            assertTrue("message names the recovery: " + expected.getMessage(),
                    expected.getMessage().contains("close this sender"));
            assertTrue("message points at the reset valve: " + expected.getMessage(),
                    expected.getMessage().contains("symbol_dict_reset")
                            && expected.getMessage().contains("resetSymbolDictionary()"));
        }

        // The refusal mutated nothing: size unchanged, the refused symbol absent,
        // existing symbols still resolve, and a retry refuses identically.
        assertEquals(QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE, dict.size());
        assertEquals(-1, dict.getId("one-too-many"));
        assertEquals(42, dict.getOrAddSymbol("f42"));
        try {
            dict.getOrAddSymbol("one-too-many");
            fail("expected LineSenderException on retry");
        } catch (LineSenderException expected) {
        }
    }

    @Test
    public void testProtocolCapConstantPinnedToServerValue() {
        // The server-side QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE (questdb OSS) is
        // 2_000_000 and the ingress decoder rejects any delta or catch-up whose
        // deltaStartId + deltaCount exceeds it. If this pin fails, the server
        // constant moved and both sides must move together.
        assertEquals(2_000_000, QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE);
    }
}
