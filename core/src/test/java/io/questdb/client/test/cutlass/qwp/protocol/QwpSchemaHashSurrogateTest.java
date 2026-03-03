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

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.cutlass.qwp.protocol.QwpSchemaHash;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.ObjList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QwpSchemaHashSurrogateTest {

    private static final byte TYPE_LONG = 0x05;

    @Test
    public void testComputeSchemaHashInvalidSurrogatePair() {
        byte[] types = {TYPE_LONG};

        // "\uD800X" has a high surrogate followed by non-low-surrogate 'X'.
        // With the fix, the high surrogate becomes '?' and 'X' is preserved,
        // so the hash should equal the hash of "?X".
        long hashInvalid = QwpSchemaHash.computeSchemaHash(
                new String[]{"\uD800X"}, types
        );
        long hashExpected = QwpSchemaHash.computeSchemaHash(
                new String[]{"?X"}, types
        );
        assertEquals(hashExpected, hashInvalid);
    }

    @Test
    public void testComputeSchemaHashLoneHighSurrogateAtEnd() {
        byte[] types = {TYPE_LONG};

        // "\uD800" is a lone high surrogate at end of string.
        // Must hash as '?' to match OffHeapAppendMemory.putUtf8().
        long hashInvalid = QwpSchemaHash.computeSchemaHash(
                new String[]{"col\uD800"}, types
        );
        long hashExpected = QwpSchemaHash.computeSchemaHash(
                new String[]{"col?"}, types
        );
        assertEquals(hashExpected, hashInvalid);
    }

    @Test
    public void testComputeSchemaHashLoneLowSurrogate() {
        byte[] types = {TYPE_LONG};

        // "\uDC00" is a lone low surrogate (not preceded by a high surrogate).
        // Must hash as '?' to match OffHeapAppendMemory.putUtf8().
        long hashInvalid = QwpSchemaHash.computeSchemaHash(
                new String[]{"col\uDC00"}, types
        );
        long hashExpected = QwpSchemaHash.computeSchemaHash(
                new String[]{"col?"}, types
        );
        assertEquals(hashExpected, hashInvalid);
    }

    @Test
    public void testComputeSchemaHashDirectInvalidSurrogatePair() {
        ObjList<QwpTableBuffer.ColumnBuffer> invalidCols = new ObjList<>();
        invalidCols.add(new QwpTableBuffer.ColumnBuffer("\uD800X", TYPE_LONG, false));

        ObjList<QwpTableBuffer.ColumnBuffer> expectedCols = new ObjList<>();
        expectedCols.add(new QwpTableBuffer.ColumnBuffer("?X", TYPE_LONG, false));

        long hashInvalid = QwpSchemaHash.computeSchemaHashDirect(invalidCols);
        long hashExpected = QwpSchemaHash.computeSchemaHashDirect(expectedCols);
        assertEquals(hashExpected, hashInvalid);
    }

    @Test
    public void testComputeSchemaHashDirectLoneHighSurrogateAtEnd() {
        ObjList<QwpTableBuffer.ColumnBuffer> invalidCols = new ObjList<>();
        invalidCols.add(new QwpTableBuffer.ColumnBuffer("col\uD800", TYPE_LONG, false));

        ObjList<QwpTableBuffer.ColumnBuffer> expectedCols = new ObjList<>();
        expectedCols.add(new QwpTableBuffer.ColumnBuffer("col?", TYPE_LONG, false));

        long hashInvalid = QwpSchemaHash.computeSchemaHashDirect(invalidCols);
        long hashExpected = QwpSchemaHash.computeSchemaHashDirect(expectedCols);
        assertEquals(hashExpected, hashInvalid);
    }

    @Test
    public void testComputeSchemaHashDirectLoneLowSurrogate() {
        ObjList<QwpTableBuffer.ColumnBuffer> invalidCols = new ObjList<>();
        invalidCols.add(new QwpTableBuffer.ColumnBuffer("col\uDC00", TYPE_LONG, false));

        ObjList<QwpTableBuffer.ColumnBuffer> expectedCols = new ObjList<>();
        expectedCols.add(new QwpTableBuffer.ColumnBuffer("col?", TYPE_LONG, false));

        long hashInvalid = QwpSchemaHash.computeSchemaHashDirect(invalidCols);
        long hashExpected = QwpSchemaHash.computeSchemaHashDirect(expectedCols);
        assertEquals(hashExpected, hashInvalid);
    }
}
