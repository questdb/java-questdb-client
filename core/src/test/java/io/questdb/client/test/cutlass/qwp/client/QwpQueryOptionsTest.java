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

import io.questdb.client.cutlass.qwp.client.QwpQueryOptions;
import org.junit.Assert;
import org.junit.Test;

/**
 * State of {@link QwpQueryOptions}: defaults, setters, validation, reset and
 * copy. What the options put on the wire is covered by
 * {@link QwpQueryClientQueryFlagsTest}.
 */
public class QwpQueryOptionsTest {

    @Test
    public void testClearRestoresDefaults() {
        QwpQueryOptions options = allSet();
        Assert.assertSame(options, options.clear());
        assertDefaults(options);
    }

    @Test
    public void testCopyFrom() {
        QwpQueryOptions source = allSet();
        QwpQueryOptions target = new QwpQueryOptions();
        Assert.assertSame(target, target.copyFrom(source));
        Assert.assertEquals(source.toString(), target.toString());
        assertAllSet(target);

        // the copy is independent of its source
        source.clear();
        assertAllSet(target);

        Assert.assertSame(target, target.copyFrom(target));
        assertAllSet(target);

        // null copies the defaults
        Assert.assertSame(target, target.copyFrom(null));
        assertDefaults(target);
    }

    @Test
    public void testDefaults() {
        QwpQueryOptions options = new QwpQueryOptions();
        assertDefaults(options);
        Assert.assertEquals("QwpQueryOptions{skipRows=0, maxRows=9223372036854775807, countRows=false, "
                + "stats=false, errorPosition=false, resetSymbolDict=false}", options.toString());
    }

    @Test
    public void testNegativeRowsRejected() {
        QwpQueryOptions options = new QwpQueryOptions().skipRows(3).maxRows(4);
        for (long rows : new long[]{-1, -128, Long.MIN_VALUE}) {
            try {
                options.skipRows(rows);
                Assert.fail("skipRows(" + rows + ") must throw");
            } catch (IllegalArgumentException e) {
                Assert.assertEquals("skipRows must be >= 0 [skipRows=" + rows + ']', e.getMessage());
            }
            try {
                options.maxRows(rows);
                Assert.fail("maxRows(" + rows + ") must throw");
            } catch (IllegalArgumentException e) {
                Assert.assertEquals("maxRows must be >= 0 [maxRows=" + rows + ']', e.getMessage());
            }
            // a rejected value leaves the previous one
            Assert.assertEquals(3, options.getSkipRows());
            Assert.assertEquals(4, options.getMaxRows());
        }
    }

    @Test
    public void testRowBounds() {
        QwpQueryOptions options = new QwpQueryOptions();
        for (long rows : new long[]{0, 1, 127, 128, Long.MAX_VALUE - 1, Long.MAX_VALUE}) {
            Assert.assertSame(options, options.skipRows(rows));
            Assert.assertEquals(rows, options.getSkipRows());
            Assert.assertSame(options, options.maxRows(rows));
            Assert.assertEquals(rows, options.getMaxRows());
        }
    }

    @Test
    public void testSetters() {
        QwpQueryOptions options = new QwpQueryOptions();
        Assert.assertSame(options, options.countRows(true));
        Assert.assertTrue(options.isRowCountRequested());
        Assert.assertSame(options, options.stats(true));
        Assert.assertTrue(options.isStatsRequested());
        Assert.assertSame(options, options.errorPosition(true));
        Assert.assertTrue(options.isErrorPositionRequested());
        Assert.assertSame(options, options.resetSymbolDict(true));
        Assert.assertTrue(options.isSymbolDictResetRequested());
        Assert.assertEquals("QwpQueryOptions{skipRows=0, maxRows=9223372036854775807, countRows=true, "
                + "stats=true, errorPosition=true, resetSymbolDict=true}", options.toString());

        options.countRows(false).stats(false).errorPosition(false).resetSymbolDict(false);
        assertDefaults(options);
    }

    private static QwpQueryOptions allSet() {
        return new QwpQueryOptions()
                .skipRows(100)
                .maxRows(50)
                .countRows(true)
                .stats(true)
                .errorPosition(true)
                .resetSymbolDict(true);
    }

    private static void assertAllSet(QwpQueryOptions options) {
        Assert.assertEquals(100, options.getSkipRows());
        Assert.assertEquals(50, options.getMaxRows());
        Assert.assertTrue(options.isRowCountRequested());
        Assert.assertTrue(options.isStatsRequested());
        Assert.assertTrue(options.isErrorPositionRequested());
        Assert.assertTrue(options.isSymbolDictResetRequested());
    }

    private static void assertDefaults(QwpQueryOptions options) {
        Assert.assertEquals(0, options.getSkipRows());
        Assert.assertEquals(Long.MAX_VALUE, options.getMaxRows());
        Assert.assertFalse(options.isRowCountRequested());
        Assert.assertFalse(options.isStatsRequested());
        Assert.assertFalse(options.isErrorPositionRequested());
        Assert.assertFalse(options.isSymbolDictResetRequested());
    }
}
