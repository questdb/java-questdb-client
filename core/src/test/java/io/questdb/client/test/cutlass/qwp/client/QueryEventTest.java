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

import io.questdb.client.cutlass.qwp.client.QueryEvent;
import io.questdb.client.cutlass.qwp.client.QwpBatchBuffer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the {@link QueryEvent} pooled bean: each {@code asX()} builder
 * sets the matching {@code KIND_*} discriminator and its payload fields, and
 * {@link QueryEvent#reset()} clears every reference / scalar so a recycled
 * event from the I/O thread's pool starts in a clean state.
 */
public class QueryEventTest {

    @Test
    public void testFreshEventDefaults() {
        // A bare new QueryEvent has kind=0 (KIND_BATCH default) and zeroed
        // primitives. The I/O thread always calls one of the asX() builders
        // before publishing, so the defaults are only relevant before first use.
        QueryEvent ev = new QueryEvent();
        Assert.assertEquals(0, ev.kind);
        Assert.assertNull(ev.buffer);
        Assert.assertNull(ev.errorMessage);
        Assert.assertEquals(0, ev.errorStatus);
        Assert.assertEquals(0, ev.opType);
        Assert.assertEquals(0L, ev.rowsAffected);
        Assert.assertEquals(0L, ev.totalRows);
    }

    @Test
    public void testAsBatchSetsBufferAndKind() {
        QueryEvent ev = new QueryEvent();
        try (QwpBatchBuffer buf = new QwpBatchBuffer(64)) {
            QueryEvent returned = ev.asBatch(buf);
            Assert.assertSame("builder must return this for chaining", ev, returned);
            Assert.assertEquals(QueryEvent.KIND_BATCH, ev.kind);
            Assert.assertSame(buf, ev.buffer);
        }
    }

    @Test
    public void testAsEndSetsTotalRowsAndClearsBuffer() {
        QueryEvent ev = new QueryEvent();
        // pre-populate to verify asEnd clears the buffer field.
        try (QwpBatchBuffer stale = new QwpBatchBuffer(64)) {
            ev.asBatch(stale);
            QueryEvent returned = ev.asEnd(123_456L);
            Assert.assertSame(ev, returned);
            Assert.assertEquals(QueryEvent.KIND_END, ev.kind);
            Assert.assertEquals(123_456L, ev.totalRows);
            Assert.assertNull("asEnd must drop the stale buffer reference", ev.buffer);
        }
    }

    @Test
    public void testAsErrorSetsStatusAndMessage() {
        QueryEvent ev = new QueryEvent();
        QueryEvent returned = ev.asError((byte) 0x05, "parse error: unexpected token");
        Assert.assertSame(ev, returned);
        Assert.assertEquals(QueryEvent.KIND_ERROR, ev.kind);
        Assert.assertEquals((byte) 0x05, ev.errorStatus);
        Assert.assertEquals("parse error: unexpected token", ev.errorMessage);
        Assert.assertNull("asError must clear buffer", ev.buffer);
    }

    @Test
    public void testAsExecDoneSetsOpTypeAndRowsAffected() {
        QueryEvent ev = new QueryEvent();
        QueryEvent returned = ev.asExecDone((short) 7, 999L);
        Assert.assertSame(ev, returned);
        Assert.assertEquals(QueryEvent.KIND_EXEC_DONE, ev.kind);
        Assert.assertEquals((short) 7, ev.opType);
        Assert.assertEquals(999L, ev.rowsAffected);
        Assert.assertNull(ev.buffer);
    }

    @Test
    public void testAsTransportErrorSetsStatusAndMessage() {
        QueryEvent ev = new QueryEvent();
        QueryEvent returned = ev.asTransportError((byte) 0x7F, "socket reset");
        Assert.assertSame(ev, returned);
        Assert.assertEquals(QueryEvent.KIND_TRANSPORT_ERROR, ev.kind);
        Assert.assertEquals((byte) 0x7F, ev.errorStatus);
        Assert.assertEquals("socket reset", ev.errorMessage);
        Assert.assertNull(ev.buffer);
    }

    @Test
    public void testKindConstantsAreDistinct() {
        // Sanity: the KIND_* discriminators must be five distinct ints, otherwise
        // a switch over event.kind would silently merge two cases. Putting them
        // in a set asserts uniqueness without hard-coding the actual byte values.
        int[] kinds = {
                QueryEvent.KIND_BATCH,
                QueryEvent.KIND_END,
                QueryEvent.KIND_ERROR,
                QueryEvent.KIND_EXEC_DONE,
                QueryEvent.KIND_TRANSPORT_ERROR
        };
        for (int i = 0; i < kinds.length; i++) {
            for (int j = i + 1; j < kinds.length; j++) {
                Assert.assertNotEquals("KIND constants must be distinct", kinds[i], kinds[j]);
            }
        }
    }

    @Test
    public void testResetClearsEverythingFromError() {
        QueryEvent ev = new QueryEvent();
        ev.asError((byte) 0x05, "boom");
        ev.reset();
        Assert.assertEquals(-1, ev.kind);
        Assert.assertNull(ev.buffer);
        Assert.assertNull(ev.errorMessage);
        Assert.assertEquals(0, ev.errorStatus);
        Assert.assertEquals(0, ev.opType);
        Assert.assertEquals(0L, ev.rowsAffected);
        Assert.assertEquals(0L, ev.totalRows);
    }

    @Test
    public void testResetClearsEverythingFromBatch() {
        QueryEvent ev = new QueryEvent();
        try (QwpBatchBuffer buf = new QwpBatchBuffer(64)) {
            ev.asBatch(buf);
            ev.reset();
            Assert.assertEquals(-1, ev.kind);
            Assert.assertNull("reset must drop the buffer reference for prompt GC", ev.buffer);
        }
    }

    @Test
    public void testResetClearsEverythingFromExecDone() {
        QueryEvent ev = new QueryEvent();
        ev.asExecDone((short) 9, 1234L);
        ev.reset();
        Assert.assertEquals(-1, ev.kind);
        Assert.assertEquals(0, ev.opType);
        Assert.assertEquals(0L, ev.rowsAffected);
    }
}
