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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Wire-format tests for the {@code STATUS_LOCAL_DURABLE_ACK} response: it
 * shares the sequence-less {@code status + tableCount + entries} layout with
 * {@code STATUS_DURABLE_ACK} and must survive a write/read round trip,
 * validate structurally, and classify via {@code isLocalDurableAck()} only.
 */
public class WebSocketResponseLocalDurableAckTest {

    @Test
    public void testClassificationIsMutuallyExclusive() {
        WebSocketResponse local = WebSocketResponse.localDurableAck("trades", 7L);
        assertTrue(local.isLocalDurableAck());
        assertFalse(local.isDurableAck());
        assertFalse(local.isSuccess());
        assertEquals("LOCAL_DURABLE_ACK", local.getStatusName());

        WebSocketResponse replicated = WebSocketResponse.durableAck("trades", 7L);
        assertTrue(replicated.isDurableAck());
        assertFalse(replicated.isLocalDurableAck());
        assertEquals("DURABLE_ACK", replicated.getStatusName());
    }

    @Test
    public void testStructurallyValid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.localDurableAck("trades", 42L);
            int size = response.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                assertEquals(size, response.writeTo(ptr));
                assertTrue(WebSocketResponse.isStructurallyValid(ptr, size));
                // A truncated frame (entry cut short) must not validate.
                assertFalse(WebSocketResponse.isStructurallyValid(ptr, size - 1));
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testToStringNamesLocalStatus() {
        WebSocketResponse response = WebSocketResponse.localDurableAck("trades", 1L);
        assertEquals("WebSocketResponse{status=LOCAL_DURABLE_ACK, tables=1}", response.toString());
    }

    @Test
    public void testWriteReadRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            WebSocketResponse out = WebSocketResponse.localDurableAck("trades", 42L);
            int size = out.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                assertEquals(size, out.writeTo(ptr));

                WebSocketResponse in = new WebSocketResponse();
                assertTrue(in.readFrom(ptr, size));
                assertEquals(WebSocketResponse.STATUS_LOCAL_DURABLE_ACK, in.getStatus());
                assertTrue(in.isLocalDurableAck());
                assertFalse("local ack must not classify as the replicated ack",
                        in.isDurableAck());
                assertEquals(-1L, in.getSequence());
                assertEquals(1, in.getTableEntryCount());
                assertEquals("trades", in.getTableName(0));
                assertEquals(42L, in.getTableSeqTxn(0));

                // Truncated input must be rejected, not misparsed.
                assertFalse(new WebSocketResponse().readFrom(ptr, size - 1));
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
