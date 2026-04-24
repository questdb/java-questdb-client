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

import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class WebSocketResponseTest {

    @Test
    public void testDurableAckFactory() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.durableAck("trades", 42L);
            Assert.assertTrue(response.isDurableAck());
            Assert.assertFalse(response.isSuccess());
            Assert.assertEquals(1, response.getTableEntryCount());
            Assert.assertEquals("trades", response.getTableName(0));
            Assert.assertEquals(42L, response.getTableSeqTxn(0));
            Assert.assertEquals(WebSocketResponse.STATUS_DURABLE_ACK, response.getStatus());
            Assert.assertEquals("DURABLE_ACK", response.getStatusName());
            Assert.assertNull(response.getErrorMessage());
        });
    }

    @Test
    public void testDurableAckIsStructurallyValid() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.durableAck("t", 7L);
            int size = response.serializedSize();

            long ptr = Unsafe.malloc(size + 1, MemoryTag.NATIVE_DEFAULT);
            try {
                response.writeTo(ptr);
                Assert.assertTrue(WebSocketResponse.isStructurallyValid(ptr, size));
                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, size + 1));
                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, size - 1));
            } finally {
                Unsafe.free(ptr, size + 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDurableAckRoundTripThroughNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse original = WebSocketResponse.durableAck("orders", 12345L);
            int size = original.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                original.writeTo(ptr);

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertTrue(parsed.isDurableAck());
                Assert.assertFalse(parsed.isSuccess());
                Assert.assertEquals(1, parsed.getTableEntryCount());
                Assert.assertEquals("orders", parsed.getTableName(0));
                Assert.assertEquals(12345L, parsed.getTableSeqTxn(0));
                Assert.assertNull(parsed.getErrorMessage());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDurableAckDoesNotCarryErrorMessage() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.durableAck("t", 99L);
            int size = response.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                response.writeTo(ptr);

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertNull(parsed.getErrorMessage());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSuccessIsNotDurableAck() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.success(10L);
            Assert.assertTrue(response.isSuccess());
            Assert.assertFalse(response.isDurableAck());
            Assert.assertEquals("OK", response.getStatusName());
        });
    }

    @Test
    public void testErrorIsNotDurableAck() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.error(
                    5L, WebSocketResponse.STATUS_PARSE_ERROR, "bad input");
            Assert.assertFalse(response.isDurableAck());
            Assert.assertFalse(response.isSuccess());
            Assert.assertEquals("PARSE_ERROR", response.getStatusName());
        });
    }

    @Test
    public void testSuccessRoundTripUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse original = WebSocketResponse.success(77L);
            int size = original.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                original.writeTo(ptr);

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertTrue(parsed.isSuccess());
                Assert.assertFalse(parsed.isDurableAck());
                Assert.assertEquals(77L, parsed.getSequence());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSuccessWithTableEntriesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            // Build a STATUS_OK frame with 2 table entries directly in native memory
            // Format: status(1) + sequence(8) + tableCount(2) + entries
            byte[] name1 = "trades".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] name2 = "orders".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int size = 1 + 8 + 2 + (2 + name1.length + 8) + (2 + name2.length + 8);
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                int offset = 0;
                Unsafe.getUnsafe().putByte(ptr + offset, WebSocketResponse.STATUS_OK);
                offset += 1;
                Unsafe.getUnsafe().putLong(ptr + offset, 42L);
                offset += 8;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) 2);
                offset += 2;
                // entry 1: trades, seqTxn=10
                Unsafe.getUnsafe().putShort(ptr + offset, (short) name1.length);
                offset += 2;
                for (int i = 0; i < name1.length; i++) {
                    Unsafe.getUnsafe().putByte(ptr + offset + i, name1[i]);
                }
                offset += name1.length;
                Unsafe.getUnsafe().putLong(ptr + offset, 10L);
                offset += 8;
                // entry 2: orders, seqTxn=20
                Unsafe.getUnsafe().putShort(ptr + offset, (short) name2.length);
                offset += 2;
                for (int i = 0; i < name2.length; i++) {
                    Unsafe.getUnsafe().putByte(ptr + offset + i, name2[i]);
                }
                offset += name2.length;
                Unsafe.getUnsafe().putLong(ptr + offset, 20L);

                Assert.assertTrue(WebSocketResponse.isStructurallyValid(ptr, size));

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertTrue(parsed.isSuccess());
                Assert.assertFalse(parsed.isDurableAck());
                Assert.assertEquals(42L, parsed.getSequence());
                Assert.assertEquals(2, parsed.getTableEntryCount());
                Assert.assertEquals("trades", parsed.getTableName(0));
                Assert.assertEquals(10L, parsed.getTableSeqTxn(0));
                Assert.assertEquals("orders", parsed.getTableName(1));
                Assert.assertEquals(20L, parsed.getTableSeqTxn(1));
                Assert.assertNull(parsed.getErrorMessage());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEmptyTableNameRejected() throws Exception {
        assertMemoryLeak(() -> {
            // STATUS_OK with nameLen=0 - a well-behaved server never emits
            // an empty table name; the frame must be rejected as structurally
            // invalid so it cannot corrupt the per-table tracker.
            int size = 1 + 8 + 2 + (2 + 8);
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                int offset = 0;
                Unsafe.getUnsafe().putByte(ptr + offset, WebSocketResponse.STATUS_OK);
                offset += 1;
                Unsafe.getUnsafe().putLong(ptr + offset, 1L);
                offset += 8;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) 1);
                offset += 2;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) 0);
                offset += 2;
                Unsafe.getUnsafe().putLong(ptr + offset, 42L);

                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, size));

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertFalse(parsed.readFrom(ptr, size));
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testErrorRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse original = WebSocketResponse.error(
                    3L, WebSocketResponse.STATUS_INTERNAL_ERROR, "oops");
            int size = original.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                original.writeTo(ptr);

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertFalse(parsed.isSuccess());
                Assert.assertFalse(parsed.isDurableAck());
                Assert.assertEquals(3L, parsed.getSequence());
                Assert.assertEquals("oops", parsed.getErrorMessage());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testLargeTableCountWithInsufficientPayload() throws Exception {
        assertMemoryLeak(() -> {
            int size = WebSocketResponse.MIN_OK_RESPONSE_SIZE;
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_OK);
                Unsafe.getUnsafe().putLong(ptr + 1, 1L);
                Unsafe.getUnsafe().putShort(ptr + 9, (short) 1000);

                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, size));

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertFalse(parsed.readFrom(ptr, size));
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTrailingGarbageBytesStatusOk() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse original = WebSocketResponse.success(1L);
            int validSize = original.serializedSize();
            int totalSize = validSize + 5;
            long ptr = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                original.writeTo(ptr);
                for (int i = 0; i < 5; i++) {
                    Unsafe.getUnsafe().putByte(ptr + validSize + i, (byte) 0xFF);
                }

                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, totalSize));
            } finally {
                Unsafe.free(ptr, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTrailingGarbageBytesWithTableEntries() throws Exception {
        assertMemoryLeak(() -> {
            byte[] name = "trades".getBytes(StandardCharsets.UTF_8);
            int validSize = 1 + 8 + 2 + (2 + name.length + 8);
            int totalSize = validSize + 3;
            long ptr = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                int offset = 0;
                Unsafe.getUnsafe().putByte(ptr + offset, WebSocketResponse.STATUS_OK);
                offset += 1;
                Unsafe.getUnsafe().putLong(ptr + offset, 5L);
                offset += 8;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) 1);
                offset += 2;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) name.length);
                offset += 2;
                for (int i = 0; i < name.length; i++) {
                    Unsafe.getUnsafe().putByte(ptr + offset + i, name[i]);
                }
                offset += name.length;
                Unsafe.getUnsafe().putLong(ptr + offset, 10L);

                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, totalSize));

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertFalse(parsed.readFrom(ptr, totalSize));
            } finally {
                Unsafe.free(ptr, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTruncatedTableEntriesDurableAck() throws Exception {
        assertMemoryLeak(() -> {
            byte[] name1 = "t1".getBytes(StandardCharsets.UTF_8);
            // STATUS_DURABLE_ACK with tableCount=2 but only 1 entry
            int size = 1 + 2 + (2 + name1.length + 8);
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                int offset = 0;
                Unsafe.getUnsafe().putByte(ptr + offset, WebSocketResponse.STATUS_DURABLE_ACK);
                offset += 1;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) 2);
                offset += 2;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) name1.length);
                offset += 2;
                for (int i = 0; i < name1.length; i++) {
                    Unsafe.getUnsafe().putByte(ptr + offset + i, name1[i]);
                }
                offset += name1.length;
                Unsafe.getUnsafe().putLong(ptr + offset, 10L);

                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, size));

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertFalse(parsed.readFrom(ptr, size));
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTruncatedTableEntriesStatusOk() throws Exception {
        assertMemoryLeak(() -> {
            byte[] name1 = "trades".getBytes(StandardCharsets.UTF_8);
            // STATUS_OK with tableCount=2 but only 1 entry
            int size = 1 + 8 + 2 + (2 + name1.length + 8);
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                int offset = 0;
                Unsafe.getUnsafe().putByte(ptr + offset, WebSocketResponse.STATUS_OK);
                offset += 1;
                Unsafe.getUnsafe().putLong(ptr + offset, 1L);
                offset += 8;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) 2);
                offset += 2;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) name1.length);
                offset += 2;
                for (int i = 0; i < name1.length; i++) {
                    Unsafe.getUnsafe().putByte(ptr + offset + i, name1[i]);
                }
                offset += name1.length;
                Unsafe.getUnsafe().putLong(ptr + offset, 10L);

                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, size));

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertFalse(parsed.readFrom(ptr, size));
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testUnknownStatusByte() throws Exception {
        assertMemoryLeak(() -> {
            int size = WebSocketResponse.MIN_ERROR_RESPONSE_SIZE;
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(ptr, (byte) 0xFF);
                Unsafe.getUnsafe().putLong(ptr + 1, 1L);
                Unsafe.getUnsafe().putShort(ptr + 9, (short) 0);

                // Unknown status falls through to error validation path
                Assert.assertTrue(WebSocketResponse.isStructurallyValid(ptr, size));

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertEquals((byte) 0xFF, parsed.getStatus());
                Assert.assertEquals("UNKNOWN(255)", parsed.getStatusName());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testZeroLengthPayload() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, 0));

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertFalse(parsed.readFrom(ptr, 0));
            } finally {
                Unsafe.free(ptr, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
