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
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class QwpSchemaFeedbackResponseTest {

    @Test
    public void testUpdatesAreAtomicAndEveryPrefixIsRejected() {
        byte[] valid = updates("orders", "trades");
        WebSocketResponse response = new WebSocketResponse();
        withNative(valid, (ptr, length) -> {
            Assert.assertTrue(response.readFrom(ptr, length, true));
            Assert.assertEquals(2, response.getSchemaUpdateCount());
            Assert.assertEquals("orders", response.getSchemaUpdateTableName(0));
            Assert.assertEquals(0, response.getSchemaUpdate(0).getRequestId());
            Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING, response.getSchemaUpdate(0).getResult());
            for (int n = 0; n < length; n++) {
                Assert.assertFalse("prefix " + n, response.readFrom(ptr, n, true));
                Assert.assertFalse(response.hasSchemaUpdates());
                Assert.assertFalse(response.isSchemaInvalidation());
            }
        });
    }

    @Test
    public void testInvalidationAndFailedReadClearVisibleFeedback() {
        WebSocketResponse response = new WebSocketResponse();
        byte[] invalidation = base((byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_INVALIDATE_ALL));
        withNative(invalidation, (ptr, length) -> {
            Assert.assertTrue(response.readFrom(ptr, length, true));
            Assert.assertTrue(response.isSchemaInvalidation());
            Assert.assertFalse(response.hasSchemaUpdates());
            Assert.assertFalse(response.readFrom(ptr, length - 1, true));
            Assert.assertFalse(response.isSchemaInvalidation());
            Assert.assertFalse(response.hasSchemaUpdates());
        });
    }

    @Test
    public void testNegotiationModesAndDurableAckRules() {
        byte[] updates = updates("t");
        withNative(updates, (ptr, length) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, length, false)));
        byte[] reserved = base((byte) (WebSocketResponse.STATUS_OK | 0x40));
        withNative(reserved, (ptr, length) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, length, true)));
        byte[] durable = new byte[]{(byte) (WebSocketResponse.STATUS_DURABLE_ACK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_INVALIDATE_ALL), 0, 0};
        withNative(durable, (ptr, length) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, length, true)));
    }

    @Test
    public void testDuplicateNamesAreCaseInsensitive() {
        byte[] duplicate = updates("foo", "FOO");
        withNative(duplicate, (ptr, length) -> {
            WebSocketResponse response = new WebSocketResponse();
            Assert.assertFalse(response.readFrom(ptr, length, true));
            Assert.assertFalse(response.hasSchemaUpdates());
        });
    }

    @Test
    public void testNestedEnvelopeAndNonZeroFeedbackRequestIdRejected() {
        byte[] nested = updatesWithPayload("t", new byte[]{'Q', 'W', 'P', '1', 1, 0, 0, 0, 0, 0, 0, 0});
        withNative(nested, (ptr, length) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, length, true)));
        byte[] payload = missingPayload();
        payload[1] = 1;
        byte[] nonZero = updatesWithPayload("t", payload);
        withNative(nonZero, (ptr, length) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, length, true)));
    }

    @Test
    public void testInvalidSchemaColumnNamesRejectWholeAckAndNackWithoutPublishingPrefix() {
        byte[] valid = missingPayload();
        byte[][] invalidSchemas = {
                knownPayload("x", "x"),
                knownPayload("x", "X"),
                knownPayload("Ä", "ä"),
                knownPayload("a/b"),
                knownPayload("a\nb")
        };
        for (byte[] invalid : invalidSchemas) {
            for (byte status : new byte[]{WebSocketResponse.STATUS_OK, WebSocketResponse.STATUS_SCHEMA_MISMATCH}) {
                byte[] feedback = updatesWithPayloads(status, new String[]{"first", "second"}, new byte[][]{valid, invalid});
                withNative(feedback, (ptr, length) -> {
                    WebSocketResponse response = new WebSocketResponse();
                    Assert.assertFalse(response.readFrom(ptr, length, true));
                    Assert.assertFalse(response.hasSchemaUpdates());
                    Assert.assertFalse(response.isSchemaInvalidation());
                });
            }
        }
    }

    @Test
    public void testNackUpdatesPreserveBaseError() {
        byte[] payload = missingPayload();
        byte[] name = "bad_table".getBytes(StandardCharsets.UTF_8);
        byte[] message = "bad row".getBytes(StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(11 + message.length + 2 + 2 + name.length + 4 + payload.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        b.put((byte) (WebSocketResponse.STATUS_SCHEMA_MISMATCH | WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES));
        b.putLong(19).putShort((short) message.length).put(message).putShort((short) 1);
        b.putShort((short) name.length).put(name).putInt(payload.length).put(payload);
        withNative(b.array(), (ptr, length) -> {
            WebSocketResponse response = new WebSocketResponse();
            Assert.assertTrue(response.readFrom(ptr, length, true));
            Assert.assertEquals(WebSocketResponse.STATUS_SCHEMA_MISMATCH, response.getStatus());
            Assert.assertEquals(19, response.getSequence());
            Assert.assertEquals("bad row", response.getErrorMessage());
            Assert.assertEquals("bad_table", response.getSchemaUpdateTableName(0));
        });
    }

    @Test
    public void testHostileCountLengthAndUtf8RejectedWithoutPublication() {
        byte[] count = base((byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES));
        count = java.util.Arrays.copyOf(count, count.length + 2);
        count[count.length - 2] = (byte) 0xff;
        count[count.length - 1] = (byte) 0xff;
        withNative(count, (ptr, length) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, length, true)));

        byte[] invalidUtf8 = updates("t");
        invalidUtf8[15] = (byte) 0xc0;
        withNative(invalidUtf8, (ptr, length) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, length, true)));

        byte[] oversized = updates("t");
        ByteBuffer.wrap(oversized).order(ByteOrder.LITTLE_ENDIAN).putInt(16, -1);
        withNative(oversized, (ptr, length) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, length, true)));
    }

    @Test
    public void testOneMiBBoundAppliesOnlyToUpdates() {
        int count = 3000;
        String prefix = new String(new char[123]).replace('\0', '\u4e00');
        int nameLength = 373;
        int length = 11 + count * (2 + nameLength + 8);
        Assert.assertTrue(length > QwpSchemaProtocol.MAX_FRAME_SIZE);
        byte[] base = new byte[length];
        ByteBuffer b = ByteBuffer.wrap(base).order(ByteOrder.LITTLE_ENDIAN);
        b.put(WebSocketResponse.STATUS_OK).putLong(1).putShort((short) count);
        for (int i = 0; i < count; i++) {
            byte[] name = (prefix + String.format(java.util.Locale.ROOT, "%04d", i)).getBytes(StandardCharsets.UTF_8);
            Assert.assertEquals(nameLength, name.length);
            b.putShort((short) nameLength).put(name).putLong(i);
        }
        withNative(base, (ptr, n) -> Assert.assertTrue(new WebSocketResponse().readFrom(ptr, n, false)));
        base[0] = (byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_INVALIDATE_ALL);
        withNative(base, (ptr, n) -> Assert.assertTrue(new WebSocketResponse().readFrom(ptr, n, true)));
        byte[] payload = missingPayload();
        byte[] update = java.util.Arrays.copyOf(base, base.length + 2 + 2 + 1 + 4 + payload.length);
        ByteBuffer trailer = ByteBuffer.wrap(update).order(ByteOrder.LITTLE_ENDIAN);
        trailer.position(base.length).putShort((short) 1).putShort((short) 1).put((byte) 't').putInt(payload.length).put(payload);
        update[0] = (byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES);
        withNative(update, (ptr, n) -> Assert.assertFalse(new WebSocketResponse().readFrom(ptr, n, true)));
    }

    private static byte[] base(byte status) {
        ByteBuffer b = ByteBuffer.allocate(11).order(ByteOrder.LITTLE_ENDIAN);
        b.put(status).putLong(7).putShort((short) 0);
        return b.array();
    }

    private static byte[] missingPayload() {
        return ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(0).put((byte) QwpSchemaProtocol.RESULT_MISSING).array();
    }

    private static byte[] knownPayload(String... columnNames) {
        int size = 26;
        for (String name : columnNames) {
            size += 2 + name.getBytes(StandardCharsets.UTF_8).length + 6;
        }
        ByteBuffer b = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        b.put(QwpSchemaProtocol.KIND_SCHEMA).putLong(0).put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                .putInt(1).putLong(1).putShort((short) -1).putShort((short) columnNames.length);
        for (String name : columnNames) {
            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
            b.putShort((short) bytes.length).put(bytes).putInt(5).putShort((short) 0);
        }
        return b.array();
    }

    private static byte[] updates(String... names) {
        return updatesWithPayloads(names, missingPayload());
    }

    private static byte[] updatesWithPayload(String name, byte[] payload) {
        return updatesWithPayloads(new String[]{name}, payload);
    }

    private static byte[] updatesWithPayloads(String[] names, byte[] payload) {
        byte[][] payloads = new byte[names.length][];
        java.util.Arrays.fill(payloads, payload);
        return updatesWithPayloads(WebSocketResponse.STATUS_OK, names, payloads);
    }

    private static byte[] updatesWithPayloads(byte status, String[] names, byte[][] payloads) {
        int size = 13;
        for (int i = 0; i < names.length; i++) {
            size += 2 + names[i].getBytes(StandardCharsets.UTF_8).length + 4 + payloads[i].length;
        }
        ByteBuffer b = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        b.put((byte) (status | WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES));
        b.putLong(7).putShort((short) 0).putShort((short) names.length);
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
            b.putShort((short) bytes.length).put(bytes).putInt(payloads[i].length).put(payloads[i]);
        }
        return b.array();
    }

    private static void withNative(byte[] bytes, NativeAssertion assertion) {
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            assertion.run(ptr, bytes.length);
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private interface NativeAssertion {
        void run(long ptr, int length);
    }
}
