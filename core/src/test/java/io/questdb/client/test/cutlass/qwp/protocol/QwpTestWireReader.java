/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.std.Unsafe;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;

/**
 * Bounds-checked cursor over native memory holding an encoded QWP message.
 * Tests decode the wire bytes with this reader rather than with production
 * decoders, so an encoder bug cannot hide behind a matching decoder bug.
 * Multi-byte reads use the native byte order, like the production encoder.
 */
final class QwpTestWireReader {
    private final long address;
    private final int limit;
    private int position;

    QwpTestWireReader(long address, int limit) {
        this.address = address;
        this.limit = limit;
    }

    /** Reads {@code length} raw bytes as a string, without a length prefix. */
    String ascii(int length) {
        return new String(bytes(length), StandardCharsets.UTF_8);
    }

    byte[] bytes(int length) {
        require(length);
        byte[] value = new byte[length];
        for (int i = 0; i < length; i++) {
            value[i] = Unsafe.getUnsafe().getByte(address + position + i);
        }
        position += length;
        return value;
    }

    float f32() {
        return Float.intBitsToFloat(i32());
    }

    double f64() {
        return Double.longBitsToDouble(i64());
    }

    short i16() {
        require(Short.BYTES);
        short value = Unsafe.getUnsafe().getShort(address + position);
        position += Short.BYTES;
        return value;
    }

    int i32() {
        require(Integer.BYTES);
        int value = Unsafe.getUnsafe().getInt(address + position);
        position += Integer.BYTES;
        return value;
    }

    long i64() {
        require(Long.BYTES);
        long value = Unsafe.getUnsafe().getLong(address + position);
        position += Long.BYTES;
        return value;
    }

    int limit() {
        return limit;
    }

    int position() {
        return position;
    }

    void skip(int length) {
        require(length);
        position += length;
    }

    /** Reads a varint-prefixed UTF-8 string. */
    String string() {
        return new String(stringBytes(), StandardCharsets.UTF_8);
    }

    /** Reads the raw bytes of a varint-prefixed string. */
    byte[] stringBytes() {
        return bytes(varint());
    }

    int u16() {
        return i16() & 0xffff;
    }

    int u8() {
        require(1);
        return Unsafe.getUnsafe().getByte(address + position++) & 0xff;
    }

    int varint() {
        int result = 0;
        for (int shift = 0; shift < 32; shift += 7) {
            int value = u8();
            result |= (value & 0x7f) << shift;
            if ((value & 0x80) == 0) {
                return result;
            }
        }
        throw new AssertionError("varint exceeds 32 bits at position " + position);
    }

    private void require(int length) {
        Assert.assertTrue(
                "read of " + length + " bytes at " + position + " exceeds limit " + limit,
                length >= 0 && position <= limit - length
        );
    }
}
