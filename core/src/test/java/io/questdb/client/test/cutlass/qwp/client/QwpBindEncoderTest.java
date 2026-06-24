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

import io.questdb.client.cutlass.qwp.client.QwpBindValues;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimals;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Wire-format unit tests for {@link QwpBindValues}. Each test encodes one or
 * more binds and asserts the exact byte layout against a hand-built expected
 * payload. No server or connection required.
 */
public class QwpBindEncoderTest {

    // Bind header bytes.
    private static final byte NON_NULL = 0x00;
    private static final byte NULL_BITMAP = 0x01;
    private static final byte NULL_FLAG = 0x01;

    @Test
    public void testBindsWithinSingleReset() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.reset();
                binds.setLong(0, 42L);
                binds.setInt(1, 7);
                Assert.assertEquals(2, binds.count());
                byte[] first = readBuffer(binds);

                binds.reset();
                Assert.assertEquals(0, binds.count());
                Assert.assertEquals(0, binds.bufferLen());

                binds.setLong(0, 42L);
                binds.setInt(1, 7);
                byte[] second = readBuffer(binds);
                Assert.assertArrayEquals(first, second);
            }
        });
    }

    @Test
    public void testEncodeBoolean() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setBoolean(0, true);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_BOOLEAN);
                    writer.write(NON_NULL);
                    writer.write(1);
                }));
            }
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setBoolean(0, false);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_BOOLEAN);
                    writer.write(NON_NULL);
                    writer.write(0);
                }));
            }
        });
    }

    @Test
    public void testEncodeByte() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setByte(0, (byte) -128);
                binds.setByte(1, (byte) 0);
                binds.setByte(2, (byte) 127);
                byte[] want = expected(writer -> {
                    writer.write(QwpConstants.TYPE_BYTE);
                    writer.write(NON_NULL);
                    writer.write((byte) -128);
                    writer.write(QwpConstants.TYPE_BYTE);
                    writer.write(NON_NULL);
                    writer.write(0);
                    writer.write(QwpConstants.TYPE_BYTE);
                    writer.write(NON_NULL);
                    writer.write(127);
                });
                assertEncoded(binds, 3, want);
            }
        });
    }

    @Test
    public void testEncodeChar() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setChar(0, 'Z');
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_CHAR);
                    writer.write(NON_NULL);
                    writeShortLe(writer, (short) 'Z');
                }));
            }
        });
    }

    @Test
    public void testEncodeDate() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setDate(0, 1_700_000_000_000L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DATE);
                    writer.write(NON_NULL);
                    writeLongLe(writer, 1_700_000_000_000L);
                }));
            }
        });
    }

    @Test
    public void testEncodeDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setDecimal128(0, 6, 0x0123456789ABCDEFL, 0x7766554433221100L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DECIMAL128);
                    writer.write(NON_NULL);
                    writer.write(6);
                    writeLongLe(writer, 0x0123456789ABCDEFL);
                    writeLongLe(writer, 0x7766554433221100L);
                }));
            }
        });
    }

    @Test
    public void testEncodeDecimal128ConvenienceNullSentinel() throws Exception {
        // The server reads the DECIMAL128 scale byte regardless of the null
        // flag (it becomes part of the bound variable's type), so the client
        // must emit it on the NULL path too -- and preserve the scale from
        // the supplied Decimal128 sentinel rather than defaulting to 0.
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                Decimal128 nullValue = new Decimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 4);
                binds.setDecimal128(0, nullValue);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DECIMAL128);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                    writer.write(4);
                }));
            }
        });
    }

    @Test
    public void testEncodeDecimal128RejectsNegativeScale() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                try {
                    binds.setDecimal128(0, -1, 1L, 0L);
                    Assert.fail("expected IllegalArgumentException");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("scale"));
                }
            }
        });
    }

    /**
     * DECIMAL128 tops out at 38 digits of precision, so scale above 38 is
     * mathematically invalid. Reject per-width rather than against the
     * DECIMAL256 ceiling (76) the shared cap used to fall back to.
     */
    @Test
    public void testEncodeDecimal128RejectsScaleAbove38() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                try {
                    binds.setDecimal128(0, 39, 1L, 0L);
                    Assert.fail("expected IllegalArgumentException");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue("message must mention DECIMAL128: " + expected.getMessage(),
                            expected.getMessage().contains("DECIMAL128"));
                }
            }
        });
    }

    @Test
    public void testEncodeDecimal128RejectsScaleAboveMax() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                try {
                    binds.setDecimal128(0, Decimals.MAX_SCALE + 1, 1L, 0L);
                    Assert.fail("expected IllegalArgumentException");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("scale"));
                }
            }
        });
    }

    @Test
    public void testEncodeDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setDecimal256(0, 10, 0x1111111111111111L, 0x2222222222222222L, 0x3333333333333333L, 0x4444444444444444L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DECIMAL256);
                    writer.write(NON_NULL);
                    writer.write(10);
                    writeLongLe(writer, 0x1111111111111111L);
                    writeLongLe(writer, 0x2222222222222222L);
                    writeLongLe(writer, 0x3333333333333333L);
                    writeLongLe(writer, 0x4444444444444444L);
                }));
            }
        });
    }

    @Test
    public void testEncodeDecimal256ConvenienceNullSentinel() throws Exception {
        // Parallel to the DECIMAL128 case: the scale from the null sentinel
        // is propagated to the wire so it survives round-trip as part of the
        // bound variable's type.
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                Decimal256 nullValue = new Decimal256(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        3);
                binds.setDecimal256(0, nullValue);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DECIMAL256);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                    writer.write(3);
                }));
            }
        });
    }

    @Test
    public void testEncodeDecimal256AllowsScale76() throws Exception {
        // Top of the DECIMAL256 range: scale 76 is the documented ceiling.
        // Verify the encoder accepts it (used to pass under the shared-cap
        // check; the per-width split must not accidentally lower the ceiling).
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setDecimal256(0, 76, 1L, 0L, 0L, 0L);
                Assert.assertEquals(1, binds.count());
            }
        });
    }

    @Test
    public void testEncodeDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setDecimal64(0, 2, 12345L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DECIMAL64);
                    writer.write(NON_NULL);
                    writer.write(2);
                    writeLongLe(writer, 12345L);
                }));
            }
        });
    }

    /**
     * DECIMAL64 holds 18 digits of precision, so scale above 18 is
     * mathematically invalid. Under the shared-cap regime a scale up to 76
     * was silently accepted; the per-width check rejects at the correct
     * boundary.
     */
    @Test
    public void testEncodeDecimal64RejectsScaleAbove18() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                try {
                    binds.setDecimal64(0, 19, 1L);
                    Assert.fail("expected IllegalArgumentException");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue("message must mention DECIMAL64: " + expected.getMessage(),
                            expected.getMessage().contains("DECIMAL64"));
                }
            }
        });
    }

    @Test
    public void testEncodeDouble() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setDouble(0, 2.718281828);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DOUBLE);
                    writer.write(NON_NULL);
                    writeLongLe(writer, Double.doubleToRawLongBits(2.718281828));
                }));
            }
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setDouble(0, Double.NaN);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DOUBLE);
                    writer.write(NON_NULL);
                    writeLongLe(writer, Double.doubleToRawLongBits(Double.NaN));
                }));
            }
        });
    }

    @Test
    public void testEncodeFloat() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setFloat(0, 3.14f);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_FLOAT);
                    writer.write(NON_NULL);
                    writeIntLe(writer, Float.floatToRawIntBits(3.14f));
                }));
            }
        });
    }

    @Test
    public void testEncodeGeohashMaxPrecision() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                long value = 0x0FFF_FFFF_FFFF_FFFFL;
                binds.setGeohash(0, 60, value);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_GEOHASH);
                    writer.write(NON_NULL);
                    writeVarint(writer, 60);
                    for (int i = 0; i < 8; i++) {
                        writer.write((byte) (value >>> (i * 8)));
                    }
                }));
            }
        });
    }

    @Test
    public void testEncodeGeohashMinPrecision() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setGeohash(0, 1, 1L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_GEOHASH);
                    writer.write(NON_NULL);
                    writeVarint(writer, 1);
                    writer.write((byte) 0x01);
                }));
            }
        });
    }

    @Test
    public void testEncodeGeohashRejectsPrecisionAboveMax() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                try {
                    binds.setGeohash(0, 61, 1L);
                    Assert.fail("expected IllegalArgumentException");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("precision"));
                }
            }
        });
    }

    /**
     * Regression: before the fix, {@code setGeohash} did not mask {@code value}
     * to {@code precisionBits}. For a precision that is not a multiple of 8
     * (e.g. 5 bits, 1 byte on the wire) the top byte would leak whatever bits
     * the caller happened to have set above bit 4, silently changing the geohash
     * the server stored. The fix masks the high bits off before encoding.
     */
    @Test
    public void testEncodeGeohashMasksHighBitsForSubByteprecision() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                // precisionBits=5 -> 1 byte on the wire, 5 low bits significant.
                // Bits above 4 must be masked away so 0xFF on the wire becomes 0x1F.
                binds.setGeohash(0, 5, 0xFFL);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_GEOHASH);
                    writer.write(NON_NULL);
                    writeVarint(writer, 5);
                    writer.write((byte) 0x1F);
                }));
            }
        });
    }

    /**
     * Same regression at the 60-bit ceiling: the top nibble (bits 60-63) must
     * never reach the wire, regardless of the caller-supplied {@code value}.
     */
    @Test
    public void testEncodeGeohashMasksHighBitsAtMaxPrecision() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                // All 64 bits set -- the encoder must zero bits 60..63.
                binds.setGeohash(0, 60, -1L);
                long expectedValue = (1L << 60) - 1L;
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_GEOHASH);
                    writer.write(NON_NULL);
                    writeVarint(writer, 60);
                    for (int i = 0; i < 8; i++) {
                        writer.write((byte) (expectedValue >>> (i * 8)));
                    }
                }));
            }
        });
    }

    @Test
    public void testEncodeGeohashRejectsPrecisionBelowMin() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                try {
                    binds.setGeohash(0, 0, 0L);
                    Assert.fail("expected IllegalArgumentException");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("precision"));
                }
            }
        });
    }

    @Test
    public void testEncodeInt() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setInt(0, Integer.MIN_VALUE);
                binds.setInt(1, 0);
                binds.setInt(2, Integer.MAX_VALUE);
                byte[] want = expected(writer -> {
                    writer.write(QwpConstants.TYPE_INT);
                    writer.write(NON_NULL);
                    writeIntLe(writer, Integer.MIN_VALUE);
                    writer.write(QwpConstants.TYPE_INT);
                    writer.write(NON_NULL);
                    writeIntLe(writer, 0);
                    writer.write(QwpConstants.TYPE_INT);
                    writer.write(NON_NULL);
                    writeIntLe(writer, Integer.MAX_VALUE);
                });
                assertEncoded(binds, 3, want);
            }
        });
    }

    @Test
    public void testEncodeLong() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setLong(0, 42L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_LONG);
                    writer.write(NON_NULL);
                    writeLongLe(writer, 42L);
                }));
            }
        });
    }

    @Test
    public void testEncodeLong256() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setLong256(0, 0x1111111111111111L, 0x2222222222222222L, 0x3333333333333333L, 0x4444444444444444L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_LONG256);
                    writer.write(NON_NULL);
                    writeLongLe(writer, 0x1111111111111111L);
                    writeLongLe(writer, 0x2222222222222222L);
                    writeLongLe(writer, 0x3333333333333333L);
                    writeLongLe(writer, 0x4444444444444444L);
                }));
            }
        });
    }

    @Test
    public void testEncodeMultiBindMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setLong(0, 1234567890L);
                binds.setVarchar(1, "hello");
                binds.setBoolean(2, true);
                binds.setDouble(3, 1.5);
                byte[] want = expected(writer -> {
                    writer.write(QwpConstants.TYPE_LONG);
                    writer.write(NON_NULL);
                    writeLongLe(writer, 1234567890L);

                    writer.write(QwpConstants.TYPE_VARCHAR);
                    writer.write(NON_NULL);
                    writeIntLe(writer, 0);
                    writeIntLe(writer, 5);
                    byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
                    for (byte b : bytes) {
                        writer.write(b);
                    }

                    writer.write(QwpConstants.TYPE_BOOLEAN);
                    writer.write(NON_NULL);
                    writer.write(1);

                    writer.write(QwpConstants.TYPE_DOUBLE);
                    writer.write(NON_NULL);
                    writeLongLe(writer, Double.doubleToRawLongBits(1.5));
                });
                assertEncoded(binds, 4, want);
            }
        });
    }

    @Test
    public void testEncodeNullScalar() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setNull(0, QwpConstants.TYPE_LONG);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_LONG);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                }));
            }
        });
    }

    /**
     * {@link QwpBindValues#setNullDecimal128(int, int)} explicitly names the
     * scale, which survives to the wire even for a NULL value (the server
     * treats scale as part of the bound variable's type). Pins the framing:
     * type + null_flag + null_bitmap + scale.
     */
    @Test
    public void testEncodeNullDecimal128WithExplicitScale() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setNullDecimal128(0, 12);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DECIMAL128);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                    writer.write(12);
                }));
            }
        });
    }

    @Test
    public void testEncodeNullDecimal256WithExplicitScale() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setNullDecimal256(0, 50);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DECIMAL256);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                    writer.write(50);
                }));
            }
        });
    }

    @Test
    public void testEncodeNullDecimal64WithExplicitScale() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setNullDecimal64(0, 3);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_DECIMAL64);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                    writer.write(3);
                }));
            }
        });
    }

    /**
     * {@link QwpBindValues#setNullGeohash(int, int)} pins the precision_bits
     * even for NULL, since the server reads the varint unconditionally before
     * inspecting the null flag.
     */
    @Test
    public void testEncodeNullGeohashWithExplicitPrecision() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setNullGeohash(0, 40);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_GEOHASH);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                    writeVarint(writer, 40);
                }));
            }
        });
    }

    @Test
    public void testEncodeNullTypesExhaustive() throws Exception {
        // DECIMAL64/128/256 and GEOHASH NULLs carry a trailing scale byte
        // (or precision_bits varint) because the server reads those fields
        // unconditionally. All other types emit only type + null flag +
        // null bitmap. The exhaustive walk pins both shapes.
        byte[] allTypes = {
                QwpConstants.TYPE_BOOLEAN,
                QwpConstants.TYPE_BYTE,
                QwpConstants.TYPE_SHORT,
                QwpConstants.TYPE_CHAR,
                QwpConstants.TYPE_INT,
                QwpConstants.TYPE_LONG,
                QwpConstants.TYPE_FLOAT,
                QwpConstants.TYPE_DOUBLE,
                QwpConstants.TYPE_DATE,
                QwpConstants.TYPE_TIMESTAMP,
                QwpConstants.TYPE_TIMESTAMP_NANOS,
                QwpConstants.TYPE_UUID,
                QwpConstants.TYPE_LONG256,
                QwpConstants.TYPE_GEOHASH,
                QwpConstants.TYPE_VARCHAR,
                QwpConstants.TYPE_DECIMAL64,
                QwpConstants.TYPE_DECIMAL128,
                QwpConstants.TYPE_DECIMAL256,
        };
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                for (int i = 0; i < allTypes.length; i++) {
                    binds.setNull(i, allTypes[i]);
                }
                Assert.assertEquals(allTypes.length, binds.count());
                byte[] got = readBuffer(binds);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                for (byte t : allTypes) {
                    out.write(t);
                    out.write(NULL_FLAG);
                    out.write(NULL_BITMAP);
                    if (t == QwpConstants.TYPE_DECIMAL64
                            || t == QwpConstants.TYPE_DECIMAL128
                            || t == QwpConstants.TYPE_DECIMAL256) {
                        // setNull defaults to scale 0 for decimals.
                        out.write(0);
                    } else if (t == QwpConstants.TYPE_GEOHASH) {
                        // setNull defaults to the minimum valid precision (1 bit),
                        // encoded as a single-byte varint.
                        out.write(1);
                    }
                }
                Assert.assertArrayEquals(out.toByteArray(), got);
            }
        });
    }

    @Test
    public void testEncodeShort() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setShort(0, Short.MIN_VALUE);
                binds.setShort(1, (short) 0);
                binds.setShort(2, Short.MAX_VALUE);
                byte[] want = expected(writer -> {
                    writer.write(QwpConstants.TYPE_SHORT);
                    writer.write(NON_NULL);
                    writeShortLe(writer, Short.MIN_VALUE);
                    writer.write(QwpConstants.TYPE_SHORT);
                    writer.write(NON_NULL);
                    writeShortLe(writer, (short) 0);
                    writer.write(QwpConstants.TYPE_SHORT);
                    writer.write(NON_NULL);
                    writeShortLe(writer, Short.MAX_VALUE);
                });
                assertEncoded(binds, 3, want);
            }
        });
    }

    @Test
    public void testEncodeTimestampMicros() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setTimestampMicros(0, 1_700_000_000_000_000L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_TIMESTAMP);
                    writer.write(NON_NULL);
                    writeLongLe(writer, 1_700_000_000_000_000L);
                }));
            }
        });
    }

    @Test
    public void testEncodeTimestampNanos() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setTimestampNanos(0, 1_700_000_000_000_000_000L);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_TIMESTAMP_NANOS);
                    writer.write(NON_NULL);
                    writeLongLe(writer, 1_700_000_000_000_000_000L);
                }));
            }
        });
    }

    @Test
    public void testEncodeUuidConvenienceFromJavaUuid() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
                binds.setUuid(0, uuid);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_UUID);
                    writer.write(NON_NULL);
                    writeLongLe(writer, uuid.getLeastSignificantBits());
                    writeLongLe(writer, uuid.getMostSignificantBits());
                }));
            }
        });
    }

    @Test
    public void testEncodeUuidExplicit() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setUuid(0, 0xFEEDFACECAFEBEEFL, 0x0BADF00DDEADBEEFL);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_UUID);
                    writer.write(NON_NULL);
                    writeLongLe(writer, 0xFEEDFACECAFEBEEFL);
                    writeLongLe(writer, 0x0BADF00DDEADBEEFL);
                }));
            }
        });
    }

    @Test
    public void testEncodeUuidNullJavaUuid() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setUuid(0, null);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_UUID);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                }));
            }
        });
    }

    @Test
    public void testEncodeVarcharAscii() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setVarchar(0, "hello");
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_VARCHAR);
                    writer.write(NON_NULL);
                    writeIntLe(writer, 0);
                    writeIntLe(writer, 5);
                    for (byte b : "hello".getBytes(StandardCharsets.UTF_8)) {
                        writer.write(b);
                    }
                }));
            }
        });
    }

    @Test
    public void testEncodeVarcharEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setVarchar(0, "");
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_VARCHAR);
                    writer.write(NON_NULL);
                    writeIntLe(writer, 0);
                    writeIntLe(writer, 0);
                }));
            }
        });
    }

    @Test
    public void testEncodeVarcharNullShortcut() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setVarchar(0, null);
                assertEncoded(binds, 1, expected(writer -> {
                    writer.write(QwpConstants.TYPE_VARCHAR);
                    writer.write(NULL_FLAG);
                    writer.write(NULL_BITMAP);
                }));
            }
        });
    }

    @Test
    public void testEncodeVarcharUnicode() throws Exception {
        assertMemoryLeak(() -> {
            String value = "café";
            byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
            assertMemoryLeak(() -> {
                try (QwpBindValues binds = new QwpBindValues()) {
                    binds.setVarchar(0, value);
                    assertEncoded(binds, 1, expected(writer -> {
                        writer.write(QwpConstants.TYPE_VARCHAR);
                        writer.write(NON_NULL);
                        writeIntLe(writer, 0);
                        writeIntLe(writer, utf8.length);
                        for (byte b : utf8) {
                            writer.write(b);
                        }
                    }));
                }
            });
        });
    }

    @Test
    public void testEncoderGrowsBufferBeyondDefault() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                String big = io.questdb.client.test.tools.TestUtils.repeat("x", 20_000);
                binds.setVarchar(0, big);
                Assert.assertEquals(1, binds.count());
                // type(1) + flag(1) + offset0(4) + len(4) + 20000 bytes = 20010
                Assert.assertEquals(1 + 1 + 4 + 4 + 20_000, binds.bufferLen());
            }
        });
    }

    @Test
    public void testRejectsDuplicateIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setLong(0, 1L);
                try {
                    binds.setLong(0, 2L);
                    Assert.fail("expected IllegalStateException");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue(expected.getMessage().contains("index"));
                }
            }
        });
    }

    @Test
    public void testRejectsOutOfOrderIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                binds.setLong(0, 1L);
                try {
                    binds.setLong(2, 3L);
                    Assert.fail("expected IllegalStateException");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue(expected.getMessage().contains("index"));
                }
            }
        });
    }

    @Test
    public void testRejectsUnsupportedNullTypeCode() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                try {
                    binds.setNull(0, QwpConstants.TYPE_BINARY);
                    Assert.fail("expected IllegalArgumentException");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("bind type"));
                }
            }
        });
    }

    @Test
    public void testTooManyBinds() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpBindValues binds = new QwpBindValues()) {
                int max = QwpConstants.MAX_COLUMNS_PER_TABLE;
                for (int i = 0; i < max; i++) {
                    binds.setInt(i, i);
                }
                try {
                    binds.setInt(max, max);
                    Assert.fail("expected IllegalStateException");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue(expected.getMessage().contains("too many"));
                }
            }
        });
    }

    private static void assertEncoded(QwpBindValues binds, int expectedCount, byte[] expected) {
        Assert.assertEquals(expectedCount, binds.count());
        Assert.assertArrayEquals(expected, readBuffer(binds));
    }

    private static byte[] expected(ExpectedWriter body) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        body.write(out);
        return out.toByteArray();
    }

    private static byte[] readBuffer(QwpBindValues binds) {
        long len = binds.bufferLen();
        byte[] out = new byte[(int) len];
        long ptr = binds.bufferPtr();
        for (int i = 0; i < out.length; i++) {
            out[i] = Unsafe.getUnsafe().getByte(ptr + i);
        }
        return out;
    }

    private static void writeIntLe(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    private static void writeLongLe(ByteArrayOutputStream out, long value) {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((value >>> (i * 8)) & 0xFF));
        }
    }

    private static void writeShortLe(ByteArrayOutputStream out, short value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        while (value > 0x7F) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) (value & 0x7F));
    }

    @FunctionalInterface
    private interface ExpectedWriter {
        void write(ByteArrayOutputStream out);
    }
}
