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

package io.questdb.client.std;

/**
 * CRC-32C (Castagnoli, polynomial 0x1EDC6F41) checksum over off-heap memory.
 * Software-only implementation using slice-by-8 with eight pre-computed
 * 256-entry tables — no SSE 4.2 / ARMv8 hardware-accelerated CRC32C
 * intrinsics, but fast enough that the SF append path is no longer
 * dominated by checksum cost (slice-by-8 is ~6× faster than the naive
 * byte-at-a-time loop on the typical 100–600 byte SF frame payloads).
 * <p>
 * Pass {@link #INIT} as the {@code seed} to start a fresh checksum. To
 * chain across multiple non-contiguous buffers, pass the previous call's
 * return value as the next call's seed:
 * <pre>{@code
 * int crc = Crc32c.INIT;
 * crc = Crc32c.update(crc, header, 8);
 * crc = Crc32c.update(crc, payload, payloadLen);
 * // crc now holds the CRC-32C of header || payload
 * }</pre>
 * The empty-input case is idempotent: {@code update(seed, _, 0) == seed}.
 */
public final class Crc32c {
    /** Seed value to start a fresh CRC-32C accumulation. */
    public static final int INIT = 0;
    private static final int[] CRC32C_TABLE = buildCrc32cTable();

    private Crc32c() {
    }

    /**
     * Update a running CRC-32C checksum with {@code len} bytes from native
     * memory starting at {@code addr}.
     *
     * @param seed previous CRC value, or {@link #INIT} to start fresh
     * @param addr off-heap address of the bytes to fold in (must point to
     *             at least {@code len} readable bytes — no validation here,
     *             a bad address will SIGSEGV the JVM)
     * @param len  number of bytes to consume; pass 0 to no-op (returns
     *             {@code seed} unchanged)
     * @return the new CRC value, suitable as the {@code seed} for a
     * subsequent chained call
     */
    public static native int update(int seed, long addr, long len);

    /**
     * Java/Unsafe slice-by-8 CRC-32C for memory that can fault while it is read,
     * such as a recovery mmap over a sparse or concurrently truncated file.
     * Keeping every load at an {@link Unsafe} intrinsic site lets HotSpot turn an
     * mmap access fault into a catchable {@link InternalError}; the native
     * {@link #update} path cannot provide that guarantee because a SIGBUS raised
     * inside JNI aborts the JVM.
     * <p>
     * The hot loop consumes each 8-byte block with two {@code getInt}s rather than
     * eight {@code getByte}s -- 9 loads per block instead of 16, for the same table
     * work. {@code getInt} is an {@link Unsafe} intrinsic exactly as {@code getByte}
     * is, so the fault-catchability above is unaffected. It does make the loop
     * little-endian, which costs nothing here: the segment and dictionary formats this
     * checksums are already little-endian throughout (their headers are read back with
     * {@code getInt}), and the native twin this must agree with asserts the same.
     *
     * @param seed previous CRC value, or {@link #INIT} to start fresh
     * @param addr off-heap address of at least {@code len} readable bytes
     * @param len  number of bytes to consume
     * @return the new CRC value
     */
    public static int updateUnsafe(int seed, long addr, long len) {
        assert len >= 0L : "CRC length must be non-negative";
        int crc = ~seed;
        int[] table = CRC32C_TABLE;
        while (len >= Long.BYTES) {
            // Little-endian: lo holds bytes 0..3 and hi bytes 4..7, ascending from the
            // low byte, so the per-byte table lookups below stay in wire order.
            int lo = Unsafe.getUnsafe().getInt(addr) ^ crc;
            int hi = Unsafe.getUnsafe().getInt(addr + Integer.BYTES);
            crc = table[7 * 256 + (lo & 0xFF)]
                    ^ table[6 * 256 + ((lo >>> 8) & 0xFF)]
                    ^ table[5 * 256 + ((lo >>> 16) & 0xFF)]
                    ^ table[4 * 256 + (lo >>> 24)]
                    ^ table[3 * 256 + (hi & 0xFF)]
                    ^ table[2 * 256 + ((hi >>> 8) & 0xFF)]
                    ^ table[256 + ((hi >>> 16) & 0xFF)]
                    ^ table[hi >>> 24];
            addr += Long.BYTES;
            len -= Long.BYTES;
        }
        while (len-- > 0L) {
            crc = (crc >>> 8) ^ table[(crc ^ Unsafe.getUnsafe().getByte(addr++)) & 0xFF];
        }
        return ~crc;
    }

    private static int[] buildCrc32cTable() {
        int[] table = new int[8 * 256];
        for (int n = 0; n < 256; n++) {
            int c = n;
            for (int k = 0; k < 8; k++) {
                c = (c & 1) != 0 ? (0x82F63B78 ^ (c >>> 1)) : (c >>> 1);
            }
            table[n] = c;
        }
        for (int slice = 1; slice < 8; slice++) {
            int previousOffset = (slice - 1) * 256;
            int offset = slice * 256;
            for (int n = 0; n < 256; n++) {
                int previous = table[previousOffset + n];
                table[offset + n] = (previous >>> 8) ^ table[previous & 0xFF];
            }
        }
        return table;
    }

    static {
        Os.init();
    }
}
