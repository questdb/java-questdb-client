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

    static {
        Os.init();
    }
}
