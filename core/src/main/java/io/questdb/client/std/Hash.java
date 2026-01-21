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

package io.questdb.client.std;

public final class Hash {

    // Constant from Rust compiler's FxHasher.
    private static final long M2 = 0x517cc1b727220a95L;

    private static final int SPREAD_HASH_BITS = 0x7fffffff;

    public static int hashLong128_32(long key1, long key2) {
        return (int) hashLong128_64(key1, key2);
    }

    public static long hashLong128_64(long key1, long key2) {
        return fmix64(key1 * M2 + key2);
    }

    public static int hashLong32(long k) {
        return (int) hashLong64(k);
    }

    public static long hashLong64(long k) {
        return fmix64(k);
    }

    /**
     * (copied from ConcurrentHashMap)
     * Spreads (XORs) higher bits of hash to lower and also forces top
     * bit to 0. Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide. (Among known examples are sets of Float keys
     * holding consecutive whole numbers in small tables.)  So we
     * apply a transform that spreads the impact of higher bits
     * downward. There is a trade-off between speed, utility, and
     * quality of bit-spreading. Because many common sets of hashes
     * are already reasonably distributed (so don't benefit from
     * spreading), and because we use trees to handle large sets of
     * collisions in bins, we just XOR some shifted bits in the
     * cheapest possible way to reduce systematic lossage, as well as
     * to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     *
     * @param h hash code
     * @return adjusted hash code
     */
    public static int spread(int h) {
        return (h ^ (h >>> 16)) & SPREAD_HASH_BITS;
    }

    /**
     * Murmur finalizer.
     */
    private static long fmix64(long h) {
        h = (h ^ (h >>> 33)) * 0xff51afd7ed558ccdL;
        h = (h ^ (h >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return h ^ (h >>> 33);
    }

}