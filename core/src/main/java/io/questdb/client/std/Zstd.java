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
 * Client-side JNI wrapper over libzstd's decompression API. QWP's RESULT_BATCH
 * frames can carry zstd-compressed bodies when the handshake negotiated
 * {@code X-QWP-Accept-Encoding: zstd}; this class is the decoder-side entry
 * point. The native implementation lives in
 * {@code share/zstd/zstd_jni.c} (bundled libzstd, decompress only).
 */
public final class Zstd {

    private Zstd() {
    }

    public static native long createDCtx();

    /**
     * Decompresses {@code srcLen} bytes at {@code srcAddr} into the buffer at
     * {@code dstAddr} (capacity {@code dstCap}). Returns the number of bytes
     * written on success; a negative value encodes a libzstd error code.
     */
    public static native long decompress(long ctx, long srcAddr, long srcLen, long dstAddr, long dstCap);

    public static native void freeDCtx(long ptr);

    /**
     * Returns the decompressed size declared in the zstd frame header at
     * {@code srcAddr} (length {@code srcLen}). Allows the caller to size the
     * destination buffer in a single allocation instead of retrying on
     * dst-too-small.
     * <ul>
     *   <li>positive: declared content size in bytes</li>
     *   <li>{@code -1}: frame valid but content size not stored
     *       (ZSTD_CONTENTSIZE_UNKNOWN; the encoder disabled the size flag)</li>
     *   <li>{@code -2}: invalid frame, truncated header, or size exceeds
     *       {@link Long#MAX_VALUE}</li>
     * </ul>
     */
    public static native long getFrameContentSize(long srcAddr, long srcLen);
}
