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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * Parses a {@code SERVER_INFO} frame payload off the wire into an immutable
 * {@link QwpServerInfo}. The payload pointer points at the start of the QWP
 * message (12-byte QWP header followed by the body), matching what the
 * WebSocket frame handler receives for every egress binary frame.
 * <p>
 * Bounds checks on every read are required because a hostile or buggy server
 * can encode a {@code u16} length that overshoots the frame remainder. The
 * decoder surfaces the mismatch as a {@link QwpDecodeException} rather than
 * reading past the end of the native buffer.
 */
public final class QwpServerInfoDecoder {

    private QwpServerInfoDecoder() {
    }

    /**
     * Decodes a {@code SERVER_INFO} frame in place from {@code payload} for
     * {@code payloadLen} bytes.
     *
     * @throws QwpDecodeException if the frame is truncated, the msg_kind is not
     *                            {@link QwpEgressMsgKind#SERVER_INFO}, or either
     *                            length prefix exceeds the remainder.
     */
    public static QwpServerInfo decode(long payload, int payloadLen) throws QwpDecodeException {
        final int fixedBytes = QwpConstants.HEADER_SIZE + 1 + 1 + 8 + 4 + 8 + 2;
        if (payloadLen < fixedBytes) {
            throw new QwpDecodeException("SERVER_INFO frame truncated [payloadLen=" + payloadLen
                    + ", minRequired=" + fixedBytes + ']');
        }
        long p = payload + QwpConstants.HEADER_SIZE;
        byte msgKind = Unsafe.getUnsafe().getByte(p);
        if (msgKind != QwpEgressMsgKind.SERVER_INFO) {
            throw new QwpDecodeException("expected SERVER_INFO msg_kind 0x"
                    + Integer.toHexString(QwpEgressMsgKind.SERVER_INFO & 0xFF)
                    + " got 0x" + Integer.toHexString(msgKind & 0xFF));
        }
        p += 1;
        byte role = Unsafe.getUnsafe().getByte(p);
        p += 1;
        long epoch = Unsafe.getUnsafe().getLong(p);
        p += 8;
        int capabilities = Unsafe.getUnsafe().getInt(p);
        p += 4;
        long serverWallNs = Unsafe.getUnsafe().getLong(p);
        p += 8;
        long payloadEnd = payload + payloadLen;
        String clusterId = readUtf8U16(p, payloadEnd, "cluster_id");
        int clusterLen = Unsafe.getUnsafe().getShort(p) & 0xFFFF;
        p += 2 + clusterLen;
        if (p + 2 > payloadEnd) {
            throw new QwpDecodeException("SERVER_INFO truncated before node_id length");
        }
        String nodeId = readUtf8U16(p, payloadEnd, "node_id");
        // Trailing zone_id is gated by the CAP_ZONE bit so a client reading a
        // server with no zone configured (capabilities=0) never reaches this
        // branch and sees the zone-less byte layout. When the bit is set the
        // trailer is mandatory; a missing or truncated zone_id is a wire error.
        String zoneId = null;
        if ((capabilities & QwpEgressMsgKind.CAP_ZONE) != 0) {
            int nodeLen = Unsafe.getUnsafe().getShort(p) & 0xFFFF;
            p += 2 + nodeLen;
            if (p + 2 > payloadEnd) {
                throw new QwpDecodeException("SERVER_INFO truncated before zone_id length");
            }
            zoneId = readUtf8U16(p, payloadEnd, "zone_id");
        }
        return new QwpServerInfo(role, epoch, capabilities, serverWallNs, clusterId, nodeId, zoneId);
    }

    /**
     * Reads a u16-length-prefixed UTF-8 string at {@code p}. Validates that the
     * length fits within the payload before copying, so a hostile {@code u16}
     * length can't drag bytes out of unrelated native memory.
     */
    private static String readUtf8U16(long p, long payloadEnd, String fieldName) throws QwpDecodeException {
        if (p + 2 > payloadEnd) {
            throw new QwpDecodeException("SERVER_INFO truncated before " + fieldName + " length");
        }
        int len = Unsafe.getUnsafe().getShort(p) & 0xFFFF;
        long bytesStart = p + 2;
        if (bytesStart + len > payloadEnd) {
            throw new QwpDecodeException("SERVER_INFO " + fieldName + " length " + len
                    + " exceeds frame remainder " + (payloadEnd - bytesStart));
        }
        if (len == 0) {
            return "";
        }
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(bytesStart + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
