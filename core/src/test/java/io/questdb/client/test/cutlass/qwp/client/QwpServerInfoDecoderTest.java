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

import io.questdb.client.cutlass.qwp.client.QwpDecodeException;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.client.QwpServerInfoDecoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Hardening + happy-path coverage for {@link QwpServerInfoDecoder}. Each test
 * crafts a SERVER_INFO frame in native memory with a controlled layout, then
 * asserts that the decoder either returns a {@link QwpServerInfo} matching the
 * encoded fields or throws {@link QwpDecodeException} with a diagnostic that
 * mentions the failing field. The frame layout is:
 * <pre>
 *  [HEADER_SIZE bytes] [msg_kind:u8] [role:u8] [epoch:u64] [capabilities:u32]
 *  [server_wall_ns:i64] [cluster_id_len:u16] [cluster_id_bytes]
 *  [node_id_len:u16] [node_id_bytes]
 *  -- present iff (capabilities &amp; CAP_ZONE) != 0 --
 *  [zone_id_len:u16] [zone_id_bytes]
 * </pre>
 */
public class QwpServerInfoDecoderTest {

    /**
     * Layout offsets after HEADER_SIZE bytes of opaque header. Mirrors
     * {@link QwpServerInfoDecoder#decode(long, int)}.
     */
    private static final int OFF_MSG_KIND = QwpConstants.HEADER_SIZE;
    private static final int OFF_ROLE = OFF_MSG_KIND + 1;
    private static final int OFF_EPOCH = OFF_ROLE + 1;
    private static final int OFF_CAPABILITIES = OFF_EPOCH + 8;
    private static final int OFF_SERVER_WALL_NS = OFF_CAPABILITIES + 4;
    private static final int OFF_CLUSTER_ID_LEN = OFF_SERVER_WALL_NS + 8;

    @Test
    public void testHappyPathRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String clusterId = "prod-cluster";
            String nodeId = "node-42";
            int len = totalLen(clusterId, nodeId);
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrame(buf, QwpEgressMsgKind.ROLE_PRIMARY, 100L, 0xCAFEBABE,
                        1_700_000_000_000_000_000L, clusterId, nodeId);
                QwpServerInfo info = QwpServerInfoDecoder.decode(buf, len);
                Assert.assertEquals(QwpEgressMsgKind.ROLE_PRIMARY, info.getRole());
                Assert.assertEquals(100L, info.getEpoch());
                Assert.assertEquals(0xCAFEBABE, info.getCapabilities());
                Assert.assertEquals(1_700_000_000_000_000_000L, info.getServerWallNs());
                Assert.assertEquals(clusterId, info.getClusterId());
                Assert.assertEquals(nodeId, info.getNodeId());
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEmptyClusterAndNodeStrings() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int len = totalLen("", "");
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrame(buf, QwpEgressMsgKind.ROLE_STANDALONE, 0L, 0, 0L, "", "");
                QwpServerInfo info = QwpServerInfoDecoder.decode(buf, len);
                Assert.assertEquals("", info.getClusterId());
                Assert.assertEquals("", info.getNodeId());
                Assert.assertEquals(QwpEgressMsgKind.ROLE_STANDALONE, info.getRole());
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testUtf8MultiByteRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Mix ASCII, accented Latin, CJK, and an emoji to exercise 1/2/3/4-byte
            // UTF-8 sequences through readUtf8U16's byte-by-byte copy.
            String clusterId = "klïster-集群-🚀";
            String nodeId = "résumé-ु";
            int len = totalLen(clusterId, nodeId);
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                // capabilities=2 keeps the bit pattern non-zero without setting
                // CAP_ZONE (0x01); we don't want the decoder to expect a zone
                // trailer here.
                writeFrame(buf, QwpEgressMsgKind.ROLE_REPLICA, 1L, 2, 1L, clusterId, nodeId);
                QwpServerInfo info = QwpServerInfoDecoder.decode(buf, len);
                Assert.assertEquals(clusterId, info.getClusterId());
                Assert.assertEquals(nodeId, info.getNodeId());
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTruncatedBeforeFixedFieldsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // payloadLen too short to even fit the fixed prelude.
            int len = QwpConstants.HEADER_SIZE + 1 + 1 + 8 + 4 + 8 + 1; // 1 byte short of fixedBytes
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                zero(buf, len);
                Unsafe.getUnsafe().putByte(buf + OFF_MSG_KIND, QwpEgressMsgKind.SERVER_INFO);
                QwpServerInfoDecoder.decode(buf, len);
                Assert.fail("decoder must reject undersized payload");
            } catch (QwpDecodeException expected) {
                Assert.assertTrue("error mentions truncation: " + expected.getMessage(),
                        expected.getMessage().contains("truncated")
                                && expected.getMessage().contains("payloadLen"));
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWrongMsgKindRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int len = totalLen("c", "n");
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrame(buf, QwpEgressMsgKind.ROLE_PRIMARY, 0L, 0, 0L, "c", "n");
                // Stomp the msg_kind with something that isn't SERVER_INFO.
                Unsafe.getUnsafe().putByte(buf + OFF_MSG_KIND, QwpEgressMsgKind.RESULT_BATCH);
                QwpServerInfoDecoder.decode(buf, len);
                Assert.fail("decoder must reject wrong msg_kind");
            } catch (QwpDecodeException expected) {
                String msg = expected.getMessage();
                Assert.assertTrue("error mentions expected msg_kind: " + msg,
                        msg.contains("SERVER_INFO") || msg.contains(Integer.toHexString(QwpEgressMsgKind.SERVER_INFO & 0xFF)));
                Assert.assertTrue("error mentions actual msg_kind hex: " + msg,
                        msg.contains(Integer.toHexString(QwpEgressMsgKind.RESULT_BATCH & 0xFF)));
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTruncatedBeforeClusterIdLengthRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // fixedBytes = HEADER_SIZE + 1+1+8+4+8+2 = OFF_CLUSTER_ID_LEN+2. Pass
            // one byte less so the up-front fixed-prelude check rejects before
            // ever reading the cluster_id length prefix.
            int payloadLen = OFF_CLUSTER_ID_LEN + 1;
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                zero(buf, payloadLen);
                Unsafe.getUnsafe().putByte(buf + OFF_MSG_KIND, QwpEgressMsgKind.SERVER_INFO);
                QwpServerInfoDecoder.decode(buf, payloadLen);
                Assert.fail("decoder must reject payload that ends before cluster_id length");
            } catch (QwpDecodeException expected) {
                Assert.assertTrue("error mentions truncation: " + expected.getMessage(),
                        expected.getMessage().contains("truncated"));
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testClusterIdLengthOvershootsPayloadRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Lay out a frame that claims a 5000-byte cluster_id but only carries 4.
            int payloadLen = OFF_CLUSTER_ID_LEN + 2 + 4; // length prefix + 4 bytes
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                zero(buf, payloadLen);
                Unsafe.getUnsafe().putByte(buf + OFF_MSG_KIND, QwpEgressMsgKind.SERVER_INFO);
                Unsafe.getUnsafe().putShort(buf + OFF_CLUSTER_ID_LEN, (short) 5000);
                QwpServerInfoDecoder.decode(buf, payloadLen);
                Assert.fail("decoder must reject cluster_id length that overshoots remainder");
            } catch (QwpDecodeException expected) {
                String msg = expected.getMessage();
                Assert.assertTrue("error mentions cluster_id: " + msg, msg.contains("cluster_id"));
                Assert.assertTrue("error mentions overshoot length: " + msg, msg.contains("5000"));
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNodeIdLengthOvershootsPayloadRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String clusterId = "ok";
            // Encode a normal cluster_id, then a node_id whose length prefix
            // claims 9999 bytes but the payload ends after 1 byte.
            int payloadLen = OFF_CLUSTER_ID_LEN + 2 + clusterId.length() + 2 + 1;
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                zero(buf, payloadLen);
                Unsafe.getUnsafe().putByte(buf + OFF_MSG_KIND, QwpEgressMsgKind.SERVER_INFO);
                int p = OFF_CLUSTER_ID_LEN;
                byte[] cBytes = clusterId.getBytes(StandardCharsets.UTF_8);
                Unsafe.getUnsafe().putShort(buf + p, (short) cBytes.length);
                p += 2;
                for (int i = 0; i < cBytes.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + p + i, cBytes[i]);
                }
                p += cBytes.length;
                Unsafe.getUnsafe().putShort(buf + p, (short) 9999);
                QwpServerInfoDecoder.decode(buf, payloadLen);
                Assert.fail("decoder must reject node_id length that overshoots remainder");
            } catch (QwpDecodeException expected) {
                String msg = expected.getMessage();
                Assert.assertTrue("error mentions node_id: " + msg, msg.contains("node_id"));
                Assert.assertTrue("error mentions overshoot length: " + msg, msg.contains("9999"));
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTruncatedBeforeNodeIdLengthRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String clusterId = "abc";
            // Drop the buffer so the node_id length prefix has only 1 of its 2 bytes.
            int payloadLen = OFF_CLUSTER_ID_LEN + 2 + clusterId.length() + 1;
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                zero(buf, payloadLen);
                Unsafe.getUnsafe().putByte(buf + OFF_MSG_KIND, QwpEgressMsgKind.SERVER_INFO);
                byte[] cBytes = clusterId.getBytes(StandardCharsets.UTF_8);
                Unsafe.getUnsafe().putShort(buf + OFF_CLUSTER_ID_LEN, (short) cBytes.length);
                for (int i = 0; i < cBytes.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + OFF_CLUSTER_ID_LEN + 2 + i, cBytes[i]);
                }
                QwpServerInfoDecoder.decode(buf, payloadLen);
                Assert.fail("decoder must reject payload that ends mid node_id length");
            } catch (QwpDecodeException expected) {
                Assert.assertTrue("error mentions node_id: " + expected.getMessage(),
                        expected.getMessage().contains("node_id"));
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testZoneIdRoundTripWhenCapZoneSet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String clusterId = "cluster-z";
            String nodeId = "node-z";
            String zoneId = "eu-west-1a";
            int len = totalLenWithZone(clusterId, nodeId, zoneId);
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrameWithZone(buf, QwpEgressMsgKind.ROLE_REPLICA, 0L,
                        QwpEgressMsgKind.CAP_ZONE, 0L, clusterId, nodeId, zoneId);
                QwpServerInfo info = QwpServerInfoDecoder.decode(buf, len);
                Assert.assertEquals(zoneId, info.getZoneId());
                Assert.assertEquals(clusterId, info.getClusterId());
                Assert.assertEquals(nodeId, info.getNodeId());
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testZoneIdEmptyStringWhenCapZoneSet() throws Exception {
        // CAP_ZONE bit + zero-length zone is a legitimate state: the server
        // advertises CAP_ZONE but the operator left zone unset on this node.
        // The trailer is still present (length 0), so the decoder returns "".
        TestUtils.assertMemoryLeak(() -> {
            int len = totalLenWithZone("c", "n", "");
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrameWithZone(buf, QwpEgressMsgKind.ROLE_PRIMARY, 0L,
                        QwpEgressMsgKind.CAP_ZONE, 0L, "c", "n", "");
                QwpServerInfo info = QwpServerInfoDecoder.decode(buf, len);
                Assert.assertEquals("", info.getZoneId());
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testZoneIdNullWhenCapZoneUnset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // capabilities=0 keeps CAP_ZONE unset; decoder must skip the
            // trailer and surface null on getZoneId().
            int len = totalLen("c", "n");
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrame(buf, QwpEgressMsgKind.ROLE_STANDALONE, 0L, 0, 0L, "c", "n");
                QwpServerInfo info = QwpServerInfoDecoder.decode(buf, len);
                Assert.assertNull(info.getZoneId());
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTruncatedBeforeZoneIdLengthRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // CAP_ZONE bit set but the buffer ends right after node_id, no
            // u16 zone-len present. Decoder must throw rather than read past
            // the end of the native buffer.
            int payloadLen = totalLen("c", "n");
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrame(buf, QwpEgressMsgKind.ROLE_PRIMARY, 0L,
                        QwpEgressMsgKind.CAP_ZONE, 0L, "c", "n");
                QwpServerInfoDecoder.decode(buf, payloadLen);
                Assert.fail("decoder must reject CAP_ZONE frame missing the zone trailer");
            } catch (QwpDecodeException expected) {
                Assert.assertTrue("error mentions zone_id: " + expected.getMessage(),
                        expected.getMessage().contains("zone_id"));
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testZoneIdLengthOvershootsPayloadRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // CAP_ZONE set, valid cluster_id and node_id, then a zone length
            // that overshoots the remainder by claiming 1234 bytes when only 2
            // are present.
            String clusterId = "c";
            String nodeId = "n";
            int payloadLen = totalLen(clusterId, nodeId) + 2 + 2;
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrame(buf, QwpEgressMsgKind.ROLE_PRIMARY, 0L,
                        QwpEgressMsgKind.CAP_ZONE, 0L, clusterId, nodeId);
                int zoneLenOff = totalLen(clusterId, nodeId);
                Unsafe.getUnsafe().putShort(buf + zoneLenOff, (short) 1234);
                QwpServerInfoDecoder.decode(buf, payloadLen);
                Assert.fail("decoder must reject zone_id length that overshoots remainder");
            } catch (QwpDecodeException expected) {
                String msg = expected.getMessage();
                Assert.assertTrue("error mentions zone_id: " + msg, msg.contains("zone_id"));
                Assert.assertTrue("error mentions overshoot length: " + msg, msg.contains("1234"));
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRoleByteIsCarriedThroughUnchanged() throws Exception {
        // Decoder is intentionally tolerant of unknown role bytes -- it surfaces
        // them via QwpServerInfo.roleName as UNKNOWN(n) rather than rejecting.
        // Verify that here so a future refactor doesn't accidentally tighten it.
        TestUtils.assertMemoryLeak(() -> {
            int len = totalLen("c", "n");
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                writeFrame(buf, (byte) 0x77, 0L, 0, 0L, "c", "n");
                QwpServerInfo info = QwpServerInfoDecoder.decode(buf, len);
                Assert.assertEquals((byte) 0x77, info.getRole());
                Assert.assertTrue(QwpServerInfo.roleName(info.getRole()).startsWith("UNKNOWN"));
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static int totalLen(String clusterId, String nodeId) {
        return OFF_CLUSTER_ID_LEN
                + 2 + clusterId.getBytes(StandardCharsets.UTF_8).length
                + 2 + nodeId.getBytes(StandardCharsets.UTF_8).length;
    }

    private static int totalLenWithZone(String clusterId, String nodeId, String zoneId) {
        return totalLen(clusterId, nodeId)
                + 2 + zoneId.getBytes(StandardCharsets.UTF_8).length;
    }

    private static void writeFrameWithZone(long buf, byte role, long epoch, int capabilities,
                                           long serverWallNs, String clusterId, String nodeId,
                                           String zoneId) {
        writeFrame(buf, role, epoch, capabilities, serverWallNs, clusterId, nodeId);
        long p = buf + totalLen(clusterId, nodeId);
        byte[] zBytes = zoneId.getBytes(StandardCharsets.UTF_8);
        Unsafe.getUnsafe().putShort(p, (short) zBytes.length);
        p += 2;
        for (byte zByte : zBytes) {
            Unsafe.getUnsafe().putByte(p++, zByte);
        }
    }

    private static void writeFrame(long buf, byte role, long epoch, int capabilities,
                                   long serverWallNs, String clusterId, String nodeId) {
        // Header bytes are not parsed by the SERVER_INFO decoder; zero them.
        for (int i = 0; i < QwpConstants.HEADER_SIZE; i++) {
            Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
        }
        Unsafe.getUnsafe().putByte(buf + OFF_MSG_KIND, QwpEgressMsgKind.SERVER_INFO);
        Unsafe.getUnsafe().putByte(buf + OFF_ROLE, role);
        Unsafe.getUnsafe().putLong(buf + OFF_EPOCH, epoch);
        Unsafe.getUnsafe().putInt(buf + OFF_CAPABILITIES, capabilities);
        Unsafe.getUnsafe().putLong(buf + OFF_SERVER_WALL_NS, serverWallNs);

        long p = buf + OFF_CLUSTER_ID_LEN;
        byte[] cBytes = clusterId.getBytes(StandardCharsets.UTF_8);
        Unsafe.getUnsafe().putShort(p, (short) cBytes.length);
        p += 2;
        for (byte cByte : cBytes) {
            Unsafe.getUnsafe().putByte(p++, cByte);
        }
        byte[] nBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        Unsafe.getUnsafe().putShort(p, (short) nBytes.length);
        p += 2;
        for (byte nByte : nBytes) {
            Unsafe.getUnsafe().putByte(p++, nByte);
        }
    }

    private static void zero(long buf, int len) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
        }
    }
}
