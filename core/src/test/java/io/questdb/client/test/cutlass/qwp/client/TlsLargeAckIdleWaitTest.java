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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Over TLS a server frame larger than the client's receive buffer is partly
 * decrypted into user-space buffers that {@code poll(2)} cannot see. The I/O
 * loop must not block on the fd while such input is pending, or the ACK is
 * never processed and {@code awaitAckedFsn}/{@code close()} time out. Plain
 * ws is the control.
 * <p>
 * The TLS record size picks which user-space buffer holds the input:
 * <ul>
 * <li>One write: ~16 KiB records. Each fill leaves the receive buffer less
 * than 1 KiB free, so it grows before the next read, and the record that did
 * not fit stays undecrypted in the TLS input buffer.</li>
 * <li>10,000-byte writes: six records fill 60,000 bytes of the 64 KiB buffer,
 * and the remaining 5,536 bytes cannot take the next record. It is decrypted
 * into the internal spill buffer and only partly handed over; the rest stays
 * there, decrypted, with nothing left on the fd.</li>
 * </ul>
 */
public class TlsLargeAckIdleWaitTest {

    @Test(timeout = 60_000L)
    public void testLargeAckOverTlsIsProcessedWhileIdle() throws Exception {
        run(true, 150 * 1024, 0);
    }

    @Test(timeout = 60_000L)
    public void testLargeAckInSmallTlsRecordsIsProcessedWhileIdle() throws Exception {
        run(true, 150 * 1024, 10_000);
    }

    @Test(timeout = 60_000L)
    public void testLargeAckOverPlainWsIsProcessedWhileIdle() throws Exception {
        run(false, 150 * 1024, 0);
    }

    /**
     * @param chunkBytes server socket write size, or 0 to write the frame at once
     */
    private static void run(boolean tls, int ackBytes, int chunkBytes) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BigAckHandler handler = new BigAckHandler(ackBytes, chunkBytes);
            try (TestWebSocketServer server = tls ? TestWebSocketServer.tls(handler) : new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = (tls ? "wss::addr=localhost:" + server.getPort() + ";tls_verify=unsafe_off;"
                        : "ws::addr=localhost:" + server.getPort() + ";")
                        + "auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;"
                        // On failure close() would otherwise drain past the test timeout
                        // and mask the assertion below.
                        + "close_flush_timeout_millis=1000;";
                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg)) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    long fsn = sender.flushAndGetSequence();
                    long t0 = System.nanoTime();
                    Assert.assertTrue("ACK of " + ackBytes + " bytes was not processed (tls=" + tls + ")",
                            sender.awaitAckedFsn(fsn, 5_000));
                    long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    Assert.assertTrue("ACK took " + millis + " ms", millis < 2_000);
                }
            }
        });
    }

    /**
     * ACKs every frame with a STATUS_OK carrying enough table entries to reach
     * the requested payload size.
     */
    private static final class BigAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final int ackBytes;
        private final int chunkBytes;
        private final AtomicLong seq = new AtomicLong();

        BigAckHandler(int ackBytes, int chunkBytes) {
            this.ackBytes = ackBytes;
            this.chunkBytes = chunkBytes;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                byte[] ack = buildAck(seq.getAndIncrement(), ackBytes);
                if (chunkBytes > 0) {
                    client.sendBinaryInChunks(ack, chunkBytes);
                } else {
                    client.sendBinary(ack);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // STATUS_OK: status u8 | sequence u64 | tableCount u16 | [nameLen u16 + name + seqTxn u64] * tableCount
        private static byte[] buildAck(long sequence, int targetBytes) {
            byte[] name = "table_name_padding_0000".getBytes(StandardCharsets.UTF_8);
            int entry = 2 + name.length + 8;
            int count = Math.max(1, (targetBytes - 11) / entry);
            ByteBuffer bb = ByteBuffer.allocate(11 + count * entry).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(sequence);
            bb.putShort((short) count);
            for (int i = 0; i < count; i++) {
                bb.putShort((short) name.length);
                bb.put(name);
                bb.putLong(i);
            }
            return bb.array();
        }
    }
}
