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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TLS stream that ends inside a record (peer killed mid-write, proxy or load
 * balancer dropping the connection) must read as a disconnect: the sender
 * reconnects and replays. Before the fix the truncated record read as "no data
 * yet" forever, so the batch was never acked and the idle I/O loop spun on the
 * readable-at-EOF socket.
 */
public class TlsTruncatedRecordReconnectTest {
    private static final int TLS_APPLICATION_DATA = 23;
    private static final int TLS_RECORD_HEADER = 5;

    @Test(timeout = 60_000L)
    public void testStreamEndingInsideTlsRecordReconnectsAndReplays() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = TestWebSocketServer.tls(new PerConnectionAckHandler())) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (TruncatingProxy proxy = new TruncatingProxy(server.getPort())) {
                    String cfg = "wss::addr=localhost:" + proxy.getPort() + ";tls_verify=unsafe_off;"
                            + "auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;"
                            + "close_flush_timeout_millis=0;";
                    try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg)) {
                        sender.table("t").longColumn("v", 1L).atNow();
                        Assert.assertTrue("first batch not acked", sender.awaitAckedFsn(sender.flushAndGetSequence(), 10_000));

                        // The ACK of the second batch is cut mid-record and followed by FIN.
                        proxy.truncateNextServerRecord.set(true);
                        sender.table("t").longColumn("v", 2L).atNow();
                        long fsn = sender.flushAndGetSequence();
                        Assert.assertTrue("batch whose ACK was truncated was not replayed and acked after reconnect",
                                sender.awaitAckedFsn(fsn, 10_000));
                        Assert.assertTrue("the record was never truncated", proxy.truncated.get());
                        Assert.assertTrue("sender did not reconnect", proxy.accepts.get() >= 2);
                    }
                }
            }
        });
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }

    private static byte[] okFrame(long wireSeq) {
        ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 2).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00); // STATUS_OK
        bb.putLong(wireSeq);
        bb.putShort((short) 0);
        return bb.array();
    }

    // Acks every binary message with the per-connection wire sequence: a reconnect restarts it at 0.
    private static final class PerConnectionAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler client;
        private long nextSeq;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (this.client != client) {
                this.client = client;
                nextSeq = 0;
            }
            try {
                client.sendBinary(okFrame(nextSeq++));
            } catch (IOException ignored) {
                // the connection died under us; the sender reconnects
            }
        }
    }

    // Relays TCP to the TLS server. Once armed, forwards only the first half of the next
    // server application-data record and then half-closes towards the client.
    private static final class TruncatingProxy implements AutoCloseable {
        final AtomicInteger accepts = new AtomicInteger();
        final AtomicBoolean truncateNextServerRecord = new AtomicBoolean();
        final AtomicBoolean truncated = new AtomicBoolean();
        private final ServerSocket listener;
        private final int upstreamPort;

        TruncatingProxy(int upstreamPort) throws IOException {
            this.upstreamPort = upstreamPort;
            this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread acceptor = new Thread(this::acceptLoop, "truncating-proxy-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        @Override
        public void close() throws IOException {
            listener.close();
        }

        int getPort() {
            return listener.getLocalPort();
        }

        private void acceptLoop() {
            try {
                while (true) {
                    Socket client = listener.accept();
                    accepts.incrementAndGet();
                    Socket upstream = new Socket(InetAddress.getLoopbackAddress(), upstreamPort);
                    start(() -> pump(client, upstream), "truncating-proxy-up");
                    start(() -> pumpServerRecords(upstream, client), "truncating-proxy-down");
                }
            } catch (IOException ignored) {
                // listener closed
            }
        }

        private void pump(Socket from, Socket to) {
            try {
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                byte[] buf = new byte[16_384];
                int n;
                while ((n = in.read(buf)) > 0) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
            } finally {
                closeQuietly(from);
                closeQuietly(to);
            }
        }

        private void pumpServerRecords(Socket server, Socket client) {
            try {
                DataInputStream in = new DataInputStream(server.getInputStream());
                OutputStream out = client.getOutputStream();
                byte[] header = new byte[TLS_RECORD_HEADER];
                while (true) {
                    in.readFully(header);
                    byte[] body = new byte[((header[3] & 0xff) << 8) | (header[4] & 0xff)];
                    in.readFully(body);
                    out.write(header);
                    if (header[0] == TLS_APPLICATION_DATA && truncateNextServerRecord.compareAndSet(true, false)) {
                        out.write(body, 0, body.length / 2);
                        out.flush();
                        truncated.set(true);
                        client.shutdownOutput();
                        return; // the client must notice the dead stream and close its socket
                    }
                    out.write(body);
                    out.flush();
                }
            } catch (IOException ignored) {
                closeQuietly(server);
                closeQuietly(client);
            }
        }

        private void start(Runnable r, String name) {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            t.start();
        }
    }
}
