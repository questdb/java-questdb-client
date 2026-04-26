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

package io.questdb.client.test.cutlass.qwp.client.sf;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SfFromConfigTest {

    private static final int TEST_PORT = 19_900 + (int) (System.nanoTime() % 100);

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-config-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        rmDir(sfDir);
    }

    @Test
    public void testFromConfigEnablesSfAndOwnsLog() throws Exception {
        int port = TEST_PORT + 1;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 42L).atNow();
                sender.flush();
            }
            // SF dir is created by the cursor engine on demand.
            Assert.assertTrue("sfDir created", Files.exists(sfDir));
        }
    }

    @Test
    public void testSfDirOnTcpRejected() {
        // sf_dir is the SF on-switch; on a TCP connect string it has no
        // legal meaning and must be rejected at parse time.
        String config = "tcp::addr=localhost:9009;sf_dir=" + sfDir + ";";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject sf_dir on TCP");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("WebSocket"));
        }
    }

    @Test
    public void testSfMaxBytesParsing() throws Exception {
        int port = TEST_PORT + 2;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir + ";sf_max_bytes=131072;";
            try (Sender sender = Sender.fromConfig(config)) {
                // Write enough data that segments rotate at ~128 KiB boundary.
                for (int i = 0; i < 50; i++) {
                    sender.table("foo").longColumn("v", (long) i).atNow();
                }
                sender.flush();
            }
            // Just confirm SF dir was populated; rotation under load is
            // exercised in the cursor SegmentRing/SegmentManager tests.
            Assert.assertTrue("sfDir was used", Files.exists(sfDir));
        }
    }

    @Test
    public void testNoSfDirMeansNoSf() throws Exception {
        // Absence of sf_dir is the only way to disable SF — no separate
        // off switch. Verify a basic SF-less sender still works end-to-end
        // and creates no directory.
        int port = TEST_PORT + 3;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
            Assert.assertFalse("no sf dir created", Files.exists(sfDir));
        }
    }

    /**
     * Regression test for the connect-string {@code sf_max_bytes} /
     * {@code sf_max_total_bytes} parser accepting values larger than
     * {@code Integer.MAX_VALUE}. The pre-cursor parser used parseInt which
     * artificially capped the SF size from the connect string at ~2 GiB.
     */
    @Test
    public void testSfMaxTotalBytesAcceptsLargeValue() throws Exception {
        int port = TEST_PORT + 8;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // 4 GiB > Integer.MAX_VALUE; pre-fix this would throw "invalid sf_max_total_bytes".
            String config = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir
                    + ";sf_max_total_bytes=" + (4L * 1024 * 1024 * 1024) + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
        }
    }

    @Test
    public void testSfDurabilityAppendNotYetSupported() {
        // sf_durability=append/flush are accepted by the parser but rejected
        // at build() — cursor doesn't fsync yet. Once cursor learns it,
        // these become happy-path tests.
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";sf_durability=append;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject sf_durability=append");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("not yet supported"));
        }
    }

    @Test
    public void testSfDurabilityFlushNotYetSupported() {
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";sf_durability=flush;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject sf_durability=flush");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("not yet supported"));
        }
    }

    @Test
    public void testInvalidSfDurabilityValueRejected() {
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir
                + ";sf_durability=maybe;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("invalid sf_durability"));
        }
    }

    @Test
    public void testSfDurabilityOnTcpRejected() {
        String config = "tcp::addr=localhost:1;sf_durability=flush;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("WebSocket"));
        }
    }

    @Test
    public void testSfWithSyncWindowRejected() {
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir
                + ";in_flight_window=1;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection of SF with sync mode");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("async"));
        }
    }

    @Test
    public void testSfMaxBytesAcceptsSizeSuffixes() throws Exception {
        int port = TEST_PORT + 9;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // 64m / 4g should parse identically to their byte-count equivalents.
            String config = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir
                    + ";sf_max_bytes=64m"
                    + ";sf_max_total_bytes=4g;";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
            Assert.assertTrue(Files.exists(sfDir));
        }
    }

    @Test
    public void testSfMaxBytesInvalidSizeSuffixRejected() {
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";sf_max_bytes=64x;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection of unknown unit suffix");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("invalid sf_max_bytes"));
        }
    }

    private static void rmDir(String dir) {
        if (dir == null || !Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find != 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(dir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    /** Acks every binary frame so the sender doesn't hang. */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }

}
