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

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for {@link QwpQueryClient} surface that does not require a
 * live socket: the pre-connect getters, the cancel/close idempotency
 * guarantees, the routing-target constants, and the bind-encoding error
 * surface. Live failover, role-mismatch, SERVER_INFO negotiation and the rest
 * of the connect()-time state machine are intentionally NOT covered here --
 * they belong to the parent QuestDB egress integration suite per the
 * package-level testing policy. See {@link QwpQueryClientPostConnectGuardTest}
 * for setter guard coverage.
 */
public class QwpQueryClientUnitTest {

    private static final QwpColumnBatchHandler NOOP_HANDLER = new QwpColumnBatchHandler() {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }
    };

    @Test
    public void testTargetConstants() {
        // The connection-string parser keys on these literal strings, and the
        // role-matching logic compares the configured target against them.
        // Pin the values so a typo refactor doesn't silently break the parse
        // and routing paths simultaneously.
        Assert.assertEquals("any", QwpQueryClient.TARGET_ANY);
        Assert.assertEquals("primary", QwpQueryClient.TARGET_PRIMARY);
        Assert.assertEquals("replica", QwpQueryClient.TARGET_REPLICA);
    }

    @Test
    public void testNewPlainTextDoesNotConnect() {
        // Constructor must not perform I/O. Pin this so a future refactor
        // doesn't accidentally make `new` block or throw on a bad host.
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            Assert.assertFalse(c.isConnected());
            Assert.assertNull("getServerInfo must be null before connect()", c.getServerInfo());
            Assert.assertEquals(0, c.getNegotiatedQwpVersion());
            Assert.assertFalse(c.wasLastCloseTimedOut());
        }
    }

    @Test
    public void testCompressionPreferenceDefaultsToRaw() {
        // Library default per the connection-string docs: no compression unless
        // the caller opts in via compression=zstd|auto.
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            Assert.assertEquals("raw", c.getCompressionPreference());
        }
    }

    @Test
    public void testCompressionPreferencePropagatedFromConfig() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=localhost:9000;compression=auto;")) {
            Assert.assertEquals("auto", c.getCompressionPreference());
        }
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=localhost:9000;compression=zstd;")) {
            Assert.assertEquals("zstd", c.getCompressionPreference());
        }
    }

    @Test
    public void testExecuteBeforeConnectThrowsIllegalState() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            try {
                c.execute("SELECT 1", NOOP_HANDLER);
                Assert.fail("execute() before connect() must throw");
            } catch (IllegalStateException expected) {
                Assert.assertTrue("error must mention connect(): " + expected.getMessage(),
                        expected.getMessage().contains("connect()"));
            }
        }
    }

    @Test
    public void testExecuteWithBindsBeforeConnectThrowsIllegalState() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            try {
                c.execute("SELECT 1", binds -> binds.setLong(0, 42L), NOOP_HANDLER);
                Assert.fail("execute(binds) before connect() must throw");
            } catch (IllegalStateException expected) {
                // expected
            }
        }
    }

    @Test
    public void testCancelOnFreshClientIsNoOp() {
        // cancel() must be safe to call before connect (e.g., a watchdog timer
        // that fires before the connect handshake completed). Documented on
        // QwpQueryClient.cancel as "No-op if no query is in flight".
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            c.cancel();
            c.cancel(); // idempotent
        }
    }

    @Test
    public void testCloseOnNeverConnectedClientIsNoOp() {
        QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000);
        c.close();
        Assert.assertFalse(c.wasLastCloseTimedOut());
        // Second close must also not throw.
        c.close();
    }

    @Test
    public void testWasLastCloseTimedOutDefaultFalse() {
        // No connection attempt -> no I/O thread to time out joining; the flag
        // must read false. (The flag is set true only by close() when the I/O
        // thread fails to exit within shutdownJoinMs.)
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            Assert.assertFalse(c.wasLastCloseTimedOut());
        }
    }

    @Test
    public void testFromConfigReturnsUsableClient() {
        // Sanity round-trip: a minimal valid config string parses, applies, and
        // leaves the client ready (but not connected). Detailed parse-branch
        // coverage lives in QwpQueryClientFromConfigTest; this is the smoke test
        // that ties fromConfig to the rest of the unit-level guarantees here.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=localhost:9000;")) {
            Assert.assertFalse(c.isConnected());
            Assert.assertNull(c.getServerInfo());
        }
    }

    @Test
    public void testGetServerInfoNullableBeforeConnect() {
        // The accessor is documented as returning null before connect(); a
        // QwpServerInfo instance materialises only after the v2 SERVER_INFO
        // frame is decoded during connect().
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            QwpServerInfo info = c.getServerInfo();
            Assert.assertNull(info);
        }
    }

    @Test
    public void testQwpMaxVersionConstant() {
        // The negotiated-version cap is the v2 protocol; pin it so a future
        // version bump that adds a new control frame must explicitly update
        // both the constant and any callers asserting the cap.
        Assert.assertEquals(2, QwpQueryClient.QWP_MAX_VERSION);
    }

    @Test
    public void testMaxBatchRowsUpperBoundMirrorsDecoderCap() {
        // The setter clamps to this; the cap matches QwpResultBatchDecoder's
        // own MAX_ROWS_PER_BATCH so a user can't ask for a per-batch row count
        // the decoder would itself refuse.
        Assert.assertEquals(1_048_576, QwpQueryClient.MAX_BATCH_ROWS_UPPER_BOUND);
    }
}
