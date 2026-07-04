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

package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.ConfigSchema;
import io.questdb.client.impl.ConfigView;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Guard test for the single QWP key registry ({@link ConfigSchema}): every key
 * it lists is recognized by both ws clients; the legacy http/tcp/udp keys are
 * absent and reject with the relocated-key hint; {@code token_x}/{@code token_y}
 * reject as plain unknowns; and the hint table is exactly the legacy keys.
 */
public class QwpConfigKeysTest {

    @Test
    public void testEverySchemaKeyIsRecognizedByBothClients() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (ConfigSchema.KeySpec spec : ConfigSchema.all()) {
                String cfg = "ws::addr=h:9000;" + spec.name() + "=" + sampleValue(spec) + ";";
                // A key may still fail a cross-key or range check; it must NOT fail
                // as an unknown key -- that would mean it is missing from the
                // registry (or that a consumer rejects a key it should ignore).
                assertNotUnknown(spec.name(), () -> Sender.builder(cfg));
                assertNotUnknown(spec.name(), () -> QwpQueryClient.fromConfig(cfg).close());
            }
        });
    }

    @Test
    public void testConnectTimeoutOverIntRejectedOnBoth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // connect_timeout flows into an int API on both clients. A bare
            // (int) cast would silently wrap 2^32+1 to a 1 ms timeout and turn
            // 2^31 into a misleading "must be > 0: -2147483648" error. Both
            // must reject up front, naming the actual limit.
            assertRejected("ws::addr=h:9000;connect_timeout=4294967297;",
                    "connect_timeout must be <= 2147483647: 4294967297");
            assertRejected("ws::addr=h:9000;connect_timeout=2147483648;",
                    "connect_timeout must be <= 2147483647: 2147483648");
            // The schema lower bound is intact end-to-end...
            assertRejected("ws::addr=h:9000;connect_timeout=0;",
                    "connect_timeout must be > 0");
            // ...and a valid value still builds on both clients.
            Sender.builder("ws::addr=h:9000;connect_timeout=7000;");
            QwpQueryClient.fromConfig("ws::addr=h:9000;connect_timeout=7000;").close();
        });
    }

    @Test
    public void testJunkKeyRejectedOnBoth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertRejected("ws::addr=h:9000;not_a_real_key=foo;",
                    "unknown configuration key: not_a_real_key");
        });
    }

    @Test
    public void testLegacyKeysRejectedWithHintOnBoth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String legacyHint = "(applies to legacy http/tcp/udp transports only)";
            assertRejected("ws::addr=h:9000;init_buf_size=1024;",
                    "unknown configuration key: init_buf_size", legacyHint);
            assertRejected("ws::addr=h:9000;max_buf_size=1024;",
                    "unknown configuration key: max_buf_size", legacyHint);
            assertRejected("ws::addr=h:9000;request_timeout=1000;",
                    "unknown configuration key: request_timeout", legacyHint);
            assertRejected("ws::addr=h:9000;request_min_throughput=1000;",
                    "unknown configuration key: request_min_throughput", legacyHint);
            assertRejected("ws::addr=h:9000;max_datagram_size=1400;",
                    "unknown configuration key: max_datagram_size", legacyHint);
            assertRejected("ws::addr=h:9000;multicast_ttl=4;",
                    "unknown configuration key: multicast_ttl", legacyHint);
            assertRejected("ws::addr=h:9000;retry_timeout=1000;",
                    "unknown configuration key: retry_timeout", "(use reconnect_max_duration_millis on ws/wss)");
            assertRejected("ws::addr=h:9000;protocol_version=2;",
                    "unknown configuration key: protocol_version", "(QWP negotiates the protocol version during the WebSocket upgrade)");
        });
    }

    @Test
    public void testRelocatedHintTableIsExactlyTheLegacyKeys() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String legacyHint = "(applies to legacy http/tcp/udp transports only)";
            Assert.assertEquals(legacyHint, ConfigView.relocatedHint("init_buf_size"));
            Assert.assertEquals(legacyHint, ConfigView.relocatedHint("max_buf_size"));
            Assert.assertEquals(legacyHint, ConfigView.relocatedHint("request_timeout"));
            Assert.assertEquals(legacyHint, ConfigView.relocatedHint("request_min_throughput"));
            Assert.assertEquals(legacyHint, ConfigView.relocatedHint("max_datagram_size"));
            Assert.assertEquals(legacyHint, ConfigView.relocatedHint("multicast_ttl"));
            Assert.assertEquals("(use reconnect_max_duration_millis on ws/wss)", ConfigView.relocatedHint("retry_timeout"));
            Assert.assertEquals("(QWP negotiates the protocol version during the WebSocket upgrade)", ConfigView.relocatedHint("protocol_version"));

            // No registry key (including POOL keys) carries a relocated hint.
            for (ConfigSchema.KeySpec spec : ConfigSchema.all()) {
                Assert.assertNull("registry key '" + spec.name() + "' must not be in the hint table",
                        ConfigView.relocatedHint(spec.name()));
            }
            // ECDSA keys are plain unknowns (only the C client handles them).
            Assert.assertNull(ConfigView.relocatedHint("token_x"));
            Assert.assertNull(ConfigView.relocatedHint("token_y"));
            Assert.assertNull(ConfigView.relocatedHint("not_a_real_key"));
        });
    }

    @Test
    public void testTokenXYRejectedWithoutHintOnBoth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertRejectedNoHint("ws::addr=h:9000;token_x=abc;", "token_x");
            assertRejectedNoHint("ws::addr=h:9000;token_y=def;", "token_y");
        });
    }

    private static void assertNotUnknown(String key, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("unknown configuration key")) {
                Assert.fail("key '" + key + "' rejected as unknown: " + msg);
            }
        }
    }

    private static void assertRejected(String cfg, String... expectedSubstrings) {
        assertRejected(() -> Sender.builder(cfg), expectedSubstrings);
        assertRejected(() -> QwpQueryClient.fromConfig(cfg).close(), expectedSubstrings);
    }

    private static void assertRejected(Runnable action, String... expectedSubstrings) {
        try {
            action.run();
            Assert.fail("expected rejection");
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            Assert.assertNotNull(msg);
            for (int i = 0; i < expectedSubstrings.length; i++) {
                Assert.assertTrue("'" + msg + "' should contain '" + expectedSubstrings[i] + "'",
                        msg.contains(expectedSubstrings[i]));
            }
        }
    }

    private static void assertRejectedNoHint(String cfg, String key) {
        assertRejectedNoHint(() -> Sender.builder(cfg), key);
        assertRejectedNoHint(() -> QwpQueryClient.fromConfig(cfg).close(), key);
    }

    private static void assertRejectedNoHint(Runnable action, String key) {
        try {
            action.run();
            Assert.fail("expected rejection of " + key);
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            Assert.assertNotNull(msg);
            Assert.assertTrue("'" + msg + "' should reject " + key,
                    msg.contains("unknown configuration key: " + key));
            Assert.assertFalse("'" + msg + "' should carry no hint", msg.contains("("));
        }
    }

    private static String sampleValue(ConfigSchema.KeySpec spec) {
        // The reject pass keys off the name, not the value, so any value proves
        // recognition; a valid enum member keeps the sample honest for enum keys.
        return spec.enumValues() != null ? spec.enumValues().getQuick(0) : "1";
    }
}
