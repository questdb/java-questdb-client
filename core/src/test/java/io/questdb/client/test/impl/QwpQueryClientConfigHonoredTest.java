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

import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.ConfigSchema;
import io.questdb.client.impl.Side;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Proves every egress key read from a {@code ws}/{@code wss} config string is
 * actually applied to the {@code QwpQueryClient} -- not merely accepted. The
 * COMMON credential keys are verified via the synthesized Authorization header,
 * the COMMON TLS keys via the trust-store snapshot. {@link #testEveryEgressKeyIsHonored}
 * ends with a drift guard, driven by the keys the assertions record, that fails
 * if a registry egress key has no honored assertion.
 */
public class QwpQueryClientConfigHonoredTest {

    private final Set<String> honored = new HashSet<>();

    @Test
    public void testEveryEgressKeyIsHonored() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertHonored("target=primary", "target", "primary");
            assertHonored("failover=off", "failover", false);
            assertHonored("failover_max_attempts=9", "failover_max_attempts", 9);
            assertHonored("failover_backoff_initial_ms=120", "failover_backoff_initial_ms", 120L);
            assertHonored("failover_backoff_max_ms=99999", "failover_backoff_max_ms", 99999L);
            assertHonored("failover_max_duration_ms=56000", "failover_max_duration_ms", 56000L);
            assertHonored("max_batch_rows=512", "max_batch_rows", 512);
            assertHonored("initial_credit=65536", "initial_credit", 65536L);
            assertHonored("buffer_pool_size=3", "buffer_pool_size", 3);
            assertHonored("compression=zstd", "compression", "zstd");
            assertHonored("compression_level=9", "compression_level", 9);
            assertHonored("client_id=probe/1.0", "client_id", "probe/1.0");
            assertHonored("zone=us-east", "zone", "us-east");
            // COMMON applied by egress.
            assertHonored("auth_timeout_ms=7777", "auth_timeout_ms", 7777L);
            assertHonored("connect_timeout=6000", "connect_timeout", 6000);

            // Credentials become the Authorization header, including the user/pass aliases.
            String basic = "Basic " + Base64.getEncoder()
                    .encodeToString("alice:secret".getBytes(StandardCharsets.UTF_8));
            Assert.assertEquals(basic, snapshot("ws::addr=h:9000;username=alice;password=secret;").get("authorization_header"));
            Assert.assertEquals(basic, snapshot("ws::addr=h:9000;user=alice;pass=secret;").get("authorization_header"));
            Assert.assertEquals("Bearer ey.abc", snapshot("ws::addr=h:9000;token=ey.abc;").get("authorization_header"));
            markHonored("username", "password", "token");

            // COMMON TLS keys applied by egress (require the wss schema). tls_verify
            // drives the validation mode; tls_roots/tls_roots_password set the trust
            // store. All three read back from the snapshot.
            Assert.assertEquals(ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE,
                    snapshot("wss::addr=h:9000;tls_verify=unsafe_off;").get("tls_verify"));
            Map<String, Object> tls = snapshot("wss::addr=h:9000;tls_roots=/ca.p12;tls_roots_password=pw;");
            Assert.assertEquals("/ca.p12", tls.get("tls_roots"));
            Assert.assertEquals("pw", tls.get("tls_roots_password"));
            markHonored("tls_verify", "tls_roots", "tls_roots_password");

            // Drift guard: every egress-applied registry key must have an assertion
            // above. The honored set is populated by the assertions themselves, so
            // deleting one trips this -- unlike a hand-maintained list, it cannot
            // silently drift from what is actually asserted.
            for (ConfigSchema.KeySpec spec : ConfigSchema.all()) {
                if (!spec.name().equals(spec.canonical())) {
                    continue; // alias (user/pass) -- covered via its canonical key
                }
                // The egress client applies its own EGRESS keys plus the COMMON keys
                // (credentials, TLS, auth_timeout_ms). addr is the endpoint list (the
                // connection target), not a snapshot value, so it is excluded.
                boolean egressApplied = spec.side() == Side.EGRESS
                        || (spec.side() == Side.COMMON && !spec.name().equals("addr"));
                if (egressApplied) {
                    Assert.assertTrue("registry egress key '" + spec.name() + "' has no honored assertion",
                            honored.contains(spec.name()));
                }
            }
        });
    }

    private void assertHonored(String kv, String snapKey, Object expected) {
        markHonored(keyOf(kv));
        Assert.assertEquals("config '" + kv + "' not honored at '" + snapKey + "'",
                expected, snapshot("ws::addr=h:9000;" + kv + ";").get(snapKey));
    }

    private void markHonored(String... keys) {
        honored.addAll(Arrays.asList(keys));
    }

    private static String keyOf(String kv) {
        return kv.substring(0, kv.indexOf('='));
    }

    private static Map<String, Object> snapshot(String cfg) {
        try (QwpQueryClient c = QwpQueryClient.fromConfig(cfg)) {
            return c.configSnapshotForTest();
        }
    }
}
