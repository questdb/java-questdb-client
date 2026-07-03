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
import io.questdb.client.impl.ConfigSchema;
import io.questdb.client.impl.Side;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Proves every ingress (and ingress-applied COMMON) key read from a {@code ws}/
 * {@code wss} config string is actually applied to the WebSocket Sender -- not
 * merely accepted. {@link #testEveryIngressKeyIsHonored} ends with a drift guard,
 * driven by the keys the assertions themselves record, that fails if a registry
 * ingress key has no honored assertion.
 */
public class WsSenderConfigHonoredTest {

    private final Set<String> honored = new HashSet<>();

    @Test
    public void testEveryIngressKeyIsHonored() {
        assertHonored("auto_flush_rows=7", "auto_flush_rows", 7);
        assertHonored("auto_flush_bytes=8192", "auto_flush_bytes", 8192);
        assertHonored("auto_flush_interval=250", "auto_flush_interval", 250);
        // auto_flush=off reaches disableAutoFlush(), which sets the interval to
        // MAX_VALUE; auto_flush_rows=off leaves the interval unset, so this field
        // distinguishes the two. That config is build-rejected on WebSocket; see
        // QuestDBBuilderTest.testMalformedIngressConfigRejectedAtBuildWithMinZero.
        assertHonored("auto_flush=off", "auto_flush_interval", Integer.MAX_VALUE);
        assertHonored("max_name_len=99", "max_name_len", 99);
        assertHonored("transaction=on", "transaction", true);
        assertHonored("request_durable_ack=on", "request_durable_ack", true);
        assertHonored("sender_id=probe-1", "sender_id", "probe-1");
        assertHonored("sf_dir=/var/probe", "sf_dir", "/var/probe");
        assertHonored("sf_max_bytes=4096", "sf_max_bytes", 4096L);
        assertHonored("sf_max_total_bytes=8192", "sf_max_total_bytes", 8192L);
        assertHonored("sf_durability=flush", "sf_durability", "FLUSH");
        assertHonored("sf_append_deadline_millis=1500", "sf_append_deadline_millis", 1500L);
        assertHonored("close_flush_timeout_millis=2500", "close_flush_timeout_millis", 2500L);
        assertHonored("durable_ack_keepalive_interval_millis=900", "durable_ack_keepalive_interval_millis", 900L);
        assertHonored("initial_connect_retry=async", "initial_connect_retry", "ASYNC");
        assertHonored("reconnect_max_duration_millis=12345", "reconnect_max_duration_millis", 12345L);
        assertHonored("reconnect_initial_backoff_millis=111", "reconnect_initial_backoff_millis", 111L);
        assertHonored("reconnect_max_backoff_millis=2222", "reconnect_max_backoff_millis", 2222L);
        assertHonored("drain_orphans=on", "drain_orphans", true);
        assertHonored("max_background_drainers=6", "max_background_drainers", 6);
        assertHonored("max_frame_rejections=6", "max_frame_rejections", 6);
        assertHonored("poison_min_escalation_window_millis=7500", "poison_min_escalation_window_millis", 7500L);
        assertHonored("error_inbox_capacity=128", "error_inbox_capacity", 128);
        assertHonored("connection_listener_inbox_capacity=64", "connection_listener_inbox_capacity", 64);
        assertHonored("token=ey.abc", "token", "ey.abc");
        assertHonored("auth_timeout_ms=4321", "auth_timeout_ms", 4321L);
        assertHonored("connect_timeout=7000", "connect_timeout", 7000);

        // username/password together (both-or-neither), and the user/pass aliases.
        Map<String, Object> creds = snapshot("ws::addr=h:9000;username=alice;password=secret;");
        Assert.assertEquals("alice", creds.get("username"));
        Assert.assertEquals("secret", creds.get("password"));
        Map<String, Object> aliasCreds = snapshot("ws::addr=h:9000;user=bob;pass=pw;");
        Assert.assertEquals("bob", aliasCreds.get("username"));
        Assert.assertEquals("pw", aliasCreds.get("password"));
        markHonored("username", "password");

        // tls keys require wss; tls_roots must be paired with its password.
        assertHonoredWss("tls_verify=unsafe_off", "tls_verify", "INSECURE");
        Map<String, Object> tls = snapshot("wss::addr=h:9000;tls_roots=/ca.p12;tls_roots_password=pw;");
        Assert.assertEquals("/ca.p12", tls.get("tls_roots"));
        Assert.assertEquals("pw", tls.get("tls_roots_password"));
        markHonored("tls_roots", "tls_roots_password");

        // Drift guard: every ingress-applied registry key must have an assertion
        // above. The honored set is populated by the assertions themselves, so
        // deleting one trips this -- unlike a hand-maintained list, it cannot
        // silently drift from what is actually asserted.
        for (ConfigSchema.KeySpec spec : ConfigSchema.all()) {
            if (!spec.name().equals(spec.canonical())) {
                continue; // alias (user/pass) -- covered via its canonical key
            }
            // INGRESS keys plus the COMMON keys the sender applies; addr is the
            // endpoint list (the connection target), not an applied config value.
            boolean ingressApplied = spec.side() == Side.INGRESS
                    || (spec.side() == Side.COMMON && !spec.name().equals("addr"));
            if (ingressApplied) {
                Assert.assertTrue("registry ingress key '" + spec.name() + "' has no honored assertion",
                        honored.contains(spec.name()));
            }
        }
    }

    private void assertHonored(String kv, String snapKey, Object expected) {
        markHonored(keyOf(kv));
        Assert.assertEquals("config '" + kv + "' not honored at '" + snapKey + "'",
                expected, snapshot("ws::addr=h:9000;" + kv + ";").get(snapKey));
    }

    private void assertHonoredWss(String kv, String snapKey, Object expected) {
        markHonored(keyOf(kv));
        Assert.assertEquals("config '" + kv + "' not honored at '" + snapKey + "'",
                expected, snapshot("wss::addr=h:9000;" + kv + ";").get(snapKey));
    }

    private void markHonored(String... keys) {
        honored.addAll(Arrays.asList(keys));
    }

    private static String keyOf(String kv) {
        return kv.substring(0, kv.indexOf('='));
    }

    private static Map<String, Object> snapshot(String cfg) {
        return Sender.builder(cfg).wsConfigSnapshotForTest();
    }
}
