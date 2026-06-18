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

import io.questdb.client.impl.ConfigStringTranslator;
import org.junit.Assert;
import org.junit.Test;

public class ConfigStringTranslatorTest {

    @Test
    public void testEmptyConfigIsRejected() {
        try {
            ConfigStringTranslator.deriveBothSides("");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("empty"));
        }
    }

    @Test
    public void testHttpInputPassesThroughAndDerivesWs() {
        ConfigStringTranslator.Bundle bundle = ConfigStringTranslator.deriveBothSides(
                "http::addr=db.host:9000;token=secret;");
        Assert.assertEquals("http::addr=db.host:9000;token=secret;", bundle.ingestConfig);
        Assert.assertEquals("ws::addr=db.host:9000;token=secret;", bundle.queryConfig);
        // No pool keys -> all defaults preserved.
        Assert.assertEquals(ConfigStringTranslator.PoolConfig.UNSET, bundle.poolConfig.senderPoolMin);
        Assert.assertEquals(ConfigStringTranslator.PoolConfig.UNSET, bundle.poolConfig.acquireTimeoutMillis);
    }

    @Test
    public void testHttpsInputDerivesWss() {
        ConfigStringTranslator.Bundle bundle = ConfigStringTranslator.deriveBothSides(
                "https::addr=db.host:9000;tls_verify=on;");
        Assert.assertEquals("https::addr=db.host:9000;tls_verify=on;", bundle.ingestConfig);
        Assert.assertEquals("wss::addr=db.host:9000;tls_verify=on;", bundle.queryConfig);
    }

    @Test
    public void testInvalidPoolValueIsRejected() {
        try {
            ConfigStringTranslator.deriveBothSides("http::addr=h:9000;sender_pool_max=notanumber;");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("sender_pool_max"));
        }
    }

    @Test
    public void testMissingAddrIsRejected() {
        try {
            ConfigStringTranslator.deriveBothSides("http::token=x;");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("addr"));
        }
    }

    @Test
    public void testPoolKeysAreExtractedAndStripped() {
        ConfigStringTranslator.Bundle bundle = ConfigStringTranslator.deriveBothSides(
                "http::addr=db.host:9000;sender_pool_min=2;sender_pool_max=16;"
                        + "query_pool_min=1;query_pool_max=4;acquire_timeout_ms=10000;"
                        + "idle_timeout_ms=30000;max_lifetime_ms=600000;housekeeper_interval_ms=2000;");

        // Pool keys must be stripped from both config strings so the downstream
        // Sender / QwpQueryClient parsers never see them.
        Assert.assertFalse(bundle.ingestConfig.contains("sender_pool"));
        Assert.assertFalse(bundle.ingestConfig.contains("query_pool"));
        Assert.assertFalse(bundle.ingestConfig.contains("timeout_ms"));
        Assert.assertFalse(bundle.queryConfig.contains("sender_pool"));
        Assert.assertFalse(bundle.queryConfig.contains("query_pool"));
        Assert.assertFalse(bundle.queryConfig.contains("timeout_ms"));

        // addr must survive on both sides.
        Assert.assertTrue(bundle.ingestConfig.contains("addr=db.host:9000"));
        Assert.assertTrue(bundle.queryConfig.contains("addr=db.host:9000"));

        // Pool values must surface on the PoolConfig.
        Assert.assertEquals(2, bundle.poolConfig.senderPoolMin);
        Assert.assertEquals(16, bundle.poolConfig.senderPoolMax);
        Assert.assertEquals(1, bundle.poolConfig.queryPoolMin);
        Assert.assertEquals(4, bundle.poolConfig.queryPoolMax);
        Assert.assertEquals(10_000L, bundle.poolConfig.acquireTimeoutMillis);
        Assert.assertEquals(30_000L, bundle.poolConfig.idleTimeoutMillis);
        Assert.assertEquals(600_000L, bundle.poolConfig.maxLifetimeMillis);
        Assert.assertEquals(2_000L, bundle.poolConfig.housekeeperIntervalMillis);
    }

    @Test
    public void testPoolKeysInterleavedWithRegularKeys() {
        // Pool keys at arbitrary positions must still be stripped and the
        // surviving keys must remain in the original order.
        ConfigStringTranslator.Bundle bundle = ConfigStringTranslator.deriveBothSides(
                "http::sender_pool_max=8;addr=h:9000;query_pool_max=2;token=t;idle_timeout_ms=5000;");
        Assert.assertTrue(bundle.ingestConfig.contains("addr=h:9000"));
        Assert.assertTrue(bundle.ingestConfig.contains("token=t"));
        Assert.assertFalse(bundle.ingestConfig.contains("pool"));
        Assert.assertFalse(bundle.ingestConfig.contains("idle"));
        Assert.assertEquals(8, bundle.poolConfig.senderPoolMax);
        Assert.assertEquals(2, bundle.poolConfig.queryPoolMax);
        Assert.assertEquals(5_000L, bundle.poolConfig.idleTimeoutMillis);
    }

    @Test
    public void testTcpSchemaIsRejected() {
        try {
            ConfigStringTranslator.deriveBothSides("tcp::addr=h:9009;");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("supports schemas"));
        }
    }

    @Test
    public void testUsernamePasswordMirroredToWsDerivation() {
        // Structured Basic-auth credentials carry over to the derived ws side;
        // QwpQueryClient synthesizes the Authorization header from them.
        ConfigStringTranslator.Bundle bundle = ConfigStringTranslator.deriveBothSides(
                "http::addr=h:9000;username=u;password=p;");
        Assert.assertEquals("http::addr=h:9000;username=u;password=p;", bundle.ingestConfig);
        Assert.assertEquals("ws::addr=h:9000;username=u;password=p;", bundle.queryConfig);
    }

    @Test
    public void testWsInputPassesThroughAndDerivesHttp() {
        ConfigStringTranslator.Bundle bundle = ConfigStringTranslator.deriveBothSides(
                "ws::addr=db.host:9000;token=foo;");
        Assert.assertEquals("ws::addr=db.host:9000;token=foo;", bundle.queryConfig);
        Assert.assertTrue(
                "expected ingest config to start with http::; got: " + bundle.ingestConfig,
                bundle.ingestConfig.startsWith("http::"));
        Assert.assertTrue(bundle.ingestConfig.contains("addr=db.host:9000"));
        Assert.assertTrue(bundle.ingestConfig.contains("token=foo"));
    }
}
