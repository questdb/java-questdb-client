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

import io.questdb.client.impl.ConfigSchema;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import io.questdb.client.impl.Side;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ConfigViewTest {

    @Test
    public void testAddrBareIpv6GetsDefaultPort() {
        Assert.assertEquals(list("fe80::1:9000"), hostPorts("ws::addr=fe80::1;"));
    }

    @Test
    public void testAddrBracketedIpv6() {
        Assert.assertEquals(list("::1:9000"), hostPorts("ws::addr=[::1];"));
        Assert.assertEquals(list("::1:9001"), hostPorts("ws::addr=[::1]:9001;"));
    }

    @Test
    public void testAddrCommaListAndDefaultPort() {
        Assert.assertEquals(list("a:9000", "b:9001"), hostPorts("ws::addr=a,b:9001;"));
    }

    @Test
    public void testAddrDuplicateRejected() {
        assertParseError("ws::addr=a,a;", "duplicate addr entry");
        assertParseError("ws::addr=a:9000,a;", "duplicate addr entry");
    }

    @Test
    public void testAddrEmptyEntryRejected() {
        assertParseError("ws::addr=a,,b;", "empty addr entry");
    }

    @Test
    public void testAddrPortOutOfRangeRejected() {
        assertParseError("ws::addr=h:0;", "port out of range in addr");
        assertParseError("ws::addr=h:70000;", "port out of range in addr");
    }

    @Test
    public void testAddrRepeatedKeysAccumulate() {
        Assert.assertEquals(list("a:1", "b:2"), hostPorts("ws::addr=a:1;addr=b:2;"));
    }

    @Test
    public void testAliasNormalization() {
        ConfigView v = view("ws::addr=h:9000;user=alice;pass=secret;", Side.EGRESS);
        Assert.assertEquals("alice", v.getStr("username"));
        Assert.assertEquals("secret", v.getStr("password"));
    }

    @Test
    public void testEnumMessage() {
        assertParseError("ws::addr=h:9000;compression=gzip;", Side.EGRESS,
                v -> v.getEnum("compression"),
                "invalid compression: gzip (expected zstd, raw, auto)");
    }

    @Test
    public void testGetIntRangeBounded() {
        assertParseError("ws::addr=h:9000;compression_level=99;", Side.EGRESS,
                v -> v.getInt("compression_level", -1),
                "compression_level must be in [1, 22]");
    }

    @Test
    public void testGetIntRangeOneSided() {
        assertParseError("ws::addr=h:9000;buffer_pool_size=0;", Side.EGRESS,
                v -> v.getInt("buffer_pool_size", -1),
                "buffer_pool_size must be >= 1");
    }

    @Test
    public void testGetLongStrictLowerBound() {
        assertParseError("ws::addr=h:9000;auth_timeout_ms=0;", Side.EGRESS,
                v -> v.getLong("auth_timeout_ms", -1),
                "auth_timeout_ms must be > 0");
    }

    @Test
    public void testLastWriteWins() {
        ConfigView v = view("ws::addr=h:9000;client_id=a;client_id=b;", Side.EGRESS);
        Assert.assertEquals("b", v.getStr("client_id"));
    }

    @Test
    public void testRepeatedTlsVerifyResolvesLastWriteWins() {
        ConfigView v = view("wss::addr=h:9000;tls_verify=on;tls_verify=unsafe_off;", Side.EGRESS);
        Assert.assertEquals("unsafe_off", v.getEnum("tls_verify"));
    }

    @Test
    public void testTokenizerSemicolonEscaping() {
        // ConfStringParser escapes ';' as ';;' inside a value.
        ConfigView v = view("ws::addr=h:9000;client_id=a;;b;", Side.EGRESS);
        Assert.assertEquals("a;b", v.getStr("client_id"));
    }

    @Test
    public void testUnknownKeyRejectedWithHint() {
        assertParseError("ws::addr=h:9000;init_buf_size=1024;", Side.INGRESS, v -> {
        }, "unknown configuration key: init_buf_size (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testUnknownKeyRejectedWithoutHint() {
        assertParseError("ws::addr=h:9000;bogus=1;", Side.INGRESS, v -> {
        }, "unknown configuration key: bogus");
    }

    private static void assertParseError(String cfg, String expected) {
        // addr-value errors (duplicate, empty, port range) surface in
        // getHostPorts, not in the constructor's reject pass.
        assertParseError(cfg, Side.INGRESS, v -> v.getHostPorts("addr", 9000, (h, p) -> {
        }), expected);
    }

    private static void assertParseError(String cfg, Side side, java.util.function.Consumer<ConfigView> use, String expected) {
        try {
            ConfigView v = view(cfg, side);
            use.accept(v);
            Assert.fail("expected error containing: " + expected);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("'" + e.getMessage() + "' should contain '" + expected + "'",
                    e.getMessage().contains(expected));
        }
    }

    private static List<String> hostPorts(String cfg) {
        List<String> got = new ArrayList<>();
        view(cfg, Side.INGRESS).getHostPorts("addr", 9000, (h, p) -> got.add(h + ":" + p));
        return got;
    }

    private static List<String> list(String... items) {
        List<String> l = new ArrayList<>();
        for (int i = 0; i < items.length; i++) {
            l.add(items[i]);
        }
        return l;
    }

    private static ConfigView view(String cfg, Side side) {
        return new ConfigView(ConfigString.parse(cfg), side);
    }
}
