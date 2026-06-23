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

package io.questdb.client.test.cutlass.line;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.IntList;
import io.questdb.client.std.ObjList;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.fail;

/**
 * Verifies that {@code Sender.fromConfig("...addr=...")} accumulates host
 * entries from both comma syntax ({@code addr=h1:p1,h2:p2}) and repeated keys
 * ({@code addr=h1:p1;addr=h2:p2}) per QWP spec wire-ingress.md §1.1 / failover.md
 * §1.1. The two forms must accumulate; empty entries (leading/trailing commas
 * or {@code ,,}) must be rejected.
 *
 * <p>The reference QWP egress client ({@code QwpQueryClient.fromConfig})
 * already accepts comma syntax; this test pins the equivalent behaviour on
 * the ingress {@link Sender} so the two sides are symmetric.
 */
public class LineSenderAddrParsingTest {

    private static final int DEFAULT_HTTP_PORT = 9000;
    private static final int DEFAULT_TCP_PORT = 9009;
    private static final int DEFAULT_UDP_PORT = 9007;
    private static final int DEFAULT_WS_PORT = 9000;

    @Test
    public void testAddrCommaAndRepeatedKeysCombine() {
        Sender.LineSenderBuilder b = Sender.builder("ws::addr=h1:8080,h2:9000;addr=h3:10000;");
        assertHosts(b, "h1", 8080, "h2", 9000, "h3", 10000);
    }

    @Test
    public void testAddrCommaSyntaxAccumulatesHttp() {
        Sender.LineSenderBuilder b = Sender.builder("http::addr=h1:8080,h2:9000,h3:10000;");
        assertHosts(b, "h1", 8080, "h2", 9000, "h3", 10000);
    }

    @Test
    public void testAddrCommaSyntaxAccumulatesHttps() {
        Sender.LineSenderBuilder b = Sender.builder("https::addr=h1:443,h2:8443;tls_verify=on;");
        assertHosts(b, "h1", 443, "h2", 8443);
    }

    @Test
    public void testAddrCommaSyntaxAccumulatesWs() {
        Sender.LineSenderBuilder b = Sender.builder("ws::addr=h1:9000,h2:9001;");
        assertHosts(b, "h1", 9000, "h2", 9001);
    }

    @Test
    public void testAddrCommaSyntaxAccumulatesWss() {
        Sender.LineSenderBuilder b = Sender.builder("wss::addr=h1:9000,h2:9001;tls_verify=on;");
        assertHosts(b, "h1", 9000, "h2", 9001);
    }

    @Test
    public void testAddrCommaSyntaxEquivalentToRepeatedKeys() {
        Sender.LineSenderBuilder commaForm =
                Sender.builder("ws::addr=alpha:9000,beta:9001,gamma:9002;");
        Sender.LineSenderBuilder repeatedForm =
                Sender.builder("ws::addr=alpha:9000;addr=beta:9001;addr=gamma:9002;");
        Assert.assertEquals(
                "comma syntax and repeated keys must produce identical host lists",
                hostsOf(repeatedForm), hostsOf(commaForm));
        Assert.assertEquals(
                "comma syntax and repeated keys must produce identical port lists",
                portsToList(portsOf(repeatedForm)), portsToList(portsOf(commaForm)));
    }

    @Test
    public void testAddrCommaWithExplicitAndDefaultPortsMixed() {
        Sender.LineSenderBuilder b = Sender.builder("ws::addr=h1:7000,h2,h3:7002;");
        assertHosts(b, "h1", 7000, "h2", DEFAULT_WS_PORT, "h3", 7002);
    }

    @Test
    public void testAddrEmptyEntryConsecutiveCommasRejected() {
        assertConfStrError("http::addr=h1,,h2;", "empty addr entry");
    }

    @Test
    public void testAddrEmptyEntryLeadingCommaRejected() {
        assertConfStrError("http::addr=,h1:9000;", "empty addr entry");
    }

    @Test
    public void testAddrEmptyEntryTrailingCommaRejected() {
        assertConfStrError("http::addr=h1:9000,;", "empty addr entry");
    }

    @Test
    public void testAddrEmptyValuePreservesLegacyError() {
        // Empty addr= must still surface the existing "empty addr entry"
        // diagnostic so downstream tooling that matches that string is undisturbed.
        assertConfStrError("http::addr=;", "empty addr entry");
    }

    @Test
    public void testAddrInvalidPortInSecondEntryRejected() {
        // The first entry parses cleanly; the failure must come from the
        // second entry's port, proving the comma-separated walk reaches it.
        assertConfStrError("ws::addr=h1:9000,h2:notaport;",
                "invalid port in addr");
    }

    @Test
    public void testAddrPortOutOfRangeInSecondEntryRejected() {
        assertConfStrError("ws::addr=h1:9000,h2:0;", "port out of range in addr");
    }

    @Test
    public void testAddrSingleEntryWithoutPortGetsHttpDefault() {
        Sender.LineSenderBuilder b = Sender.builder("http::addr=h1;");
        assertHosts(b, "h1", DEFAULT_HTTP_PORT);
    }

    @Test
    public void testAddrSingleEntryWithoutPortGetsTcpDefault() {
        Sender.LineSenderBuilder b = Sender.builder("tcp::addr=h1;");
        assertHosts(b, "h1", DEFAULT_TCP_PORT);
    }

    @Test
    public void testAddrSingleEntryWithoutPortGetsUdpDefault() {
        Sender.LineSenderBuilder b = Sender.builder("udp::addr=h1;");
        assertHosts(b, "h1", DEFAULT_UDP_PORT);
    }

    @Test
    public void testAddrSingleEntryWithoutPortGetsWsDefault() {
        Sender.LineSenderBuilder b = Sender.builder("ws::addr=h1;");
        assertHosts(b, "h1", DEFAULT_WS_PORT);
    }

    @Test
    public void testAddrTcpMultiHostFailsAtBuild() {
        // TCP only supports a single address. Comma syntax must parse cleanly,
        // then the build-time validator must surface the single-address error.
        // The fact that we reach the validator (rather than a parse error) is
        // itself evidence that comma syntax is now accepted by the ingress
        // parser.
        try (Sender ignored = Sender.fromConfig("tcp::addr=h1:9009,h2:9009;")) {
            fail("TCP multi-host must fail at build");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(),
                    "only a single address (host:port) is supported for TCP transport");
        }
    }

    @Test
    public void testAddrUdpMultiHostFailsAtBuild() {
        try (Sender ignored = Sender.fromConfig("udp::addr=h1:9007,h2:9007;")) {
            fail("UDP multi-host must fail at build");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(),
                    "only a single address (host:port) is supported for UDP transport");
        }
    }

    @Test
    public void testAddrWithoutPortInCommaListGetsDefault() {
        Sender.LineSenderBuilder b = Sender.builder("ws::addr=h1,h2,h3;");
        assertHosts(b, "h1", DEFAULT_WS_PORT, "h2", DEFAULT_WS_PORT, "h3", DEFAULT_WS_PORT);
    }

    @Test
    public void testRepeatedAddrKeysOnlyAccumulateRegression() {
        // Pre-existing form must still work after the comma-syntax change.
        Sender.LineSenderBuilder b =
                Sender.builder("ws::addr=h1:9000;addr=h2:9001;addr=h3:9002;");
        assertHosts(b, "h1", 9000, "h2", 9001, "h3", 9002);
    }

    private static void assertConfStrError(String conf, String expectedSubstring) {
        try {
            try (Sender ignored = Sender.fromConfig(conf)) {
                fail("expected LineSenderException containing '" + expectedSubstring + "'");
            }
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), expectedSubstring);
        }
    }

    /**
     * Asserts that the builder's parsed {@code (host, port)} pairs equal
     * the supplied alternating {@code host, port, host, port, ...} sequence,
     * in order. Inspects the package-private {@code hosts}/{@code ports}
     * fields via reflection because the builder does not expose accessors.
     */
    private static void assertHosts(Sender.LineSenderBuilder builder, Object... hostPortPairs) {
        if ((hostPortPairs.length & 1) != 0) {
            throw new IllegalArgumentException("hostPortPairs must be alternating host, port");
        }
        ObjList<String> hosts = hostsOf(builder);
        IntList ports = portsOf(builder);
        int expectedSize = hostPortPairs.length / 2;
        Assert.assertEquals("host list size", expectedSize, hosts.size());
        Assert.assertEquals("port list size", expectedSize, ports.size());
        for (int i = 0; i < expectedSize; i++) {
            String expectedHost = (String) hostPortPairs[i * 2];
            int expectedPort = (Integer) hostPortPairs[i * 2 + 1];
            Assert.assertEquals("host[" + i + "]", expectedHost, hosts.get(i));
            Assert.assertEquals("port[" + i + "]", expectedPort, ports.getQuick(i));
        }
    }

    @SuppressWarnings("unchecked")
    private static ObjList<String> hostsOf(Sender.LineSenderBuilder builder) {
        return (ObjList<String>) readField(builder, "hosts");
    }

    private static IntList portsOf(Sender.LineSenderBuilder builder) {
        return (IntList) readField(builder, "ports");
    }

    private static java.util.List<Integer> portsToList(IntList ports) {
        java.util.ArrayList<Integer> out = new java.util.ArrayList<>(ports.size());
        for (int i = 0, n = ports.size(); i < n; i++) {
            out.add(ports.getQuick(i));
        }
        return out;
    }

    private static Object readField(Object target, String name) {
        try {
            Field f = target.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.get(target);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("cannot read field '" + name + "' on " + target.getClass(), e);
        }
    }
}
