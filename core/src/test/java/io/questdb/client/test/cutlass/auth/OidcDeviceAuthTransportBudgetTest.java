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

package io.questdb.client.test.cutlass.auth;

import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.cutlass.auth.OidcDeviceAuth;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

/**
 * Pins that the HTTP clients {@link OidcDeviceAuth} builds take their CONNECTION budgets from
 * {@code httpTimeoutMillis}, not from the transport defaults.
 * <p>
 * {@code HttpClient.connect} reads both: it leaves the TCP connect to the OS when the connect timeout is 0,
 * and it sizes the TLS handshake as {@code connectTimeout > 0 ? connectTimeout : defaultTimeout}. Taking
 * {@code DefaultHttpClientConfiguration.INSTANCE} - 0 and 600s - therefore gave the handshake alone a 600s
 * budget derived from nothing the caller set, so neither {@code MAX_HTTP_TIMEOUT_MILLIS} (120s) nor
 * {@code Builder.build()}'s {@code lockStaleMillis} floor constrained it.
 * <p>
 * The consequence is not a slow request. A silent refresh runs inside {@code FileTokenStore}'s cross-process
 * lock, whose file is stamped once at creation and never re-stamped, so a hold outrunning
 * {@code DEFAULT_LOCK_STALE_MILLIS} (600s) is judged abandoned and stolen by a peer. Both holders then POST
 * the same rotating refresh token, and an identity provider with reuse detection revokes the whole family -
 * on a headless producer, ingestion stops until a human re-runs the device flow.
 * <p>
 * Asserted on the configuration rather than end to end because a real TLS handshake stall needs a
 * certificate, and the client's test tree has none - the TLS fixture ({@code TlsProxyRule}) lives in the
 * Enterprise tree, where {@code OidcDeviceAuthTlsTest} drives the flow over a real TLS socket. Reflection
 * because every test class here is in {@code io.questdb.client.test.*}, so a package-private hook would not
 * be reachable either; {@code FileTokenStoreTest} reaches this class's private statics the same way.
 */
public class OidcDeviceAuthTransportBudgetTest {

    @Test
    public void testConnectionBudgetsDeriveFromTheHttpTimeout() throws Exception {
        final Method httpConfig = OidcDeviceAuth.class.getDeclaredMethod("httpConfig", int.class);
        httpConfig.setAccessible(true);

        // A value distinct from every default in play (0, 30_000, 600_000), so a config that quietly fell
        // back to any of them fails rather than coincidentally matching.
        final int timeoutMillis = 7_777;
        final HttpClientConfiguration config = (HttpClientConfiguration) httpConfig.invoke(null, timeoutMillis);

        Assert.assertEquals("the TLS handshake budget is connectTimeout when it is positive, so leaving this "
                        + "at 0 hands the handshake the 600s request-timeout default instead",
                timeoutMillis, config.getConnectTimeout());
        Assert.assertEquals("the request timeout must be the caller's figure, not the 600s default",
                timeoutMillis, config.getTimeout());

        // Guard the premise: a zero connect timeout is precisely what routes HttpClient.connect to the OS
        // for the TCP connect and to defaultTimeout for the handshake, so a regression to the shared
        // DefaultHttpClientConfiguration.INSTANCE reads as 0 here.
        Assert.assertTrue("a positive connect timeout is what bounds both the TCP connect and the TLS "
                + "handshake; 0 restores the unbounded shape", config.getConnectTimeout() > 0);
    }

    @Test
    public void testDiscoveryClientsCarryTheSameDerivedBudgets() throws Exception {
        // Discovery runs before an instance exists, so it cannot take a builder value - but it reads
        // /settings and .well-known from the same untrusted network position and must be bounded too.
        final java.lang.reflect.Field discoveryConfig = OidcDeviceAuth.class.getDeclaredField("DISCOVERY_HTTP_CONFIG");
        discoveryConfig.setAccessible(true);
        final HttpClientConfiguration config = (HttpClientConfiguration) discoveryConfig.get(null);

        final java.lang.reflect.Field defaultTimeout = OidcDeviceAuth.class.getDeclaredField("DEFAULT_HTTP_TIMEOUT_MILLIS");
        defaultTimeout.setAccessible(true);
        final int expected = (Integer) defaultTimeout.get(null);

        Assert.assertEquals("discovery's connect/TLS budget must be the default HTTP timeout, not 0",
                expected, config.getConnectTimeout());
        Assert.assertEquals("discovery's request timeout must be the default HTTP timeout, not 600s",
                expected, config.getTimeout());
    }
}
