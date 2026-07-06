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

import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class QwpQueryClientConnectTimeoutTest {

    /**
     * A connect-phase timeout must be reported as a connect_timeout failure, not
     * relabeled as an "exceeded auth_timeout" overage.
     * <p>
     * {@code QwpQueryClient.runUpgradeWithTimeout} used to wrap the {@code connect()}
     * and {@code upgrade()} calls in one try block, so the timeout-flagged exception
     * thrown by the (in-diff) connect_timeout path was caught by the {@code isTimeout()}
     * branch intended for upgrade() and rewritten with the (much larger, and wrong)
     * auth_timeout value -- e.g. a connect that bailed after 500 ms reported
     * "exceeded auth_timeout=15000ms". The ingest side never had this because it
     * routes through {@code QwpUpgradeFailures.classify}, which leaves the
     * connect-timeout exception unmodified.
     */
    @Test(timeout = 30_000)
    public void testConnectTimeoutNotReportedAsAuthTimeout() {
        // 192.0.2.0/24 is TEST-NET-1 (RFC 5737): on a normal network the SYN is
        // silently dropped, so the TCP connect stalls and our application-level
        // connect_timeout (500 ms) fires -- long before auth_timeout_ms (15000 ms).
        // The WebSocket upgrade phase is never reached.
        try (QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=192.0.2.1:9009;connect_timeout=500;auth_timeout_ms=15000;failover=off;target=any;")) {
            long start = System.currentTimeMillis();
            try {
                client.connect();
                Assert.fail("expected connect to fail");
            } catch (HttpClientException ex) {
                long elapsed = System.currentTimeMillis() - start;
                String msg = ex.getMessage();

                // The connect_timeout path is only exercised when the runner routes
                // TEST-NET-1 into a black hole (dropped SYN). Skip -- rather than
                // flake -- on the other two outcomes:
                //  - no route: a fast ENETUNREACH surfaces as "could not connect".
                //  - (rare) the host accepts the connect: the upgrade then runs the
                //    full auth_timeout, so elapsed ~ auth_timeout (>5 s).
                // Neither gate keys on the connect-vs-auth label, so neither can mask
                // the regression: a black-holed connect always bails at ~500 ms with
                // a message that is "connect timed out" (fixed) or "...auth_timeout..."
                // (the bug) -- both reach the assertions below.
                Assume.assumeFalse("no route to TEST-NET-1 black hole on this runner: " + msg,
                        msg.contains("could not connect"));
                Assume.assumeTrue("TEST-NET-1 is not a black hole on this runner (elapsed=" + elapsed + "ms): " + msg,
                        elapsed < 5_000);

                // It bailed at connect_timeout=500 ms, nowhere near auth_timeout=15000 ms.
                // Regression: name the connect phase, never auth_timeout.
                Assert.assertFalse("connect-phase timeout misreported as auth_timeout: " + msg,
                        msg.contains("auth_timeout"));
                Assert.assertTrue("expected a connect-timeout diagnostic, got: " + msg,
                        msg.contains("connect timed out"));
            }
        }
    }
}
