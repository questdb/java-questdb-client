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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.test.cutlass.auth.MockOidcServer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the whole-call bound on the response HEAD read, {@code ResponseHeaders.await(int)}.
 * <p>
 * The body reads got this bound first ({@code AbstractResponse.recv}, {@code AbstractChunkedResponse.recv});
 * the head read kept re-arming the full timeout on every pass. That is not a slower version of the same
 * thing, it is unbounded: {@code recvOrDie} returns 0 whenever a read yields no application bytes, a 0
 * leaves {@code totalBytesReceived} unmoved, and an unmoved counter neither advances the header parser nor
 * fills its buffer - so the "header is too large" escape never fires either.
 * <p>
 * It matters because {@code OidcDeviceAuth} reads this head from an identity provider on the
 * {@code getToken()} path, which an ILP sender built with {@code httpTokenProvider} calls once per flush.
 * The IdP endpoints are required to be {@code https}, and a partial TLS record decrypting to no application
 * bytes is exactly the 0-length read above.
 * <p>
 * Driven over plaintext with a head dribbled a byte at a time rather than with a stubbed {@code recvOrDie}:
 * the point is the elapsed-time bound a caller asked for, and a dribbling peer defeats it the same way.
 */
public class HttpClientResponseHeadTimeoutTest {

    @Test(timeout = 30_000)
    public void testAwaitHonoursTotalTimeoutWhileTheHeadDribbles() throws Exception {
        // 500ms against a head dribbled at 50ms/byte: the bound must fire in ~500ms. Without it every read
        // makes progress inside its own re-armed 500ms, so await() runs for (bytes x 50ms) and the @Test
        // timeout fires instead of this assertion.
        final int timeoutMillis = 500;
        final HttpClientConfiguration config = new DefaultHttpClientConfiguration() {
            @Override
            public int getTimeout() {
                return timeoutMillis;
            }
        };
        try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.dribbleHead())) {
            try (HttpClient client = HttpClientFactory.newPlainTextInstance(config)) {
                HttpClient.Request request = client.newRequest("127.0.0.1", server.port())
                        .GET()
                        .url("/head");
                final HttpClient.ResponseHeaders headers = request.send(timeoutMillis);
                final long startNanos = System.nanoTime();
                try {
                    headers.await(timeoutMillis);
                    Assert.fail("expected await to time out while the response head dribbled");
                } catch (HttpClientException e) {
                    // Either terminator is the bound working. Against a peer that dribbles, the shrinking
                    // per-pass budget starves ioWait's poll first, so the throw comes from there
                    // ("timed out [errno=..]"); against one whose reads yield no application bytes at all -
                    // the partial-TLS-record case, which consumes no budget - the loop's own deadline check
                    // fires instead ("timed out reading the response head"). What neither can do is keep
                    // running, which is what the elapsed assertion below pins.
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
                }
                final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                // The ceiling is the assertion that matters: it is what a per-read re-arm cannot satisfy.
                // Generous against the 500ms budget so a loaded CI box does not turn a real bound into a
                // red test, and still an order of magnitude below the unbounded behaviour.
                Assert.assertTrue("await must abort on its own deadline, not run on with the dribble; took "
                        + elapsedMillis + "ms against a " + timeoutMillis + "ms budget", elapsedMillis < 10_000);
            }
        }
    }
}
