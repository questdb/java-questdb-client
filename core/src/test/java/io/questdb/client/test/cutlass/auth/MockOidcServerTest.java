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

import io.questdb.client.cutlass.auth.OidcAuthException;
import io.questdb.client.cutlass.auth.OidcDeviceAuth;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Self-tests for {@link MockOidcServer}, the harness the OIDC suites assert through.
 * <p>
 * Its load-bearing property is not that it serves JSON - every OIDC test would fail loudly if it did not -
 * but that a {@link MockOidcServer.Handler} failure REACHES THE TEST THREAD. A handler runs on a daemon
 * connection thread, where an uncaught throwable is otherwise invisible: the client sees a dropped
 * connection, which most of these tests tolerate as one more transport failure, so a broken assertion inside
 * a handler reads as a passing test. {@code handleConnection} captures the first such throwable and
 * {@code close()} rethrows it, and that path had no test of its own - every suite depended on it while
 * nothing proved it worked.
 */
public class MockOidcServerTest {

    private static final String DEVICE_PATH = "/device";

    @Test(timeout = 30_000)
    public void testAHandlerAssertionFailureReachesTheTestThread() throws Exception {
        assertMemoryLeak(() -> {
            // The shape that matters: an assertion inside a handler. Without the capture-and-rethrow, the
            // client below sees a dropped connection, turns it into an OidcAuthException the test could
            // easily be written to expect, and the broken assertion is never heard from again.
            AssertionError thrownByHandler = new AssertionError("the handler asserted something and it failed");
            boolean rethrown = false;
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> {
                throw thrownByHandler;
            })) {
                try (OidcDeviceAuth auth = newAuth(server)) {
                    auth.signIn();
                    Assert.fail("the handler threw, so the client cannot have completed a sign-in");
                } catch (OidcAuthException expected) {
                    // the client's view of a handler failure: the connection simply dropped
                }
            } catch (AssertionError e) {
                rethrown = true;
                Assert.assertSame("close() must resurface the handler's OWN throwable, not a copy",
                        thrownByHandler, e);
            }
            Assert.assertTrue("close() must resurface a handler failure on the test thread", rethrown);
        });
    }

    @Test(timeout = 30_000)
    public void testAHealthyRunClosesQuietlyAndRecordsItsRequests() throws Exception {
        assertMemoryLeak(() -> {
            // The control for the test above: without it, a close() that rethrew unconditionally - or a
            // server that recorded a phantom failure - would look exactly like a working propagation path.
            AtomicInteger deviceCalls = new AtomicInteger();
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, "{\"device_code\":\"DEV-CODE\",\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_uri\":\"https://verify.example/device\",\"expires_in\":300,"
                            + "\"interval\":1}");
                }
                return MockOidcServer.json(200, "{\"token_type\":\"Bearer\",\"expires_in\":3600,"
                        + "\"access_token\":\"ACCESS-1\"}");
            })) {
                try (OidcDeviceAuth auth = newAuth(server)) {
                    Assert.assertEquals("ACCESS-1", auth.signIn());
                }
                Assert.assertEquals(1, deviceCalls.get());
                // requestAuthHeaders() records one entry per request READ, header or not - the OIDC endpoints
                // are unauthenticated, so these are nulls, and the count is what QwpQueryClientTokenProviderTest
                // asserts on
                List<String> headers = server.requestAuthHeaders();
                Assert.assertTrue("every request read must be recorded: " + headers, headers.size() >= 2);
            } // a clean close: no throwable to resurface, so this must not throw
        });
    }

    private static OidcDeviceAuth newAuth(MockOidcServer server) {
        return OidcDeviceAuth.builder()
                .clientId("questdb")
                .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                .tokenEndpoint(server.httpUrl("/token"))
                .allowInsecureTransport(true)
                .prompt(challenge -> {
                })
                .build();
    }
}
