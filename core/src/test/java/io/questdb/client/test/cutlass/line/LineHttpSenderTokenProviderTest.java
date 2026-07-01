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

import io.questdb.client.HttpTokenProvider;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.test.cutlass.auth.MockOidcServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Verifies that a {@link Sender} built with {@link Sender.LineSenderBuilder#httpTokenProvider}
 * does not query the provider on the build path: the first token pull is deferred to the first
 * row. That lets a provider which signs in lazily - the documented
 * {@code .httpTokenProvider(auth::getToken)} - be wired before the interactive sign-in
 * has completed.
 * <p>
 * The deferral tests pin an explicit {@code protocol_version} to keep {@link Sender.LineSenderBuilder#build()}
 * from probing the server, and disable auto-flush, so rows buffer against a port nobody listens on without
 * opening a connection. The end-to-end tests instead flush against a {@link MockOidcServer} and assert the
 * pulled token actually reaches the {@code Authorization: Bearer} header on the wire, is re-queried per
 * request as a rotating provider refreshes, and is re-sent verbatim (not re-pulled) on a retry. Each test
 * runs under {@code assertMemoryLeak} so the sender's native buffers are proven freed on close.
 */
public class LineHttpSenderTokenProviderTest {

    @Test
    public void testBuildSucceedsWhenProviderHasNotSignedInYet() throws Exception {
        assertMemoryLeak(() -> {
            // a provider that throws until the caller has signed in, mirroring OidcDeviceAuth::getToken
            AtomicBoolean signedIn = new AtomicBoolean(false);
            HttpTokenProvider provider = () -> {
                if (!signedIn.get()) {
                    throw new LineSenderException("no token has been obtained yet");
                }
                return "TOKEN";
            };
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("127.0.0.1:1")
                    .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                    .disableAutoFlush()
                    .httpTokenProvider(provider)
                    .build()) {
                // build() must succeed even though the provider cannot supply a token yet, so the natural
                // "construct the sender, sign in, then send" ordering is possible
                try {
                    sender.table("t").longColumn("v", 1L).atNow();
                    Assert.fail("expected the not-yet-signed-in provider to fail the first row");
                } catch (LineSenderException e) {
                    // the deferred pull surfaces the provider's error at first use, not at build time
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no token has been obtained yet"));
                }
                // after signing in, the still-pending stamp is retried and the row is accepted
                signedIn.set(true);
                sender.table("t").longColumn("v", 1L).atNow();
                Assert.assertTrue("row must be buffered after signing in", sender.bufferView().size() > 0);
            }
        });
    }

    @Test
    public void testControlOrNonAsciiProviderTokenIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // a token carrying a control or non-ASCII char is forbidden by the HttpTokenProvider contract: a
            // CR/LF would inject into the request line and a non-ASCII byte is silently truncated by the ASCII
            // header writer, so the sender must reject it at first use rather than splice a corrupt or injected
            // "Authorization: Bearer " header onto the wire. Strings are built with explicit char values to keep
            // this source pure ASCII.
            assertProviderTokenRejected(() -> "abc" + (char) 0x0d + (char) 0x0a + "def", "control or non-ASCII character"); // CR/LF
            assertProviderTokenRejected(() -> "tok" + (char) 0x00 + "en", "control or non-ASCII character"); // NUL
            assertProviderTokenRejected(() -> (char) 0x1b + "[31mred", "control or non-ASCII character"); // ANSI escape
            assertProviderTokenRejected(() -> "tok" + (char) 0xe9 + "n", "control or non-ASCII character"); // non-ASCII
        });
    }

    @Test(timeout = 30_000)
    public void testFailedFlushReSendsSameTokenWithoutRePull() throws Exception {
        assertMemoryLeak(() -> {
            // a failed flush preserves the buffered request - token included - and re-sends it verbatim on retry
            // rather than re-pulling the provider (the documented contract on httpTokenProvider()). Here the first
            // send gets a retryable 500 and the retry must carry the SAME baked token, with the provider queried
            // only once - so a rotating provider does not change the credential mid-retry of one buffered batch.
            AtomicInteger requests = new AtomicInteger();
            try (MockOidcServer server = new MockOidcServer((method, path, body) ->
                    requests.incrementAndGet() == 1
                            ? MockOidcServer.chunkedJson(500, "boom") // first send: retryable server error
                            : MockOidcServer.json(204, ""))) {        // retry: success
                AtomicInteger calls = new AtomicInteger();
                HttpTokenProvider provider = () -> "TOKEN-" + calls.incrementAndGet();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .httpTokenProvider(provider)
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    sender.flush(); // first send 500 -> retry -> 204
                }
                Assert.assertEquals("the provider must be pulled once, not re-pulled on retry", 1, calls.get());
                List<String> auth = server.requestAuthHeaders();
                Assert.assertEquals("the failed send plus its retry must be two requests", 2, auth.size());
                Assert.assertEquals("the first send carries the pulled token", "Bearer TOKEN-1", auth.get(0));
                Assert.assertEquals("the retry must re-send the same baked token", "Bearer TOKEN-1", auth.get(1));
            }
        });
    }

    @Test
    public void testNullOrEmptyProviderTokenIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // the HttpTokenProvider contract forbids a null or empty token; the sender must reject it with a
            // clear LineSenderException at first use, rather than silently send a malformed "Authorization:
            // Bearer " header that the server only answers with a 401 far from the cause
            assertProviderTokenRejected(() -> null, "null or empty token");
            assertProviderTokenRejected(() -> "", "null or empty token");
            assertProviderTokenRejected(() -> "   ", "null or empty token");
        });
    }

    @Test
    public void testProviderTokenNotPulledAtBuildAndPulledOnFirstRow() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger calls = new AtomicInteger();
            HttpTokenProvider provider = () -> {
                calls.incrementAndGet();
                return "TOKEN";
            };
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("127.0.0.1:1")
                    .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                    .disableAutoFlush()
                    .httpTokenProvider(provider)
                    .build()) {
                // build() must not query the provider: a lazily-signing-in provider would not have a token yet
                Assert.assertEquals("provider must not be queried at build time", 0, calls.get());
                // the first row pulls the deferred token so the first send will carry it
                sender.table("t").longColumn("v", 1L).atNow();
                Assert.assertEquals("provider must be queried when the first row starts", 1, calls.get());
                // a second row in the same un-flushed batch reuses the same request, so it does not re-pull
                sender.table("t").longColumn("v", 2L).atNow();
                Assert.assertEquals("provider must not be re-queried within the same batch", 1, calls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenReachesAuthorizationHeaderAndRotatesPerFlush() throws Exception {
        assertMemoryLeak(() -> {
            // end-to-end against a real socket: the pulled token must reach the "Authorization: Bearer" header
            // on the wire (not merely be pulled), and a rotating provider must be re-queried per request so a
            // long-lived sender follows token refreshes rather than sending a token captured once.
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.json(204, ""))) {
                AtomicInteger calls = new AtomicInteger();
                HttpTokenProvider provider = () -> "TOKEN-" + calls.incrementAndGet();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .httpTokenProvider(provider)
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    sender.flush();
                    sender.table("t").longColumn("v", 2L).atNow();
                    sender.flush();
                }
                List<String> auth = server.requestAuthHeaders();
                Assert.assertEquals("two flushes must send two requests", 2, auth.size());
                Assert.assertEquals("the first request must carry the first pulled token", "Bearer TOKEN-1", auth.get(0));
                Assert.assertEquals("the second flush must re-query the provider and carry the rotated token", "Bearer TOKEN-2", auth.get(1));
            }
        });
    }

    private static void assertProviderTokenRejected(HttpTokenProvider provider, String expectedMessage) {
        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                .address("127.0.0.1:1")
                .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                .disableAutoFlush()
                .httpTokenProvider(provider)
                .build()) {
            try {
                sender.table("t").longColumn("v", 1L).atNow();
                Assert.fail("expected an invalid provider token to be rejected");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
            }
        }
    }
}
