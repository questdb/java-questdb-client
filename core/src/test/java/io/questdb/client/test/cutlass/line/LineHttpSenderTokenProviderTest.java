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
import org.junit.Assert;
import org.junit.Test;

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
 * An explicit {@code protocol_version} keeps {@link Sender.LineSenderBuilder#build()} from probing
 * the server, and auto-flush is disabled, so rows can be buffered against a port nobody listens on
 * without ever opening a connection. Each test runs under {@code assertMemoryLeak} so the sender's
 * native buffers are proven freed on close.
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
