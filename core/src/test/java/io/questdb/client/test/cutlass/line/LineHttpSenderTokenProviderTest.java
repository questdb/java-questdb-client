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
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.client.std.str.Utf8String;
import io.questdb.client.test.cutlass.auth.MockOidcServer;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
    public void testBufferViewIsEmptyNotSentinelWhileTheTokenIsPending() {
        // With a provider configured, newRequest() leaves the request at the header stage - withContent() is
        // deferred until the first row stamps the Authorization header - so contentStart holds its -1
        // sentinel between every flush and the next row. getContentLength() already reported 0 for that
        // state, so bufferView() handed out a view that is empty by length but whose base address is a
        // non-zero, unusable pointer: a ptr() != 0 test reads as true, and arithmetic on it is nonsense.
        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                .address("127.0.0.1:1")
                .httpTokenProvider(() -> "TOKEN")
                .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                .disableAutoFlush()
                .build()) {
            DirectByteSlice pending = sender.bufferView();
            Assert.assertEquals("an empty buffer must report a zero base address, not the -1 sentinel",
                    0L, pending.ptr());
            Assert.assertEquals(0, pending.size());

            // and once a row stamps the token and opens the content section, the view is real again
            sender.table("t").longColumn("v", 1L).atNow();
            DirectByteSlice afterRow = sender.bufferView();
            Assert.assertTrue("a stamped request must expose a usable base address", afterRow.ptr() > 0);
            Assert.assertTrue("and a non-empty buffer", afterRow.size() > 0);
        }
    }

    @Test(timeout = 30_000)
    public void testAtWithoutTableDoesNotCorruptTheAuthorizationHeader() throws Exception {
        assertMemoryLeak(() -> {
            // Regression: at() used to write the leading space and the timestamp BEFORE atNow() validated the
            // row state. With a provider, newRequest() leaves the request at the header stage (withContent()
            // deferred until the first row stamps the Authorization header), so those bytes landed in the HTTP
            // HEADER block, on a line of their own. The next row's "Authorization: Bearer ..." was then appended
            // to that line, making it an obs-fold continuation of User-Agent (RFC 7230) instead of a header of
            // its own - so the flush went out with NO credential and the server answered 401, after which
            // close() dropped the buffered rows. cancelRow() could not undo it: trimContentToLen only rewinds
            // within the content section, and it early-returns while the token is pending anyway.
            // Both at() overloads are covered, over V1 and V2 (V3 inherits V2's).
            int[] versions = {Sender.PROTOCOL_VERSION_V1, Sender.PROTOCOL_VERSION_V2};
            for (int i = 0; i < versions.length; i++) {
                for (int overload = 0; overload < 2; overload++) {
                    try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.json(204, ""))) {
                        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                                .address("127.0.0.1:" + server.port())
                                .protocolVersion(versions[i])
                                .disableAutoFlush()
                                .httpTokenProvider(() -> "TOKEN")
                                .build()) {
                            try {
                                if (overload == 0) {
                                    sender.at(1_700_000_000_000_000_000L, ChronoUnit.NANOS);
                                } else {
                                    sender.at(Instant.ofEpochMilli(1_700_000_000_000L));
                                }
                                Assert.fail("expected at() with no table name to be rejected");
                            } catch (LineSenderException e) {
                                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no table name was provided"));
                            }
                            // the documented recovery, and the sender must still be usable afterwards
                            sender.cancelRow();
                            sender.table("t").longColumn("v", 1L).atNow();
                            sender.flush();
                        }
                        List<String> auth = server.requestAuthHeaders();
                        Assert.assertEquals("exactly one flush must reach the server", 1, auth.size());
                        // null here means the rejected at() spliced bytes ahead of the header, so the mock's
                        // parser never saw a line whose field name is "Authorization"
                        Assert.assertEquals("the token must reach the wire as its own header",
                                "Bearer TOKEN", auth.get(0));
                    }
                }
            }
        });
    }

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

    @Test(timeout = 30_000)
    public void testCancelRowWithPendingTokenDoesNotCorruptRequest() throws Exception {
        assertMemoryLeak(() -> {
            // Regression: with an httpTokenProvider, newRequest() defers the token and leaves the request at the
            // header stage (withContent() not yet run), so the native contentStart is still the -1 sentinel and
            // no row bytes are buffered. cancelRow() must be a safe no-op in that window: trimContentToLen(0)
            // would otherwise set the write pointer to contentStart + 0 == -1, and the next buffer write (the
            // deferred Authorization header on the following row) would segfault the JVM. The window is entered
            // after build() and again after every flush (reset() re-arms the pending token); a rejected table
            // name - validateTableName() runs BEFORE the token is stamped - is a mainstream way to reach a
            // cancelRow() with the token still pending.
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.json(204, ""))) {
                AtomicInteger calls = new AtomicInteger();
                HttpTokenProvider provider = () -> "TOKEN-" + calls.incrementAndGet();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .httpTokenProvider(provider)
                        .build()) {
                    // (1) cancelRow immediately after build(), token pending, nothing buffered: before the fix the
                    // write pointer went to -1 and the following row's write segfaulted the JVM
                    sender.cancelRow();
                    Assert.assertEquals("cancelRow must not pull the deferred token", 0, calls.get());

                    // the sender is still usable: a real row buffers, flushes, and carries the token to the wire
                    sender.table("t").longColumn("v", 1L).atNow();
                    sender.flush();

                    // (2) after a flush the token is pending again; cancelRow in that window must also be a safe
                    // no-op, and the next row must still send its (rotated) token
                    sender.cancelRow();
                    sender.table("t").longColumn("v", 2L).atNow();
                    sender.flush();
                }
                List<String> auth = server.requestAuthHeaders();
                Assert.assertEquals("both flushes must reach the server", 2, auth.size());
                Assert.assertEquals("Bearer TOKEN-1", auth.get(0));
                Assert.assertEquals("Bearer TOKEN-2", auth.get(1));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testChangedProviderTokenIsRevalidated() throws Exception {
        assertMemoryLeak(() -> {
            // every pulled token is validated per flush, so a token that CHANGES to a bad one must be rejected.
            // First flush a valid token, then return a distinct CR/LF token and require the next flush to reject
            // it rather than splice it onto the wire. (The same-instance-mutated case - a reused buffer whose
            // content changes - is covered by testMutatedSameInstanceProviderTokenIsRevalidated.)
            AtomicInteger calls = new AtomicInteger();
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.json(204, ""))) {
                HttpTokenProvider provider = () -> calls.incrementAndGet() == 1
                        ? "GOODTOKEN"
                        : "abc" + (char) 0x0d + (char) 0x0a + "def"; // second pull: CR/LF injected
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .httpTokenProvider(provider)
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    sender.flush(); // first flush: GOODTOKEN validated and sent
                    try {
                        // the second flush's first row re-pulls the provider -> the changed, bad token
                        sender.table("t").longColumn("v", 2L).atNow();
                        sender.flush();
                        Assert.fail("a changed, bad token must be re-validated and rejected");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("control or non-ASCII character"));
                    }
                }
                Assert.assertEquals("the provider is re-pulled per flush", 2, calls.get());
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

    @Test(timeout = 30_000)
    public void testMutatedSameInstanceProviderTokenIsRevalidated() throws Exception {
        assertMemoryLeak(() -> {
            // A provider may reuse one CharSequence buffer (the idiomatic zero-alloc style) and return the SAME
            // instance every call. HttpTokenProvider.getToken() makes no immutability promise, so the sender
            // must re-validate EVERY pulled token, not trust instance identity: a token mutated in place to
            // carry a CR/LF between flushes must be rejected, not spliced verbatim into the "Authorization:
            // Bearer" header (authToken writes it with no CR/LF filtering). This pins the fix that dropped the
            // identity-cache skip; before it, the second flush injected a header past the auth line.
            StringBuilder token = new StringBuilder("GOODTOKEN"); // one instance, mutated in place below
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.json(204, ""))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .httpTokenProvider(() -> token) // always the SAME instance
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    sender.flush(); // first flush: GOODTOKEN validated and sent
                    // mutate the SAME instance to inject a CR/LF header break
                    token.setLength(0);
                    token.append("abc").append((char) 0x0d).append((char) 0x0a).append("X-Injected: pwned");
                    try {
                        sender.table("t").longColumn("v", 2L).atNow();
                        sender.flush();
                        Assert.fail("a mutated same-instance token carrying CR/LF must be re-validated and rejected");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("control or non-ASCII character"));
                    }
                }
                // only the first (valid) flush reached the wire; the injected token was rejected before any send
                List<String> auth = server.requestAuthHeaders();
                Assert.assertEquals("only the valid first flush must reach the server", 1, auth.size());
                Assert.assertEquals("Bearer GOODTOKEN", auth.get(0));
            }
        });
    }

    @Test
    public void testTokenMutatedBetweenValidationAndTheHeaderCannotSplice() throws Exception {
        assertMemoryLeak(() -> {
            // The sibling test above covers a buffer mutated BETWEEN flushes, which re-validation catches.
            // This is the window inside ONE flush: validateToken scanned the provider's sequence and
            // authToken then re-read it, so a mutation landing between those two reads passed the check and
            // was spliced verbatim into the Authorization header. HttpTokenProvider.getToken() explicitly
            // invites a reused mutable buffer, and the SPI is exported, so the reader has to be the one that
            // makes this safe: the pulled value is snapshotted before it is validated, and the bytes checked
            // are the bytes sent.
            //
            // HandOffToken swaps its content the instant a full scan completes - i.e. exactly when
            // validateToken finishes - so every later read sees the CR/LF splice.
            final String clean = "GOODTOKEN";
            final String spliced = "abc" + (char) 0x0d + (char) 0x0a + "X-Injected: pwned";
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.json(204, ""))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .httpTokenProvider(() -> new HandOffToken(clean, spliced))
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    sender.flush();
                }
                List<String> auth = server.requestAuthHeaders();
                Assert.assertEquals(1, auth.size());
                Assert.assertEquals("the header must carry the bytes that were validated, not a value "
                        + "swapped in after the scan", "Bearer " + clean, auth.get(0));
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
    public void testPutRawMessageStampsPendingToken() throws Exception {
        assertMemoryLeak(() -> {
            // putRawMessage() sends a pre-formatted ILP line as the first row; it must stamp the deferred provider
            // token first, or the raw message would ship with no Authorization header. F7: covers the
            // stampTokenIfPending() call that putRawMessage() gained.
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.json(204, ""))) {
                AtomicInteger calls = new AtomicInteger();
                HttpTokenProvider provider = () -> "TOKEN-" + calls.incrementAndGet();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .httpTokenProvider(provider)
                        .build()) {
                    ((AbstractLineHttpSender) sender).putRawMessage(new Utf8String("t v=1i\n"));
                    sender.flush();
                }
                List<String> auth = server.requestAuthHeaders();
                Assert.assertEquals("the raw-message flush must reach the server", 1, auth.size());
                Assert.assertEquals("putRawMessage must carry the provider token", "Bearer TOKEN-1", auth.get(0));
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

    /**
     * A provider buffer that hands off its content the moment a full scan of it completes: the first
     * traversal reads {@code clean}, and every read after that reads {@code spliced}. That is the shape of a
     * reused zero-allocation buffer refreshed by another thread the instant the validating scan finishes -
     * the narrowest version of the window, and the one a reader that validates and then re-reads loses.
     */
    private static final class HandOffToken implements CharSequence {
        private final String spliced;
        private CharSequence current;
        private boolean handedOff;

        HandOffToken(String clean, String spliced) {
            this.current = clean;
            this.spliced = spliced;
        }

        @Override
        public char charAt(int index) {
            final char c = current.charAt(index);
            if (!handedOff && index == current.length() - 1) {
                handedOff = true;
                current = spliced;
            }
            return c;
        }

        @Override
        public int length() {
            return current.length();
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return current.subSequence(start, end);
        }

        @Override
        public String toString() {
            // what a StringBuilder-backed buffer does: materialise whatever it currently holds
            return current.toString();
        }
    }
}
