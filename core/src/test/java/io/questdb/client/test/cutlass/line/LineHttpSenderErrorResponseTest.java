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
import io.questdb.client.test.cutlass.auth.MockOidcServer;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Verifies that the error body a QuestDB HTTP endpoint returns on a failed flush is rendered safely
 * into the {@link LineSenderException} message. A JSON error body has its string escapes resolved by the
 * lexer, so a {@code message} or {@code errorId} field arrives fully decoded; an auth (401/403) body, a
 * non-JSON body, and a body that fails to parse as JSON are echoed verbatim. In every case a hostile or
 * proxied endpoint could otherwise smuggle real control characters, ANSI escapes or bidi overrides that
 * forge a log line or rewrite a terminal when the exception text is printed. The sender must escape them,
 * just as it does for column names in an error message.
 * <p>
 * The dangerous bytes are built at runtime via {@code (char) 0x1b} (ESC) and {@code (char) 0x202e} (a
 * right-to-left override), so this source file stays pure ASCII and carries none of the chars it guards.
 */
public class LineHttpSenderErrorResponseTest {

    // ESC: the lead byte of an ANSI escape sequence (terminal hijack)
    private static final char ESC = 0x1b;
    // U+202E RIGHT-TO-LEFT OVERRIDE: reorders displayed text (visual spoofing)
    private static final char RLO = 0x202e;

    @Test(timeout = 30_000)
    public void testProtocolDetectionErrorBodyControlAndBidiAreEscaped() throws Exception {
        assertMemoryLeak(() -> {
            // when the caller does not pin a protocol version, build() probes the server for one; a
            // non-success, non-404 probe response body is captured into the "Failed to detect server line
            // protocol version" exception. A hostile or proxied endpoint must not splice control, ANSI or
            // bidi chars into that message any more than into a flush error
            String errorBody = "probe denied " + ESC + "[2J forged\n" + RLO + "moc.live";
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.chunkedJson(400, errorBody))) {
                try {
                    // no protocolVersion(...) -> build() runs the detection probe; retryTimeoutMillis(0) makes
                    // it give up after the first failed probe instead of retrying to a deadline
                    Sender.builder(Sender.Transport.HTTP)
                            .address("127.0.0.1:" + server.port())
                            .retryTimeoutMillis(0)
                            .build()
                            .close();
                    Assert.fail("expected protocol detection to fail and surface the server body");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(msg, msg.contains("Failed to detect server line protocol version"));
                    Assert.assertTrue("visible text must be preserved: " + msg, msg.contains("probe denied"));
                    Assert.assertTrue("the ESC must be escaped: " + msg, msg.contains("\\u001b"));
                    Assert.assertTrue("the bidi override must be escaped: " + msg, msg.contains("\\u202e"));
                    Assert.assertFalse("a raw ESC must not leak: " + msg, msg.indexOf(0x1b) >= 0);
                    Assert.assertFalse("a raw newline must not leak: " + msg, msg.indexOf('\n') >= 0);
                    Assert.assertFalse("a raw bidi override must not leak: " + msg, msg.indexOf(0x202e) >= 0);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFlushResponseBodyDribbleAbortsOnRequestTimeout() throws Exception {
        assertMemoryLeak(() -> {
            // A flush whose response BODY dribbles (chunked headers sent, then the chunk-size line one byte at
            // a time, never completing) must abort the read on the configured request timeout: the no-arg
            // recv() the flush uses now bounds the WHOLE body read, not each socket read. Drives that bound end
            // to end over a real socket from a real flush (the Response classes are unit-tested in isolation;
            // the ILP flush path - consumeChunkedResponse -> recv() - is covered here). Without the whole-read
            // bound the dribble would re-arm the per-read timeout forever and this test would hit its @Test
            // timeout.
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.dribble())) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1) // skip the build-time probe: only the flush hits the dribble
                        .httpTimeoutMillis(1_000)                    // the whole-body-read bound the no-arg recv() applies
                        .retryTimeoutMillis(0)                       // give up after the first aborted read, not retry to a deadline
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    long startNanos = System.nanoTime();
                    try {
                        sender.flush();
                        Assert.fail("expected the dribbled response-body read to abort the flush");
                    } catch (LineSenderException e) {
                        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                        // aborted on the ~1s whole-read bound. The mock dribbles for ~10s, so the old per-read
                        // re-arm behavior would not abort until ~11s (then the 30s @Test timeout); the < 5s
                        // ceiling fails on that path while giving the 1s bound generous CI headroom.
                        Assert.assertTrue("aborted too fast to be the 1s read bound: " + elapsedMillis + "ms", elapsedMillis >= 500);
                        Assert.assertTrue("aborted too slowly - re-armed per-read instead of bounding the whole read? " + elapsedMillis + "ms", elapsedMillis < 5_000);
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testServerAuthErrorBodyControlAndBidiAreEscaped() throws Exception {
        assertMemoryLeak(() -> {
            // a 401/403 body is echoed into the exception verbatim (read as raw bytes, not through the JSON
            // parser), so a hostile or proxied endpoint could splice raw control, ANSI or bidi chars straight
            // into the LineSenderException; the sender must escape them just like the JSON-field path
            String errorBody = "denied " + ESC + "[2J forged\n" + RLO + "moc.live";
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.chunkedJson(401, errorBody))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the server's auth error to surface as a LineSenderException");
                    } catch (LineSenderException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(msg, msg.contains("authentication error"));
                        Assert.assertTrue("visible text must be preserved: " + msg, msg.contains("denied"));
                        Assert.assertTrue("the ESC must be escaped: " + msg, msg.contains("\\u001b"));
                        Assert.assertTrue("the bidi override must be escaped: " + msg, msg.contains("\\u202e"));
                        Assert.assertFalse("a raw ESC must not leak: " + msg, msg.indexOf(0x1b) >= 0);
                        Assert.assertFalse("a raw newline must not leak: " + msg, msg.indexOf('\n') >= 0);
                        Assert.assertFalse("a raw bidi override must not leak: " + msg, msg.indexOf(0x202e) >= 0);
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testServerErrorStatusLineControlCharsAreEscaped() throws Exception {
        assertMemoryLeak(() -> {
            // the HTTP status-line token is echoed into the exception as "[http-status=...]". The header parser
            // copies it verbatim between the two spaces, so a hostile or proxied endpoint can smuggle control or
            // ANSI bytes there; a non-3-char token bypasses the numeric status checks and reaches the generic
            // error path, so the status render must escape them too, not just the body. A bidi override is a
            // multi-byte char the raw-response writer's US-ASCII encoding would drop, so this case uses an ESC;
            // the bidi cases above cover the body
            String body = "upstream error";
            // a malformed status code "400<ESC>[m" (6 chars, not 3) carries an ESC between the two spaces;
            // text/plain keeps it off the JSON parser, so it reaches the generic path that renders the status
            String rawResponse = "HTTP/1.1 400" + ESC + "[m FORGED\r\n"
                    + "Content-Type: text/plain\r\n"
                    + "Transfer-Encoding: chunked\r\n\r\n"
                    + Integer.toHexString(body.length()) + "\r\n" + body + "\r\n"
                    + "0\r\n\r\n";
            try (MockOidcServer server = new MockOidcServer((method, path, b) -> MockOidcServer.raw(rawResponse))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the server's error to surface as a LineSenderException");
                    } catch (LineSenderException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(msg, msg.contains("Could not flush buffer"));
                        // the ESC smuggled into the status token arrives escaped, never as a raw byte that
                        // could drive an ANSI terminal sequence
                        Assert.assertTrue("the status-line ESC must be escaped: " + msg, msg.contains("\\u001b"));
                        Assert.assertFalse("a raw ESC must not leak from the status line: " + msg, msg.indexOf(0x1b) >= 0);
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testServerJsonErrorBidiAndZeroWidthAreEscaped() throws Exception {
        assertMemoryLeak(() -> {
            // beyond C0 controls, a hostile or proxied endpoint can smuggle bidi overrides and zero-width
            // characters (as JSON \\uXXXX escapes the lexer decodes) that reorder or hide text in a terminal.
            // The sender must escape these too, matching the OIDC display sanitizer, so the rendered message
            // cannot be visually spoofed
            String errorBody = "{"
                    + "\"code\":\"invalid\","
                    + "\"message\":\"safe\\u202ehidden\\u200bend\","
                    + "\"line\":1,"
                    + "\"errorId\":\"E1\""
                    + "}";
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.chunkedJson(400, errorBody))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the server's JSON error to surface as a LineSenderException");
                    } catch (LineSenderException e) {
                        String msg = e.getMessage();
                        // the visible text survives, but the bidi override (U+202E) and the zero-width space
                        // (U+200B) arrive escaped, never as raw code points that could reorder or hide text
                        Assert.assertTrue("visible text must be preserved: " + msg, msg.contains("safe"));
                        Assert.assertTrue("visible text must be preserved: " + msg, msg.contains("hidden"));
                        Assert.assertTrue("the bidi override must be escaped: " + msg, msg.contains("\\u202e"));
                        Assert.assertTrue("the zero-width space must be escaped: " + msg, msg.contains("\\u200b"));
                        Assert.assertFalse("a raw bidi override must not leak: " + msg, msg.indexOf(0x202e) >= 0);
                        Assert.assertFalse("a raw zero-width space must not leak: " + msg, msg.indexOf(0x200b) >= 0);
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testServerJsonErrorControlCharsAreEscaped() throws Exception {
        assertMemoryLeak(() -> {
            // the server's error body carries control characters as JSON escapes: an ESC and a newline in
            // the message, and an ESC in the errorId. The lexer decodes them to real bytes, so the sender's
            // error rendering is what must neutralize them
            String errorBody = "{"
                    + "\"code\":\"invalid\","
                    + "\"message\":\"bad\\u001b[m\\nthing\","
                    + "\"line\":42,"
                    + "\"errorId\":\"E\\u001bID\""
                    + "}";
            // a chunked 400 with Content-Type application/json drives the flush failure through the sender's
            // JSON error parser (a 4xx response is asserted to be chunked before parsing)
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.chunkedJson(400, errorBody))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        // an explicit protocol version keeps build() from probing the server, so the only
                        // request is the flush below
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the server's JSON error to surface as a LineSenderException");
                    } catch (LineSenderException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(msg, msg.contains("Could not flush buffer"));
                        // the decoded message text survives...
                        Assert.assertTrue("decoded message text must be preserved: " + msg, msg.contains("bad"));
                        Assert.assertTrue("decoded message text must be preserved: " + msg, msg.contains("thing"));
                        Assert.assertTrue("errorId must be present with its ESC escaped: " + msg, msg.contains("id: E\\u001bID"));
                        // ...but no raw control byte reaches the message: no ESC (ANSI injection) and no
                        // newline (log-line forging); both arrive escaped instead
                        Assert.assertTrue("the decoded ESC must be escaped, not raw: " + msg, msg.contains("\\u001b"));
                        Assert.assertFalse("a raw ESC must not leak into the message: " + msg, msg.indexOf(0x1b) >= 0);
                        Assert.assertFalse("a raw newline must not leak into the message: " + msg, msg.indexOf('\n') >= 0);
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testServerMalformedJsonErrorBodyControlAndBidiAreEscaped() throws Exception {
        assertMemoryLeak(() -> {
            // a body sent as application/json but not parseable as a QuestDB error object (a proxy/WAF page,
            // or an unexpected first key) makes the JSON parser throw; the fallback renders the raw body, which
            // must still be escaped. The unexpected first key "forged" forces the parse failure; the ESC and
            // bidi override ride in the value and must surface escaped, not raw
            String errorBody = "{\"forged\":\"x " + ESC + "[2J y " + RLO + " z\"}";
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.chunkedJson(400, errorBody))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the malformed server response to surface as a LineSenderException");
                    } catch (LineSenderException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(msg, msg.contains("Could not flush buffer"));
                        // the raw body is shown (so the user can diagnose the unexpected response)...
                        Assert.assertTrue("the raw body must be preserved: " + msg, msg.contains("forged"));
                        // ...but the smuggled control and bidi chars arrive escaped, never raw
                        Assert.assertTrue("the ESC must be escaped: " + msg, msg.contains("\\u001b"));
                        Assert.assertTrue("the bidi override must be escaped: " + msg, msg.contains("\\u202e"));
                        Assert.assertFalse("a raw ESC must not leak: " + msg, msg.indexOf(0x1b) >= 0);
                        Assert.assertFalse("a raw bidi override must not leak: " + msg, msg.indexOf(0x202e) >= 0);
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testServerNonJsonErrorBodyControlCharsAreEscaped() throws Exception {
        assertMemoryLeak(() -> {
            // a proxy or WAF can return a non-JSON error body (here text/plain) with raw ANSI/control bytes;
            // it reaches the generic error path, which must escape them before they hit a log or terminal.
            // The body is all ASCII (a real ESC and a newline) so it survives the raw response writer's
            // US-ASCII encoding; bidi is covered by the auth/malformed cases above
            String body = "upstream down " + ESC + "[31m forged\nsecond line";
            // hand-craft a chunked text/plain response: the generic path only reads the body when chunked, and
            // a non-application/json content type keeps it off the JSON parser
            String rawResponse = "HTTP/1.1 400 Bad Request\r\n"
                    + "Content-Type: text/plain\r\n"
                    + "Transfer-Encoding: chunked\r\n\r\n"
                    + Integer.toHexString(body.length()) + "\r\n" + body + "\r\n"
                    + "0\r\n\r\n";
            try (MockOidcServer server = new MockOidcServer((method, path, b) -> MockOidcServer.raw(rawResponse))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the server's non-JSON error to surface as a LineSenderException");
                    } catch (LineSenderException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(msg, msg.contains("Could not flush buffer"));
                        Assert.assertTrue("visible text must be preserved: " + msg, msg.contains("upstream down"));
                        Assert.assertTrue("the ESC must be escaped: " + msg, msg.contains("\\u001b"));
                        Assert.assertFalse("a raw ESC must not leak: " + msg, msg.indexOf(0x1b) >= 0);
                        Assert.assertFalse("a raw newline must not leak: " + msg, msg.indexOf('\n') >= 0);
                    }
                }
            }
        });
    }
}
