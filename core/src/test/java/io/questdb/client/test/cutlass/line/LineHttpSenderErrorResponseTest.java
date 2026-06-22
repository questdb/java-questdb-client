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
 * Verifies that the JSON error body a QuestDB HTTP endpoint returns on a failed flush is rendered
 * safely into the {@link LineSenderException} message. The JSON lexer resolves string escapes, so a
 * {@code message} or {@code errorId} field arrives fully decoded; a hostile or proxied endpoint could
 * otherwise smuggle real control characters or ANSI escapes that forge a log line or rewrite a
 * terminal when the exception text is printed. The sender must escape them, just as it does for column
 * names in an error message.
 */
public class LineHttpSenderErrorResponseTest {

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
                        Assert.assertFalse("a raw ESC must not leak into the message: " + msg, msg.indexOf('\u001b') >= 0);
                        Assert.assertFalse("a raw newline must not leak into the message: " + msg, msg.indexOf('\n') >= 0);
                    }
                }
            }
        });
    }
}
