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
 * Covers {@link LineSenderException#isRetryable()}.
 * <p>
 * The class documentation tells a caller to act on exactly this distinction - retry {@code flush()} on a
 * transient failure, close or {@code reset()} on a permanent one - and the sender already computes the
 * answer for every failure it raises. The two-argument constructor accepted that classification and then
 * dropped it on the floor, so the advice was unactionable: six call sites passed a flag no caller could
 * read.
 */
public class LineSenderExceptionRetryableTest {

    @Test
    public void testConstructorsWithoutAClassificationReportNotRetryable() {
        // false means "not classified as retryable", not "proven permanent". These constructors carry no
        // classification at all, and false is the conservative direction for a caller that retries only
        // on true - it stops rather than spins.
        Assert.assertFalse(new LineSenderException("boom").isRetryable());
        Assert.assertFalse(new LineSenderException(new RuntimeException("boom")).isRetryable());
        Assert.assertFalse(new LineSenderException("boom", new RuntimeException("boom")).isRetryable());
    }

    @Test
    public void testExplicitClassificationSurvivesConstruction() {
        Assert.assertTrue(new LineSenderException("transient", true).isRetryable());
        Assert.assertFalse(new LineSenderException("permanent", false).isRetryable());
        // the flag must survive the fluent message building every call site does after construction
        LineSenderException built = new LineSenderException("transient", true)
                .put(" [http-status=").put(503).put(']');
        Assert.assertTrue("building the message must not lose the classification", built.isRetryable());
    }

    @Test(timeout = 30_000)
    public void testADefinitiveStatusFromTheSenderIsNotRetryable() throws Exception {
        assertMemoryLeak(() -> {
            // End to end, through the sender's own classification rather than a hand-built exception: a 401
            // is definitive, so a caller must be able to tell it from a 503 and stop instead of re-flushing
            // into an endpoint that will keep refusing.
            // A CHUNKED 401: flush0 asserts response.isChunked() on the error branch, and MockOidcServer.json
            // writes a Content-Length body, so the plain helper trips that assert (with -ea on) before the
            // classification is ever reached.
            final String errorBody = "{\"code\":\"unauthorized\"}";
            final String chunked401 = "HTTP/1.1 401 Unauthorized\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Transfer-Encoding: chunked\r\n\r\n"
                    + Integer.toHexString(errorBody.length()) + "\r\n" + errorBody + "\r\n0\r\n\r\n";
            try (MockOidcServer server = new MockOidcServer((method, path, body) ->
                    MockOidcServer.raw(chunked401))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .httpTimeoutMillis(1_000)
                        .retryTimeoutMillis(1_000)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the 401 to surface");
                    } catch (LineSenderException e) {
                        Assert.assertFalse("a 401 is definitive; a caller told to retry on it would spin "
                                + "against an endpoint that keeps refusing: " + e.getMessage(),
                                e.isRetryable());
                    }
                }
            }
        });
    }
}
