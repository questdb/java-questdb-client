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

    @Test(timeout = 30_000)
    public void testARetryableStatusFromTheSenderIsRetryable() throws Exception {
        assertMemoryLeak(() -> {
            // The other direction, and the one that carries the risk. assertFalse cannot tell a CLASSIFIED
            // "permanent" from an UNCLASSIFIED exception, because the three constructors that carry no
            // classification also report false - so the 401 test above passes just as happily against a
            // sender that stopped classifying altogether. Only a true here proves the flag is computed and
            // survives the throw.
            //
            // A 503 exhausts the retry budget and then throws with retryable=true. Chunked, because flush0
            // asserts response.isChunked() on the error branch and the plain helper writes Content-Length.
            final String errorBody = "{\"code\":\"unavailable\"}";
            final String chunked503 = "HTTP/1.1 503 Service Unavailable\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Transfer-Encoding: chunked\r\n\r\n"
                    + Integer.toHexString(errorBody.length()) + "\r\n" + errorBody + "\r\n0\r\n\r\n";
            try (MockOidcServer server = new MockOidcServer((method, path, body) ->
                    MockOidcServer.raw(chunked503))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .httpTimeoutMillis(1_000)
                        .retryTimeoutMillis(100)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the 503 to surface once the retry budget is spent");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("503"));
                        Assert.assertTrue("a 503 is transient; a caller told to close or reset() on it would "
                                        + "tear down a healthy sender and drop the buffered batch: "
                                        + e.getMessage(),
                                e.isRetryable());
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testATransportFailureFromTheSenderIsRetryable() throws Exception {
        assertMemoryLeak(() -> {
            // The sender's other retryable=true site: the give-up throw after the retry budget is spent on a
            // transport error rather than a status. It reaches the caller through a different constructor
            // call than the status path, so it needs its own assertion.
            final int deadPort;
            try (java.net.ServerSocket probe = new java.net.ServerSocket(0, 1,
                    java.net.InetAddress.getLoopbackAddress())) {
                deadPort = probe.getLocalPort();
            }
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("127.0.0.1:" + deadPort)
                    .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                    .httpTimeoutMillis(1_000)
                    .retryTimeoutMillis(100)
                    .disableAutoFlush()
                    .build()) {
                sender.table("t").longColumn("v", 1L).atNow();
                try {
                    sender.flush();
                    Assert.fail("expected the unreachable endpoint to surface");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("Connection Failed"));
                    Assert.assertTrue("a transport failure is transient by definition: " + e.getMessage(),
                            e.isRetryable());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testAnUnreadableRetryableBodyStaysRetryable() throws Exception {
        assertMemoryLeak(() -> {
            // The wrapper path, and the direction that carries the risk on it. Every other test here reaches
            // throwOnHttpErrorResponse0, which builds its exception beside the body it just read. When that
            // read ABORTS - a dribbled body, a peer that vanished, a mangled chunk - the outer
            // throwOnHttpErrorResponse catches it and builds a different exception from the status alone,
            // re-passing `retryable` by hand. Nothing pinned that hand-off, so hardcoding it either way was
            // green.
            //
            // A 503 whose body dribbles: flush0 retries on the status until retryTimeoutMillis is spent (the
            // head arrives promptly each time, so those passes are fast), then reads the body to build the
            // message and aborts on the whole-read bound. Told this was permanent, a caller following the
            // documented advice closes or reset()s a healthy sender and drops the buffered batch over a fault
            // that was going to clear.
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.dribble(503))) {
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("127.0.0.1:" + server.port())
                        .protocolVersion(Sender.PROTOCOL_VERSION_V1)
                        .httpTimeoutMillis(1_000)
                        .retryTimeoutMillis(100)
                        .disableAutoFlush()
                        .build()) {
                    sender.table("t").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the 503 to surface once the retry budget is spent");
                    } catch (LineSenderException e) {
                        String msg = e.getMessage();
                        // the wrapper's own shape, so a later refactor that routes this through the
                        // body-reading path instead cannot satisfy the assertion below by accident
                        Assert.assertTrue("expected the unreadable-body wrapper: " + msg,
                                msg.contains("could not read the error response body"));
                        Assert.assertTrue("the real status must survive the wrapper: " + msg,
                                msg.contains("http-status=503"));
                        Assert.assertTrue("a 503 is transient however unreadable its body: a caller told to "
                                        + "close or reset() on it tears down a healthy sender and drops the "
                                        + "buffered batch: " + msg,
                                e.isRetryable());
                    }
                }
            }
        });
    }
}
