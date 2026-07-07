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

package io.questdb.client.test;

import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Builder-level validation for the SenderError API knobs. Doesn't actually
 * connect — only verifies that parsing, validation, and the per-protocol
 * gating throws the right exceptions.
 */
public class SenderBuilderErrorApiTest {

    @Test
    public void testConnectStringParsesErrorInboxCapacity() {
        // Lazy verification: pinning that the connect string accepts the key
        // without complaining; we don't attempt to connect.
        // build() will fail on the connect step, but parse should succeed
        // first.
        try {
            Sender.builder("ws::addr=127.0.0.1:1;error_inbox_capacity=512;").build().close();
            Assert.fail("expected LineSenderException from connect attempt");
        } catch (LineSenderException expected) {
            // Failed on connect, NOT on connect-string parse — different
            // failure mode. Verify it's not a parse complaint.
            String msg = expected.getMessage();
            Assert.assertFalse("error_inbox_capacity must parse: " + msg,
                    msg.toLowerCase().contains("error_inbox_capacity"));
        }
    }

    @Test
    public void testConnectStringRejectsBadInboxCapacity() {
        // Any non-int value must surface a parse error referencing the key.
        try {
            Sender.builder("ws::addr=127.0.0.1:1;error_inbox_capacity=NaN;").build().close();
            Assert.fail("expected LineSenderException for non-numeric capacity");
        } catch (LineSenderException expected) {
            Assert.assertTrue("expected parse complaint about error_inbox_capacity: "
                            + expected.getMessage(),
                    expected.getMessage().contains("error_inbox_capacity"));
        }
    }

    @Test
    public void testConnectStringRejectsInboxCapacityOnNonWebSocket() {
        // Spec: dispatcher knobs are WebSocket-only.
        try {
            Sender.builder("http::addr=127.0.0.1:1;error_inbox_capacity=10;").build().close();
            Assert.fail("expected LineSenderException — http transport rejects error_inbox_capacity");
        } catch (LineSenderException expected) {
            Assert.assertTrue("expected WebSocket-only complaint: " + expected.getMessage(),
                    expected.getMessage().contains("error_inbox_capacity"));
        }
    }

    @Test
    public void testErrorHandlerRejectedOnNonWebSocketProtocol() {
        SenderErrorHandler h = err -> { /* no-op */ };
        try {
            Sender.builder(Sender.Transport.HTTP).address("127.0.0.1:1").errorHandler(h);
            Assert.fail("expected LineSenderException");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage().contains("error_handler"));
            Assert.assertTrue(expected.getMessage().contains("WebSocket"));
        }
    }

    @Test
    public void testErrorInboxCapacityRejectsBelowSpecFloor() {
        // sf-client.md section 4.4: minimum is 16. Values below the floor
        // (including 0 and negative) must surface the floor in the message
        // so users can fix their configuration.
        int[] rejected = {-5, 0, 1, 2, 15};
        for (int v : rejected) {
            try {
                Sender.builder(Sender.Transport.WEBSOCKET).errorInboxCapacity(v);
                Assert.fail("capacity " + v + " must be rejected; spec floor is 16");
            } catch (LineSenderException expected) {
                Assert.assertTrue("missing key in message: " + expected.getMessage(),
                        expected.getMessage().contains("error_inbox_capacity"));
                Assert.assertTrue("missing floor (16) in message: " + expected.getMessage(),
                        expected.getMessage().contains(">= 16"));
            }
        }
        // The floor itself is accepted.
        Sender.builder(Sender.Transport.WEBSOCKET).errorInboxCapacity(16);
    }

    @Test
    public void testConnectStringRejectsErrorInboxCapacityBelowFloor() {
        // The connect-string path delegates to the builder, so the floor
        // applies there too. A value below 16 must surface the floor.
        try {
            Sender.builder("ws::addr=127.0.0.1:1;error_inbox_capacity=8;").build().close();
            Assert.fail("capacity 8 must be rejected; spec floor is 16");
        } catch (LineSenderException expected) {
            Assert.assertTrue("missing key in message: " + expected.getMessage(),
                    expected.getMessage().contains("error_inbox_capacity"));
            Assert.assertTrue("missing floor (16) in message: " + expected.getMessage(),
                    expected.getMessage().contains(">= 16"));
        }
    }

    @Test
    public void testErrorInboxCapacityRejectedOnNonWebSocketProtocol() {
        try {
            Sender.builder(Sender.Transport.HTTP).address("127.0.0.1:1").errorInboxCapacity(100);
            Assert.fail("expected LineSenderException");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage().contains("error_inbox_capacity"));
            Assert.assertTrue(expected.getMessage().contains("WebSocket"));
        }
    }

    @Test
    public void testNullHandlerIsAcceptedAsResetSignal() {
        // Passing null on the builder must NOT throw; spec says null
        // resets to the default handler. Builder-level setter accepts;
        // sender setter (called from connect) interprets null → default.
        Sender.builder(Sender.Transport.WEBSOCKET).errorHandler(null);
        // (no exception expected)
    }

    @Test
    public void testWebSocketBuilderAcceptsErrorHandler() {
        // Sanity: WebSocket protocol allows the setter; setter is fluent
        // and returns the same builder.
        Sender.LineSenderBuilder b = Sender.builder(Sender.Transport.WEBSOCKET)
                .address("127.0.0.1:1")
                .errorHandler(err -> { /* no-op */ })
                .errorInboxCapacity(64)
                .connectionListener(ev -> { /* no-op */ })
                .connectionListenerInboxCapacity(64);
        Assert.assertNotNull(b);
    }

    @Test
    public void testConnectionListenerInboxCapacityRejectsZeroAndNegative() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).connectionListenerInboxCapacity(0);
            Assert.fail("zero capacity must be rejected");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage().contains("connection_listener_inbox_capacity"));
            Assert.assertTrue(expected.getMessage().contains(">="));
        }
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).connectionListenerInboxCapacity(-5);
            Assert.fail("negative capacity must be rejected");
        } catch (LineSenderException expected) {
            // ok
        }
    }

    @Test
    public void testConnectionListenerInboxCapacityRejectedOnNonWebSocketProtocol() {
        try {
            Sender.builder(Sender.Transport.HTTP).address("127.0.0.1:1").connectionListenerInboxCapacity(100);
            Assert.fail("expected LineSenderException");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage().contains("connection_listener_inbox_capacity"));
            Assert.assertTrue(expected.getMessage().contains("WebSocket"));
        }
    }

    @Test
    public void testConnectStringParsesConnectionListenerInboxCapacity() {
        // Parse must accept the key without complaining; build() will fail
        // on the connect step against an unreachable port, but that's a
        // different failure mode from a connect-string parse error.
        try {
            Sender.builder("ws::addr=127.0.0.1:1;connection_listener_inbox_capacity=512;").build().close();
            Assert.fail("expected LineSenderException from connect attempt");
        } catch (LineSenderException expected) {
            String msg = expected.getMessage();
            Assert.assertFalse("connection_listener_inbox_capacity must parse: " + msg,
                    msg.toLowerCase().contains("connection_listener_inbox_capacity"));
        }
    }

    @Test
    public void testConnectStringRejectsBadConnectionListenerInboxCapacity() {
        try {
            Sender.builder("ws::addr=127.0.0.1:1;connection_listener_inbox_capacity=NaN;").build().close();
            Assert.fail("expected LineSenderException for non-numeric capacity");
        } catch (LineSenderException expected) {
            Assert.assertTrue("expected parse complaint about connection_listener_inbox_capacity: "
                            + expected.getMessage(),
                    expected.getMessage().contains("connection_listener_inbox_capacity"));
        }
    }

    @Test
    public void testConnectStringRejectsConnectionListenerInboxCapacityOnNonWebSocket() {
        try {
            Sender.builder("http::addr=127.0.0.1:1;connection_listener_inbox_capacity=10;").build().close();
            Assert.fail("expected LineSenderException — http transport rejects connection_listener_inbox_capacity");
        } catch (LineSenderException expected) {
            Assert.assertTrue("expected WebSocket-only complaint: " + expected.getMessage(),
                    expected.getMessage().contains("connection_listener_inbox_capacity"));
        }
    }

    @Test
    public void testCategoryAndPolicyAreStillEnumerable() {
        // Cross-check that the enum surface is fully reachable from
        // user-side code via the builder import path.
        SenderError.Category c = SenderError.Category.SCHEMA_MISMATCH;
        SenderError.Policy p = SenderError.Policy.RETRIABLE;
        Assert.assertNotNull(c);
        Assert.assertNotNull(p);
    }
}
