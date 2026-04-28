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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pure-mapping tests for the wire-byte → category → policy classification used
 * by the cursor SF send loop's response handler. End-to-end DROP_AND_CONTINUE
 * vs HALT integration is exercised against a real QuestDB server (questdb
 * repo).
 */
public class CursorWebSocketSendLoopErrorClassificationTest {

    @Test
    public void testClassifySchemaMismatch() {
        Assert.assertEquals(SenderError.Category.SCHEMA_MISMATCH,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_SCHEMA_MISMATCH));
    }

    @Test
    public void testClassifyParseError() {
        Assert.assertEquals(SenderError.Category.PARSE_ERROR,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_PARSE_ERROR));
    }

    @Test
    public void testClassifyInternalError() {
        Assert.assertEquals(SenderError.Category.INTERNAL_ERROR,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_INTERNAL_ERROR));
    }

    @Test
    public void testClassifySecurityError() {
        Assert.assertEquals(SenderError.Category.SECURITY_ERROR,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_SECURITY_ERROR));
    }

    @Test
    public void testClassifyWriteError() {
        Assert.assertEquals(SenderError.Category.WRITE_ERROR,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_WRITE_ERROR));
    }

    @Test
    public void testClassifyUnknownStatusByte() {
        // Forward-compat: any byte the client doesn't recognize → UNKNOWN.
        // Don't crash, don't misclassify — let the policy resolver halt loudly.
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify((byte) 0x42));
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify((byte) 0xFF));
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify((byte) 0x7F));
    }

    @Test
    public void testDefaultPolicyDropForSchemaAndWriteErrors() {
        // Spec: server-side rejection that replay can't fix → drop the batch
        // and continue draining. Halting would block other tables on the
        // same connection.
        Assert.assertEquals(SenderError.Policy.DROP_AND_CONTINUE,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.SCHEMA_MISMATCH));
        Assert.assertEquals(SenderError.Policy.DROP_AND_CONTINUE,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.WRITE_ERROR));
    }

    @Test
    public void testDefaultPolicyHaltForBugCategoriesAndUnknown() {
        // Spec: PARSE_ERROR is a client bug; INTERNAL_ERROR is unspecified;
        // SECURITY_ERROR is misconfig; PROTOCOL_VIOLATION breaks the
        // connection; UNKNOWN is forward-compat conservatism. All halt.
        Assert.assertEquals(SenderError.Policy.HALT,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.PARSE_ERROR));
        Assert.assertEquals(SenderError.Policy.HALT,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.INTERNAL_ERROR));
        Assert.assertEquals(SenderError.Policy.HALT,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.SECURITY_ERROR));
        Assert.assertEquals(SenderError.Policy.HALT,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.PROTOCOL_VIOLATION));
        Assert.assertEquals(SenderError.Policy.HALT,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.UNKNOWN));
    }

    @Test
    public void testDefaultPolicyCoversEveryCategory() {
        // Defense against silent drift if a category is added without
        // updating defaultPolicyFor. The switch's default branch returns
        // HALT (forward-compat conservatism), so this also locks that in.
        for (SenderError.Category c : SenderError.Category.values()) {
            SenderError.Policy p = CursorWebSocketSendLoop.defaultPolicyFor(c);
            Assert.assertNotNull("default policy must be set for " + c, p);
        }
    }

    @Test
    public void testTerminalCloseCodes() {
        // Per spec § "WS close frames": these codes signal the server has
        // rejected the wire bytes themselves. Replay won't help; halt.
        Assert.assertTrue(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.PROTOCOL_ERROR));
        Assert.assertTrue(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.UNSUPPORTED_DATA));
        Assert.assertTrue(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.INVALID_PAYLOAD_DATA));
        Assert.assertTrue(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.POLICY_VIOLATION));
        Assert.assertTrue(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.MESSAGE_TOO_BIG));
        Assert.assertTrue(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.MANDATORY_EXTENSION));
    }

    @Test
    public void testReconnectEligibleCloseCodes() {
        // Normal/abnormal disconnects: server didn't reject the wire bytes,
        // it just went away. Reconnect retry loop should pick up — these must
        // NOT be classified terminal.
        Assert.assertFalse(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.NORMAL_CLOSURE));
        Assert.assertFalse(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.GOING_AWAY));
        Assert.assertFalse(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.NO_STATUS_RECEIVED));
        Assert.assertFalse(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.ABNORMAL_CLOSURE));
        Assert.assertFalse(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.INTERNAL_ERROR));
        Assert.assertFalse(CursorWebSocketSendLoop.isTerminalCloseCode(WebSocketCloseCode.TLS_HANDSHAKE));
        // Application-defined and library-defined close codes default to
        // "reconnect-eligible" — server hasn't given us a reasoned
        // rejection of payload bytes.
        Assert.assertFalse(CursorWebSocketSendLoop.isTerminalCloseCode(3000));
        Assert.assertFalse(CursorWebSocketSendLoop.isTerminalCloseCode(4001));
    }

    @Test
    public void testStatusOkAndDurableAckAreNotErrorCategories() {
        // STATUS_OK and STATUS_DURABLE_ACK are not error codes — but if
        // classify() were ever called on them (e.g. by a future caller
        // bypassing the success branch), it must not pretend they're real
        // categories. Under the current mapping they fall through to
        // UNKNOWN, which preserves halt-on-confusion semantics.
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_OK));
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_DURABLE_ACK));
    }
}
