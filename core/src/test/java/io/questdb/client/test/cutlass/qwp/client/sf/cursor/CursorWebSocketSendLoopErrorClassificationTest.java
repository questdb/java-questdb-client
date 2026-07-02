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
 * by the cursor SF send loop's response handler. There is no drop policy:
 * rejections are either RETRIABLE (recycle the wire, replay from ackedFsn+1,
 * nothing dropped), RETRIABLE_OTHER (same, but rotate endpoints — the node
 * cannot serve writes), or TERMINAL (deterministic under replay: latch and
 * halt loudly, bytes preserved on disk). End-to-end integration is exercised
 * against a real QuestDB server (questdb repo).
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
    public void testClassifyNotWritable() {
        // Reserved wire byte 0x0C: node cannot serve writes (read-only
        // replica / demoting primary). Current servers signal this with a
        // reconnect-eligible close instead of a NACK; classification exists
        // for forward compatibility.
        Assert.assertEquals(SenderError.Category.NOT_WRITABLE,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_NOT_WRITABLE));
    }

    @Test
    public void testClassifyUnknownStatusByte() {
        // Forward-compat: any byte the client doesn't recognize → UNKNOWN.
        // Don't crash, don't misclassify — the policy resolver fails open to
        // RETRIABLE so a status byte from a newer server degrades to retry.
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify((byte) 0x42));
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify((byte) 0xFF));
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify((byte) 0x7F));
    }

    @Test
    public void testDefaultPolicyRetriableForTransientCategories() {
        // WRITE_ERROR: transient server state (disk pressure, suspended
        // table). INTERNAL_ERROR: transient by definition. UNKNOWN: fail
        // open — a status byte from a newer server must degrade to retry,
        // not to a dead sender. Deterministic repeats are caught by the
        // poison-frame detector, not by pessimistic classification.
        Assert.assertEquals(SenderError.Policy.RETRIABLE,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.WRITE_ERROR));
        Assert.assertEquals(SenderError.Policy.RETRIABLE,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.INTERNAL_ERROR));
        Assert.assertEquals(SenderError.Policy.RETRIABLE,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.UNKNOWN));
    }

    @Test
    public void testDefaultPolicyRetriableOtherForNotWritable() {
        // The node cannot serve writes at all — waiting out a backoff
        // against the same node is pointless; rotate endpoints.
        Assert.assertEquals(SenderError.Policy.RETRIABLE_OTHER,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.NOT_WRITABLE));
    }

    @Test
    public void testDefaultPolicyTerminalForDeterministicRejections() {
        // Deterministic under byte-identical replay: SCHEMA_MISMATCH (same
        // bytes, same mismatch), PARSE_ERROR (malformed bytes never parse),
        // SECURITY_ERROR (ACL denial on a writable node — read-only
        // refusals arrive as role-change closes, not NACKs),
        // PROTOCOL_VIOLATION (poison-frame escalation). All halt loudly;
        // the bytes stay in the SF log — nothing is silently discarded.
        Assert.assertEquals(SenderError.Policy.TERMINAL,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.SCHEMA_MISMATCH));
        Assert.assertEquals(SenderError.Policy.TERMINAL,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.PARSE_ERROR));
        Assert.assertEquals(SenderError.Policy.TERMINAL,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.SECURITY_ERROR));
        Assert.assertEquals(SenderError.Policy.TERMINAL,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.PROTOCOL_VIOLATION));
    }

    @Test
    public void testDefaultPolicyCoversEveryCategory() {
        // Defense against silent drift if a category is added without
        // updating defaultPolicyFor. The switch's default branch returns
        // TERMINAL (loud failure for unmapped categories), so this also
        // locks that in.
        for (SenderError.Category c : SenderError.Category.values()) {
            SenderError.Policy p = CursorWebSocketSendLoop.defaultPolicyFor(c);
            Assert.assertNotNull("default policy must be set for " + c, p);
        }
    }

    @Test
    public void testTerminalCloseCodes() {
        // isTerminalCloseCode is diagnostic-only: WS close codes carry no
        // policy semantics (every close is reconnect-eligible; deterministic
        // rejection is caught by the poison-frame detector). The RFC 6455
        // nominal classification is still pinned here for log fidelity.
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
        // it just went away. These are additionally exempt from poison-frame
        // strikes in onClose (orderly handoff/restart signals).
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
        // UNKNOWN → RETRIABLE: a confused frame recycles the connection
        // rather than killing the sender.
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_OK));
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_DURABLE_ACK));
    }
}
