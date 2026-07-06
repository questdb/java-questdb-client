/*******************************************************************************
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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression: once the cursor I/O loop has recorded a terminal error,
 * the next public Sender API call must surface it. Pre-fix the
 * row-level entry points ({@code table}, {@code stringColumn},
 * {@code longColumn}, {@code atNow}, etc.) only ran {@code checkNotClosed}
 * → {@code checkConnectionError}, and {@code connectionError} was never
 * populated (the {@code recordConnectionFailure} method was defined but
 * never called). So callers could keep accumulating rows into the
 * encoder long after the I/O thread had gone terminal — the error
 * surfaced only on the eventual {@code flush()} or {@code close()}.
 * <p>
 * Public API methods must surface I/O thread failures on the very next
 * call so the caller sees the failure as close as possible to its root
 * cause, not at an arbitrary later point.
 * <p>
 * Note: the fixture uses {@link WebSocketResponse#STATUS_PARSE_ERROR}
 * (TERMINAL-policy). Only TERMINAL records a terminal error; RETRIABLE
 * statuses (e.g. {@code STATUS_WRITE_ERROR}) recycle the connection and
 * replay, so the test's "next call throws" contract is specifically the
 * TERMINAL contract.
 */
public class IoThreadErrorSurfacedOnRowApiTest {

    @Test
    public void testRowApiMethodSurfacesIoThreadTerminalError() throws Exception {
        ErrorAckHandler handler = new ErrorAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                // Batch 1: produces a frame the server rejects with
                // STATUS_PARSE_ERROR. The cursor I/O loop's response
                // handler routes the rejection through recordFatal, marking
                // the loop terminal.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                // Wait for the I/O thread to record the error. After this,
                // cursorSendLoop.terminalError is populated and the loop has
                // exited.
                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                long deadline = System.currentTimeMillis() + 3_000L;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        wss.flush();
                    } catch (LineSenderException expected) {
                        break;
                    }
                    Thread.sleep(20);
                }

                // The next row-level API call must surface the terminal
                // failure — not silently accept the row and defer the
                // throw to the next flush().
                LineSenderException thrown = null;
                try {
                    sender.table("foo");
                } catch (LineSenderException e) {
                    thrown = e;
                }
                Assert.assertNotNull(
                        "table() must surface the I/O thread terminal failure "
                                + "instead of accepting more rows after the "
                                + "loop has gone fatal",
                        thrown);
                Assert.assertTrue(
                        "exception should reflect the underlying server "
                                + "rejection; got: " + thrown.getMessage(),
                        thrown.getMessage() != null
                                && (thrown.getMessage().contains("rejected")
                                    || thrown.getMessage().contains("error")
                                    || thrown.getMessage().contains("terminal")));
            } catch (LineSenderException expectedOnClose) {
                // Sender close may also surface the same error; that's fine.
            }
        }
    }

    /** Returns STATUS_PARSE_ERROR (HALT-policy) for every received frame. */
    private static class ErrorAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildErrorAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // status u8 | seq u64 | msgLen u16 | msg UTF-8
        private static byte[] buildErrorAck(long seq) {
            byte[] msg = "parse error".getBytes(StandardCharsets.UTF_8);
            byte[] buf = new byte[1 + 8 + 2 + msg.length];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put(WebSocketResponse.STATUS_PARSE_ERROR);
            bb.putLong(seq);
            bb.putShort((short) msg.length);
            bb.put(msg);
            return buf;
        }
    }
}
