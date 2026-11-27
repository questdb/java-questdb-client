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

package io.questdb.client.test.cutlass.qwp.websocket;

import io.questdb.client.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins what a client observes when a {@link TestWebSocketServer.WebSocketServerHandler} drops the connection
 * from inside its own callback -- the "send the acks, then {@code client.close()}" script that reconnect,
 * failover and drainer tests use to model a mid-stream wire drop.
 * <p>
 * Every frame the handler wrote before the drop must reach the client, followed by an orderly end of stream.
 * The condition that breaks this is ordinary: the client still has frames in flight. They sit unread in the
 * server's receive queue when the handler drops, and closing a socket with unread input aborts the connection
 * with an RST, which also discards whatever the kernel has not transmitted yet -- the frames the handler wrote
 * a moment earlier. The client then sees "connection reset" instead of the acks, so "ack N frames, then drop"
 * quietly becomes "drop", and anything that depends on those acks (a drainer's progress-based escalation
 * budget, for one) fails at random.
 * <p>
 * The test forces both halves of that race rather than waiting for the scheduler to produce them: the trailing
 * client frame is larger than one server read, so its tail is still unread when the handler acts, and the
 * filler ahead of the ack is larger than the client's receive window, so the ack is still queued on the server
 * at the moment of the drop.
 */
public class TestWebSocketServerHandlerDropTest {

    private static final byte[] ACK = "ack-written-before-drop".getBytes(StandardCharsets.US_ASCII);
    // Applied before connect(), so it caps the window the client advertises.
    private static final int CLIENT_RECEIVE_BUFFER_BYTES = 4096;
    // Several times the client's receive window: most of it, and the ack queued behind it, has not left the
    // server when the handler drops.
    private static final int FILLER_BYTES = 32 * 1024;
    // Several times the server's 8 KiB socket read, so the frame that triggers the drop is dispatched while the
    // tail of this one is still unread in the server's receive queue.
    private static final int TRAILING_FRAME_BYTES = 32 * 1024;

    @Test
    public void testFramesWrittenBeforeHandlerDropReachClientAheadOfEndOfStream() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final byte[] filler = new byte[FILLER_BYTES];
            Arrays.fill(filler, (byte) 'f');
            TestWebSocketServer.WebSocketServerHandler handler = new TestWebSocketServer.WebSocketServerHandler() {
                private boolean dropped;

                @Override
                public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                    if (dropped) {
                        return;
                    }
                    dropped = true;
                    try {
                        client.sendBinary(filler);
                        client.sendBinary(ACK);
                    } catch (IOException e) {
                        throw new AssertionError("server could not queue its frames", e);
                    }
                    client.close(); // the scripted mid-stream wire drop
                }
            };
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (Socket socket = new Socket()) {
                    socket.setReceiveBufferSize(CLIENT_RECEIVE_BUFFER_BYTES);
                    socket.setSoTimeout(10_000);
                    socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getPort()), 5_000);
                    OutputStream out = socket.getOutputStream();
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    upgrade(out, in);

                    // A single write, so both frames are on the wire before the server reads anything.
                    ByteArrayOutputStream frames = new ByteArrayOutputStream();
                    writeMaskedBinaryFrame(frames, new byte[]{1});
                    writeMaskedBinaryFrame(frames, new byte[TRAILING_FRAME_BYTES]);
                    out.write(frames.toByteArray());
                    out.flush();

                    int framesReceived = 0;
                    try {
                        assertFrame(in, filler, "filler");
                        framesReceived++;
                        assertFrame(in, ACK, "ack");
                        framesReceived++;
                        assertEquals("the drop must end the stream right after the frames written before it",
                                -1, in.read());
                    } catch (IOException e) {
                        fail("the handler's drop lost frames it had written before dropping [framesReceived="
                                + framesReceived + " of 2]: " + e);
                    }
                }
            }
        });
    }

    private static void assertFrame(DataInputStream in, byte[] expectedPayload, String what) throws IOException {
        assertEquals(what + ": FIN + BINARY", 0x80 | WebSocketOpcode.BINARY, in.readUnsignedByte());
        int lengthByte = in.readUnsignedByte();
        assertEquals(what + ": server frames are unmasked", 0, lengthByte & 0x80);
        int length = lengthByte & 0x7F;
        if (length == 126) {
            length = in.readUnsignedShort();
        } else if (length == 127) {
            long longLength = in.readLong();
            assertTrue(what + ": implausible frame length " + longLength, longLength <= Integer.MAX_VALUE);
            length = (int) longLength;
        }
        assertEquals(what + ": payload length", expectedPayload.length, length);
        byte[] payload = new byte[length];
        in.readFully(payload);
        assertArrayEquals(what + ": payload", expectedPayload, payload);
    }

    private static void upgrade(OutputStream out, DataInputStream in) throws IOException {
        String request = "GET /write/v4 HTTP/1.1\r\n"
                + "Host: localhost\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + "\r\n";
        out.write(request.getBytes(StandardCharsets.US_ASCII));
        out.flush();
        StringBuilder response = new StringBuilder();
        while (response.indexOf("\r\n\r\n") < 0) {
            int b = in.read();
            if (b < 0) {
                fail("server closed the connection during the upgrade [response=" + response + ']');
            }
            response.append((char) b);
        }
        assertTrue("upgrade rejected: " + response, response.toString().startsWith("HTTP/1.1 101"));
    }

    private static void writeMaskedBinaryFrame(ByteArrayOutputStream out, byte[] payload) {
        int length = payload.length;
        out.write(0x80 | WebSocketOpcode.BINARY);
        if (length <= 125) {
            out.write(0x80 | length);
        } else if (length <= 0xFFFF) {
            out.write(0x80 | 126);
            out.write(length >>> 8);
            out.write(length & 0xFF);
        } else {
            throw new IllegalArgumentException("frame too large for this helper: " + length);
        }
        // Client frames must be masked; an all-zero key leaves the payload bytes unchanged.
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(payload, 0, length);
    }
}
