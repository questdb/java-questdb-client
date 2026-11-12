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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Egress endpoint that advertises the given {@code SERVER_INFO} capabilities,
 * records every {@code QUERY_REQUEST} it receives, and answers each with the
 * frames a test-supplied {@link Responder} builds from it. A responder that
 * returns null drops the connection instead, the way a node that dies
 * mid-query does.
 */
public final class QwpEgressMockServer implements AutoCloseable {
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<QwpEgressFrames.QueryRequest> requests = new CopyOnWriteArrayList<>();
    private final TestWebSocketServer server;

    public QwpEgressMockServer(int capabilities, Responder responder) throws IOException, InterruptedException {
        server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                if (data.length == 0 || data[0] != QwpEgressMsgKind.QUERY_REQUEST) {
                    return;
                }
                byte[][] frames;
                try {
                    QwpEgressFrames.QueryRequest request = QwpEgressFrames.parseQueryRequest(data);
                    requests.add(request);
                    frames = responder.respond(request);
                } catch (Throwable th) {
                    // A broken request or responder must fail the test, not hang it.
                    failure.compareAndSet(null, th);
                    frames = null;
                }
                if (frames == null) {
                    // ClientHandler.close() joins this read thread, so close from another one
                    Thread dropper = new Thread(client::close, "mock-egress-drop");
                    dropper.setDaemon(true);
                    dropper.start();
                    return;
                }
                try {
                    for (byte[] frame : frames) {
                        client.sendBinary(frame);
                    }
                } catch (IOException e) {
                    // surfaces to the client as a transport error
                }
            }
        });
        server.setSendServerInfo(true);
        server.setCapabilities(capabilities);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
    }

    /**
     * Fails when parsing a request or building an answer threw on the server.
     */
    public void assertNoFailure() {
        Throwable th = failure.get();
        if (th != null) {
            throw new AssertionError("mock egress server failed", th);
        }
    }

    @Override
    public void close() {
        server.close();
    }

    public String getAddress() {
        return "localhost:" + server.getPort();
    }

    public List<QwpEgressFrames.QueryRequest> getRequests() {
        return requests;
    }

    @FunctionalInterface
    public interface Responder {
        /**
         * Returns the frames that answer {@code request}, or null to drop the
         * connection.
         */
        byte[][] respond(QwpEgressFrames.QueryRequest request);
    }
}
