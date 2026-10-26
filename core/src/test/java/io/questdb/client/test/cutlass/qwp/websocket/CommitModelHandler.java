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

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link TestWebSocketServer} handler that answers like a real server by
 * driving a {@link QwpServerModel}: each WebSocket client becomes one model
 * connection, opened on its first message, and each binary message is one frame.
 * The handler writes the model's responses back with
 * {@link TestWebSocketServer.ClientHandler#sendBinary}. It records no socket
 * writes: it sees WebSocket messages, not the client's writes.
 */
public final class CommitModelHandler implements TestWebSocketServer.WebSocketServerHandler {
    private final Map<TestWebSocketServer.ClientHandler, Integer> connections = new IdentityHashMap<>();
    private final QwpServerModel model;

    public CommitModelHandler(QwpServerModel model) {
        this.model = model;
    }

    @Override
    public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
        List<byte[]> responses = model.receive(connectionOf(client), data);
        try {
            for (int i = 0, n = responses.size(); i < n; i++) {
                client.sendBinary(responses.get(i));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int connectionOf(TestWebSocketServer.ClientHandler client) {
        synchronized (connections) {
            Integer connection = connections.get(client);
            if (connection == null) {
                connection = model.openConnection();
                connections.put(client, connection);
            }
            return connection;
        }
    }
}
