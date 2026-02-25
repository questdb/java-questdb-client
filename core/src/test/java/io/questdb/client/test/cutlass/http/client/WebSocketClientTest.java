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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketSendBuffer;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class WebSocketClientTest {

    @Test
    public void testSendCloseFrameDoesNotClobberSendBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (StubWebSocketClient client = new StubWebSocketClient()) {
                WebSocketSendBuffer sendBuffer = client.getSendBuffer();

                // User starts building a data frame
                sendBuffer.beginFrame();
                sendBuffer.putLong(0xDEADBEEFL);
                int posBeforeClose = sendBuffer.getWritePos();
                Assert.assertTrue("sendBuffer should have data", posBeforeClose > 0);

                // sendCloseFrame() should use controlFrameBuffer, not sendBuffer
                try {
                    client.sendCloseFrame(1000, null, 1000);
                } catch (HttpClientException ignored) {
                    // Expected: doSend() fails because there's no real socket
                }

                // Verify sendBuffer was NOT clobbered
                Assert.assertEquals(
                        "sendCloseFrame() must not reset the main sendBuffer",
                        posBeforeClose,
                        sendBuffer.getWritePos()
                );
            }
        });
    }

    @Test
    public void testSendPingDoesNotClobberSendBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (StubWebSocketClient client = new StubWebSocketClient()) {
                // Set upgraded=true so checkConnected() passes
                setField(client, "upgraded", true);

                WebSocketSendBuffer sendBuffer = client.getSendBuffer();

                // User starts building a data frame
                sendBuffer.beginFrame();
                sendBuffer.putLong(0xCAFEBABEL);
                int posBeforePing = sendBuffer.getWritePos();
                Assert.assertTrue("sendBuffer should have data", posBeforePing > 0);

                // sendPing() should use controlFrameBuffer, not sendBuffer
                try {
                    client.sendPing(1000);
                } catch (HttpClientException ignored) {
                    // Expected: doSend() fails because there's no real socket
                }

                // Verify sendBuffer was NOT clobbered
                Assert.assertEquals(
                        "sendPing() must not reset the main sendBuffer",
                        posBeforePing,
                        sendBuffer.getWritePos()
                );
            }
        });
    }

    private static void setField(Object obj, String fieldName, Object value) throws Exception {
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(obj, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    /**
     * Minimal concrete WebSocketClient that throws on any I/O,
     * allowing us to test buffer management without a real socket.
     */
    private static class StubWebSocketClient extends WebSocketClient {

        StubWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        protected void ioWait(int timeout, int op) {
            throw new HttpClientException("stub: no socket");
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }
}
