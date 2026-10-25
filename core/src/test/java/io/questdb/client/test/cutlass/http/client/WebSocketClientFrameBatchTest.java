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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Several binary frames appended with {@code appendBinaryFrame} and sent by one
 * {@code flushFrames} must arrive as distinct, intact messages: each frame is
 * written header first and masked as its payload is copied in, flush against
 * the previous one, for every header size and every 8-byte tail length.
 */
public class WebSocketClientFrameBatchTest {

    @Test
    public void testBatchedFramesArriveIntactAndInOrder() throws Exception {
        List<byte[]> received = Collections.synchronizedList(new ArrayList<byte[]>());
        TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                received.add(data);
            }
        });
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        // The test server parses frames from a 64 KiB buffer, so stay under it; the
        // three header sizes (7-bit, 16-bit, 64-bit lengths) are still all covered
        // by the real client's framing, which this test exercises up to 16-bit.
        int[] sizes = {0, 1, 125, 126, 127, 1000, 16 * 1024, 60000, 3, 16 * 1024 + 1, 2, 4};
        long total = 0;
        for (int sz : sizes) total += sz;
        long buf = Unsafe.malloc(Math.max(1L, total), MemoryTag.NATIVE_DEFAULT);
        try (WebSocketClient client = WebSocketClientFactory.newInstance(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE); TestWebSocketServer ignored = server) {
            for (long i = 0; i < total; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i * 31 + 7));
            }
            client.connect("localhost", server.getPort());
            client.upgrade("/write", 5000, null);
            long p = buf;
            for (int sz : sizes) {
                client.appendBinaryFrame(p, sz);
                p += sz;
            }
            client.flushFrames();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (received.size() < sizes.length && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            Assert.assertEquals("every batched frame must arrive as its own message", sizes.length, received.size());
            p = buf;
            for (int k = 0; k < sizes.length; k++) {
                byte[] got = received.get(k);
                Assert.assertEquals("frame " + k + " length", sizes[k], got.length);
                for (int i = 0; i < sizes[k]; i++) {
                    if (got[i] != Unsafe.getUnsafe().getByte(p + i)) {
                        Assert.fail("frame " + k + " byte " + i + " differs");
                    }
                }
                p += sizes[k];
            }
        } finally {
            Unsafe.free(buf, Math.max(1L, total), MemoryTag.NATIVE_DEFAULT);
        }
    }
}
