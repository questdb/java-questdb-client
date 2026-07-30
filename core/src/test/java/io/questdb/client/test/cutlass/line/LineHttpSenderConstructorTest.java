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

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.client.cutlass.line.http.LineHttpSenderV2;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.PlainSocket;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketFactory;
import io.questdb.client.std.IntList;
import io.questdb.client.std.Misc;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Rnd;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Covers the rollback the sender constructor runs when {@code newRequest()} throws. The sender owns
 * its client from the assignment on - {@code close()} frees it whether the caller handed it in or
 * the constructor built it - but a failed constructor hands no reference back, so nothing else can
 * close it. {@link AbstractLineHttpSender#createLineSender} cannot cover this either: it builds the
 * sender in a return expression, past every catch it has.
 */
public class LineHttpSenderConstructorTest {
    // Smaller than "POST /write HTTP/1.1\r\n", so newRequest() cannot fit the preamble and throws
    // with the client fully built. A third-party HttpClientConfiguration can reach this in
    // production; LineSenderBuilder floors the maximum at 64 KiB, which puts a heap
    // OutOfMemoryError between the assignment and newRequest() in the same window.
    private static final int TINY_BUFFER_SIZE = 16;
    private static final HttpClientConfiguration TINY_BUFFER_CONFIGURATION = new DefaultHttpClientConfiguration() {
        @Override
        public int getInitialRequestBufferSize() {
            return TINY_BUFFER_SIZE;
        }

        @Override
        public int getMaximumRequestBufferSize() {
            return TINY_BUFFER_SIZE;
        }
    };

    @Test
    public void testHandedInClientIsReleasedWhenRequestPreambleDoesNotFit() throws Exception {
        // The auto-detecting createLineSender() builds the client itself and hands it to the
        // constructor, so the constructor has to free a client it did not create. The counting
        // socket observes the file descriptor the leak check cannot see.
        TestUtils.assertMemoryLeak(() -> {
            final CountingSocketFactory socketFactory = new CountingSocketFactory();
            final HttpClient client = HttpClientFactory.newInstance(TINY_BUFFER_CONFIGURATION, socketFactory);
            try {
                buildAndClose(newSender(client));
                Assert.fail("expected HttpClientException");
            } catch (HttpClientException ignore) {
                // newRequest() could not fit the preamble
            }
            // Two closes: newRequest() disconnects first because the sender's host differs from the
            // client's, then the rollback closes the client itself. Without the rollback the count
            // is 1 - and the client's native buffers stay live for the enclosing leak check.
            Assert.assertEquals("the constructor must close the client it was handed", 2, socketFactory.closeCount);
        });
    }

    @Test
    public void testSelfBuiltClientIsReleasedWhenRequestPreambleDoesNotFit() throws Exception {
        // An explicit protocol version skips detection, so createLineSender() passes a null client
        // and the constructor builds its own. HttpClientFactory pins PlainSocketFactory on that
        // path, so the leak check on the client's native buffers is the link here.
        TestUtils.assertMemoryLeak(() -> {
            try {
                buildAndClose(AbstractLineHttpSender.createLineSender(
                        new ObjList<>("localhost"),
                        IntList.createWithValues(9000),
                        "/write",
                        TINY_BUFFER_CONFIGURATION,
                        null,
                        1000,
                        null,
                        null,
                        null,
                        127,
                        0,
                        0,
                        0,
                        Long.MAX_VALUE,
                        Sender.PROTOCOL_VERSION_V2
                ));
                Assert.fail("expected HttpClientException");
            } catch (HttpClientException ignore) {
                // newRequest() could not fit the preamble
            }
        });
    }

    private static void buildAndClose(Sender sender) {
        // Closing here keeps the leak check honest on the path where the constructor unexpectedly
        // succeeds: dropping a built sender would leak on top of the failure the caller asserts and
        // bury it.
        Misc.free(sender);
    }

    private static LineHttpSenderV2 newSender(HttpClient client) {
        return new LineHttpSenderV2(
                new ObjList<>("localhost"),
                IntList.createWithValues(9000),
                "/write",
                TINY_BUFFER_CONFIGURATION,
                null,
                client,
                1000,
                null,
                null,
                null,
                127,
                0,
                0,
                0,
                Long.MAX_VALUE,
                0,
                new Rnd()
        );
    }

    private static class CountingSocketFactory implements SocketFactory {
        int closeCount;

        @Override
        public Socket newInstance(NetworkFacade nf, Logger log) {
            return new PlainSocket(nf, log) {
                @Override
                public synchronized void close() {
                    closeCount++;
                    super.close();
                }
            };
        }
    }
}
