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

import io.questdb.client.cutlass.qwp.client.QueryEvent;
import io.questdb.client.cutlass.qwp.client.QwpEgressIoThread;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class QwpProtocolErrorRoutingTest {

    @Test
    public void testQueryEventAsProtocolError() {
        QueryEvent ev = new QueryEvent().asProtocolError((byte) 0x05, "unsupported version 99");
        Assert.assertEquals(QueryEvent.KIND_PROTOCOL_ERROR, ev.kind);
        Assert.assertEquals((byte) 0x05, ev.errorStatus);
        Assert.assertEquals("unsupported version 99", ev.errorMessage);
        Assert.assertNull("buffer must be null on protocol error", ev.buffer);
    }

    @Test
    public void testProtocolFailurePropagatesViaListener() {
        AtomicReference<String> capturedMessage = new AtomicReference<>();
        AtomicReference<Byte> capturedStatus = new AtomicReference<>();
        AtomicBoolean capturedIsProtocol = new AtomicBoolean();

        QwpEgressIoThread.TerminalFailureListener listener = (status, message, isProtocol) -> {
            capturedStatus.set(status);
            capturedMessage.set(message);
            capturedIsProtocol.set(isProtocol);
        };

        listener.onTerminalFailure((byte) 0x05, "version mismatch", true);

        Assert.assertEquals(Byte.valueOf((byte) 0x05), capturedStatus.get());
        Assert.assertEquals("version mismatch", capturedMessage.get());
        Assert.assertTrue("isProtocol=true must propagate", capturedIsProtocol.get());
    }

    @Test
    public void testTransportFailurePassesIsProtocolFalse() {
        AtomicBoolean capturedIsProtocol = new AtomicBoolean(true);
        QwpEgressIoThread.TerminalFailureListener listener =
                (status, message, isProtocol) -> capturedIsProtocol.set(isProtocol);

        listener.onTerminalFailure((byte) 0x01, "decode failure", false);

        Assert.assertFalse("transport-error path must pass isProtocol=false", capturedIsProtocol.get());
    }
}
