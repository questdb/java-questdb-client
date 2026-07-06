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

import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.client.cutlass.qwp.client.QwpWebSocketSender.DEFAULT_BACKGROUND_CONNECT_TIMEOUT_MS;
import static io.questdb.client.cutlass.qwp.client.QwpWebSocketSender.effectiveConnectTimeoutMs;

/**
 * Background (drainer) connect walks must never inherit the untimed native
 * connect that connect_timeout=0 (the default) means for the foreground.
 * <p>
 * During an outage a drainer is routinely parked inside a blocking native
 * connect ({@code nf.connectAddrInfo}) that neither unpark nor interrupt
 * cancels. The drainer pool's close sequence (2.5s graceful drain +
 * requestStop + 500ms + shutdownNow) then reliably lands on the failed-stop
 * teardown protocol: the WebSocket client and microbatch buffers are
 * deliberately leaked and the SF slot lock is held until the OS connect
 * deadline (SYN retries, 60-130s on Linux) resolves the stuck call. A finite
 * background default bounds that window to seconds. Foreground semantics are
 * intentionally untouched: an explicit user value is honoured verbatim on
 * both paths, and the foreground's unset default stays untimed.
 */
public class BackgroundConnectTimeoutDefaultTest {

    @Test
    public void testBackgroundExplicitValueHonoured() {
        Assert.assertEquals(500, effectiveConnectTimeoutMs(true, 500));
        Assert.assertEquals(60_000, effectiveConnectTimeoutMs(true, 60_000));
    }

    @Test
    public void testBackgroundUnsetGetsFiniteDefault() {
        Assert.assertEquals(DEFAULT_BACKGROUND_CONNECT_TIMEOUT_MS, effectiveConnectTimeoutMs(true, 0));
        // Defensive: builder validation rejects negatives, but the resolver
        // must not turn a bad value back into an untimed background connect.
        Assert.assertEquals(DEFAULT_BACKGROUND_CONNECT_TIMEOUT_MS, effectiveConnectTimeoutMs(true, -1));
    }

    @Test
    public void testDefaultIsFinite() {
        Assert.assertTrue(DEFAULT_BACKGROUND_CONNECT_TIMEOUT_MS > 0);
    }

    @Test
    public void testForegroundExplicitValueHonoured() {
        Assert.assertEquals(500, effectiveConnectTimeoutMs(false, 500));
    }

    @Test
    public void testForegroundUnsetStaysUntimed() {
        // 0 => WebSocketClient falls back to nf.connectAddrInfo (OS-bounded).
        // Historical foreground behaviour, deliberately preserved.
        Assert.assertEquals(0, effectiveConnectTimeoutMs(false, 0));
    }
}
