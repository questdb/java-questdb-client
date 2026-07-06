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

package io.questdb.client.test.network;

import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.NetworkFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * Exercises the native non-blocking connect-with-timeout primitive
 * ({@link NetworkFacade#connectAddrInfoTimeout}).
 */
public class NetConnectTimeoutTest {

    private static final NetworkFacade NF = NetworkFacadeImpl.INSTANCE;

    @Test
    public void testConnectRefusedReturnsErrorNotTimeout() throws Exception {
        // Bind then immediately close to obtain a port with no listener; a
        // connect to it is refused (RST) rather than timed out.
        int port;
        try (ServerSocket ss = new ServerSocket()) {
            ss.bind(new InetSocketAddress("127.0.0.1", 0));
            port = ss.getLocalPort();
        }

        long addrInfo = NF.getAddrInfo("127.0.0.1", port);
        Assert.assertNotEquals(-1, addrInfo);
        int fd = NF.socketTcp(true);
        try {
            int rc = NF.connectAddrInfoTimeout(fd, addrInfo, 5_000);
            Assert.assertNotEquals("refused connect must not report success", 0, rc);
            Assert.assertNotEquals("refused connect must not be reported as a timeout",
                    NetworkFacade.CONNECT_TIMEOUT, rc);
        } finally {
            NF.freeAddrInfo(addrInfo);
            NF.close(fd);
        }
    }

    @Test
    public void testConnectSucceedsWithinTimeout() throws Exception {
        try (ServerSocket ss = new ServerSocket()) {
            ss.bind(new InetSocketAddress("127.0.0.1", 0));
            int port = ss.getLocalPort();

            long addrInfo = NF.getAddrInfo("127.0.0.1", port);
            Assert.assertNotEquals(-1, addrInfo);
            int fd = NF.socketTcp(true);
            try {
                int rc = NF.connectAddrInfoTimeout(fd, addrInfo, 5_000);
                Assert.assertEquals("loopback connect should succeed", 0, rc);
            } finally {
                NF.freeAddrInfo(addrInfo);
                NF.close(fd);
            }
        }
    }

    @Test
    public void testConnectToBlackholeTimesOut() {
        // 192.0.2.0/24 is TEST-NET-1 (RFC 5737); packets are silently dropped on
        // a normal network, so the SYN goes unanswered and the timeout fires
        // instead of the (much longer) OS connect timeout.
        long addrInfo = NF.getAddrInfo("192.0.2.1", 9009);
        Assert.assertNotEquals(-1, addrInfo);
        int fd = NF.socketTcp(true);
        try {
            long start = System.nanoTime();
            int rc = NF.connectAddrInfoTimeout(fd, addrInfo, 500);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

            // Whatever the outcome, the key guarantee is that we never blocked
            // on the (multi-minute) OS connect timeout.
            Assert.assertTrue("connect must return near the budget, was " + elapsedMs + "ms", elapsedMs < 5_000);

            // The deterministic outcome depends on the runner's routing for
            // TEST-NET-1: a dropped SYN yields a real timeout (the path under
            // test), while a runner with no route to 192.0.2.0/24 fails fast
            // with ENETUNREACH/EHOSTUNREACH (rc == -1) and a rare appliance may
            // even accept it (rc == 0). Only the timeout case is assertable; the
            // others can't exercise the timeout, so skip rather than flake.
            Assume.assumeTrue("no route to blackhole on this runner (rc=" + rc + ")",
                    rc == NetworkFacade.CONNECT_TIMEOUT);
            Assert.assertEquals("blackhole connect should time out", NetworkFacade.CONNECT_TIMEOUT, rc);
        } finally {
            NF.freeAddrInfo(addrInfo);
            NF.close(fd);
        }
    }
}
