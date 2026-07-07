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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public final class TestPorts {

    private TestPorts() {
    }

    public static int findUnusedPort() {
        try (ServerSocket s = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("failed to allocate an ephemeral port", e);
        }
    }

    /**
     * Allocates {@code n} DISTINCT ephemeral ports. All {@code n} probe
     * sockets are held open simultaneously, so the kernel is forced to hand
     * out {@code n} different ports; they are closed together only after
     * every port has been collected.
     * <p>
     * Do NOT emulate this with repeated {@link #findUnusedPort()} calls:
     * that helper is bind-close-return, and once its probe socket closes the
     * port returns to the kernel's ephemeral pool — Linux readily hands the
     * just-released port straight back to the next {@code bind(0)}, so two
     * back-to-back calls can return the SAME port. That exact race made a
     * multi-addr config fail validation with "duplicate addr entry" in CI.
     */
    public static int[] findUnusedPorts(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must be > 0: " + n);
        }
        ServerSocket[] sockets = new ServerSocket[n];
        int[] ports = new int[n];
        try {
            for (int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
                ports[i] = sockets[i].getLocalPort();
            }
            return ports;
        } catch (IOException e) {
            throw new RuntimeException("failed to allocate " + n + " ephemeral ports", e);
        } finally {
            for (ServerSocket s : sockets) {
                if (s != null) {
                    try {
                        s.close();
                    } catch (IOException ignored) {
                        // best-effort; the probe socket carries no state
                    }
                }
            }
        }
    }
}
