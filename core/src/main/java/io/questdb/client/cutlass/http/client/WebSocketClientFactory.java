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

package io.questdb.client.cutlass.http.client;

import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.network.JavaTlsClientSocketFactory;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.network.SocketFactory;
import io.questdb.client.std.Os;

/**
 * Factory for creating platform-specific {@link WebSocketClient} instances.
 * <p>
 * Usage:
 * <pre>
 * // Plain text connection
 * WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
 *
 * // TLS connection
 * WebSocketClient client = WebSocketClientFactory.newTlsInstance(config, tlsConfig);
 *
 * // Connect and upgrade
 * client.connect("localhost", 9000);
 * client.upgrade("/ws");
 *
 * // Send data
 * WebSocketSendBuffer buf = client.getSendBuffer();
 * buf.beginBinaryFrame();
 * buf.putLong(data);
 * WebSocketSendBuffer.FrameInfo frame = buf.endBinaryFrame();
 * client.sendFrame(frame);
 * buf.reset();
 *
 * // Receive data
 * client.receiveFrame(handler);
 *
 * client.close();
 * </pre>
 */
public class WebSocketClientFactory {

    // Utility class -- no instantiation
    private WebSocketClientFactory() {
    }

    /**
     * Creates a new WebSocket client with insecure TLS (no certificate validation).
     * <p>
     * WARNING: Only use this for testing. Production code should use proper TLS validation.
     *
     * @return a new WebSocket client with insecure TLS
     */
    public static WebSocketClient newInsecureTlsInstance() {
        return newInstance(DefaultHttpClientConfiguration.INSTANCE, JavaTlsClientSocketFactory.INSECURE_NO_VALIDATION);
    }

    /**
     * Creates a new WebSocket client with the specified configuration and socket factory.
     *
     * @param configuration the HTTP client configuration
     * @param socketFactory the socket factory for creating sockets
     * @return a new platform-specific WebSocket client
     */
    public static WebSocketClient newInstance(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        switch (Os.type) {
            case Os.LINUX:
                return new WebSocketClientLinux(configuration, socketFactory);
            case Os.DARWIN:
            case Os.FREEBSD:
                return new WebSocketClientOsx(configuration, socketFactory);
            case Os.WINDOWS:
                return new WebSocketClientWindows(configuration, socketFactory);
            default:
                throw new UnsupportedOperationException("Unsupported platform: " + Os.type);
        }
    }

    /**
     * Creates a new plain text WebSocket client with default configuration.
     *
     * @return a new plain text WebSocket client
     */
    public static WebSocketClient newPlainTextInstance() {
        return newPlainTextInstance(DefaultHttpClientConfiguration.INSTANCE);
    }

    /**
     * Creates a new plain text WebSocket client with the specified configuration.
     *
     * @param configuration the HTTP client configuration
     * @return a new plain text WebSocket client
     */
    public static WebSocketClient newPlainTextInstance(HttpClientConfiguration configuration) {
        return newInstance(configuration, PlainSocketFactory.INSTANCE);
    }

    /**
     * Creates a new WebSocket client with the specified TLS configuration.
     *
     * @param tlsConfig the TLS configuration (controls certificate validation and trust store)
     * @return a new WebSocket client with TLS
     */
    public static WebSocketClient newTlsInstance(ClientTlsConfiguration tlsConfig) {
        return newInstance(DefaultHttpClientConfiguration.INSTANCE, new JavaTlsClientSocketFactory(tlsConfig));
    }
}
