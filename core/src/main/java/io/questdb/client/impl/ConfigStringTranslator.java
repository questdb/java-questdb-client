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

package io.questdb.client.impl;

import io.questdb.client.std.Chars;
import io.questdb.client.std.str.StringSink;

/**
 * Translates a unified configuration string into the three things needed to
 * build a {@code QuestDB}: an ingest-side config (Sender), an egress-side
 * config (QwpQueryClient), and an optional pool-tuning bundle.
 * <p>
 * <strong>Pool-tuning keys</strong> are stripped from the connection-config
 * strings (neither downstream parser accepts them) and surfaced separately
 * via {@link PoolConfig}:
 * <ul>
 *   <li>{@code sender_pool_min}, {@code sender_pool_max}</li>
 *   <li>{@code query_pool_min}, {@code query_pool_max}</li>
 *   <li>{@code acquire_timeout_ms}</li>
 *   <li>{@code idle_timeout_ms}</li>
 *   <li>{@code max_lifetime_ms}</li>
 *   <li>{@code housekeeper_interval_ms}</li>
 * </ul>
 * <p>
 * <strong>Schema translation:</strong> http&lt;-&gt;ws, https&lt;-&gt;wss.
 * A curated subset of keys carries over to the derived side (addr,
 * credentials -- token or username/password -- and TLS settings); everything
 * else stays on the input side only.
 * <p>
 * The parser runs once at {@code QuestDB.connect(...)} time. Allocation here
 * is one-shot startup cost; the hot borrow / submit paths never see it.
 */
public final class ConfigStringTranslator {

    private ConfigStringTranslator() {
    }

    /**
     * Returns the ingest and query configuration strings plus the pool config
     * extracted from a unified input.
     */
    public static Bundle deriveBothSides(CharSequence config) {
        if (config == null || config.length() == 0) {
            throw new IllegalArgumentException("configuration string cannot be empty");
        }
        StringSink sink = new StringSink();
        int pos = ConfStringParser.of(config, sink);
        if (pos < 0) {
            throw new IllegalArgumentException("invalid configuration string: " + sink);
        }
        boolean isHttp;
        boolean isTls;
        if (Chars.equals("http", sink)) {
            isHttp = true;
            isTls = false;
        } else if (Chars.equals("https", sink)) {
            isHttp = true;
            isTls = true;
        } else if (Chars.equals("ws", sink)) {
            isHttp = false;
            isTls = false;
        } else if (Chars.equals("wss", sink)) {
            isHttp = false;
            isTls = true;
        } else {
            throw new IllegalArgumentException(
                    "QuestDB.connect(single config) supports schemas [http, https, ws, wss]; got: " + sink
                            + ". Use QuestDB.connect(ingestConfig, queryConfig) for other transports.");
        }

        // Curated keys are mirrored to the derived side too.
        StringSink addr = new StringSink();
        StringSink token = new StringSink();
        StringSink username = new StringSink();
        StringSink password = new StringSink();
        StringSink tlsRoots = new StringSink();
        StringSink tlsRootsPassword = new StringSink();
        StringSink tlsVerify = new StringSink();
        boolean hasAddr = false;
        boolean hasToken = false;
        boolean hasUsername = false;
        boolean hasPassword = false;
        boolean hasTlsRoots = false;
        boolean hasTlsRootsPassword = false;
        boolean hasTlsVerify = false;

        // Input-side passthrough: schema:: + every non-pool key encountered.
        // We always re-serialize rather than pass the raw string through, so
        // pool keys can be stripped cleanly even when they sit between two
        // unrelated keys.
        StringSink inputPassthrough = new StringSink();
        inputPassthrough.put(isHttp ? (isTls ? "https::" : "http::") : (isTls ? "wss::" : "ws::"));

        PoolConfig poolConfig = new PoolConfig();

        while (ConfStringParser.hasNext(config, pos)) {
            pos = ConfStringParser.nextKey(config, pos, sink);
            if (pos < 0) {
                throw new IllegalArgumentException("invalid configuration string: " + sink);
            }
            String key = sink.toString();
            pos = ConfStringParser.value(config, pos, sink);
            if (pos < 0) {
                throw new IllegalArgumentException("invalid configuration string: " + sink);
            }
            // First, try to consume as a pool key. If matched, do NOT echo to
            // the passthrough (downstream parsers reject these).
            if (consumePoolKey(key, sink, poolConfig)) {
                continue;
            }
            // Capture curated keys for the derived-side rebuild, but also echo
            // them to the input-side passthrough (the matching parser still
            // needs to see them).
            switch (key) {
                case "addr":
                    addr.clear();
                    addr.put(sink);
                    hasAddr = true;
                    break;
                case "token":
                    token.clear();
                    token.put(sink);
                    hasToken = true;
                    break;
                case "username":
                    username.clear();
                    username.put(sink);
                    hasUsername = true;
                    break;
                case "password":
                    password.clear();
                    password.put(sink);
                    hasPassword = true;
                    break;
                case "tls_roots":
                    tlsRoots.clear();
                    tlsRoots.put(sink);
                    hasTlsRoots = true;
                    break;
                case "tls_roots_password":
                    tlsRootsPassword.clear();
                    tlsRootsPassword.put(sink);
                    hasTlsRootsPassword = true;
                    break;
                case "tls_verify":
                    tlsVerify.clear();
                    tlsVerify.put(sink);
                    hasTlsVerify = true;
                    break;
                default:
                    break;
            }
            appendKv(inputPassthrough, key, sink);
        }
        if (!hasAddr) {
            throw new IllegalArgumentException("configuration string is missing 'addr'");
        }

        String ingest;
        String query;
        if (isHttp) {
            ingest = inputPassthrough.toString();
            query = buildQueryConfig(isTls, addr, hasToken, token,
                    hasUsername, username, hasPassword, password,
                    hasTlsRoots, tlsRoots, hasTlsRootsPassword, tlsRootsPassword,
                    hasTlsVerify, tlsVerify);
        } else {
            query = inputPassthrough.toString();
            ingest = buildIngestConfig(isTls, addr, hasToken, token, hasUsername, username,
                    hasPassword, password,
                    hasTlsRoots, tlsRoots, hasTlsRootsPassword, tlsRootsPassword,
                    hasTlsVerify, tlsVerify);
        }
        return new Bundle(ingest, query, poolConfig);
    }

    private static void appendKv(StringSink out, String key, CharSequence value) {
        out.put(key).put('=');
        // Values may contain ';' which must be doubled (per ConfStringParser).
        for (int i = 0, n = value.length(); i < n; i++) {
            char c = value.charAt(i);
            out.put(c);
            if (c == ';') {
                out.put(';');
            }
        }
        out.put(';');
    }

    private static String buildIngestConfig(
            boolean isTls,
            CharSequence addr,
            boolean hasToken, CharSequence token,
            boolean hasUsername, CharSequence username,
            boolean hasPassword, CharSequence password,
            boolean hasTlsRoots, CharSequence tlsRoots,
            boolean hasTlsRootsPassword, CharSequence tlsRootsPassword,
            boolean hasTlsVerify, CharSequence tlsVerify
    ) {
        StringSink out = new StringSink();
        out.put(isTls ? "https::" : "http::");
        appendKv(out, "addr", addr);
        if (hasToken) {
            appendKv(out, "token", token);
        }
        if (hasUsername) {
            appendKv(out, "username", username);
        }
        if (hasPassword) {
            appendKv(out, "password", password);
        }
        if (hasTlsRoots) {
            appendKv(out, "tls_roots", tlsRoots);
        }
        if (hasTlsRootsPassword) {
            appendKv(out, "tls_roots_password", tlsRootsPassword);
        }
        if (hasTlsVerify) {
            appendKv(out, "tls_verify", tlsVerify);
        }
        return out.toString();
    }

    private static String buildQueryConfig(
            boolean isTls,
            CharSequence addr,
            boolean hasToken, CharSequence token,
            boolean hasUsername, CharSequence username,
            boolean hasPassword, CharSequence password,
            boolean hasTlsRoots, CharSequence tlsRoots,
            boolean hasTlsRootsPassword, CharSequence tlsRootsPassword,
            boolean hasTlsVerify, CharSequence tlsVerify
    ) {
        StringSink out = new StringSink();
        out.put(isTls ? "wss::" : "ws::");
        appendKv(out, "addr", addr);
        // Mirror the structured credentials; QwpQueryClient synthesizes the
        // Authorization header from them downstream (Bearer from token, Basic
        // from username/password).
        if (hasToken) {
            appendKv(out, "token", token);
        }
        if (hasUsername) {
            appendKv(out, "username", username);
        }
        if (hasPassword) {
            appendKv(out, "password", password);
        }
        if (isTls) {
            if (hasTlsRoots) {
                appendKv(out, "tls_roots", tlsRoots);
            }
            if (hasTlsRootsPassword) {
                appendKv(out, "tls_roots_password", tlsRootsPassword);
            }
            if (hasTlsVerify) {
                appendKv(out, "tls_verify", tlsVerify);
            }
        }
        return out.toString();
    }

    private static boolean consumePoolKey(String key, CharSequence value, PoolConfig out) {
        switch (key) {
            case "sender_pool_min":
                out.senderPoolMin = parseInt(key, value);
                return true;
            case "sender_pool_max":
                out.senderPoolMax = parseInt(key, value);
                return true;
            case "query_pool_min":
                out.queryPoolMin = parseInt(key, value);
                return true;
            case "query_pool_max":
                out.queryPoolMax = parseInt(key, value);
                return true;
            case "acquire_timeout_ms":
                out.acquireTimeoutMillis = parseLong(key, value);
                return true;
            case "idle_timeout_ms":
                out.idleTimeoutMillis = parseLong(key, value);
                return true;
            case "max_lifetime_ms":
                out.maxLifetimeMillis = parseLong(key, value);
                return true;
            case "housekeeper_interval_ms":
                out.housekeeperIntervalMillis = parseLong(key, value);
                return true;
            default:
                return false;
        }
    }

    private static int parseInt(String key, CharSequence value) {
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid " + key + ": " + value);
        }
    }

    private static long parseLong(String key, CharSequence value) {
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid " + key + ": " + value);
        }
    }

    /**
     * The full result of translating a single connect string: an ingest-side
     * config, an egress-side config, and any pool-tuning values that the
     * string carried (or all-unset {@link PoolConfig} if it carried none).
     */
    public static final class Bundle {
        public final String ingestConfig;
        public final PoolConfig poolConfig;
        public final String queryConfig;

        Bundle(String ingestConfig, String queryConfig, PoolConfig poolConfig) {
            this.ingestConfig = ingestConfig;
            this.queryConfig = queryConfig;
            this.poolConfig = poolConfig;
        }
    }

    /**
     * Pool tuning extracted from the connect string. Each field starts at
     * {@link #UNSET} (-1); the builder applies only those that were actually
     * present in the string, leaving the rest at the builder defaults.
     */
    public static final class PoolConfig {
        public static final long UNSET = -1L;

        public long acquireTimeoutMillis = UNSET;
        public long housekeeperIntervalMillis = UNSET;
        public long idleTimeoutMillis = UNSET;
        public long maxLifetimeMillis = UNSET;
        public int queryPoolMax = (int) UNSET;
        public int queryPoolMin = (int) UNSET;
        public int senderPoolMax = (int) UNSET;
        public int senderPoolMin = (int) UNSET;
    }
}
