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

import io.questdb.client.std.ObjList;

import java.util.HashMap;
import java.util.Map;

/**
 * Layer 2 of the QWP connect-string parser: the key registry. This is the
 * {@code ws}/{@code wss} vocabulary -- every key the WebSocket {@code Sender},
 * the {@code QwpQueryClient}, and the facade pool accept, with its owning
 * {@link Side}, value type, numeric range, enum values, and alias.
 * <p>
 * {@link ConfigView} drives validation off this registry; the consumers read
 * the keys their side owns. There is one vocabulary, so the registry is static.
 */
public final class ConfigSchema {

    private static final long OPEN = Long.MIN_VALUE; // open lower bound
    private static final long OPEN_MAX = Long.MAX_VALUE; // open upper bound
    private static final Map<String, KeySpec> BY_NAME = new HashMap<>();

    static {
        // COMMON -- both clients apply.
        hostPort("addr");
        str("username", Side.COMMON);
        str("password", Side.COMMON);
        alias("user", "username");
        alias("pass", "password");
        str("token", Side.COMMON);
        enumKey("tls_verify", Side.COMMON, "on", "unsafe_off");
        str("tls_roots", Side.COMMON);
        str("tls_roots_password", Side.COMMON);
        longRange("auth_timeout_ms", Side.COMMON, 0, OPEN_MAX, true, false); // > 0
        longRange("connect_timeout", Side.COMMON, 0, OPEN_MAX, true, false); // > 0

        // INGRESS -- the WebSocket Sender applies. STRING in the registry; the
        // Sender parses suffix/mode values (off/on, 64k, durability) with its
        // own helpers, byte-for-byte.
        str("auto_flush", Side.INGRESS);
        str("auto_flush_bytes", Side.INGRESS);
        str("auto_flush_interval", Side.INGRESS);
        str("auto_flush_rows", Side.INGRESS);
        str("close_flush_timeout_millis", Side.INGRESS);
        str("connection_listener_inbox_capacity", Side.INGRESS);
        str("drain_orphans", Side.INGRESS);
        str("durable_ack_keepalive_interval_millis", Side.INGRESS);
        str("error_inbox_capacity", Side.INGRESS);
        str("initial_connect_retry", Side.INGRESS);
        str("max_background_drainers", Side.INGRESS);
        // Poison-frame detector threshold (see CursorWebSocketSendLoop): the
        // Sender applies it; the QwpQueryClient accepts it as a syntactic no-op
        // (one vocabulary, side-owned application).
        str("max_frame_rejections", Side.INGRESS);
        str("poison_min_escalation_window_millis", Side.INGRESS);
        str("max_name_len", Side.INGRESS);
        str("reconnect_initial_backoff_millis", Side.INGRESS);
        str("reconnect_max_backoff_millis", Side.INGRESS);
        str("reconnect_max_duration_millis", Side.INGRESS);
        str("request_durable_ack", Side.INGRESS);
        str("sender_id", Side.INGRESS);
        str("sf_append_deadline_millis", Side.INGRESS);
        str("sf_dir", Side.INGRESS);
        str("sf_durability", Side.INGRESS);
        str("sf_max_bytes", Side.INGRESS);
        str("sf_max_total_bytes", Side.INGRESS);
        str("transaction", Side.INGRESS);

        // EGRESS -- the QwpQueryClient applies. Typed where there is a range or
        // enum, so ConfigView enforces it with the colon-dialect message.
        enumKey("target", Side.EGRESS, "any", "primary", "replica");
        boolOnOff("failover", Side.EGRESS);
        intRange("failover_max_attempts", Side.EGRESS, 1, OPEN_MAX, false, false); // >= 1
        longRange("failover_backoff_initial_ms", Side.EGRESS, 0, OPEN_MAX, false, false); // >= 0
        longRange("failover_backoff_max_ms", Side.EGRESS, 0, OPEN_MAX, false, false); // >= 0
        longRange("failover_max_duration_ms", Side.EGRESS, 0, OPEN_MAX, false, false); // >= 0
        intRange("max_batch_rows", Side.EGRESS, 1, 1_048_576, false, false); // [1, 1048576]
        longRange("initial_credit", Side.EGRESS, 0, OPEN_MAX, false, false); // >= 0
        intRange("buffer_pool_size", Side.EGRESS, 1, OPEN_MAX, false, false); // >= 1
        enumKey("compression", Side.EGRESS, "zstd", "raw", "auto");
        intRange("compression_level", Side.EGRESS, 1, 22, false, false); // [1, 22]
        str("client_id", Side.EGRESS);
        str("zone", Side.EGRESS);

        // POOL -- the facade applies; the two clients ignore. Open ranges: the
        // facade feeds the value through the existing builder setter, which owns
        // the range message.
        intRange("sender_pool_min", Side.POOL, OPEN, OPEN_MAX, false, false);
        intRange("sender_pool_max", Side.POOL, OPEN, OPEN_MAX, false, false);
        intRange("query_pool_min", Side.POOL, OPEN, OPEN_MAX, false, false);
        intRange("query_pool_max", Side.POOL, OPEN, OPEN_MAX, false, false);
        longRange("acquire_timeout_ms", Side.POOL, OPEN, OPEN_MAX, false, false);
        longRange("query_close_timeout_ms", Side.POOL, OPEN, OPEN_MAX, false, false);
        longRange("idle_timeout_ms", Side.POOL, OPEN, OPEN_MAX, false, false);
        longRange("max_lifetime_ms", Side.POOL, OPEN, OPEN_MAX, false, false);
        longRange("housekeeper_interval_ms", Side.POOL, OPEN, OPEN_MAX, false, false);
        boolOnOff("lazy_connect", Side.POOL); // facade flag: tolerant non-blocking startup (async ingest + lazy reads)

        // RESERVED -- accepted no-op (error-policy keys reserved by the spec).
        str("on_internal_error", Side.RESERVED);
        str("on_parse_error", Side.RESERVED);
        str("on_schema_error", Side.RESERVED);
        str("on_security_error", Side.RESERVED);
        str("on_server_error", Side.RESERVED);
        str("on_write_error", Side.RESERVED);
    }

    private ConfigSchema() {
    }

    /**
     * Every key spec in the registry, including alias entries ({@code user},
     * {@code pass}). For the guard test.
     */
    public static Iterable<KeySpec> all() {
        return BY_NAME.values();
    }

    /**
     * The spec for {@code name}, or null if the key is not in the registry.
     * Includes alias entries ({@code user}, {@code pass}).
     */
    public static KeySpec spec(String name) {
        return BY_NAME.get(name);
    }

    private static void add(KeySpec spec) {
        BY_NAME.put(spec.name, spec);
    }

    private static void alias(String name, String canonical) {
        add(new KeySpec(name, Side.COMMON, OPEN, OPEN_MAX, false, false, null, canonical));
    }

    private static void boolOnOff(String name, Side side) {
        add(new KeySpec(name, side, OPEN, OPEN_MAX, false, false, null, name));
    }

    private static void enumKey(String name, Side side, String... values) {
        ObjList<String> list = new ObjList<>();
        for (int i = 0; i < values.length; i++) {
            list.add(values[i]);
        }
        add(new KeySpec(name, side, OPEN, OPEN_MAX, false, false, list, name));
    }

    private static void hostPort(String name) {
        add(new KeySpec(name, Side.COMMON, OPEN, OPEN_MAX, false, false, null, name));
    }

    private static void intRange(String name, Side side, long min, long max, boolean minOpen, boolean maxOpen) {
        add(new KeySpec(name, side, min, max, minOpen, maxOpen, null, name));
    }

    private static void longRange(String name, Side side, long min, long max, boolean minOpen, boolean maxOpen) {
        add(new KeySpec(name, side, min, max, minOpen, maxOpen, null, name));
    }

    private static void str(String name, Side side) {
        add(new KeySpec(name, side, OPEN, OPEN_MAX, false, false, null, name));
    }

    /**
     * One key's contract. {@code min == Long.MIN_VALUE} means no lower bound,
     * {@code max == Long.MAX_VALUE} means no upper bound. {@code minOpen} /
     * {@code maxOpen} render and enforce a strict ({@code >} / {@code <}) rather
     * than inclusive ({@code >=} / {@code <=}) bound.
     */
    public static final class KeySpec {
        final String canonical;
        final ObjList<String> enumValues;
        final long max;
        final boolean maxOpen;
        final long min;
        final boolean minOpen;
        final String name;
        final Side side;

        KeySpec(
                String name, Side side,
                long min, long max, boolean minOpen, boolean maxOpen,
                ObjList<String> enumValues, String canonical
        ) {
            this.name = name;
            this.side = side;
            this.min = min;
            this.max = max;
            this.minOpen = minOpen;
            this.maxOpen = maxOpen;
            this.enumValues = enumValues;
            this.canonical = canonical;
        }

        public String canonical() {
            return canonical;
        }

        public ObjList<String> enumValues() {
            return enumValues;
        }

        public String name() {
            return name;
        }

        public Side side() {
            return side;
        }
    }
}
