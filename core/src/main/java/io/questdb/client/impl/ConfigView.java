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

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.ObjList;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Layer 3 of the QWP connect-string parser: a typed, validated view over a
 * {@link ConfigString} for the {@link ConfigSchema} registry. The constructor
 * runs the reject pass once -- any key absent from the schema throws
 * {@code unknown configuration key: <key>}, plus a relocated-key hint for keys
 * that belong to the legacy http/tcp/udp transports.
 * <p>
 * Found keys are recorded alias-normalized ({@code user}->{@code username},
 * {@code pass}->{@code password}), so a consumer pulling the canonical name sees
 * a value written under either. Non-multi keys resolve last-write-wins. The view
 * does not filter by {@link Side}: each consumer reads the keys it needs, and a
 * key owned by another consumer is accepted syntactically here and validated by
 * its owning consumer.
 */
public final class ConfigView {

    /**
     * Keys that live in the legacy http/tcp/udp vocabulary. On a {@code ws}/
     * {@code wss} string they reject with a hint pointing at the right place.
     */
    private static final Map<String, String> RELOCATED_HINTS;

    static {
        Map<String, String> hints = new HashMap<>();
        hints.put("retry_timeout", "(use reconnect_max_duration_millis on ws/wss)");
        hints.put("protocol_version", "(QWP negotiates the protocol version during the WebSocket upgrade)");
        hints.put("init_buf_size", "(applies to legacy http/tcp/udp transports only)");
        hints.put("max_buf_size", "(applies to legacy http/tcp/udp transports only)");
        hints.put("request_timeout", "(applies to legacy http/tcp/udp transports only)");
        hints.put("request_min_throughput", "(applies to legacy http/tcp/udp transports only)");
        hints.put("max_datagram_size", "(applies to legacy http/tcp/udp transports only)");
        hints.put("multicast_ttl", "(applies to legacy http/tcp/udp transports only)");
        RELOCATED_HINTS = Collections.unmodifiableMap(hints);
    }

    private final ObjList<String> normKeys = new ObjList<>();
    private final ObjList<String> normValues = new ObjList<>();

    public ConfigView(ConfigString cs) {
        for (int i = 0, n = cs.size(); i < n; i++) {
            String raw = cs.key(i);
            ConfigSchema.KeySpec spec = ConfigSchema.spec(raw);
            if (spec == null) {
                String hint = RELOCATED_HINTS.get(raw);
                if (hint != null) {
                    throw new IllegalArgumentException("unknown configuration key: " + raw + " " + hint);
                }
                throw new IllegalArgumentException("unknown configuration key: " + raw);
            }
            normKeys.add(spec.canonical);
            normValues.add(cs.value(i));
        }
    }

    /**
     * Returns the relocated-key hint for {@code key}, or null. Exposed for the
     * guard test that pins the hint table.
     */
    public static String relocatedHint(String key) {
        return RELOCATED_HINTS.get(key);
    }

    /**
     * A boolean flag accepting {@code true}/{@code false} (and {@code on}/{@code off}
     * for consistency with the rest of the connect-string surface). Returns
     * {@code dflt} when the key is absent; throws on any other value.
     */
    public boolean getBool(String key, boolean dflt) {
        String v = getStr(key);
        if (v == null) {
            return dflt;
        }
        if ("true".equals(v) || "on".equals(v)) {
            return true;
        }
        if ("false".equals(v) || "off".equals(v)) {
            return false;
        }
        throw new IllegalArgumentException("invalid " + key + ": " + v + " (expected true, false, on, off)");
    }

    public boolean getBoolOnOff(String key, boolean dflt) {
        String v = getStr(key);
        if (v == null) {
            return dflt;
        }
        if ("on".equals(v)) {
            return true;
        }
        if ("off".equals(v)) {
            return false;
        }
        throw new IllegalArgumentException("invalid " + key + ": " + v + " (expected on, off)");
    }

    /**
     * The value of an enum key, or null if absent. Validated against the
     * registry's allowed values.
     */
    public String getEnum(String key) {
        String v = getStr(key);
        if (v == null) {
            return null;
        }
        ObjList<String> allowed = ConfigSchema.spec(key).enumValues;
        for (int i = 0, n = allowed.size(); i < n; i++) {
            if (allowed.getQuick(i).equals(v)) {
                return v;
            }
        }
        throw new IllegalArgumentException("invalid " + key + ": " + v + " (expected " + join(allowed) + ")");
    }

    /**
     * Splits the address list under {@code key} (multivalue, IPv6-aware),
     * resolves each port (explicit, else {@code defaultPort}), rejects duplicate
     * {@code (host, port)} pairs, and emits each unique pair to {@code sink}.
     */
    public void getHostPorts(String key, int defaultPort, HostPortSink sink) {
        HashSet<String> seen = new HashSet<>();
        for (int i = 0, n = normKeys.size(); i < n; i++) {
            if (!normKeys.getQuick(i).equals(key)) {
                continue;
            }
            String value = normValues.getQuick(i);
            int start = 0;
            int len = value.length();
            for (int j = 0; j <= len; j++) {
                if (j == len || value.charAt(j) == ',') {
                    String entry = value.substring(start, j).trim();
                    if (entry.isEmpty()) {
                        throw new IllegalArgumentException("empty addr entry");
                    }
                    parseEntry(entry, defaultPort, seen, sink);
                    start = j + 1;
                }
            }
        }
    }

    public int getInt(String key, int unset) {
        String v = getStr(key);
        if (v == null) {
            return unset;
        }
        // Parse in the long domain so a numeric-but-over-int value (e.g.
        // 4294967297) is distinguishable from garbage: it must fail with the
        // actual int limit, not wrap through an (int) cast or report a generic
        // "invalid" for a perfectly numeric string.
        long parsed;
        try {
            parsed = Numbers.parseLong(v);
        } catch (NumericException e) {
            throw new IllegalArgumentException("invalid " + key + ": " + v);
        }
        ConfigSchema.KeySpec spec = ConfigSchema.spec(key);
        // Schema range first: it is the tighter, key-specific constraint, so
        // "compression_level=4294967297" says "must be in [1, 22]".
        if (outOfRange(spec, parsed)) {
            throw new IllegalArgumentException(rangeMessage(spec));
        }
        if (parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(key + " must be <= " + Integer.MAX_VALUE + ": " + v);
        }
        if (parsed < Integer.MIN_VALUE) {
            throw new IllegalArgumentException(key + " must be >= " + Integer.MIN_VALUE + ": " + v);
        }
        return (int) parsed;
    }

    public long getLong(String key, long unset) {
        String v = getStr(key);
        if (v == null) {
            return unset;
        }
        long parsed;
        try {
            parsed = Numbers.parseLong(v);
        } catch (NumericException e) {
            throw new IllegalArgumentException("invalid " + key + ": " + v);
        }
        ConfigSchema.KeySpec spec = ConfigSchema.spec(key);
        if (outOfRange(spec, parsed)) {
            throw new IllegalArgumentException(rangeMessage(spec));
        }
        return parsed;
    }

    /**
     * The last value written for {@code key} (last-write-wins), or null if
     * absent. {@code key} is the canonical name; values written under an alias
     * are found too.
     */
    public String getStr(String key) {
        for (int i = normKeys.size() - 1; i >= 0; i--) {
            if (normKeys.getQuick(i).equals(key)) {
                return normValues.getQuick(i);
            }
        }
        return null;
    }

    public boolean has(String key) {
        for (int i = 0, n = normKeys.size(); i < n; i++) {
            if (normKeys.getQuick(i).equals(key)) {
                return true;
            }
        }
        return false;
    }

    private static String join(ObjList<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, n = values.size(); i < n; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(values.getQuick(i));
        }
        return sb.toString();
    }

    private static boolean outOfRange(ConfigSchema.KeySpec spec, long v) {
        if (spec.min != Long.MIN_VALUE && (spec.minOpen ? v <= spec.min : v < spec.min)) {
            return true;
        }
        return spec.max != Long.MAX_VALUE && (spec.maxOpen ? v >= spec.max : v > spec.max);
    }

    private static int parsePort(String portStr, String entry) {
        int port;
        try {
            port = Numbers.parseInt(portStr.trim());
        } catch (NumericException e) {
            throw new IllegalArgumentException("invalid port in addr: " + entry);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port out of range in addr: " + entry + " (must be 1-65535)");
        }
        return port;
    }

    private static String rangeMessage(ConfigSchema.KeySpec spec) {
        boolean hasMin = spec.min != Long.MIN_VALUE;
        boolean hasMax = spec.max != Long.MAX_VALUE;
        if (hasMin && hasMax) {
            return spec.name + " must be in [" + spec.min + ", " + spec.max + "]";
        }
        if (hasMin) {
            return spec.name + " must be " + (spec.minOpen ? "> " : ">= ") + spec.min;
        }
        return spec.name + " must be " + (spec.maxOpen ? "< " : "<= ") + spec.max;
    }

    private void parseEntry(String entry, int defaultPort, HashSet<String> seen, HostPortSink sink) {
        String host;
        int port;
        if (entry.charAt(0) == '[') {
            int closeBracket = entry.indexOf(']');
            if (closeBracket < 0) {
                throw new IllegalArgumentException("missing closing ']' in IPv6 addr entry: " + entry);
            }
            host = entry.substring(1, closeBracket);
            if (closeBracket == entry.length() - 1) {
                port = defaultPort;
            } else if (entry.charAt(closeBracket + 1) != ':') {
                throw new IllegalArgumentException("expected ':' after ']' in IPv6 addr entry: " + entry);
            } else {
                port = parsePort(entry.substring(closeBracket + 2), entry);
            }
        } else if (entry.indexOf(':') != entry.lastIndexOf(':')) {
            // Unbracketed multi-colon: bare IPv6 host, default port. A custom
            // port on IPv6 requires brackets.
            host = entry;
            port = defaultPort;
        } else {
            int colon = entry.indexOf(':');
            if (colon < 0) {
                host = entry;
                port = defaultPort;
            } else {
                host = entry.substring(0, colon).trim();
                port = parsePort(entry.substring(colon + 1), entry);
            }
        }
        if (host.isEmpty()) {
            throw new IllegalArgumentException("empty host in addr entry: " + entry);
        }
        // port is numeric and neither hostnames nor IPv6 literals contain '/',
        // so this key is unique per (host, port).
        if (!seen.add(port + "/" + host)) {
            throw new IllegalArgumentException("duplicate addr entry: " + entry);
        }
        sink.accept(host, port);
    }
}
