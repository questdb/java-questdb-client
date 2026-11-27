/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.client.cutlass.qwp.client;

/**
 * The QWP durable-ack tier set a sender can request, encoded as a bitmask:
 * <ul>
 *   <li>{@link #LOCAL} — the server emits {@code STATUS_LOCAL_DURABLE_ACK}
 *       frames once commits are fdatasync-durable on its disk
 *       (power-loss-safe).</li>
 *   <li>{@link #REPLICATED} — the server emits {@code STATUS_DURABLE_ACK}
 *       frames once commits reach the object store (failover-safe).</li>
 * </ul>
 * {@link #LEGACY_TRUE} is a modifier bit for requests made through the
 * boolean {@code requestDurableAck(true)} API or the {@code on} config
 * value: the request header carries the literal {@code "true"} and the
 * server confirms the grant with the {@code "enabled"} token. It always
 * combines with {@link #REPLICATED}.
 * <p>
 * The server grants the full requested set or denies the request entirely
 * (no confirmation header); it never substitutes a weaker guarantee. The
 * sender trims its store-and-forward copy on the strongest requested tier's
 * ack; with both tiers requested, local acks arrive as progress signals only.
 */
public final class DurableAckTiers {

    public static final int NONE = 0;
    public static final int LOCAL = 1;
    public static final int REPLICATED = 2;
    // Modifier bit, only ever combined with REPLICATED: send the "true"
    // request token and expect the "enabled" confirmation.
    public static final int LEGACY_TRUE = 4;

    private DurableAckTiers() {
    }

    /**
     * The config-string form of a tier set, the inverse of
     * {@link #parseConfigValue(CharSequence)} (legacy sets print as "on").
     */
    public static String configValue(int tiers) {
        if ((tiers & LEGACY_TRUE) != 0) {
            return "on";
        }
        switch (tiers & (LOCAL | REPLICATED)) {
            case LOCAL:
                return "local";
            case REPLICATED:
                return "replicated";
            case LOCAL | REPLICATED:
                return "local,replicated";
            default:
                return "off";
        }
    }

    /**
     * The X-QWP-Durable-Ack confirmation token the server must echo for this
     * request, or null when no tier is requested. A legacy request expects
     * the {@code "enabled"} token; explicit tier requests expect their own
     * token set back verbatim.
     */
    public static String expectedConfirmToken(int tiers) {
        if ((tiers & LEGACY_TRUE) != 0) {
            return "enabled";
        }
        return explicitToken(tiers);
    }

    public static boolean hasLocal(int tiers) {
        return (tiers & LOCAL) != 0;
    }

    public static boolean hasReplicated(int tiers) {
        return (tiers & REPLICATED) != 0;
    }

    /**
     * True when the sender's trim trigger is {@code STATUS_LOCAL_DURABLE_ACK}:
     * the local tier is requested without the replicated one. Any request
     * including the replicated tier trims on {@code STATUS_DURABLE_ACK} — the
     * strongest requested guarantee wins.
     */
    public static boolean isTrimOnLocalAck(int tiers) {
        return hasLocal(tiers) && !hasReplicated(tiers);
    }

    /**
     * Parses a {@code request_durable_ack} value into a tier set, or -1 for
     * an unrecognized value. {@code on} maps to the replicated tier with
     * {@link #LEGACY_TRUE} set; {@code off} is {@link #NONE}.
     */
    public static int parseConfigValue(CharSequence value) {
        if (value == null) {
            return -1;
        }
        String v = value.toString().trim();
        if (v.equalsIgnoreCase("off")) {
            return NONE;
        }
        if (v.equalsIgnoreCase("on")) {
            return REPLICATED | LEGACY_TRUE;
        }
        if (v.equalsIgnoreCase("local")) {
            return LOCAL;
        }
        if (v.equalsIgnoreCase("replicated")) {
            return REPLICATED;
        }
        if (v.equalsIgnoreCase("local,replicated") || v.equalsIgnoreCase("replicated,local")) {
            return LOCAL | REPLICATED;
        }
        return -1;
    }

    /**
     * The X-QWP-Request-Durable-Ack header value for a tier set, or null when
     * no tier is requested. A legacy set sends the literal {@code "true"},
     * the only request value servers without tier support recognize.
     */
    public static String requestHeaderValue(int tiers) {
        if ((tiers & LEGACY_TRUE) != 0) {
            return "true";
        }
        return explicitToken(tiers);
    }

    private static String explicitToken(int tiers) {
        switch (tiers & (LOCAL | REPLICATED)) {
            case LOCAL:
                return "local";
            case REPLICATED:
                return "replicated";
            case LOCAL | REPLICATED:
                return "local,replicated";
            default:
                return null;
        }
    }
}
