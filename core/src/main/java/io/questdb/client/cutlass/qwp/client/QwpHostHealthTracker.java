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

package io.questdb.client.cutlass.qwp.client;

/**
 * Per-client bookkeeping that ranks the configured endpoint list when picking
 * the next host to try. Mirrors the .NET client's QwpHostHealthTracker.
 * <p>
 * Within a round, {@link #pickNext()} returns the highest-priority host that
 * has not yet been attempted; the caller advances the round via
 * {@link #beginRound(boolean)}. Priority is the lexicographic
 * {@code (state, zone_tier)} tuple per failover.md §2 -- state outranks zone
 * so a known-good cross-zone host is picked before an untried local host.
 * <p>
 * Each method is internally synchronized, but pickNext + recordX is not atomic
 * across the pair. Callers of the SHARED-round API (pickNext / beginRound /
 * isRoundExhausted) must externally serialize a pick → record sequence (the
 * ingest sender does this by keeping its foreground connect walk single-file
 * behind a lock; the query client via its documented one-execute-at-a-time
 * contract).
 * <p>
 * Concurrent walkers that must not consume or poison the shared round --
 * the ingest sender's background orphan drainers -- use a private
 * {@link RoundCursor} ({@link #newRoundCursor()}) paired with the
 * health-only record overloads ({@code markRoundAttempted=false}): the
 * cursor's attempted set is walker-local (claim-at-pick, so concurrent
 * cursors never race on the pick → record pair), while state/zone updates
 * flow into the shared health ledger that orders everyone's picks.
 */
public final class QwpHostHealthTracker {
    public enum HostState {
        UNKNOWN,
        HEALTHY,
        TRANSIENT_REJECT,
        TRANSPORT_ERROR,
        TOPOLOGY_REJECT,
    }

    /**
     * Server-zone classification relative to the client's configured
     * {@code zone=} value (failover.md §2). {@code SAME} ranks first,
     * {@code UNKNOWN} second, {@code OTHER} last. When the client zone is
     * unset or {@code target=primary}, every host's tier collapses to
     * {@code SAME}, degenerating selection to state-only ordering.
     */
    public enum ZoneTier {
        SAME,
        UNKNOWN,
        OTHER,
    }

    private static final HostState[] PRIORITY_ORDER = {
            HostState.HEALTHY,
            HostState.UNKNOWN,
            HostState.TRANSIENT_REJECT,
            HostState.TRANSPORT_ERROR,
            HostState.TOPOLOGY_REJECT,
    };
    private static final ZoneTier[] ZONE_PRIORITY_ORDER = {
            ZoneTier.SAME,
            ZoneTier.UNKNOWN,
            ZoneTier.OTHER,
    };

    private final boolean[] attemptedThisRound;
    // Configured client zone, lower-cased and trimmed at construction. Null
    // when the user did not pass zone= -- in that case every host's tier is
    // collapsed to SAME (zone-blind selection, identical to the pre-zone
    // behaviour). Comparison against server-advertised zone_id is
    // case-insensitive per failover.md §1.1.
    private final String configuredZone;
    private final int hostCount;
    private final long[] lastSuccessEpoch;
    private final Object lock = new Object();
    private final HostState[] states;
    // True when target=primary. Per failover.md §2, every host's zone tier
    // collapses to SAME because writers must be followed across zones.
    // recordZone is still called per the spec pseudocode but the tier never
    // leaves SAME.
    private final boolean targetPrimary;
    private final ZoneTier[] zoneTiers;
    private long successEpoch;

    /**
     * Constructs a zone-blind tracker; equivalent to
     * {@link #QwpHostHealthTracker(int, String, boolean)} with no client zone
     * and {@code targetPrimary=false}.
     */
    public QwpHostHealthTracker(int hostCount) {
        this(hostCount, null, false);
    }

    /**
     * @param hostCount     number of configured endpoints (must be &gt; 0).
     * @param clientZone    client {@code zone=} value, opaque case-insensitive
     *                      string (e.g. {@code eu-west-1a}). Null or empty
     *                      collapses every host's zone tier to {@code SAME},
     *                      matching the pre-zone selection behaviour.
     * @param targetPrimary {@code true} when {@code target=primary}. Collapses
     *                      every host's zone tier to {@code SAME} regardless
     *                      of {@code clientZone}; writers must be followed
     *                      across zones (failover.md §2).
     */
    public QwpHostHealthTracker(int hostCount, String clientZone, boolean targetPrimary) {
        if (hostCount <= 0) {
            throw new IllegalArgumentException("hostCount must be > 0");
        }
        this.hostCount = hostCount;
        this.targetPrimary = targetPrimary;
        if (clientZone != null) {
            String trimmed = clientZone.trim();
            this.configuredZone = trimmed.isEmpty() ? null : trimmed;
        } else {
            this.configuredZone = null;
        }
        this.states = new HostState[hostCount];
        this.attemptedThisRound = new boolean[hostCount];
        this.lastSuccessEpoch = new long[hostCount];
        this.zoneTiers = new ZoneTier[hostCount];
        // When no zone preference is in effect, default every host to SAME so
        // selection degenerates to state-only ordering. Otherwise start at
        // UNKNOWN: the tier flips to SAME or OTHER the first time a zone is
        // observed via recordZone.
        ZoneTier initialTier = (configuredZone == null || targetPrimary) ? ZoneTier.SAME : ZoneTier.UNKNOWN;
        for (int i = 0; i < hostCount; i++) {
            states[i] = HostState.UNKNOWN;
            zoneTiers[i] = initialTier;
        }
    }

    /**
     * Resets attempted flags. With {@code forgetClassifications}, every host
     * except the most-recently-successful {@code (HEALTHY, SAME)} entry is
     * reset to {@link HostState#UNKNOWN}; the sticky-Healthy keeps the last
     * same-zone successful host first in line on the next round. Cross-zone
     * (zone tier {@code OTHER}) {@code HEALTHY} entries are reset to
     * {@code UNKNOWN} rather than preserved -- a sticky pin in another zone
     * would otherwise defeat same-zone preference. Recency uses the
     * {@code recordSuccess} epoch counter, not array order.
     * <p>
     * Per failover.md §2.1, {@code zone_tier} is NOT cleared by this method --
     * once observed it persists across rounds.
     */
    public void beginRound(boolean forgetClassifications) {
        synchronized (lock) {
            int stickyIndex = -1;
            if (forgetClassifications) {
                long bestEpoch = -1L;
                for (int i = 0; i < hostCount; i++) {
                    if (states[i] == HostState.HEALTHY
                            && zoneTiers[i] == ZoneTier.SAME
                            && lastSuccessEpoch[i] > bestEpoch) {
                        bestEpoch = lastSuccessEpoch[i];
                        stickyIndex = i;
                    }
                }
            }
            for (int i = 0; i < hostCount; i++) {
                attemptedThisRound[i] = false;
                if (forgetClassifications && i != stickyIndex) {
                    states[i] = HostState.UNKNOWN;
                }
            }
        }
    }

    public int count() {
        return hostCount;
    }

    public HostState getState(int idx) {
        synchronized (lock) {
            return states[idx];
        }
    }

    public ZoneTier getZoneTier(int idx) {
        synchronized (lock) {
            return zoneTiers[idx];
        }
    }

    public boolean isRoundExhausted() {
        synchronized (lock) {
            for (int i = 0; i < hostCount; i++) {
                if (!attemptedThisRound[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Returns the highest-priority host not yet attempted this round, or -1
     * when the round is exhausted. Priority is the lexicographic
     * {@code (state, zone_tier)} tuple: state outranks zone, so a known-good
     * cross-zone host is picked before an untried local host. Within a tied
     * {@code (state, zone_tier)} bucket, the lowest array index wins.
     * <p>
     * The caller is expected to be externally serialized (see class doc): the
     * returned index is intended to be paired with a follow-up
     * {@code recordX(idx)} on the same logical thread.
     */
    public int pickNext() {
        synchronized (lock) {
            for (HostState p : PRIORITY_ORDER) {
                for (ZoneTier z : ZONE_PRIORITY_ORDER) {
                    for (int i = 0; i < hostCount; i++) {
                        if (!attemptedThisRound[i] && states[i] == p && zoneTiers[i] == z) {
                            return i;
                        }
                    }
                }
            }
            return -1;
        }
    }

    /**
     * Demotes a previously-Healthy host on send/receive failure. No-op when the
     * prior state is anything other than {@link HostState#HEALTHY} so a single
     * hiccup does not erase an already-captured topology or transient reject.
     * <p>
     * Per failover spec §2.1, this MUST NOT touch the round's attempted bit --
     * that flag is owned by the round lifecycle (record* / beginRound) and
     * reflects whether the loop has tried this host in the current round, which
     * is independent of mid-stream demotion. After the demote the host stays
     * pickable in the current round, but at TRANSPORT_ERROR priority it will
     * lose to any untried Healthy / Unknown / TransientReject peer.
     */
    public void recordMidStreamFailure(int idx) {
        synchronized (lock) {
            if (states[idx] == HostState.HEALTHY) {
                states[idx] = HostState.TRANSPORT_ERROR;
            }
        }
    }

    public void recordRoleReject(int idx, boolean isTransient) {
        recordRoleReject(idx, isTransient, true);
    }

    /**
     * Variant with an explicit round-bit policy. {@code markRoundAttempted =
     * false} updates only the shared health ledger (state), leaving the
     * shared round's attempted bit untouched — for walkers on a private
     * {@link RoundCursor} whose attempts must stay invisible to the shared
     * round (the ingest sender's background drainers).
     */
    public void recordRoleReject(int idx, boolean isTransient, boolean markRoundAttempted) {
        synchronized (lock) {
            states[idx] = isTransient ? HostState.TRANSIENT_REJECT : HostState.TOPOLOGY_REJECT;
            if (markRoundAttempted) {
                attemptedThisRound[idx] = true;
            }
        }
    }

    public void recordSuccess(int idx) {
        recordSuccess(idx, true);
    }

    /**
     * Variant with an explicit round-bit policy; see
     * {@link #recordRoleReject(int, boolean, boolean)}. The success epoch
     * (sticky-Healthy recency) is recorded either way — a background
     * walker's success is real health data.
     */
    public void recordSuccess(int idx, boolean markRoundAttempted) {
        synchronized (lock) {
            states[idx] = HostState.HEALTHY;
            if (markRoundAttempted) {
                attemptedThisRound[idx] = true;
            }
            lastSuccessEpoch[idx] = ++successEpoch;
        }
    }

    public void recordTransportError(int idx) {
        recordTransportError(idx, true);
    }

    /**
     * Variant with an explicit round-bit policy; see
     * {@link #recordRoleReject(int, boolean, boolean)}.
     */
    public void recordTransportError(int idx, boolean markRoundAttempted) {
        synchronized (lock) {
            states[idx] = HostState.TRANSPORT_ERROR;
            if (markRoundAttempted) {
                attemptedThisRound[idx] = true;
            }
        }
    }

    /**
     * Creates a walker-private full-sweep cursor over the host list. Each
     * {@link RoundCursor#next()} returns the highest-priority host this
     * cursor has not yet returned — priority is the same live
     * {@code (state, zone_tier)} tuple {@link #pickNext()} uses — and
     * claims it at pick time in the cursor's OWN attempted set, so:
     * <ul>
     *   <li>every cursor sweeps every host exactly once regardless of what
     *       other walkers do concurrently (no endpoint stealing);</li>
     *   <li>the pick → record pair needs no external serialization — the
     *       claim is cursor-local, and the health records are atomic;</li>
     *   <li>the shared round (attempted bits, {@link #beginRound},
     *       {@link #isRoundExhausted}) is never consulted nor mutated.</li>
     * </ul>
     * Pair with the {@code markRoundAttempted=false} record overloads so the
     * walker's results update shared health without touching the shared
     * round.
     */
    public RoundCursor newRoundCursor() {
        return new RoundCursor();
    }

    /** See {@link #newRoundCursor()}. Not thread-safe for sharing a single
     *  instance across walkers; create one per walk. */
    public final class RoundCursor {
        private final boolean[] attempted = new boolean[hostCount];

        private RoundCursor() {
        }

        /**
         * Highest-priority host this cursor has not yet returned, claimed at
         * pick time; -1 once the cursor has swept every host. Ordering reads
         * the LIVE shared health state under the tracker lock, so a state
         * change recorded by any walker between two calls re-ranks the
         * remaining hosts.
         */
        public int next() {
            synchronized (lock) {
                for (HostState p : PRIORITY_ORDER) {
                    for (ZoneTier z : ZONE_PRIORITY_ORDER) {
                        for (int i = 0; i < hostCount; i++) {
                            if (!attempted[i] && states[i] == p && zoneTiers[i] == z) {
                                attempted[i] = true;
                                return i;
                            }
                        }
                    }
                }
                return -1;
            }
        }
    }

    /**
     * Records a server-advertised zone for the given host index per
     * failover.md §2.1. Called once with {@code SERVER_INFO.zone_id} after a
     * successful upgrade (gated by {@code CAP_ZONE}), and
     * once with the {@code X-QuestDB-Zone} HTTP header on a {@code 421}
     * upgrade reject.
     * <p>
     * Update rules:
     * <ul>
     *   <li>{@code zoneId} null/empty (after trimming): no-op. The existing
     *       zone tier is preserved so a missing header on a {@code 421} does
     *       not erase a tier set by an earlier {@code SERVER_INFO}.</li>
     *   <li>Client zone unset or {@code target=primary}: tier = {@code SAME}
     *       regardless of {@code zoneId}.</li>
     *   <li>Otherwise: tier = {@code SAME} when {@code zoneId} matches
     *       {@code clientZone} case-insensitively, else {@code OTHER}.</li>
     * </ul>
     */
    public void recordZone(int idx, String zoneId) {
        if (zoneId == null) {
            return;
        }
        String trimmed = zoneId.trim();
        if (trimmed.isEmpty()) {
            return;
        }
        synchronized (lock) {
            if (configuredZone == null || targetPrimary) {
                zoneTiers[idx] = ZoneTier.SAME;
            } else if (trimmed.equalsIgnoreCase(configuredZone)) {
                zoneTiers[idx] = ZoneTier.SAME;
            } else {
                zoneTiers[idx] = ZoneTier.OTHER;
            }
        }
    }
}
