/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;

import java.util.ArrayDeque;

/** Process-local lease ownership and one pending schema-retirement range. */
public final class SchemaRejectionState {
    private final ArrayDeque<Lease> leases = new ArrayDeque<>();
    private Pending pending;
    private CursorSendEngine engine;
    private volatile long acknowledgedFsn = -1L;
    private volatile long failedGeneration = -1L;
    private volatile long stopFsn = -1L;

    public void setEngine(CursorSendEngine engine) {
        this.engine = engine;
    }

    public synchronized void beginLease(long generation, long firstFsn, boolean transactional) {
        prune(acknowledgedFsn);
        Lease tail = leases.peekLast();
        if (tail != null && tail.active) {
            throw new IllegalStateException("previous lease is still active");
        }
        leases.addLast(new Lease(generation, firstFsn, transactional));
    }

    /**
     * Ends a lease and seals an open transactional rejection. The caller is
     * the sole producer and must exclude further publication before taking
     * {@code publishedFsn}.
     */
    public synchronized LineSenderServerException endLease(long generation, long publishedFsn) {
        Lease lease = findGeneration(generation);
        if (lease == null || !lease.active) {
            return null;
        }
        lease.endFsn = publishedFsn;
        lease.active = false;
        if (publishedFsn < lease.firstFsn && lease.rawError == null) {
            // Empty borrows carry no attribution history, even behind an unacked lease.
            leases.removeLast();
            return null;
        }
        sealIfNeeded(lease, publishedFsn);
        if (failedGeneration == generation) {
            failedGeneration = -1L;
        }
        return lease.failure;
    }

    /**
     * Returns this generation's immutable owned failure. For an open
     * transaction, the first producer observation seals its end at the supplied
     * publication snapshot. Calls on one sender are single-producer by contract.
     */
    public synchronized LineSenderServerException ownedFailure(long generation, long publishedFsn) {
        Lease lease = findGeneration(generation);
        if (lease == null || lease.rawError == null) {
            return null;
        }
        sealIfNeeded(lease, publishedFsn);
        return lease.failure;
    }

    public boolean hasOwnedFailure(long generation) {
        return failedGeneration == generation;
    }

    /** I/O-thread install. Returns false while an earlier retirement is pending. */
    public synchronized boolean reject(long rejectedFsn, long spanStart, SenderError rawError) {
        prune(acknowledgedFsn);
        if (pending != null) {
            return false;
        }
        Lease owner = findOwner(rejectedFsn);
        if (owner == null) {
            // Transaction mode is not persisted. A recovered deferred group must
            // therefore be treated conservatively as transactional, regardless of
            // the new producer's settings. Bound it by the recovered namespace so
            // a new producer's closer cannot become part of the old transaction.
            long recoveredTip = engine != null && engine.wasRecoveredFromDisk()
                    ? Math.max(engine.recoveredCommitBoundaryFsn(), engine.recoveredOrphanTipFsn())
                    : -1L;
            boolean recovered = rejectedFsn <= recoveredTip;
            owner = new Lease(-1L, spanStart, recovered);
            owner.active = false;
            owner.endFsn = recovered ? recoveredTip : rejectedFsn;
        } else if (owner.rawError == null) {
            owner.rawError = rawError;
            failedGeneration = owner.generation;
        }
        long end = rejectedFsn;
        if (owner.transactional) {
            long tip = owner.active && engine != null ? engine.publishedFsn() : owner.endFsn;
            long closer = firstCommitFsn(rejectedFsn, tip);
            end = closer >= 0 ? closer : owner.active ? -1L : tip;
        }
        pending = new Pending(spanStart, end, owner, rawError);
        stopFsn = spanStart;
        if (end >= spanStart) {
            finishFailure(end);
        }
        return true;
    }

    public long stopFsn() {
        return stopFsn;
    }

    public synchronized Range sealedRange() {
        if (pending == null || pending.error == null) {
            return null;
        }
        return new Range(pending.firstFsn, pending.lastFsn, pending.error);
    }

    public synchronized void completeRetirement(long lastFsn) {
        if (pending == null || pending.lastFsn != lastFsn) {
            throw new IllegalStateException("retirement range changed");
        }
        acknowledgedThrough(lastFsn);
        pending.owner.retired = true;
        pending = null;
        stopFsn = -1L;
        prune(lastFsn);
    }

    public void acknowledgedThrough(long fsn) {
        if (fsn > acknowledgedFsn) {
            acknowledgedFsn = fsn;
        }
    }

    private void sealIfNeeded(Lease lease, long publishedFsn) {
        if (pending == null || pending.owner != lease || pending.error != null) {
            return;
        }
        long end = pending.lastFsn;
        if (lease.transactional) {
            long closer = firstCommitFsn(pending.rawError.getRejectedFsn(), publishedFsn);
            end = closer >= 0 ? closer : publishedFsn;
        }
        finishFailure(end);
    }

    private long firstCommitFsn(long first, long last) {
        if (engine == null) {
            return -1L;
        }
        for (long fsn = first; fsn <= last; fsn++) {
            int flags = engine.liveQwpFrameFlags(fsn);
            if (flags < 0) {
                throw new IllegalStateException("missing frame while resolving transaction at FSN " + fsn);
            }
            if ((flags & io.questdb.client.cutlass.qwp.protocol.QwpConstants.FLAG_DEFER_COMMIT) == 0) {
                return fsn;
            }
        }
        return -1L;
    }

    private void finishFailure(long lastFsn) {
        pending.lastFsn = lastFsn;
        pending.error = pending.rawError.withRejectionSpan(pending.firstFsn, lastFsn);
        Lease lease = pending.owner;
        if (lease.failure == null && lease.generation >= 0) {
            lease.failure = new LineSenderServerException(pending.error);
        }
    }

    private Lease findGeneration(long generation) {
        Lease tail = leases.peekLast();
        if (tail != null && tail.generation == generation) {
            return tail;
        }
        for (Lease lease : leases) {
            if (lease.generation == generation) {
                return lease;
            }
        }
        return null;
    }

    private Lease findOwner(long fsn) {
        for (Lease lease : leases) {
            if (fsn >= lease.firstFsn && (lease.active || fsn <= lease.endFsn)) {
                return lease;
            }
        }
        return null;
    }

    private void prune(long fsn) {
        while (true) {
            Lease head = leases.peekFirst();
            if (head == null || head.active || (head.rawError != null && !head.retired) || head.endFsn > fsn) {
                return;
            }
            leases.removeFirst();
        }
    }

    public static final class Range {
        public final SenderError error;
        public final long firstFsn;
        public final long lastFsn;

        private Range(long firstFsn, long lastFsn, SenderError error) {
            this.firstFsn = firstFsn;
            this.lastFsn = lastFsn;
            this.error = error;
        }
    }

    private static final class Pending {
        private final long firstFsn;
        private long lastFsn;
        private final Lease owner;
        private final SenderError rawError;
        private SenderError error;

        private Pending(long firstFsn, long lastFsn, Lease owner, SenderError rawError) {
            this.firstFsn = firstFsn;
            this.lastFsn = lastFsn;
            this.owner = owner;
            this.rawError = rawError;
        }
    }

    private static final class Lease {
        private final long firstFsn;
        private final long generation;
        private final boolean transactional;
        private boolean active = true;
        private long endFsn = -1L;
        private LineSenderServerException failure;
        private SenderError rawError;
        private boolean retired;

        private Lease(long generation, long firstFsn, boolean transactional) {
            this.generation = generation;
            this.firstFsn = firstFsn;
            this.transactional = transactional;
        }
    }
}
