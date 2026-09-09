/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

/** Current borrow and one pending retirement. Returned borrows have no observation history. */
public final class SchemaRejectionState {
    private Lease current;
    private Pending pending;
    private CursorSendEngine engine;
    private volatile long failedGeneration = -1L;
    private volatile long stopFsn = -1L;

    public void setEngine(CursorSendEngine engine) {
        this.engine = engine;
    }

    public synchronized void beginLease(long generation, long firstFsn, boolean transactional) {
        if (current != null) {
            if (current.active) {
                throw new IllegalStateException("previous lease is still active");
            }
            if (current.transactional != transactional) {
                throw new IllegalStateException("transaction mode must remain fixed for a sender");
            }
        }
        failedGeneration = -1L;
        current = new Lease(generation, firstFsn, transactional);
    }

    /**
     * Ends a lease and seals an open transactional rejection. The caller is
     * the sole producer and must exclude further publication before taking
     * {@code publishedFsn}.
     */
    public synchronized LineSenderServerException endLease(long generation, long publishedFsn) {
        Lease lease = current;
        if (lease == null || lease.generation != generation || !lease.active) {
            return null;
        }
        lease.endFsn = publishedFsn;
        sealIfNeeded(lease, publishedFsn);
        // Pool return must close normal transactions. A failed open tail may
        // return only after sealing the range which prevents its resurrection
        // by the next borrow's commit. Check before giving up producer ownership.
        if (engine != null && lease.transactional && publishedFsn >= lease.firstFsn
                && publishedFsn > engine.ackedFsn()
                && (pending == null || pending.lastFsn < publishedFsn)) {
            int flags = engine.liveQwpFrameFlags(publishedFsn);
            // ACK/trim can race this cold lookup. An already resolved closer
            // needs no longer to be present in the ring.
            if ((flags < 0 && publishedFsn > engine.ackedFsn())
                    || (flags >= 0 && (flags & QwpConstants.FLAG_DEFER_COMMIT) != 0)) {
                throw new IllegalStateException("returned transaction has no commit or rejection boundary");
            }
        }
        lease.active = false;
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
        Lease lease = current;
        if (lease == null || !lease.active || lease.generation != generation || lease.rawError == null) {
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
        if (pending != null) {
            return false;
        }
        Lease owner = current != null && current.active && rejectedFsn >= current.firstFsn ? current : null;
        if (owner == null) {
            // Transaction mode is not persisted. A recovered deferred group must
            // therefore be treated conservatively as transactional, regardless of
            // the new producer's settings. Bound it by the recovered namespace so
            // a new producer's closer cannot become part of the old transaction.
            long recoveredTip = engine != null && engine.wasRecoveredFromDisk()
                    ? Math.max(engine.recoveredCommitBoundaryFsn(), engine.recoveredOrphanTipFsn())
                    : -1L;
            boolean recovered = rejectedFsn <= recoveredTip;
            owner = new Lease(-1L, spanStart, recovered || (current != null && current.transactional));
            owner.active = false;
            owner.endFsn = recovered ? recoveredTip : current == null ? rejectedFsn
                    : current.active ? current.firstFsn - 1L : current.endFsn;
        } else if (owner.generation >= 0 && owner.rawError == null) {
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
        pending = null;
        stopFsn = -1L;
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
            if ((flags & QwpConstants.FLAG_DEFER_COMMIT) == 0) {
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

        private Lease(long generation, long firstFsn, boolean transactional) {
            this.generation = generation;
            this.firstFsn = firstFsn;
            this.transactional = transactional;
        }
    }
}
