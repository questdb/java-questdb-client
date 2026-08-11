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

import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLockContentionException;
import io.questdb.client.std.Files;
import io.questdb.client.std.IntList;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;

/**
 * Elastic pool of {@link Sender} instances, each wrapped in a
 * {@link PooledSender} decorator. The pool keeps at least {@code minSize}
 * connections warm, grows on demand up to {@code maxSize}, and lets the
 * housekeeper reap slots that have idled longer than {@code idleTimeoutMillis}
 * or aged past {@code maxLifetimeMillis} (with {@code minSize} respected at
 * all times).
 * <p>
 * The hot borrow / return path takes a {@link ReentrantLock} but does no
 * per-call allocation; the underlying {@link ArrayDeque} of free decorators
 * is pre-sized to {@code maxSize}.
 * <p>
 * Connection creation happens outside the lock so a slow connect (TLS
 * handshake, DNS) does not block other borrowers or the housekeeper. The
 * pool tracks in-flight creations via {@code inFlightCreations} so the cap
 * check ({@code allSize + inFlightCreations + closingSlots + leakedSlots <
 * maxSize}) stays correct under concurrent borrows.
 * <p>
 * <b>Store-and-forward slots.</b> When the configuration enables SF
 * ({@code sf_dir} set), every sender owns an exclusive on-disk slot
 * {@code <sf_dir>/<sender_id>} guarded by a {@code flock}. A pool reuses one
 * immutable config string for every sender, so without intervention all
 * senders would inherit the same {@code sender_id}, point at the same slot,
 * and every sender after the first would die with "sf slot already in use".
 * The pool therefore hands each slot a distinct id {@code <base>-<index>},
 * where {@code <base>} is the configured {@code sender_id} (default
 * {@code "default"}) and {@code <index>} is a stable pool slot index in
 * {@code [0, maxSize)}. Indices are reused deterministically (lowest free
 * first), so across a restart the same slot dirs are re-adopted and any
 * unacked data they hold is recovered on creation. A slot is only returned
 * to the free set once its delegate has released the {@code flock}, tracked
 * via {@code closingSlots} so a concurrent borrow can never reclaim a slot
 * dir whose lock is still held.
 * <p>
 * Recovery of managed SF slots is asynchronous: a direct pool owns one
 * long-lived background recovery thread, while a pooled {@code QuestDB}
 * handle delegates the same serial scan to its {@link PoolHousekeeper}.
 * Construction never performs network recovery inline.
 */
public final class SenderPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SenderPool.class);
    // Per-slot wall-clock cap on a single startup-recovery drain. Kept BELOW the
    // PoolHousekeeper stop/join budget (PoolHousekeeper.STOP_TIMEOUT_MILLIS) so a
    // recovery drain still in flight when close() arrives cannot outlive the
    // driver join -- the residual-budget bound that, together with the early
    // markClosing() signal, keeps close() prompt (C1 fix).
    //
    // This caps only the DRAIN. The recovery build that precedes it is bounded
    // separately: recovery delegates force initial_connect_mode=OFF (see
    // defaultRecoverySender) so the build does at most ONE connect attempt
    // rather than a SYNC reconnect-budget retry loop (M1). One in-flight
    // connect against a black-holed host still blocks on the OS connect timeout
    // -- the residual window documented on recoverOneSlotStep -- because the
    // transport has no application-level connect timeout to clamp it.
    private static final long RECOVERY_DRAIN_BUDGET_MILLIS = 1_000;
    // The scan parks ONE candidate slot and moves on (C2) on the slot's Nth
    // (3rd) consecutive recoverOneSlotStep failure: two retries in place,
    // park on the third. The retry-in-place policy
    // assumes a failure is server-wide and will repeat for every remaining
    // slot, but a slot-specific persistent condition (e.g. a corrupt slot dir
    // that fails the recovery build every time) would otherwise pin the cursor
    // forever: the driver would livelock retrying that one slot each second
    // while every higher-index slot's durable orphan data starves. Small
    // enough to bound how long one slot can block the scan; large enough that
    // the dominant server-wide transient failure keeps its retry-the-same-
    // candidate behavior for the first attempts.
    private static final int RECOVERY_MAX_SLOT_FAILURE_STREAK = 3;
    // A direct SenderPool has no PoolHousekeeper to retry a transient startup
    // recovery failure. Its private driver waits this long between failed
    // attempts so an unavailable server does not cause a hot retry loop.
    private static final long RECOVERY_RETRY_INTERVAL_MILLIS = 1_000;
    // Hard cap on close()'s wait for a creation that is blocked in DNS, TCP,
    // TLS, or WebSocket setup. The creator retains ownership after this budget:
    // once construction returns, borrow() observes closed and tears down the
    // delegate before releasing its reservation (including an SF slot).
    static final long MAX_CLOSE_CREATION_WAIT_MILLIS = 5_000;
    // Hard cap on close()'s outstanding-lease wait. The acquire timeout is a
    // BORROW policy -- Long.MAX_VALUE legitimately means "block until a slot
    // frees" -- and must never unbound SHUTDOWN: without this cap a forgotten
    // lease would hang close() forever. Mirrors the egress twin, whose
    // shutdown join is capped at QueryWorker.SHUTDOWN_JOIN_MILLIS (same value)
    // regardless of user config.
    static final long MAX_CLOSE_LEASE_WAIT_MILLIS = 5_000;
    private final long acquireTimeoutMillis;
    private final ArrayList<SenderSlot> all;
    private final ArrayDeque<SenderSlot> available;
    // Test-only constructor seam. Runs immediately before a possibly-started
    // recovery driver is joined after constructor failure. Null in production;
    // lifecycle tests use it to release a deliberately held driver and prove
    // delegate cleanup happens only after the join.
    private final Runnable beforeFailedStartupRecoveryJoinHook;
    private final String configurationString;
    // Signals completion of internally owned creation lifecycles. Kept
    // separate from slotReleased so a capacity waiter cannot consume the only
    // wakeup intended for close().
    private final Condition creationFinished;
    // User-supplied ingest callbacks, shared across every pooled Sender this
    // pool builds. Null -> each sender keeps its loud-not-silent default.
    private final SenderConnectionListener connectionListener;
    private final BackgroundDrainerListener drainerListener;
    private final SenderErrorHandler errorHandler;
    private final long idleTimeoutMillis;
    // Delivery channel for recovery-delegate errors that pass the
    // isRecoveryEventUserRelevant filter. Pool-owned so a slow user handler
    // can never stall the recovery driver / housekeeper thread or overrun
    // its stop budget; the dispatcher thread starts lazily on first offer,
    // so pools that never hit a recovery event pay zero thread cost. Null
    // when the user registered no errorHandler or SF is off.
    private final SenderErrorDispatcher recoveryErrorDispatcher;
    // Test seam. Production builds delegates via defaultSender(); white-box
    // tests in io.questdb.client.test.impl reach the package-private
    // constructor by reflection to inject a factory that throws a non-
    // RuntimeException Throwable (e.g. an -ea AssertionError) mid-prewarm,
    // exercising the Error-safe delegate cleanup loop.
    private final IntFunction<Sender> senderFactory;
    // Test seam: runs immediately after a delegate factory returns, before
    // listener registration and SenderSlot construction. Null in production;
    // error-safety tests inject a preallocated throwable at this ownership gap.
    private final Runnable postFactoryHook;
    // Factory for startup-recovery delegates. Distinct from senderFactory so a
    // recoverer can force a non-blocking initial connect (initial_connect_mode=
    // OFF) regardless of user config: a recovery build runs on a private direct
    // driver or the PoolHousekeeper thread and must NOT inherit SYNC
    // (auto-enabled by any reconnect_* knob), which would retry the connect for
    // the whole reconnect budget inside build() -- far past
    // PoolHousekeeper.STOP_TIMEOUT_MILLIS, so a close() landing during that
    // build could make its driver join time out and leave the recoverer holding
    // the slot flock after close() returned (M1). Mirrors senderFactory's test seam: an injected factory
    // (non-null) drives BOTH paths so white-box recovery tests keep control.
    private final IntFunction<Sender> recoverySenderFactory;
    private final ReentrantLock lock = new ReentrantLock();
    private final long maxLifetimeMillis;
    private final int maxSize;
    private final int minSize;
    // SF slot base id (configured sender_id, default "default") when SF is
    // enabled; null otherwise. Each pooled sender's slot id is
    // {@code slotBaseId + "-" + slotIndex}.
    private final String slotBaseId;
    // SF group root (sf_dir) when SF is enabled; null otherwise. Used to
    // locate this pool's own managed slot dirs <sfDir>/<slotBaseId>-<index>
    // for startup recovery of unacked data left by a previous run.
    private final String sfDir;
    // Reservation bitmap for SF slot indices [0, maxSize). Guarded by lock.
    // null when SF is disabled (no per-slot identity needed).
    private final boolean[] slotInUse;
    private final Condition slotReleased;
    // True iff the configuration enables store-and-forward (sf_dir set).
    private final boolean storeAndForward;
    // Direct SF pools own one long-lived private recovery driver. It starts
    // after construction state is initialized, scans only in the background,
    // and parks when recovery completes; a later release re-arms the same scan
    // and unparks this same thread. Deferred pools use PoolHousekeeper and
    // leave this null. Joined by stopStartupRecoveryDriver() on close.
    private final Thread startupRecoveryThread;
    // Driver policy: deferred pools are driven by PoolHousekeeper (never spawn
    // a private driver); direct SF pools own the single thread above.
    private final boolean deferStartupRecovery;
    private final ThreadFactory recoveryThreadFactory;
    // Test-only constructor seam for the failed-retry operation. Production
    // uses waitForStartupRecoveryRetry(), which performs the one-second signal
    // wait; lifecycle tests inject an event barrier without wall-clock checks.
    private final Runnable startupRecoveryWaiter;
    // Test seam: runs after the direct recovery driver's bounded join returns.
    // Null in production; lifecycle tests prove the joined thread is quiescent.
    private volatile Runnable afterStartupRecoveryJoinHook;
    // Test seam: runs immediately before a capacity-starved borrow enters its
    // condition wait, while it still holds the pool lock. Null in production;
    // concurrency tests use a latch here to prove that several borrowers have
    // all reached the wait path before recovering retired capacity.
    private volatile Runnable beforeBorrowWaitHook;
    // Test seam: runs immediately before the direct recovery driver's bounded
    // join. Null in production; lifecycle tests coordinate close() with a
    // deliberately held driver.
    private volatile Runnable beforeStartupRecoveryJoinHook;
    // Test seam: runs after a capacity-starved borrow's condition wait has
    // exhausted its positive timeout, before the loop's terminal pass. Null in
    // production; regression tests release a retired slot here to prove that
    // the terminal pass re-probes returned capacity before throwing.
    private volatile Runnable borrowWaitExpiredHook;
    // Test seam invoked after close() handles an interrupt and confirms the
    // original creation-wait deadline still permits another wait. Null in
    // production; lifecycle tests use it to acknowledge distinct retries
    // without inspecting transient Condition queue membership.
    private volatile Runnable creationWaitRetryHook;
    // Slots removed from `all` whose delegate is still releasing its flock.
    // They keep reserving capacity (and their slotInUse mark) until the
    // flock drops, so the cap check and the slot allocator stay consistent
    // and no concurrent borrow can reclaim a slot dir that is still locked.
    // Guarded by lock. Only ever ticks for SF slots.
    private int closingSlots;
    // Shutdown signal: "the pool is shutting down". markClosing() raises it early
    // (without tearing down delegates) so an in-flight startup-recovery step
    // stops promptly between slots; close() also raises it. Read on the hot
    // paths (borrow/giveBack/discardBroken/reapIdle/recovery).
    private volatile boolean closed;
    // True once close() has begun the one-and-only delegate teardown. Distinct
    // from `closed` so markClosing() can raise the shutdown signal early
    // (cancelling recovery) WITHOUT making a later close() short-circuit the
    // teardown. Guarded by lock.
    private boolean closeStarted;
    private int inFlightCreations;
    // Lease teardowns currently running on borrower threads (retireLease's
    // delegate-close section, outside the lock). close() counts these as
    // outstanding so it does not return while a delegate is still being torn
    // down on another thread. Guarded by lock.
    private int pendingLeaseTeardowns;
    // Slots whose delegate close() returned with the SF flock still held
    // because an I/O or manager worker did not stop. Consumed while retired:
    // never freed and never reused, so no borrow ever hands out a still-
    // locked slot dir. Counted in the cap check so the lost capacity is
    // accounted for. NOT necessarily permanent: engine cleanup may be pending
    // on a worker/I/O-thread exit path, so reprobeRetiredSlots() re-checks
    // retiredSlots and returns any index whose flock has since dropped.
    // Guarded by lock; only ever ticks for SF slots.
    private int leakedSlots;
    // Deterministic white-box complexity counter. Counts delegate release
    // probes performed by direct callbacks and fallback scans. Guarded by lock.
    private long retiredSlotProbeCount;
    // The retired slots behind the leakedSlots count: runtime reclaim paths
    // (discardBroken/reapIdle via reclaimSlot) and the in-range startup-
    // recovery pass (recoverOneSlotStep, which retains the recoverer slot for
    // exactly this purpose). Re-probed by reprobeRetiredSlots() so a late
    // flock release (deferred engine cleanup on a worker exit path) restores
    // the pool's capacity instead of ratcheting it down until process exit.
    // Out-of-range startup recoverers are NEVER added: they carry no
    // leakedSlots tick and their index has no slotInUse entry to free.
    // Pre-sized to maxSize (every entry keeps a distinct in-range slot index
    // reserved, so size can never exceed maxSize): add() never grows the
    // backing array, so a retire (leakedSlots++ then add, under lock) cannot
    // fail on allocation and strand a counted-but-untracked slot that
    // reprobeRetiredSlots() could never recover. Guarded by lock.
    private final ArrayList<SenderSlot> retiredSlots;
    // SF slots currently held by the in-range startup-recovery pass
    // (recoverOneSlotStep): each is reserved under `lock` for the
    // duration of its drain and counted in the borrow() cap check so a
    // concurrent borrow can neither over-allocate past maxSize nor target a
    // dir being recovered. Both the direct-pool background driver and deferred
    // housekeeper driver can overlap borrow()/return after publication.
    // Guarded by lock; only ever ticks for SF slots.
    private int recoveringSlots;
    // Resumable startup-recovery scan cursor. Advanced only by the pool's
    // single recovery driver -- the direct pool's long-lived private thread or
    // the PoolHousekeeper thread (the sole deferred driver) -- so the cursor
    // itself needs no lock; the per-slot reservation it performs
    // (slotInUse/recoveringSlots) is still taken under `lock` because borrow()
    // races it. recoveryInRangeNext is the next in-range index in [0, maxSize)
    // for pass 1; recoveryOutOfRange / recoveryOutOfRangeNext are the lazily
    // built pass-2 work list (same-base slots at index >= maxSize) and its
    // cursor; recoveryComplete latches true only when the whole scan finishes.
    // A transient build failure or drain timeout leaves the current candidate
    // pending so a later tick or explicit drive can retry it on the same pool.
    // A slot-SPECIFIC failure -- flock contention with another live owner, or
    // RECOVERY_MAX_SLOT_FAILURE_STREAK consecutive failures on one slot -- is
    // instead "parked": the cursor advances past it so it cannot starve the
    // remaining slots, recoveryDeferredThisCycle records the park, and once
    // both passes finish with parked slots outstanding the cursors rewind for
    // another cycle on a later tick instead of latching recoveryComplete.
    // A RETIRED index whose dir still holds data is deferred the same way on
    // every walk (see the reserved-skip branch in recoverOneSlotStep) so the
    // latch can never strand a retired slot's data while the pool lives.
    // If the retire lands only AFTER the scan latched (a runtime
    // discardBroken/reapIdle reclaim), the late flock release re-arms the
    // scan instead: recoverRetiredSlotAt() rewinds the cursors, clears
    // recoveryComplete and unparks the direct pool's existing driver. The
    // volatile un-latch (written under `lock` by release callbacks and
    // reprobes) is visible to its unlocked gate; deferred pools observe it on
    // the next housekeeper tick.
    // recoveryFailStreak/-Slot track consecutive failures on one candidate;
    // recoveryWarnedSlots dedups the per-slot WARNs so an indefinitely
    // retried slot logs once per failure episode, not once per retry. All of
    // this state is owned by the single recovery driver like the cursors.
    private int recoveryInRangeNext;
    private IntList recoveryOutOfRange;
    private int recoveryOutOfRangeNext;
    private volatile boolean recoveryComplete;
    // Set by rearmRecoveryScanIfStranded() whenever a freed index leaves a
    // dir that is still a candidate orphan while the scan is alive -- reached
    // from a retired slot's late flock release (recoverRetiredSlotAt) or from
    // reclaimSlot's clean-release arm -- and consumed (under `lock`) by the
    // scan's end-of-cycle latch decision. Closes the mid-cycle window: the
    // release can land while the scan is alive but after the cursor already
    // passed that index (it was LIVE when walked -- no retired-candidate
    // deferral -- and recoveryComplete was still false at release, so the
    // post-latch re-arm does not apply). Without this flag such a cycle
    // could end with zero deferrals and latch past the freed dir's stranded
    // data. Producer and consumer both run under `lock`, so no set can slip
    // between the driver's check and its latch write.
    private volatile boolean recoveryRearmRequested;
    private int recoveryDeferredThisCycle;
    private int recoveryFailStreak;
    private int recoveryFailStreakSlot = -1;
    private final IntList recoveryWarnedSlots = new IntList();

    public SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, null, false, null, null, null, null, null, null, null);
    }

    // Test-only constructor exposing the senderFactory seam: production builds
    // via the full constructor below (senderFactory null -> the real
    // defaultSender()). White-box tests inject a factory that throws a
    // non-RuntimeException Throwable mid-prewarm. A direct SF pool owns one
    // background recovery thread; the pooled QuestDB handle uses the 8-arg
    // overload to defer recovery to the housekeeper thread.
    @TestOnly
    public SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, senderFactory, false);
    }

    // Test-only constructor adding the deferStartupRecovery toggle.
    // deferStartupRecovery=true leaves the pool without a private recovery
    // thread; the owner (QuestDBImpl) drives recovery one slot per tick on the
    // PoolHousekeeper thread via runStartupRecoveryStep(). White-box SF tests
    // call this directly; the in-range recovery pass is concurrency-safe
    // against borrow()/return after pool publication -- see
    // recoverOneSlotStep().
    @TestOnly
    public SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory,
            boolean deferStartupRecovery
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, senderFactory,
                deferStartupRecovery, null, null, null, null, null, null, null);
    }

    // Test-only constructor adding a deterministic fault hook for the ownership
    // gap after a delegate factory returns.
    @TestOnly
    public SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory,
            boolean deferStartupRecovery,
            Runnable postFactoryHook
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, senderFactory,
                deferStartupRecovery, null, null, null, postFactoryHook, null, null, null);
    }

    @TestOnly
    public static SenderPool createWithRecoveryControlsForTesting(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            IntFunction<Sender> senderFactory,
            ThreadFactory recoveryThreadFactory,
            Runnable recoveryWaiter,
            Runnable beforeFailedRecoveryJoinHook
    ) {
        return new SenderPool(configurationString, minSize, maxSize, acquireTimeoutMillis,
                Long.MAX_VALUE, Long.MAX_VALUE, senderFactory, false,
                null, null, null, null, recoveryThreadFactory, recoveryWaiter,
                beforeFailedRecoveryJoinHook);
    }

    // Full constructor adding the user-supplied ingest callbacks (error
    // handler, connection listener and background-drainer listener), applied
    // to every Sender the pool builds (see buildManagedSlotSender). The public
    // 6-arg ctor and the test-only senderFactory overloads above both delegate
    // here with null callbacks; the pooled QuestDB handle calls this directly.
    SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory,
            boolean deferStartupRecovery,
            SenderErrorHandler errorHandler,
            SenderConnectionListener connectionListener,
            BackgroundDrainerListener drainerListener
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, senderFactory,
                deferStartupRecovery, errorHandler, connectionListener,
                drainerListener, null, null, null, null);
    }

    private SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory,
            boolean deferStartupRecovery,
            SenderErrorHandler errorHandler,
            SenderConnectionListener connectionListener,
            BackgroundDrainerListener drainerListener,
            Runnable postFactoryHook,
            ThreadFactory recoveryThreadFactory,
            Runnable recoveryWaiter,
            Runnable beforeFailedRecoveryJoinHook
    ) {
        if (minSize < 0 || maxSize < 1 || minSize > maxSize) {
            throw new IllegalArgumentException("invalid pool sizing: min=" + minSize + ", max=" + maxSize);
        }
        this.errorHandler = errorHandler;
        this.connectionListener = connectionListener;
        this.drainerListener = drainerListener;
        this.senderFactory = senderFactory != null ? senderFactory : this::defaultSender;
        // An injected factory (tests) drives recovery too, preserving the
        // white-box recovery seam; production recovery forces OFF-mode connects
        // via defaultRecoverySender (see field comment / createRecoverer).
        this.recoverySenderFactory = senderFactory != null ? senderFactory : this::defaultRecoverySender;
        this.configurationString = configurationString;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.maxLifetimeMillis = maxLifetimeMillis;
        this.postFactoryHook = postFactoryHook;
        this.beforeFailedStartupRecoveryJoinHook = beforeFailedRecoveryJoinHook;
        this.deferStartupRecovery = deferStartupRecovery;
        this.recoveryThreadFactory = recoveryThreadFactory;
        this.startupRecoveryWaiter = recoveryWaiter != null
                ? recoveryWaiter
                : this::waitForStartupRecoveryRetry;
        this.all = new ArrayList<>(maxSize);
        this.available = new ArrayDeque<>(maxSize);
        this.retiredSlots = new ArrayList<>(maxSize);
        this.creationFinished = lock.newCondition();
        this.slotReleased = lock.newCondition();
        // Probe the config once, up front: this validates it eagerly (so a
        // bad config fails at construction even when minSize == 0) and tells
        // us whether SF is on and, if so, the base slot id to derive
        // per-sender ids from.
        Sender.LineSenderBuilder probe = Sender.builder(configurationString);
        this.storeAndForward = probe.isStoreAndForwardEnabled();
        this.slotBaseId = this.storeAndForward ? probe.getConfiguredSenderId() : null;
        this.sfDir = this.storeAndForward ? probe.getConfiguredSfDir() : null;
        this.slotInUse = this.storeAndForward ? new boolean[maxSize] : null;
        this.recoveryErrorDispatcher = (errorHandler != null && this.storeAndForward)
                ? new SenderErrorDispatcher(errorHandler, SenderErrorDispatcher.DEFAULT_CAPACITY,
                        "qdb-sf-pool-recovery-errors")
                : null;
        // Pre-warm minSize connections. Pre-warm runs single-threaded in the
        // constructor, so slots 0..minSize-1 are reserved directly.
        int built = 0;
        try {
            for (int i = 0; i < minSize; i++) {
                if (storeAndForward) {
                    slotInUse[i] = true;
                }
                SenderSlot ps = createUnlocked(storeAndForward ? i : -1);
                all.add(ps);
                available.add(ps);
                built++;
            }
        } catch (Throwable e) {
            // Catch Throwable, not just RuntimeException: createUnlocked() runs a
            // heavy native build path (mmap, flock, WebSocket connect) that can
            // throw an Error -- e.g. an -ea AssertionError or OutOfMemoryError --
            // mid-prewarm. If we only caught RuntimeException the Error would
            // propagate without running the cleanup below, leaking every
            // already-built delegate's flock + mmap'd ring + I/O thread and
            // resurrecting "sf slot already in use" on the next attempt.
            closePrewarmedDelegates(built);
            throw e;
        }
        // Prewarm succeeded. Direct SF pools recover exclusively on one
        // background thread; deferred pools are driven by PoolHousekeeper.
        // Assign the final field before start() so the runnable and every
        // callback observe the one published driver reference.
        Thread recoveryThread = null;
        try {
            if (storeAndForward && !deferStartupRecovery) {
                ThreadFactory threadFactory = recoveryThreadFactory != null
                        ? recoveryThreadFactory
                        : SenderPool::createStartupRecoveryThread;
                recoveryThread = threadFactory.newThread(this::runStartupRecoveryLoop);
                recoveryThread.setDaemon(true);
            }
            this.startupRecoveryThread = recoveryThread;
            if (recoveryThread != null) {
                recoveryThread.start();
            }
        } catch (Throwable e) {
            // Thread allocation, configuration, or start can throw after
            // prewarm transferred delegate ownership to this constructor. The
            // pool cannot be returned, so stop a possibly-started driver before
            // tearing down delegates it could otherwise still recover against.
            // Preserve the construction throwable; cleanup is best-effort.
            markClosing();
            stopFailedStartupRecoveryDriver(recoveryThread);
            closePrewarmedDelegates(built);
            throw e;
        }
    }

    /**
     * Drives startup SF recovery toward completion in a single call, bounded by
     * one shared {@code acquireTimeoutMillis} wall-clock budget (and each
     * individual drain by {@link #RECOVERY_DRAIN_BUDGET_MILLIS}). Used only by
     * manual / test drives on pools without a private recovery thread. Budget
     * exhaustion or a transient slot failure leaves the cursor pending for a
     * later drive. No-op when SF is off, the pool is shutting down, or recovery
     * has already finished. Idempotent.
     */
    private void runStartupRecoveryWithinBudget() {
        if (!storeAndForward) {
            return;
        }
        // One shared wall-clock budget for the WHOLE scan, not per slot: without
        // it a reachable-but-not-acking server would pay a full drain timeout on
        // every stranded slot. One acquire timeout is the ceiling already
        // accepted for a single borrow, so we reuse it as the total budget; once
        // spent, the remaining slots wait for a later attempt (data stays
        // durable on disk).
        final long budgetNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        final long startNanos = System.nanoTime();
        while (!closed && !recoveryComplete) {
            // Subtract nanoTime samples before comparing them with the duration.
            // This avoids an overflowed absolute deadline when
            // acquireTimeoutMillis is Long.MAX_VALUE and keeps the intended
            // duration comparison explicit.
            long elapsedNanos = System.nanoTime() - startNanos;
            if (elapsedNanos >= budgetNanos) {
                LOG.warn("startup SF recovery: {}ms budget exhausted; "
                        + "deferring remaining slots", acquireTimeoutMillis);
                return;
            }
            long remainingNanos = budgetNanos - elapsedNanos;
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
            if (remainingMillis == 0) {
                // drain() accepts milliseconds; preserve a positive sub-ms
                // remainder instead of treating truncation as exhaustion.
                remainingMillis = 1;
            }
            if (!recoverOneSlotStep(Math.min(remainingMillis, RECOVERY_DRAIN_BUDGET_MILLIS))) {
                return;
            }
        }
    }

    private void runStartupRecoveryLoop() {
        while (!closed && !Thread.currentThread().isInterrupted()) {
            boolean hasImmediateWork = false;
            if (!recoveryComplete) {
                try {
                    hasImmediateWork = runStartupRecoveryStep();
                } catch (Throwable ignored) {
                    // Match PoolHousekeeper: startup recovery is best-effort
                    // and a delegate Error must not kill its only driver.
                }
            }
            if (closed || Thread.currentThread().isInterrupted()) {
                return;
            }
            if (!hasImmediateWork) {
                // Remain the sole owner for the pool's lifetime. The default
                // waiter parks indefinitely after completion and for a bounded
                // retry interval after failure. A release callback supplies an
                // unpark permit, which survives a callback-before-park race.
                startupRecoveryWaiter.run();
            }
        }
    }

    /**
     * Recovers at most ONE stranded managed slot and reports whether more remain.
     * The private direct driver or {@link PoolHousekeeper} drives steps
     * back-to-back: each step performs at most one drain bounded by
     * {@link #RECOVERY_DRAIN_BUDGET_MILLIS} -- kept below the driver stop/join
     * budget -- on a delegate whose initial connect is forced OFF
     * ({@link #defaultRecoverySender}) so the build makes at most one connect
     * attempt. Each owner raises the {@code closed} shutdown signal before
     * joining its driver, so a {@code close()} landing mid-recovery normally
     * only has to wait out a single bounded drain. The one
     * exception is an in-flight connect to a black-holed host, which blocks on
     * the OS connect timeout -- see the residual-window note on
     * {@link #recoverOneSlotStep}.
     * No-op (returns {@code false}) when SF is off, the pool is shutting down, or
     * recovery has already finished.
     *
     * @return {@code true} if recovery has more work immediately; {@code false}
     * when recovery is complete, the pool is shutting down, or a transient
     * failure deferred the current candidate until a later tick
     */
    boolean runStartupRecoveryStep() {
        if (!storeAndForward || closed || recoveryComplete) {
            return false;
        }
        return recoverOneSlotStep(RECOVERY_DRAIN_BUDGET_MILLIS);
    }

    /**
     * Best-effort recovery of unacked data left in this pool's own managed SF
     * slots by a previous run -- both the in-range slots {@code [0, maxSize)}
     * (pass 1) and the same-base slots a larger previous run left OUT of range
     * ({@code <base>-i} with {@code i >= maxSize}, pass 2). Performs at most ONE
     * actual drain per call, advancing the resumable scan cursor, and returns
     * whether more stranded slots remain.
     * <p>
     * Every pooled SF sender's orphan drainer deliberately excludes the whole
     * {@code [0, maxSize)} managed range (via
     * {@code orphanDrainExcludeManagedSlots}) so a sibling never adopts a slot
     * dir/lock the pool intends to (re)create -- that exclusion is what keeps the
     * per-slot ids from resurfacing "sf slot already in use". The trade-off is
     * that an in-range slot left holding unacked data is otherwise recovered ONLY
     * when the pool happens to (re)create that index; under steady low load the
     * pool may never grow to a high index, stranding that slot's data (durable on
     * disk, but undelivered) until a later restart or load spike. An out-of-range
     * slot is worse off still: the pool never re-creates its index, and the
     * per-sender drainer only adopts it when {@code drain_orphans=on}. This scan
     * closes both gaps regardless of {@code drain_orphans}.
     * <p>
     * The in-range pass reserves each slot index under {@code lock} for the
     * duration of its recovery AND counts it in the borrow() capacity check (via
     * {@code recoveringSlots}), so a concurrent borrow after pool publication
     * can neither target the dir being recovered nor over-allocate past
     * {@code maxSize}. Prewarmed/borrowed slots (already live, holding their
     * flock) are skipped, as are empty slots (a cheap directory probe); only a
     * slot that actually holds stranded data spends the step's single drain. A
     * RETIRED index (a wedged close() kept its flock; see {@link #reclaimSlot}
     * and the retire branch below) is likewise never drained in place, but
     * while its dir still holds data it counts as a deferral, so the scan keeps
     * cycling instead of latching {@code recoveryComplete} past stranded data
     * that only a restart or a lucky borrow of that index would ever deliver. The
     * out-of-range pass needs no reservation: those indices have no
     * {@code slotInUse} entry and are never allocated by borrow().
     * <p>
     * Best-effort throughout: a build/close Error or a slow drain is logged and
     * never propagates, since the data stays durable on disk for a later attempt.
     * A build failure or drain timeout stops the current drive but leaves its
     * candidate pending so a later driver attempt can retry after a transient
     * condition clears; it does not poison recovery for the life of the pool.
     * A slot-SPECIFIC condition -- flock contention with another live owner, or
     * {@link #RECOVERY_MAX_SLOT_FAILURE_STREAK} consecutive failures on the
     * same candidate -- instead parks that slot: the cursor advances past it so
     * one wedged slot cannot starve the remaining slots' recovery, and once
     * both passes finish with parked slots outstanding the scan rewinds for
     * another cycle on a later tick (nothing is abandoned while the pool
     * lives). Per-slot failure WARNs are deduplicated across retries via
     * {@link #warnSlotOnce}.
     * <p>
     * <b>Boundedness / residual window.</b> A continuing recovery step runs on
     * either the direct pool's private driver or the PoolHousekeeper thread, and
     * {@code close()} relies on a step finishing within
     * {@code PoolHousekeeper.STOP_TIMEOUT_MILLIS}. A step is build + drain +
     * close. The drain is capped by {@link #RECOVERY_DRAIN_BUDGET_MILLIS}
     * and the build forces {@code initial_connect_mode=OFF} (see
     * {@link #defaultRecoverySender}), so it makes at most ONE connect attempt
     * instead of a SYNC reconnect-budget retry loop. That removes the
     * minutes-long block a {@code reconnect_*}-tuned config used to cause (M1).
     * One residual window remains and is NOT closed here: a single in-flight
     * connect to a black-holed/firewalled host blocks on the OS connect timeout
     * (the transport exposes no application-level connect timeout to clamp it).
     * If {@code close()} lands during that one connect, its driver join can
     * still time out and the detached build releases the slot flock shortly
     * after {@code close()} returns. No data is lost (the slot stays durable on
     * disk); the exposure is a brief "sf slot already in use" window on an
     * immediate reopen, bounded by a single OS connect timeout.
     *
     * @return {@code true} if a drain was performed and more slots may remain;
     * {@code false} once the scan is complete or the pool is shutting down
     */
    private boolean recoverOneSlotStep(long stepBudgetMillis) {
        if (sfDir == null || !Files.exists(sfDir)) {
            recoveryComplete = true;
            return false;
        }
        final SenderSlot[] retained = new SenderSlot[1];

        // Pass 1: in-range managed slots [0, maxSize). Skip live and empty slots
        // cheaply; spend the step on the first slot that actually holds data.
        while (recoveryInRangeNext < maxSize) {
            if (closed) {
                return false;
            }
            int i = recoveryInRangeNext;
            String slotPath = sfDir + "/" + slotBaseId + "-" + i;
            // Reserve this index unless prewarm (or a concurrent borrow after
            // publication) already holds it live. Count the reservation in
            // recoveringSlots so the borrow() cap check cannot over-allocate
            // while this slot is held for recovery.
            boolean reserved;
            boolean retired = false;
            lock.lock();
            try {
                reserved = slotInUse[i];
                if (!reserved) {
                    slotInUse[i] = true;
                    recoveringSlots++;
                } else {
                    retired = isRetiredSlotIndex(i);
                }
            } finally {
                lock.unlock();
            }
            if (reserved) {
                // A reserved index is normally LIVE (prewarm or a concurrent
                // borrow owns it) and is none of recovery's business. A RETIRED
                // index is different: its flock is held by this pool's own
                // wedged former delegate, and any unacked data in its dir is
                // exactly as stranded as a CONTENDED slot's. Without counting
                // it as a deferral the scan would latch recoveryComplete and
                // abandon that data until a restart or a lucky borrow of this
                // exact index -- which steady low load may never produce. Count
                // it -- the same rule as a CONTENDED park -- so the end-of-scan
                // rewind keeps the cycle alive; once the deferred engine
                // cleanup releases the flock, reprobeRetiredSlots()/the release
                // callback frees the index and a later cycle reserves and
                // drains it right here. The isCandidateOrphan probe (a few
                // syscalls, outside the lock) keeps an already-clean retired
                // dir from cycling the scan forever, and racing a concurrent
                // recover/borrow can only over-count -- costing one extra
                // cheap rewound walk, never a missed candidate.
                if (retired && OrphanScanner.isCandidateOrphan(slotPath)) {
                    recoveryDeferredThisCycle++;
                }
                recoveryInRangeNext++;
                continue;
            }
            if (!OrphanScanner.isCandidateOrphan(slotPath)) {
                // No stranded data: release the reservation and keep scanning;
                // an empty slot must not cost a whole step.
                lock.lock();
                try {
                    recoveringSlots--;
                    slotInUse[i] = false;
                    slotReleased.signal();
                } finally {
                    lock.unlock();
                }
                recoveryInRangeNext++;
                continue;
            }
            // A real candidate -> spend the step on it. Advance the cursor only
            // after success so a transient build/drain failure remains retryable.
            RecoveryDrainOutcome outcome = drainCandidateSlotForRecovery(i, slotPath, stepBudgetMillis, retained);
            lock.lock();
            try {
                // Release the recovery reservation accounting; from here either
                // leakedSlots (retire) or the freed index carries the cap math.
                recoveringSlots--;
                if (retained[0] != null) {
                    // close() retained the flock because an I/O or manager
                    // worker did not stop. Retire the slot (mirror
                    // discardBroken/reapIdle): keep slotInUse[i] set and count it
                    // in leakedSlots so the borrow() cap math accounts for the
                    // lost capacity and no later borrow ever reuses the
                    // still-locked dir. Keep the recoverer in retiredSlots so
                    // reprobeRetiredSlots() restores the capacity once the
                    // deferred engine cleanup releases the flock — without it
                    // the retirement would be permanent even after the release
                    // (fatal at maxSize=1: every later borrow would time out).
                    leakedSlots++;
                    addRetiredSlot(retained[0]);
                    LOG.warn("startup SF recovery: slot {} retired: delegate close() returned with "
                                    + "the flock still held (I/O or manager worker did not stop); pool capacity reduced by 1, "
                                    + "now {} of {} usable [leakedSlots={}]; the slot is re-probed and recovered "
                                    + "if the worker releases the flock later",
                            i, maxSize - leakedSlots, maxSize, leakedSlots);
                } else {
                    slotInUse[i] = false;
                    // On a post-publication drive, a borrow may be waiting on
                    // capacity this recovery held; the freed index can now admit
                    // a creation.
                    slotReleased.signal();
                }
            } finally {
                lock.unlock();
            }
            if (outcome == RecoveryDrainOutcome.CONTENDED) {
                // Slot-specific by construction: another LIVE owner holds this
                // slot's flock and may do so indefinitely. Park the slot --
                // advance past it; the end-of-scan cycle rewind re-probes it
                // later -- so it cannot starve the remaining slots, and keep
                // scanning within this step (no drain was spent on it).
                recoveryDeferredThisCycle++;
                recoveryInRangeNext++;
                continue;
            }
            if (outcome == RecoveryDrainOutcome.FAILED) {
                if (parkAfterFailure(i)) {
                    // The same candidate failed RECOVERY_MAX_SLOT_FAILURE_STREAK
                    // consecutive times: stop presuming the failure is
                    // server-wide. Park the slot and move on; the end-of-scan
                    // cycle rewind retries it later, so nothing is abandoned.
                    recoveryDeferredThisCycle++;
                    recoveryInRangeNext++;
                    return true;
                }
                // Presumed transient/server-wide: stop this drive without
                // advancing the cursor. The same live pool retries this
                // candidate after the transient condition is removed instead
                // of paying a likely-identical failure per remaining slot.
                return false;
            }
            clearFailStreak(i);
            recoveryInRangeNext++;
            return true;
        }

        // Pass 1 done. Build the pass-2 work list once: same-base slots a
        // previous run left OUT of the current index range (<base>-i with
        // i >= maxSize, from a run with a larger maxSize). The pool never
        // re-creates these indices, and the per-sender drainer only adopts them
        // when drain_orphans=on, so without this pass their unacked data would
        // sit durable-but-undelivered under the default config. They are outside
        // [0, maxSize), have no slotInUse entry, and never affect the borrow()
        // cap math, so no reservation is needed.
        if (recoveryOutOfRange == null) {
            recoveryOutOfRange = OrphanScanner.listStrandedOutOfRangeManagedSlots(
                    sfDir, slotBaseId, maxSize);
            recoveryOutOfRangeNext = 0;
        }
        while (recoveryOutOfRangeNext < recoveryOutOfRange.size()) {
            if (closed) {
                return false;
            }
            int idx = recoveryOutOfRange.getQuick(recoveryOutOfRangeNext);
            String slotPath = sfDir + "/" + slotBaseId + "-" + idx;
            if (!OrphanScanner.isCandidateOrphan(slotPath)) {
                recoveryOutOfRangeNext++;
                continue;
            }
            RecoveryDrainOutcome outcome = drainCandidateSlotForRecovery(idx, slotPath, stepBudgetMillis, retained);
            if (retained[0] != null) {
                // Out of the pool's [0, maxSize) capacity range: there is no
                // slotInUse entry to retire and no future borrow targets this
                // dir, so a still-held flock only leaks this recoverer's
                // worker-reachable resources (a best-effort teardown loss,
                // logged). Crucially we do
                // NOT touch leakedSlots -- that would wrongly shrink the
                // in-range pool capacity -- and we do NOT add to retiredSlots:
                // there is no capacity to recover, and freeSlotIndex(idx)
                // would index past the slotInUse array (sized maxSize).
                LOG.warn("startup SF recovery: out-of-range slot {} closed with the flock still held "
                                + "(I/O or manager worker did not stop); its data is durable on disk for a later attempt",
                        slotPath);
            }
            if (outcome == RecoveryDrainOutcome.CONTENDED) {
                // Same parking rule as the in-range pass: a live flock holder
                // (e.g. a sibling drainer that adopted this out-of-range slot)
                // is slot-specific and possibly long-lived; the end-of-scan
                // cycle rewind re-probes it after the holder lets go. No
                // capacity bookkeeping is involved out of range.
                recoveryDeferredThisCycle++;
                recoveryOutOfRangeNext++;
                continue;
            }
            if (outcome == RecoveryDrainOutcome.FAILED) {
                if (parkAfterFailure(idx)) {
                    recoveryDeferredThisCycle++;
                    recoveryOutOfRangeNext++;
                    return true;
                }
                // Keep the out-of-range cursor on this candidate so a later
                // tick retries it after the presumed-transient condition
                // clears; no capacity bookkeeping is involved.
                return false;
            }
            clearFailStreak(idx);
            recoveryOutOfRangeNext++;
            return true;
        }

        if (recoveryDeferredThisCycle > 0) {
            // At least one slot was parked this cycle (contended, persistently
            // failing, or retired with data still on disk). Its data stays durable on disk, so
            // instead of latching recoveryComplete -- which would abandon it
            // until a restart or a lucky borrow of that index -- rewind the
            // scan and let the driver's retry cadence run another cycle.
            // Recovered, live and empty slots are re-skipped cheaply; parked
            // slots get a fresh (bounded) attempt. Per-slot WARNs stay
            // deduplicated across cycles via recoveryWarnedSlots.
            recoveryDeferredThisCycle = 0;
            recoveryInRangeNext = 0;
            recoveryOutOfRangeNext = 0;
            return false;
        }
        // Latch decision under `lock`: a mid-cycle release (a retired slot's
        // late flock release or a clean reclaim) can free a still-candidate
        // dir AFTER this cycle's cursor passed its index (it was live/retired
        // at walk time -- no deferral) and BEFORE the latch (so the
        // post-latch re-arm in rearmRecoveryScanIfStranded does not apply
        // either). rearmRecoveryScanIfStranded raises recoveryRearmRequested
        // under the same lock, so consuming it here mutually excludes the
        // race: no release can slip between this check and the latch write.
        lock.lock();
        try {
            if (recoveryRearmRequested) {
                recoveryRearmRequested = false;
                recoveryDeferredThisCycle = 0;
                recoveryInRangeNext = 0;
                recoveryOutOfRangeNext = 0;
                return false;
            }
            recoveryComplete = true;
        } finally {
            lock.unlock();
        }
        return false;
    }

    private void closePrewarmedDelegates(int built) {
        for (int i = 0; i < built; i++) {
            try {
                all.get(i).delegate().close();
            } catch (Throwable ignored) {
                // Best-effort cleanup: one delegate close failure must not
                // strand later prewarmed delegates or replace the original
                // construction throwable.
            }
        }
    }

    private static Thread createStartupRecoveryThread(Runnable runnable) {
        return new Thread(runnable, "questdb-sender-pool-recovery");
    }

    /**
     * Drains one candidate orphan slot dir within {@code remainingMillis},
     * best-effort and never throwing. Builds a recoverer on {@code slotIndex}
     * (whose {@link #defaultSender} derives the dir {@code <base>-slotIndex}),
     * drains its unacked data, and closes the delegate. Shared by both recovery
     * passes -- the in-range pass and the out-of-range pass -- which differ only
     * in their slot bookkeeping, handled by the caller via {@code retainedOut}.
     *
     * @param retainedOut single-element out-param set to the recoverer iff one
     *                    was built and its {@code close()} returned with the
     *                    flock still held because a worker did not stop; the
     *                    in-range caller keeps it in {@link #retiredSlots} so a
     *                    late flock release can be re-probed. {@code null} when
     *                    the flock was released (or no recoverer was built).
     * @return {@link RecoveryDrainOutcome#CONTENDED} when the slot flock is
     * held by another live owner (slot-specific: park it and keep scanning);
     * {@link RecoveryDrainOutcome#FAILED} on any other build/drain failure
     * (presumed server-wide, likely to repeat for every remaining slot);
     * {@link RecoveryDrainOutcome#DRAINED} when the candidate was drained or
     * was no longer a candidate
     */
    private RecoveryDrainOutcome drainCandidateSlotForRecovery(int slotIndex, String slotPath,
                                                               long remainingMillis, SenderSlot[] retainedOut) {
        retainedOut[0] = null;
        // Hoisted so the flock check after the try can consult it:
        // createRecoverer() takes the slot flock on <base>-slotIndex, and
        // delegate().close() can retain it when an I/O or manager worker does
        // not stop.
        SenderSlot recoverer = null;
        RecoveryDrainOutcome outcome = RecoveryDrainOutcome.DRAINED;
        try {
            if (!OrphanScanner.isCandidateOrphan(slotPath)) {
                return RecoveryDrainOutcome.DRAINED;
            }
            // O(1) contention pre-probe (flock only). A slot parked as
            // CONTENDED is re-probed on every retry cycle for as long as its
            // live owner runs -- potentially that owner's whole lifetime --
            // and the full recovery build below (config re-parse, builder
            // graph, parent-dir fsync barriers in periodic durability, owned
            // SegmentManager allocation) would exist only to reach
            // SlotLock.acquire and throw. Ask the flock directly first so
            // the steady-state cost of a held slot is a few syscalls per
            // cycle, not a build. Races are benign in both directions: a
            // free probe can still lose the acquire inside the build (the
            // contention catch below parks exactly as before), and a held
            // probe that goes stale is re-observed on the next cycle. An
            // indeterminate probe (null) falls through to the build, which
            // owns real error classification.
            String probedHolder = SlotLock.probeHolder(slotPath);
            if (probedHolder != null) {
                if (warnSlotOnce(slotIndex)) {
                    LOG.warn("startup SF recovery: slot {} is held by another live owner (holder={}); "
                            + "parking it and continuing with the remaining slots",
                            slotPath, probedHolder);
                }
                return RecoveryDrainOutcome.CONTENDED;
            }
            try {
                // Recovery delegate: forced OFF-mode initial connect (see
                // createRecoverer / defaultRecoverySender), so this build does
                // at most ONE connect attempt -- it never inherits the SYNC
                // reconnect-budget retry loop that would block this recovery
                // driver for minutes (M1).
                recoverer = createRecoverer(slotIndex);
            } catch (SlotLockContentionException contention) {
                // The slot flock is held by another LIVE owner. That condition
                // is scoped to THIS slot dir by construction -- it says nothing
                // about the server or the remaining slots -- and it can persist
                // for the owner's whole lifetime, so retrying it in place would
                // starve every remaining slot (C2). Report it distinctly so the
                // scan parks this slot and continues.
                if (warnSlotOnce(slotIndex)) {
                    LOG.warn("startup SF recovery: slot {} is held by another live owner ({}); "
                            + "parking it and continuing with the remaining slots",
                            slotPath, contention.toString());
                }
                return RecoveryDrainOutcome.CONTENDED;
            } catch (Throwable buildErr) {
                // A build/connect failure (e.g. server unreachable) will very
                // likely repeat for every remaining slot, so stop here rather
                // than pay a connect timeout per slot.
                if (warnSlotOnce(slotIndex)) {
                    LOG.warn("startup SF recovery: could not open slot {} ({}); "
                            + "deferring this and remaining slots", slotPath, buildErr.toString());
                }
                return RecoveryDrainOutcome.FAILED;
            }
            try {
                // Cap the drain at the remaining shared budget and short-circuit
                // on a timeout: a server that fails to ack within the budget
                // will very likely do the same for every remaining slot -- the
                // same reasoning as the build-failure case above.
                if (!recoverer.delegate().drain(remainingMillis)) {
                    if (warnSlotOnce(slotIndex)) {
                        LOG.warn("startup SF recovery: drain did not ack slot {} "
                                + "within {}ms; deferring this and remaining slots",
                                slotPath, remainingMillis);
                    }
                    outcome = RecoveryDrainOutcome.FAILED;
                }
            } catch (Throwable drainErr) {
                if (warnSlotOnce(slotIndex)) {
                    LOG.warn("startup SF recovery: drain failed for slot {} ({}); deferring it",
                            slotPath, drainErr.toString());
                }
                outcome = RecoveryDrainOutcome.FAILED;
            } finally {
                try {
                    recoverer.delegate().close();
                } catch (Throwable ignored) {
                    // Best-effort close: a teardown Error must not abort
                    // recovery of the remaining slots.
                }
            }
        } catch (Throwable scanErr) {
            if (warnSlotOnce(slotIndex)) {
                LOG.warn("startup SF recovery: scan failed for slot {} ({}); deferring it",
                        slotPath, scanErr.toString());
            }
            outcome = RecoveryDrainOutcome.FAILED;
        }
        if (recoverer != null && !flockReleased(recoverer)) {
            retainedOut[0] = recoverer;
        }
        if (outcome == RecoveryDrainOutcome.DRAINED && recoveryWarnedSlots.contains(slotIndex)) {
            recoveryWarnedSlots.remove(slotIndex);
            LOG.info("startup SF recovery: slot {} recovered after earlier deferrals", slotPath);
        }
        return outcome;
    }

    /**
     * Clears the consecutive-failure streak after a successful drain so the
     * next candidate starts from a clean count. Driver-thread state (see the
     * scan cursor comment); no lock needed.
     */
    private void clearFailStreak(int slotIndex) {
        if (recoveryFailStreakSlot == slotIndex) {
            recoveryFailStreakSlot = -1;
            recoveryFailStreak = 0;
        }
    }

    /**
     * Records one FAILED drain attempt on a candidate slot and reports whether
     * the scan should park it: {@code true} once the SAME slot has failed
     * {@link #RECOVERY_MAX_SLOT_FAILURE_STREAK} consecutive times, meaning the
     * failure is evidently slot-specific rather than server-wide and must not
     * pin the cursor any longer (C2). Driver-thread state; no lock needed.
     */
    private boolean parkAfterFailure(int slotIndex) {
        if (recoveryFailStreakSlot != slotIndex) {
            recoveryFailStreakSlot = slotIndex;
            recoveryFailStreak = 0;
        }
        if (++recoveryFailStreak < RECOVERY_MAX_SLOT_FAILURE_STREAK) {
            return false;
        }
        recoveryFailStreakSlot = -1;
        recoveryFailStreak = 0;
        return true;
    }

    /**
     * Dedups per-slot recovery WARNs: a contended or persistently failing slot
     * is retried indefinitely (its data stays durable on disk), and warning on
     * every retry produced an unbounded one-WARN-per-second stream for the life
     * of the process (C2). Returns {@code true} only the first time a slot
     * fails since its last successful drain, so each failure episode logs
     * exactly once (a follow-up failure with a DIFFERENT cause is deliberately
     * folded into the same episode). Driver-thread state; no lock needed.
     */
    private boolean warnSlotOnce(int slotIndex) {
        if (recoveryWarnedSlots.contains(slotIndex)) {
            return false;
        }
        recoveryWarnedSlots.add(slotIndex);
        return true;
    }

    public PooledSender borrow() {
        // Track remaining wait via awaitNanos's return value (canonical pattern):
        // awaitNanos consumes from the budget on each wait and reports what is
        // left; <= 0 means the budget is exhausted.
        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        lock.lock();
        try {
            while (true) {
                if (closed) {
                    throw new LineSenderException("QuestDB handle is closed");
                }
                if (!available.isEmpty()) {
                    SenderSlot s = available.pollFirst();
                    // Stamp a fresh lease id under the lock so the PooledSender
                    // wrapper handed out can be told apart from any prior,
                    // now-stale borrow of the same slot.
                    s.bumpGeneration();
                    return new PooledSender(s, s.generation());
                }
                if (all.size() + inFlightCreations + closingSlots + leakedSlots + recoveringSlots < maxSize) {
                    inFlightCreations++;
                    // Reserve a slot index under the lock so concurrent
                    // creations never target the same SF slot dir. -1 when
                    // SF is off (no per-slot identity needed).
                    int slotIndex = storeAndForward ? allocateSlotIndex() : -1;
                    lock.unlock();
                    SenderSlot created;
                    try {
                        created = createUnlocked(slotIndex);
                    } catch (Throwable e) {
                        // Catch Throwable, not just RuntimeException:
                        // createUnlocked() runs a heavy native build path
                        // (mmap, flock, WebSocket connect) that can throw an
                        // Error -- e.g. an -ea AssertionError or
                        // OutOfMemoryError. If we only caught RuntimeException
                        // the Error would propagate with inFlightCreations
                        // still incremented and the SF slot index still
                        // reserved (slotInUse[idx] stuck true), permanently
                        // lowering pool capacity. The cleanup below is
                        // idempotent, so undoing the reservation for any
                        // throwable is safe.
                        lock.lock();
                        try {
                            inFlightCreations--;
                            freeSlotIndex(slotIndex);
                            creationFinished.signalAll();
                            slotReleased.signal();
                        } finally {
                            lock.unlock();
                        }
                        throw e;
                    }
                    lock.lock();
                    if (closed) {
                        // Pool was closed mid-creation. Keep inFlightCreations
                        // reserved while the delegate is closed outside the
                        // lock: close() waits on that counter, so the creation
                        // remains internally owned until its teardown completes.
                        boolean reserved = created.slotIndex() >= 0;
                        if (reserved) {
                            closingSlots++;
                        }
                        lock.unlock();
                        try {
                            created.delegate().close();
                        } catch (Throwable ignored) {
                            // Best-effort: an Error (e.g. -ea AssertionError)
                            // from teardown must not mask the closed-pool signal.
                        } finally {
                            lock.lock();
                            try {
                                if (reserved) {
                                    reclaimSlot(created, " after closed-mid-creation teardown");
                                }
                                inFlightCreations--;
                                creationFinished.signalAll();
                                slotReleased.signalAll();
                            } finally {
                                lock.unlock();
                            }
                        }
                        throw new LineSenderException("QuestDB handle is closed");
                    }
                    all.add(created);
                    created.bumpGeneration();
                    inFlightCreations--;
                    creationFinished.signalAll();
                    return new PooledSender(created, created.generation());
                }
                // Capacity-starved: re-probe retired slots BEFORE the terminal
                // timeout check — a deferred engine cleanup may have released a
                // flock since the retire, and the freed index can admit a
                // creation right now. The delegate normally signals this pool
                // after deferred release, while this probe also covers delegates
                // that do not expose that notification and release/listener races.
                // Ordering matters twice over: a
                // zero-timeout (try-once) borrow must get its one probe before
                // throwing, and a borrower whose awaitNanos budget just expired
                // must get a final probe on its wake-up pass instead of timing
                // out on capacity that has already come back.
                if (reprobeRetiredSlots()) {
                    continue;
                }
                if (remainingNanos <= 0) {
                    throw new LineSenderException(
                            "timed out waiting for a Sender from the pool after " + acquireTimeoutMillis + "ms");
                }
                try {
                    Runnable beforeWaitHook = beforeBorrowWaitHook;
                    if (beforeWaitHook != null) {
                        beforeWaitHook.run();
                    }
                    remainingNanos = slotReleased.awaitNanos(remainingNanos);
                    if (remainingNanos <= 0) {
                        Runnable hook = borrowWaitExpiredHook;
                        if (hook != null) {
                            hook.run();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LineSenderException("interrupted while waiting for a Sender from the pool");
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @TestOnly
    public Sender buildRecoverySenderForTesting(int slotIndex) {
        return defaultRecoverySender(slotIndex);
    }

    @TestOnly
    public Sender buildSenderForTesting(int slotIndex) {
        return defaultSender(slotIndex);
    }

    @TestOnly
    public void discardBrokenForTesting(PooledSender sender) {
        discardBroken(sender);
    }

    @TestOnly
    public int getInFlightCreationsForTesting() {
        lock.lock();
        try {
            return inFlightCreations;
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public int getRetiredSlotCountForTesting() {
        lock.lock();
        try {
            return retiredSlots.size();
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public long getRetiredSlotProbeCountForTesting() {
        lock.lock();
        try {
            return retiredSlotProbeCount;
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public Thread getStartupRecoveryThreadForTesting() {
        return startupRecoveryThread;
    }

    @TestOnly
    public boolean hasCreationWaiterForTesting() {
        lock.lock();
        try {
            return lock.hasWaiters(creationFinished);
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public boolean isCloseStartedForTesting() {
        lock.lock();
        try {
            return closeStarted;
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public boolean isClosedForTesting() {
        return closed;
    }

    @TestOnly
    public boolean isRecoveryCompleteForTesting() {
        return recoveryComplete;
    }

    @TestOnly
    public boolean isSlotInUseForTesting(int slotIndex) {
        lock.lock();
        try {
            return slotInUse[slotIndex];
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public void markClosingForTesting() {
        markClosing();
    }

    @TestOnly
    public void runStartupRecoveryWithinBudgetForTesting() {
        if (startupRecoveryThread != null) {
            throw new IllegalStateException("manual recovery drive requires a deferred pool");
        }
        runStartupRecoveryWithinBudget();
    }

    @TestOnly
    public boolean runStartupRecoveryStepForTesting() {
        if (startupRecoveryThread != null) {
            throw new IllegalStateException("manual recovery drive requires a deferred pool");
        }
        return runStartupRecoveryStep();
    }

    @TestOnly
    public void setBeforeBorrowWaitHook(Runnable hook) {
        this.beforeBorrowWaitHook = hook;
    }

    @TestOnly
    public void setBorrowWaitExpiredHook(Runnable hook) {
        this.borrowWaitExpiredHook = hook;
    }

    @TestOnly
    public void setCreationWaitRetryHookForTesting(Runnable hook) {
        this.creationWaitRetryHook = hook;
    }

    /**
     * Raises the shutdown signal early -- without tearing down live delegates --
     * so an in-flight startup-recovery step stops promptly between slots. Direct
     * {@link #close()} calls this before joining its private driver;
     * {@link QuestDBImpl#close()} calls it before stopping the housekeeper.
     * Idle-delegate teardown and the
     * outstanding-lease wait still happen in {@link #close()} (guarded by
     * {@code closeStarted}, so this early signal never short-circuits them);
     * borrowed delegates returned after this signal are torn down on the
     * returning thread via {@link #retireLease}. Idempotent; safe to call
     * repeatedly.
     */
    void markClosing() {
        closed = true;
        // LockSupport retains a permit when shutdown races the driver's park,
        // so the one direct recovery thread cannot miss its close wake-up.
        LockSupport.unpark(startupRecoveryThread);
    }

    /**
     * Shuts the pool down. NEVER tears down a borrowed delegate: a producer
     * thread may be inside one right now (mid-append, mid-flush), and closing
     * it from here would flush table buffers that thread is mutating and then
     * free their native memory under its feet -- a use-after-free / SEGV, not
     * an exception (C1). Instead:
     * <ol>
     * <li>waits boundedly for internally owned creations; a late creator keeps
     * ownership and performs its closed-mid-creation teardown asynchronously;
     * then</li>
     * <li>waits boundedly (up to {@code acquireTimeoutMillis}, hard-capped at
     * {@link #MAX_CLOSE_LEASE_WAIT_MILLIS}) for outstanding leases to come home -- {@link #giveBack} and {@link #discardBroken}
     * observe {@code closed} and tear each delegate down on the returning
     * borrower's own thread, its exclusive user at that point
     * ({@link #retireLease}); then</li>
     * <li>closes the delegates of idle slots only, outside the lock.</li>
     * </ol>
     * A lease that never returns leaks its delegate (logged): a logged leak is
     * recoverable, a freed buffer under a live producer is a JVM crash. This
     * mirrors the egress twin, {@code QueryWorker.shutdown()}'s bounded
     * interrupt+join before {@code client.close()}. Idempotent.
     */
    @Override
    public void close() {
        // Direct pools own one long-lived recovery driver. Stop it before
        // snapshotting/closing delegates; deferred pools have a null thread and
        // QuestDBImpl stops their external PoolHousekeeper before calling here.
        markClosing();
        stopStartupRecoveryDriver();
        SenderSlot[] idleSnapshot;
        lock.lock();
        try {
            if (closeStarted) {
                return;
            }
            closeStarted = true;
            // Raise the shutdown signal too (a direct, non-pooled caller may
            // close() without a prior markClosing()); harmless if already set.
            closed = true;
            // Wake parked borrowers so they observe the shutdown and throw.
            slotReleased.signalAll();
            // A creator can block in DNS, TCP, TLS, or WebSocket setup. Wait
            // boundedly rather than turning an unset connect_timeout into an
            // unbounded QuestDB.close(). Timing out does not abandon ownership:
            // the reservation and any SF slot stay assigned to the creator,
            // which observes closed and tears down a late result before releasing
            // them. Preserve the caller's interrupt status while still applying
            // the same finite wait budget.
            final long creationWaitMillis = Math.max(0,
                    Math.min(acquireTimeoutMillis, MAX_CLOSE_CREATION_WAIT_MILLIS));
            final long creationWaitNanos = TimeUnit.MILLISECONDS.toNanos(creationWaitMillis);
            final long creationWaitDeadlineNanos = System.nanoTime() + creationWaitNanos;
            long creationRemainingNanos = creationWaitNanos;
            boolean creationWaitInterrupted = false;
            while (inFlightCreations > 0 && creationRemainingNanos > 0) {
                boolean isRetryingAfterInterrupt = false;
                try {
                    creationFinished.awaitNanos(creationRemainingNanos);
                } catch (InterruptedException e) {
                    creationWaitInterrupted = true;
                    isRetryingAfterInterrupt = true;
                }
                creationRemainingNanos = creationWaitDeadlineNanos - System.nanoTime();
                if (isRetryingAfterInterrupt && inFlightCreations > 0 && creationRemainingNanos > 0) {
                    Runnable hook = creationWaitRetryHook;
                    if (hook != null) {
                        hook.run();
                    }
                }
            }
            if (creationWaitInterrupted) {
                Thread.currentThread().interrupt();
            }
            if (inFlightCreations > 0) {
                LOG.warn("SenderPool.close(): {} sender creation(s) still in flight after {}ms; "
                                + "each creator retains cleanup ownership and releases its SF slot when construction returns",
                        inFlightCreations, creationWaitMillis);
            }
            // Bounded graceful wait for outstanding leases. A slot is borrowed
            // iff it is in `all` but not in `available`; retireLease's
            // delegate-close section (running outside the lock on a returning
            // borrower's thread) is tracked by pendingLeaseTeardowns so this
            // method does not return while a teardown is still in flight.
            // The budget is the acquire timeout hard-capped at
            // MAX_CLOSE_LEASE_WAIT_MILLIS: a huge/infinite acquire timeout is
            // a borrow policy, not a licence for close() to hang forever on a
            // lease that never comes home.
            final long waitMillis = Math.min(acquireTimeoutMillis, MAX_CLOSE_LEASE_WAIT_MILLIS);
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
            while ((all.size() > available.size() || pendingLeaseTeardowns > 0) && remainingNanos > 0) {
                try {
                    remainingNanos = slotReleased.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    // Preserve the interrupt and stop waiting: idle delegates
                    // are still torn down below, and stragglers take the
                    // delegated-teardown path whenever they return.
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            int outstanding = all.size() - available.size() + pendingLeaseTeardowns;
            if (outstanding > 0) {
                LOG.warn("SenderPool.close(): {} borrowed sender lease(s) still outstanding after {}ms; "
                                + "each connection is torn down when its lease is closed, or leaks if it never is",
                        outstanding, waitMillis);
            }
            // Only idle slots are safe to close from this thread: no lease
            // means no user thread inside the delegate. `available` cannot
            // grow after this point -- borrow() throws on `closed` and
            // giveBack() retires instead of requeueing -- so the snapshot is
            // complete.
            idleSnapshot = available.toArray(new SenderSlot[0]);
        } finally {
            lock.unlock();
        }
        // Close each idle delegate outside the lock so a slow real-close()
        // doesn't keep the pool latched.
        for (int i = 0; i < idleSnapshot.length; i++) {
            try {
                idleSnapshot[i].delegate().close();
            } catch (Throwable ignored) {
                // Best-effort: an Error from one delegate's teardown must not
                // abort the loop and strand the remaining delegates unclosed.
            }
        }
        // After delegate teardown so a quarantine fired by a late recovery
        // step still gets its bounded (100 ms) delivery window.
        if (recoveryErrorDispatcher != null) {
            recoveryErrorDispatcher.close();
        }
    }

    /**
     * Evicts a slot whose delegate has failed (typically a {@code flush()}
     * failure observed in {@link PooledSender#close()}). The slot is removed
     * from {@code all} so the pool can grow back into a fresh slot on demand.
     * The underlying delegate is closed outside the lock so a slow real-close
     * does not stall other borrowers.
     * <p>
     * Safe during shutdown too: {@link #close()} never touches borrowed
     * delegates, so the calling (borrower's) thread is the delegate's
     * exclusive user and {@link #retireLease} can tear it down without racing
     * the close() loop.
     */
    void discardBroken(PooledSender ps) {
        retireLease(ps, "");
    }

    public void giveBack(PooledSender ps) {
        SenderSlot s = ps.slot();
        long gen = ps.generation();
        lock.lock();
        try {
            if (!closed) {
                if (s.generation() != gen) {
                    // Stale return: this lease was already given back and the slot
                    // possibly re-borrowed (or this is a duplicate close). Dropping
                    // it keeps Sender.close() idempotent under a concurrent
                    // re-borrow -- without it a double close would enqueue the slot
                    // twice and hand it to two borrowers writing into one delegate.
                    return;
                }
                s.bumpGeneration();
                s.markIdleAt(System.currentTimeMillis());
                assert !available.contains(s) : "slot already present in available deque on giveBack";
                available.addLast(s);
                slotReleased.signal();
                return;
            }
        } finally {
            lock.unlock();
        }
        // Pool is shutting down: never requeue. close() deliberately does not
        // close borrowed delegates -- a producer thread could still be inside
        // one, and freeing its native buffers mid-append is a use-after-free /
        // SEGV (C1) -- so teardown is delegated HERE, to the returning
        // borrower's thread, the delegate's exclusive user at this point.
        // retireLease re-validates the lease generation under the lock and
        // signals the close() thread waiting for outstanding leases.
        retireLease(ps, " during pool shutdown");
    }

    @TestOnly
    public void setRetiredSlotProbeCountForTesting(long count) {
        lock.lock();
        try {
            retiredSlotProbeCount = count;
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public void setStartupRecoveryJoinHooksForTesting(Runnable beforeJoinHook, Runnable afterJoinHook) {
        this.beforeStartupRecoveryJoinHook = beforeJoinHook;
        this.afterStartupRecoveryJoinHook = afterJoinHook;
    }

    private void stopFailedStartupRecoveryDriver(Thread recoveryThread) {
        if (recoveryThread == null || recoveryThread == Thread.currentThread()) {
            return;
        }
        if (beforeFailedStartupRecoveryJoinHook != null) {
            beforeFailedStartupRecoveryJoinHook.run();
        }
        boolean interrupted = false;
        while (recoveryThread.isAlive()) {
            try {
                recoveryThread.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void stopStartupRecoveryDriver() {
        if (startupRecoveryThread != null && startupRecoveryThread != Thread.currentThread()) {
            if (beforeStartupRecoveryJoinHook != null) {
                beforeStartupRecoveryJoinHook.run();
            }
            try {
                startupRecoveryThread.join(PoolHousekeeper.STOP_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (afterStartupRecoveryJoinHook != null) {
                afterStartupRecoveryJoinHook.run();
            }
        }
    }

    private void waitForStartupRecoveryRetry() {
        if (closed) {
            return;
        }
        if (recoveryComplete) {
            LockSupport.park(this);
        } else {
            LockSupport.parkNanos(
                    this,
                    TimeUnit.MILLISECONDS.toNanos(RECOVERY_RETRY_INTERVAL_MILLIS));
        }
    }

    /**
     * Retires one lease on the calling (borrower's) thread: validates the
     * lease generation under the lock, removes the slot from {@code all},
     * closes the delegate OUTSIDE the lock, and reclaims the SF slot index.
     * Shared by {@link #discardBroken} (broken delegate) and by
     * {@link #giveBack} when the pool is shutting down (delegated teardown --
     * see {@link #close()}).
     * <p>
     * Single-owner teardown: the caller holds the only live lease on this
     * slot and {@link #close()} never touches borrowed delegates, so no other
     * thread can be inside the delegate when it is closed here.
     * {@code pendingLeaseTeardowns} keeps the out-of-lock close visible to
     * close()'s outstanding-lease wait, so the pool does not report itself
     * closed while a delegate is still being torn down.
     *
     * @param ps      the lease being retired
     * @param context phrase woven into the SF retire WARN naming the reclaim
     *                path (e.g. {@code ""} or {@code " during pool shutdown"})
     */
    private void retireLease(PooledSender ps, String context) {
        SenderSlot s = ps.slot();
        long gen = ps.generation();
        boolean reserved = false;
        lock.lock();
        try {
            if (s.generation() != gen) {
                // Stale retire: the slot was already returned/discarded and
                // possibly re-borrowed. Dropping it avoids evicting a slot a
                // different borrower now owns and double-closing its delegate.
                return;
            }
            s.bumpGeneration();
            boolean removed = all.remove(s);
            // For an SF slot, keep its index reserved (move the reservation
            // from `all` to `closingSlots`) until the delegate below releases
            // the flock. Capacity stays accounted for, so a concurrent borrow
            // cannot reclaim this slot dir while its lock is still held.
            if (removed && s.slotIndex() >= 0) {
                closingSlots++;
                reserved = true;
            }
            pendingLeaseTeardowns++;
            // Wake all waiters: the cap check in borrow() may now admit a
            // creation attempt (on a *different* slot), and a close() in
            // progress must re-check its outstanding-lease count.
            slotReleased.signalAll();
        } finally {
            lock.unlock();
        }
        // Close the delegate outside the lock (releases the SF flock). Always
        // attempt it so native resources are freed even on the defensive path
        // where the wrapper had already left `all`.
        try {
            s.delegate().close();
        } catch (Throwable ignored) {
            // Best-effort teardown: a delegate close() can throw an Error
            // (e.g. an -ea AssertionError) as well as a RuntimeException.
            // Either way the accounting in the finally below MUST run,
            // otherwise an SF slot stays reserved forever (slotInUse stuck
            // true, closingSlots over-counted), the pool leaks capacity until
            // borrow() can only ever time out, and a concurrent close() would
            // wait out its full budget on a teardown that already happened.
        } finally {
            lock.lock();
            try {
                pendingLeaseTeardowns--;
                if (reserved) {
                    // Free the index only when the flock was released; a slot
                    // left locked is retired into retiredSlots, recoverable
                    // by reprobeRetiredSlots() if the deferred cleanup drops
                    // the flock later.
                    reclaimSlot(s, context);
                }
                slotReleased.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Closes idle slots that have exceeded {@code idleTimeoutMillis} or that
     * have aged past {@code maxLifetimeMillis}. Never shrinks below
     * {@code minSize}. Called by the {@link PoolHousekeeper} on its tick.
     */
    public void reapIdle() {
        if (closed) {
            return;
        }
        long now = System.currentTimeMillis();
        ArrayList<SenderSlot> toClose = null;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            // Housekeeper tick doubles as the retired-slot recovery driver:
            // a slot retired because its worker did not stop is re-probed
            // here and returns to the free set once the deferred cleanup
            // finally released its flock.
            reprobeRetiredSlots();
            Iterator<SenderSlot> it = available.iterator();
            while (it.hasNext() && all.size() > minSize) {
                SenderSlot s = it.next();
                boolean idleExpired = idleTimeoutMillis < Long.MAX_VALUE
                        && (now - s.idleSinceMillis()) >= idleTimeoutMillis;
                boolean overAge = maxLifetimeMillis < Long.MAX_VALUE
                        && (now - s.createdAtMillis()) >= maxLifetimeMillis;
                if (idleExpired || overAge) {
                    it.remove();
                    all.remove(s);
                    // Keep the SF slot reserved until its flock is released
                    // below (see discardBroken for the rationale).
                    if (s.slotIndex() >= 0) {
                        closingSlots++;
                    }
                    if (toClose == null) {
                        toClose = new ArrayList<>();
                    }
                    toClose.add(s);
                }
            }
        } finally {
            lock.unlock();
        }
        if (toClose != null) {
            for (int i = 0, n = toClose.size(); i < n; i++) {
                try {
                    toClose.get(i).delegate().close();
                } catch (Throwable ignored) {
                    // Best-effort: a single delegate close() failure (including
                    // an Error such as an -ea AssertionError) must not abort the
                    // loop -- that would leave sibling flocks unreleased -- nor
                    // skip the slot-accounting release block below, which would
                    // strand every reaped index (slotInUse stuck true,
                    // closingSlots over-counted) and leak pool capacity.
                }
            }
            // Return reserved SF slot indices to the free set -- but only for
            // slots whose delegate confirmed the flock was released. A slot
            // left locked because a worker did not stop is retired permanently.
            if (storeAndForward) {
                lock.lock();
                try {
                    for (int i = 0, n = toClose.size(); i < n; i++) {
                        SenderSlot s = toClose.get(i);
                        if (s.slotIndex() >= 0) {
                            reclaimSlot(s, " during idle reaping");
                        }
                    }
                    slotReleased.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /** Snapshot of the number of idle slots. For tests and introspection. */
    public int availableSize() {
        lock.lock();
        try {
            return available.size();
        } finally {
            lock.unlock();
        }
    }

    /** Snapshot of the total number of live slots (idle + in-use). For tests and introspection. */
    public int totalSize() {
        lock.lock();
        try {
            return all.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Snapshot of the number of SF slots currently retired because a
     * delegate {@code close()} returned with the slot flock still held after
     * an I/O or manager worker did not stop. Each leaked slot lowers the
     * pool's effective capacity ({@code maxSize - leakedSlotCount()}) while
     * retired. Retired slots are re-probed (housekeeper tick and
     * capacity-starved borrows) and recovered once the delegate's deferred
     * cleanup releases the flock, so the count can go back down; a non-zero,
     * persistent value means a worker is still wedged and explains a pool
     * that has started timing out {@code borrow()}. For metrics and tests.
     */
    public int leakedSlotCount() {
        lock.lock();
        try {
            return leakedSlots;
        } finally {
            lock.unlock();
        }
    }

    private SenderSlot createSlot(IntFunction<Sender> factory, int slotIndex) {
        Sender delegate = factory.apply(slotIndex);
        try {
            if (postFactoryHook != null) {
                postFactoryHook.run();
            }
            SenderSlot slot = new SenderSlot(delegate, this, slotIndex);
            if (delegate instanceof QwpWebSocketSender) {
                ((QwpWebSocketSender) delegate).setSlotLockReleaseListener(
                        () -> recoverReleasedSlot(slot));
            }
            return slot;
        } catch (Throwable failure) {
            if (delegate instanceof QwpWebSocketSender) {
                try {
                    ((QwpWebSocketSender) delegate).setSlotLockReleaseListener(null);
                } catch (Throwable deregistrationFailure) {
                    addSuppressed(failure, deregistrationFailure);
                }
            }
            if (delegate != null) {
                try {
                    delegate.close();
                } catch (Throwable closeFailure) {
                    addSuppressed(failure, closeFailure);
                }
            }
            throw failure;
        }
    }

    private SenderSlot createUnlocked(int slotIndex) {
        return createSlot(senderFactory, slotIndex);
    }

    /**
     * Builds a {@link SenderSlot} for startup recovery of one stranded slot.
     * Routes through {@link #recoverySenderFactory}, which in production forces
     * a non-blocking initial connect ({@link #defaultRecoverySender}) so a
     * single recovery step stays bounded -- see that method and
     * {@link #drainCandidateSlotForRecovery}.
     */
    private SenderSlot createRecoverer(int slotIndex) {
        return createSlot(recoverySenderFactory, slotIndex);
    }

    private static void addSuppressed(Throwable failure, Throwable cleanupFailure) {
        if (failure != cleanupFailure) {
            try {
                failure.addSuppressed(cleanupFailure);
            } catch (Throwable ignored) {
                // Preserve the original construction failure even if recording
                // the secondary cleanup failure cannot allocate.
            }
        }
    }

    private Sender defaultSender(int slotIndex) {
        return buildManagedSlotSender(slotIndex, false);
    }

    /**
     * Same managed-slot delegate as {@link #defaultSender}, but with the
     * initial connect forced to {@link Sender.InitialConnectMode#OFF}. Used
     * only for startup recovery, which runs on a private direct driver or the
     * PoolHousekeeper thread: OFF makes the build do at most ONE connect attempt
     * instead of retrying for the whole reconnect budget (SYNC, auto-enabled by
     * any reconnect_* knob),
     * keeping a recovery step bounded below
     * {@code PoolHousekeeper.STOP_TIMEOUT_MILLIS}. See M1 / the residual-window
     * note on {@link #recoverOneSlotStep}.
     * <p>
     * Also forces {@code drain_orphans=off} (see
     * {@link #buildManagedSlotSender}): a recovery delegate must never spin up a
     * BackgroundDrainerPool, whose {@code close()} could block ~3s and overrun
     * the step / {@code STOP_TIMEOUT_MILLIS} budget while still holding the slot
     * flock.
     */
    private Sender defaultRecoverySender(int slotIndex) {
        return buildManagedSlotSender(slotIndex, true);
    }

    private Sender.LineSenderBuilder applyRecoveryCallbacks(Sender.LineSenderBuilder builder) {
        if (recoveryErrorDispatcher != null) {
            builder.errorHandler(new SenderErrorHandler() {
                @Override
                public void onError(SenderError error) {
                    if (isRecoveryEventUserRelevant(error)) {
                        recoveryErrorDispatcher.offer(error);
                    }
                }
            });
        }
        return builder;
    }

    // Applies the user-supplied ingest callbacks to a sender builder. Null
    // callbacks are skipped so the sender keeps its loud-not-silent default.
    private Sender.LineSenderBuilder applyUserCallbacks(Sender.LineSenderBuilder builder) {
        if (errorHandler != null) {
            builder.errorHandler(errorHandler);
        }
        if (connectionListener != null) {
            builder.connectionListener(connectionListener);
        }
        if (drainerListener != null) {
            builder.drainerListener(drainerListener);
        }
        return builder;
    }

    // Provenance, not severity: "did a server judge these bytes, or is the
    // client reporting they are gone?" A recovery delegate's own environment
    // troubles -- the never-connected auth / durable-ack TERMINALs that repeat
    // roughly once a second while a misconfiguration lasts -- all carry
    // NO_STATUS_BYTE and stay suppressed; a plain transport failure dispatches
    // no SenderError at all, so an unreachable server costs this path nothing.
    private static boolean isRecoveryEventUserRelevant(SenderError e) {
        return e.getCategory() == SenderError.Category.DATA_LOSS
                || e.getServerStatusByte() != SenderError.NO_STATUS_BYTE;
    }

    private Sender buildManagedSlotSender(int slotIndex, boolean forRecovery) {
        if (!storeAndForward) {
            return applyUserCallbacks(Sender.builder(configurationString)).build();
        }
        // Give this pooled sender its own slot dir <sf_dir>/<base>-<index>
        // so concurrent SF senders sharing one sf_dir never collide on
        // the slot flock. senderId() is only legal on WebSocket transport,
        // which is exactly when storeAndForward is true.
        //
        // Also fence off the pool's own live slot set [0, maxSize) from
        // orphan draining: the pool co-manages every <base>-<index> slot it
        // can re-create and recovers each slot's unacked data when it
        // (re)creates it, so a sibling's startup drainer must never adopt
        // another live pool slot's dir/lock (that would resurrect "sf slot
        // already in use"). The bound is maxSize, NOT the whole "<base>-"
        // prefix: a same-base slot at an index >= maxSize (left behind when
        // a previous run used a larger maxSize) is out of the pool's index
        // range forever, so it is left drainable here. Its unacked data is
        // delivered by the pool's own startup recovery
        // (recoverOneSlotStep, pass 2), which adopts these
        // out-of-range same-base slots at construction REGARDLESS of
        // drain_orphans -- so the default config never strands it. The
        // per-sender drainer is an additional path that only runs when
        // drain_orphans=on; foreign leftovers under other names are drained
        // only by that path.
        Sender.LineSenderBuilder builder = Sender.builder(configurationString)
                .senderId(slotBaseId + "-" + slotIndex)
                .orphanDrainExcludeManagedSlots(slotBaseId, maxSize);
        if (forRecovery) {
            // Force OFF so the recovery build never blocks on the reconnect
            // budget (see defaultRecoverySender). An explicit mode wins over
            // the SYNC auto-promotion the user's reconnect_* knobs would
            // otherwise trigger.
            builder.initialConnectMode(Sender.InitialConnectMode.OFF);
            // Force drain_orphans OFF on recovery delegates regardless of the
            // shared config string. A recovery delegate's sole job is to drain
            // its OWN slot (the one recoverOneSlotStep is processing); it must
            // never start a BackgroundDrainerPool for sibling/foreign orphans.
            // If it did, the delegate's close() -- called from
            // drainCandidateSlotForRecovery() on the recovery driver, BEFORE
            // its cursorEngine.close() releases the slot flock -- would
            // block in BackgroundDrainerPool.close() for up to
            // GRACEFUL_DRAIN_MILLIS + STOP_GRACE_MILLIS (3s) against a
            // reachable-but-not-acking server. That overruns a recovery step's
            // budget (RECOVERY_DRAIN_BUDGET_MILLIS) and PoolHousekeeper
            // .STOP_TIMEOUT_MILLIS, so a close() landing mid-step times out its
            // join and returns while the recoverer still holds the slot flock
            // -- resurrecting the "sf slot already in use" window this pool's
            // per-slot ids exist to eliminate. Sibling in-range slots are
            // covered by recoverOneSlotStep's own passes; foreign/out-of-range
            // orphans are covered by the LIVE pooled senders' drainers (which
            // keep drain_orphans=on and whose close() senderPool.close() awaits
            // synchronously, so they release their flock before close()
            // returns).
            builder.drainOrphans(false);
        }
        // Recovery delegates drain the user's OWN data from a previous run, so
        // the two things they can say about it -- "it was abandoned"
        // (DATA_LOSS from a build()-time quarantine) and "the server rejected
        // it" (a NACK carrying its wire status byte) -- must reach the user's
        // errorHandler: this client ships slf4j-api with no binding, so
        // LOG.error alone can announce them nowhere. What stays excluded is
        // the delegate's own environment noise: connection events (up to one
        // sweep per second while a slot stays stranded) and the
        // never-connected auth / durable-ack TERMINALs, which the recovery
        // scan already logs and dedupes per slot. See applyRecoveryCallbacks:
        // delivery is filtered on provenance and routed through the pool's own
        // SenderErrorDispatcher so a slow handler cannot stall the recovery
        // driver or housekeeper thread. connectionListener and drainerListener
        // remain unset on recovery builds.
        return (forRecovery ? applyRecoveryCallbacks(builder) : applyUserCallbacks(builder)).build();
    }

    /**
     * Reserves and returns the lowest free SF slot index. The borrow() cap
     * check ({@code all.size() + inFlightCreations + closingSlots + leakedSlots
     * < maxSize}) guarantees a free index exists whenever a creation is
     * admitted, so this never fails in practice; the guard throws defensively rather than
     * silently colliding two senders on one slot dir. Caller must hold
     * {@code lock}.
     */
    private int allocateSlotIndex() {
        for (int i = 0; i < slotInUse.length; i++) {
            if (!slotInUse[i]) {
                slotInUse[i] = true;
                return i;
            }
        }
        throw new IllegalStateException(
                "no free SF slot index -- pool capacity invariant violated");
    }

    /**
     * Returns an SF slot index to the free set. No-op for non-SF pools and
     * for the {@code -1} sentinel. Caller must hold {@code lock}.
     */
    private void freeSlotIndex(int idx) {
        if (idx >= 0 && slotInUse != null) {
            slotInUse[idx] = false;
        }
    }

    /**
     * Whether the delegate's {@code close()} released the SF slot flock. A
     * non-QWP delegate never holds an SF flock, so it is always treated as
     * released. A {@link QwpWebSocketSender} reports it via
     * {@link QwpWebSocketSender#isSlotLockReleased()} -- false means close()
     * retained the flock because an I/O or manager worker did not stop.
     */
    private static boolean flockReleased(SenderSlot s) {
        Sender d = s.delegate();
        return !(d instanceof QwpWebSocketSender) || ((QwpWebSocketSender) d).isSlotLockReleased();
    }

    /**
     * Reclaims one SF slot after its delegate's {@code close()} has been
     * attempted. When the flock was released the index returns to the free
     * set; when {@code close()} returned with the flock still held because an
     * I/O or manager worker did not stop, the slot is retired --
     * {@code leakedSlots++}, {@code slotInUse[idx]} stays set, and the sender
     * joins {@code retiredSlots} -- so the cap math accounts for the lost
     * capacity and no borrow reuses the still-locked dir unless
     * {@link #reprobeRetiredSlots} later observes the deferred cleanup's
     * release and recovers the index. Either way {@code closingSlots} is
     * decremented. Any release that frees a dir still holding unacked data
     * -- the immediate clean release here or a retired slot's later flock
     * release -- also re-arms the startup recovery scan (see
     * {@link #rearmRecoveryScanIfStranded}) so the data is drained
     * in-process instead of waiting for a restart or a lucky borrow of that
     * index.
     * <p>
     * Caller must hold {@code lock} and is responsible for signalling waiters
     * (only the free path admits a new creation). Shared by
     * {@link #discardBroken} and {@link #reapIdle}.
     *
     * @param s       sender whose slot is being reclaimed ({@code slotIndex() >= 0})
     * @param context phrase woven into the retire WARN to name the reclaim
     *                path (e.g. {@code ""} or {@code " during idle reaping"})
     * @return {@code true} if the index was freed, {@code false} if retired
     */
    private boolean reclaimSlot(SenderSlot s, String context) {
        closingSlots--;
        if (flockReleased(s)) {
            freeSlotIndex(s.slotIndex());
            // A clean release can free a dir that still holds unacked durable
            // data: discardBroken of a sender whose server went away tears
            // down its workers fine and drops the flock, but the rows stay on
            // disk and no borrow may ever land on this index again. Same
            // stranding hazard as a retired slot's late release -- probe and
            // re-arm the scan on this path too.
            rearmRecoveryScanIfStranded(s.slotIndex());
            return true;
        }
        leakedSlots++;
        addRetiredSlot(s);
        LOG.warn("SF slot {} retired{}: delegate close() returned with the flock still held " +
                        "(I/O or manager worker did not stop); pool capacity reduced by 1, now {} of {} usable " +
                        "[leakedSlots={}]; the slot is re-probed and recovered if the worker releases the flock later",
                s.slotIndex(), context, maxSize - leakedSlots, maxSize, leakedSlots);
        return false;
    }

    private boolean recoverReleasedSlot(SenderSlot s) {
        lock.lock();
        try {
            int retiredIndex = s.retiredIndex();
            if (retiredIndex < 0
                    || retiredIndex >= retiredSlots.size()
                    || retiredSlots.get(retiredIndex) != s) {
                // The callback raced retirement, or is stale/duplicate. The
                // retirement path probes before insertion, and periodic scans
                // remain as a fallback, so no capacity can be lost here.
                return false;
            }
            retiredSlotProbeCount++;
            if (!flockReleased(s)) {
                return false;
            }
            recoverRetiredSlotAt(retiredIndex);
            slotReleased.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Re-probes every retired slot (see {@link #reclaimSlot}) and returns to
     * the free set any whose delegate now reports the flock released — the
     * deferred engine cleanup (manager-worker or I/O-thread exit path) has
     * run since the retire. Restores {@code leakedSlots} capacity and signals
     * waiters so a parked borrow can admit a creation immediately.
     * <p>
     * Caller must hold {@code lock}. The probe is cheap on the delegate side
     * ({@code isSlotLockReleased()} reads volatiles, and only re-arms the
     * shared flock-release retry in the rare orphaned-retry state), so
     * holding the pool lock across it cannot stall behind delegate teardown.
     *
     * @return {@code true} if at least one slot's capacity was recovered
     */
    private boolean reprobeRetiredSlots() {
        boolean recovered = false;
        for (int i = retiredSlots.size() - 1; i >= 0; i--) {
            SenderSlot s = retiredSlots.get(i);
            retiredSlotProbeCount++;
            if (flockReleased(s)) {
                recoverRetiredSlotAt(i);
                recovered = true;
            }
        }
        if (recovered) {
            slotReleased.signalAll();
        }
        return recovered;
    }

    private void addRetiredSlot(SenderSlot s) {
        s.retiredIndex(retiredSlots.size());
        retiredSlots.add(s);
    }

    /**
     * Whether {@code idx} is currently held by a RETIRED slot (see
     * {@link #reclaimSlot} and the in-range recovery retire branch) rather
     * than a live one. Lets the recovery scan tell "reserved because
     * borrowed/prewarmed" apart from "reserved because a wedged close() left
     * the flock held" for its deferral accounting: only the latter's dir can
     * hold stranded data no borrow is coming for. Caller must hold
     * {@code lock}; retiredSlots is bounded by maxSize, so the walk is cheap.
     */
    private boolean isRetiredSlotIndex(int idx) {
        for (int i = 0, n = retiredSlots.size(); i < n; i++) {
            if (retiredSlots.get(i).slotIndex() == idx) {
                return true;
            }
        }
        return false;
    }

    private void recoverRetiredSlotAt(int retiredIndex) {
        SenderSlot s = retiredSlots.get(retiredIndex);
        int last = retiredSlots.size() - 1;
        if (retiredIndex < last) {
            SenderSlot moved = retiredSlots.get(last);
            retiredSlots.set(retiredIndex, moved);
            moved.retiredIndex(retiredIndex);
        }
        retiredSlots.remove(last);
        s.retiredIndex(-1);
        leakedSlots--;
        freeSlotIndex(s.slotIndex());
        LOG.info("SF slot {} recovered: deferred cleanup released the flock after retirement; " +
                        "pool capacity restored, now {} of {} usable [leakedSlots={}]",
                s.slotIndex(), maxSize - leakedSlots, maxSize, leakedSlots);
        // The freed dir may still hold unacked durable data: a runtime retire
        // (discardBroken/reapIdle) can land AFTER the startup scan latched
        // recoveryComplete, and close() never drains an unowned dir. Shared
        // re-arm tail with reclaimSlot's clean-release path.
        rearmRecoveryScanIfStranded(s.slotIndex());
    }

    /**
     * Re-arms the startup recovery scan when a just-freed slot index left a
     * dir that is still a candidate orphan (unacked durable data, no failure
     * sentinel). Shared tail of every path that returns an SF index to the
     * free set: the retired slot's late flock release
     * ({@link #recoverRetiredSlotAt}) and the immediate clean release
     * ({@link #reclaimSlot}'s freed arm). Without it the freed dir is
     * unowned -- the scan is latched, close() never drains an unowned dir --
     * so the data would wait for a restart or a lucky borrow of exactly this
     * index, which steady low load may never produce.
     * <p>
     * When the latch is already set, rewind the cursors FIRST, then clear
     * the volatile latch (the write order publishes the rewound cursors to
     * the direct driver's unlocked gate read). Since the sole direct driver
     * remains parked for the pool's lifetime, the callback only has to unpark
     * it; no thread creation or ownership handoff occurs. When the scan is
     * still ALIVE, only raise {@code recoveryRearmRequested}: the cursor may
     * already have passed this index while it was live/retired, and the flag
     * makes the end-of-cycle latch decision rewind instead -- the cursors
     * stay strictly driver-owned. The pass-2 work list is kept and only its
     * cursor rewound, exactly like the end-of-scan cycle rewind.
     * <p>
     * No-op on a closed pool: teardown must not flip the latch; data at rest
     * on disk is the designed safe state and the next process's startup scan
     * delivers it. The isCandidateOrphan probe is a bounded directory scan;
     * holding the pool lock across it mirrors {@link #reprobeRetiredSlots}'
     * delegate probes. Caller must hold {@code lock}.
     */
    private void rearmRecoveryScanIfStranded(int slotIdx) {
        if (closed
                || sfDir == null
                || !OrphanScanner.isCandidateOrphan(sfDir + "/" + slotBaseId + "-" + slotIdx)) {
            return;
        }
        if (recoveryComplete) {
            recoveryInRangeNext = 0;
            recoveryOutOfRangeNext = 0;
            recoveryDeferredThisCycle = 0;
            recoveryRearmRequested = false;
            recoveryComplete = false;
            LOG.info("SF slot {} released with stranded data still on disk; re-arming the "
                    + "startup recovery scan to drain it", slotIdx);
        } else {
            recoveryRearmRequested = true;
            LOG.info("SF slot {} released with stranded data while the recovery scan is "
                    + "mid-cycle; flagging a rewind so this cycle cannot latch past it",
                    slotIdx);
        }
        // The permit is retained if the callback wins the race with park().
        // Deferred pools have no private thread, so unpark(null) is a no-op.
        LockSupport.unpark(startupRecoveryThread);
    }

    // Outcome of one drainCandidateSlotForRecovery attempt, letting the scan
    // tell a slot-specific contention (park the slot and keep scanning) apart
    // from a failure presumed server-wide (retry the same candidate, bounded
    // by RECOVERY_MAX_SLOT_FAILURE_STREAK).
    private enum RecoveryDrainOutcome {
        // createRecoverer() lost the slot flock to another live owner -- a
        // condition scoped to that slot dir by construction that can outlive
        // any retry interval.
        CONTENDED,
        // The candidate was drained, or was no longer a candidate.
        DRAINED,
        // Any other build/drain failure, presumed transient and server-wide.
        FAILED
    }
}
