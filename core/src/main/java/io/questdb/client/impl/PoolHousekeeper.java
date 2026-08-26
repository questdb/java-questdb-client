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

import java.util.concurrent.TimeUnit;

/**
 * Daemon thread that periodically asks both pools to reap idle / over-age
 * slots. Owned by {@link QuestDBImpl}; one instance per {@code QuestDB}
 * handle.
 */
final class PoolHousekeeper {

    // How long stop() waits for the daemon to exit. Kept ABOVE
    // SenderPool.RECOVERY_DRAIN_BUDGET_MILLIS so a startup-recovery drain still
    // in flight when close() arrives finishes well within this join (C1 fix).
    // The recovery build that precedes the drain is bounded separately --
    // recoverers force initial_connect_mode=OFF, so the build makes at most one
    // connect attempt rather than a SYNC reconnect-budget retry (M1). A recovery
    // build also pulls a credential when a token provider is configured, and
    // that wait dwarfs this join; stop() escalates to an interrupt for it. The
    // lone case that survives even that is an in-flight connect to a black-holed
    // host, which blocks in a syscall no interrupt breaks (the transport exposes
    // no application-level connect timeout); see the residual-window note on
    // SenderPool.recoverOneSlotStep.
    static final long STOP_TIMEOUT_MILLIS = 2_000;

    private final long intervalMillis;
    private final QueryClientPool queryPool;
    private final SenderPool senderPool;
    private final Object signalLock = new Object();
    private final Thread thread;
    private volatile boolean stop;

    PoolHousekeeper(SenderPool senderPool, QueryClientPool queryPool, long intervalMillis) {
        this.senderPool = senderPool;
        this.queryPool = queryPool;
        this.intervalMillis = intervalMillis;
        this.thread = new Thread(this::runLoop, "questdb-pool-housekeeper");
        this.thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    void stop() {
        stop = true;
        synchronized (signalLock) {
            signalLock.notifyAll();
        }
        // Clear the caller's cancellation for the duration and hand it back at the end -- the shape
        // QueryWorker.shutdown() and QwpQueryClient.close() already use. Thread.join(millis) consults the
        // CALLING thread's interrupt flag before it ever looks at whether the target is alive, so a caller
        // that arrives interrupted -- a close() from a task cancelled by shutdownNow(), or from a finally on
        // a thread the application cancelled -- made the first join throw at 0 ms and skip the escalation
        // below entirely. That is the one case the escalation is most needed in: it exists because the
        // target may be parked in a credential pull only an interrupt can break, and skipping it returns
        // from close() with the recoverer still holding its store-and-forward slot flock. The flag is
        // restored before returning, so the caller's own cancellation bookkeeping still sees it. A fresh
        // interrupt during either join is handled the same way: remember it, finish the shutdown protocol,
        // then restore it.
        boolean callerWasInterrupted = Thread.interrupted();
        try {
            callerWasInterrupted |= joinIgnoringCallerInterrupts(thread, STOP_TIMEOUT_MILLIS);
            if (thread.isAlive()) {
                // The stop flag only reaches the loop BETWEEN steps. A step blocked inside a recovery
                // build is unreachable by it, and since recovery builds acquired a token provider the
                // longest such block is a credential pull: OidcDeviceAuth.getToken() documents a wait of up
                // to six times httpTimeoutMillis behind a peer's refresh, plus a token-store lock wait,
                // which together dwarf this join. Returning anyway leaves the recoverer holding its
                // store-and-forward slot flock after close() has returned, so an immediate reopen fails
                // with "sf slot already in use" and the detached build's engine, mmaps and I/O thread leak
                // -- the very window this pool's per-slot ids and the drain_orphans(false) forced on
                // recovery builds exist to eliminate.
                //
                // Interrupt and re-join. The waits this is aimed at are interruptible: acquireForGetToken
                // polls a timed tryLock, and FileTokenStore's two lock waits abandon and re-assert the flag.
                // The pull then throws, the step's caller swallows it (recovery is best-effort), and the
                // loop reaches its stop check and releases the flock on its own.
                //
                // Not ALL of the pull is interruptible, and the join above is the only bound on the rest:
                // the token POST's connect, send, await and parse phases run on the native HTTP client
                // (raw fd + epoll/kqueue), which no interrupt breaks -- each is bounded by
                // httpTimeoutMillis, and DNS resolution is not bounded at all. So a pull already inside its
                // round trip outlives both joins, exactly as an in-flight connect to a black-holed host
                // does. This escalation shortens the common case; it does not make the window impossible.
                //
                // The flag must not outlive the interrupt's target. Sender.close() and QwpQueryClient
                // close() are interrupt-neutral precisely because this thread goes on to close delegates:
                // a CARRIED flag makes CountDownLatch.await return instantly and would report a flock still
                // held that was released fine.
                thread.interrupt();
                callerWasInterrupted |= joinIgnoringCallerInterrupts(thread, STOP_TIMEOUT_MILLIS);
            }
        } finally {
            if (callerWasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Waits up to the supplied budget without letting cancellation skip the caller's remaining shutdown
     * work. Every {@link InterruptedException} clears the caller's flag, so remember it and spend only the
     * remainder of the original budget before handing the information back to {@link #stop()}.
     */
    private static boolean joinIgnoringCallerInterrupts(Thread target, long timeoutMillis) {
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        final long deadlineNanos = System.nanoTime() + timeoutNanos;
        boolean callerWasInterrupted = false;
        while (target.isAlive()) {
            final long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                break;
            }
            final long waitMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
            final int waitNanos = (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(waitMillis));
            try {
                target.join(waitMillis, waitNanos);
            } catch (InterruptedException e) {
                callerWasInterrupted = true;
            }
        }
        return callerWasInterrupted;
    }

    private void runLoop() {
        while (!stop) {
            // Per-slot startup SF recovery, driven on THIS (the reap-loop)
            // thread, one stranded slot per iteration. Doing it here rather than
            // in the SenderPool constructor keeps QuestDB.build() from blocking
            // on a slow or reachable-but-not-acking server. Each step does at
            // most one drain bounded by SenderPool.RECOVERY_DRAIN_BUDGET_MILLIS
            // (< STOP_TIMEOUT_MILLIS) on a recoverer whose initial connect is
            // forced OFF (at most one connect attempt, never a SYNC
            // reconnect-budget retry -- M1), and we re-check stop every step, so
            // a close() landing mid-recovery normally only waits out a single
            // bounded drain and the join in stop() does not time out. A step
            // blocked in a credential pull is broken by stop()'s interrupt; the
            // sole residual overrun is an in-flight connect to a black-holed
            // host, which no interrupt breaks. See SenderPool.recoverOneSlotStep.
            // While recovery still has work we skip the idle wait so the backlog
            // drains promptly; once done we fall back to the normal interval.
            // No-op once recovery completes or the pool is closing. Best-effort:
            // a recovery failure (including an Error) must never kill this
            // daemon, so swallow Throwable -- exactly as the reap guards below.
            boolean recovering;
            try {
                recovering = senderPool.runStartupRecoveryStep();
            } catch (Throwable ignored) {
                recovering = false;
            }
            synchronized (signalLock) {
                if (stop) {
                    return;
                }
                if (!recovering) {
                    try {
                        signalLock.wait(intervalMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            if (stop) {
                return;
            }
            try {
                senderPool.reapIdle();
            } catch (Throwable ignored) {
                // Defensive, intentionally unreachable in normal operation:
                // SenderPool.reapIdle() already swallows per-delegate close()
                // failures internally. The outer catch is a belt-and-braces
                // guard. Reaping must not propagate -- it's best-effort
                // housekeeping. Catch Throwable (not just RuntimeException) so
                // an Error from a delegate teardown can never kill this daemon
                // thread and stop all future reaping for the life of the handle.
            }
            try {
                queryPool.reapIdle();
            } catch (Throwable ignored) {
                // Same rationale as the senderPool guard above: best-effort,
                // must never propagate, and Throwable (not RuntimeException) so
                // an Error from query-client teardown cannot kill the daemon.
            }
        }
    }
}
