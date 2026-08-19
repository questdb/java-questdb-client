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

package io.questdb.client.cutlass.auth;

/**
 * Persists the token state of an {@link OidcDeviceAuth} so a restarted process can resume from a saved
 * refresh token instead of running the interactive device flow again. Persistence is opt-in: an
 * {@code OidcDeviceAuth} with no store keeps its tokens in memory only (the previous behaviour).
 * <p>
 * The default implementation is {@link FileTokenStore} (a strict-permissions file under the user's home
 * directory). Supply your own to back persistence with an OS keychain, a secrets manager, or a vault -
 * for example to encrypt the refresh token at rest, which the file store does not do.
 * <p>
 * Entries are keyed by {@link TokenStoreKey} (the non-secret identity: endpoints, client id, scope,
 * audience, groups-in-token mode), so a token minted for one identity is never returned for another.
 * Calls are made while {@code OidcDeviceAuth} holds its own instance lock, so an implementation does not
 * need to be thread-safe against concurrent calls from one {@code OidcDeviceAuth} instance; it does,
 * however, share its backing storage with other processes (and other language clients), so it must keep a
 * concurrent reader from observing a half-written entry - see {@link FileTokenStore} for how the file
 * store does that and coordinates a rotating refresh token across processes.
 * <p>
 * A store reports a failure by throwing; {@code OidcDeviceAuth} treats persistence as best-effort and a
 * thrown failure as non-fatal - it logs a warning through SLF4J at WARN and continues with the in-memory
 * token, which is valid regardless of whether it could be saved.
 */
public interface TokenStore {
    /**
     * Removes any persisted entry for this identity. Called from {@link OidcDeviceAuth#clearCache()}.
     * A no-op when nothing is stored.
     *
     * @param key the identity whose entry to remove
     */
    void clear(TokenStoreKey key);

    /**
     * Runs {@code action} while holding a cross-process lock scoped to {@code key}, so a refresh by
     * another process sharing this identity is observed rather than raced. The action re-reads the store
     * inside the lock and refreshes only if still needed, which keeps a rotating refresh token consistent
     * across processes.
     * <p>
     * The default runs {@code action} with no locking, which is correct for a single process or a
     * non-rotating refresh token; {@link FileTokenStore} overrides it with a lock-file protocol. An
     * implementation that cannot acquire the lock should run {@code action} anyway (degrade) rather than
     * fail a sign-in.
     * <p>
     * {@code OidcDeviceAuth} calls this while holding its own instance lock, and {@code action} runs
     * synchronously on the calling thread. An implementation therefore must not call back into the owning
     * {@code OidcDeviceAuth} (for example {@code signIn()}/{@code getToken()}) from {@code inLock},
     * {@code load}, or {@code save}, and must not block waiting on another thread that could need that
     * instance lock - either would re-enter or deadlock. Do the store I/O only.
     * <p>
     * Like {@code load} and {@code save}, this is BEST-EFFORT: an implementation that throws must not be
     * able to fail a sign-in the caller could otherwise complete. {@code OidcDeviceAuth} therefore degrades
     * on a throw rather than propagating it, and what it does depends on whether {@code action} ran: a
     * throw before the action runs falls back to a single uncoordinated refresh, while a throw after the
     * action completed - releasing a lock, closing a handle - keeps the action's result, because the
     * refresh already happened and re-running it is the duplicate POST of a rotating refresh token this
     * lock exists to prevent. An exception from {@code action} itself is the caller's own and propagates
     * untouched. An implementation should still absorb its own bookkeeping failures and degrade to running
     * {@code action} unlocked, rather than lean on that fallback.
     * <p>
     * An implementation that waits for its lock must make that wait INTERRUPTIBLE and, on an interrupt,
     * return {@code false} without running {@code action}. The wait can outlast the caller's own shutdown
     * budget - QWP's connect cancellation interrupts a thread stuck in a credential pull precisely so
     * {@code close()} can reclaim its native resources - and an uninterruptible wait defeats that, leaving
     * the client, the cursor engine and the store-and-forward slot lock to a delegated teardown. The
     * {@code false} return reads as "no refresh happened", which {@code OidcDeviceAuth} already handles.
     *
     * @param key    the identity to lock
     * @param action the critical section; its boolean result is returned unchanged
     * @return whatever {@code action} returned, or {@code false} if an interrupt abandoned the wait before
     * {@code action} could run
     */
    default boolean inLock(TokenStoreKey key, CriticalSection action) {
        return action.run();
    }

    /**
     * Loads the persisted token for this identity, or returns {@code null} if there is none usable (no
     * entry, or an entry that does not match {@code key}, or one that cannot be read as a valid token).
     * A {@code null} return makes {@code OidcDeviceAuth} fall back to a refresh or an interactive sign-in,
     * so an unreadable or stale entry is recoverable rather than fatal.
     *
     * @param key the identity to load
     * @return the persisted token, or {@code null}
     */
    PersistedToken load(TokenStoreKey key);

    /**
     * Persists (atomically replaces) the token for this identity.
     *
     * @param key   the identity to store under
     * @param token the token state to persist
     */
    void save(TokenStoreKey key, PersistedToken token);

    /**
     * A unit of work {@link #inLock(TokenStoreKey, CriticalSection)} runs while holding the per-identity
     * lock. Returns whether a valid token resulted.
     */
    @FunctionalInterface
    interface CriticalSection {
        boolean run();
    }
}
