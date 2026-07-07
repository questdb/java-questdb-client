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

package io.questdb.client;

import org.jetbrains.annotations.NotNull;

/**
 * User-supplied callback invoked when the QWP ingress client observes a
 * connection-state transition: initial connect, primary failover, an endpoint
 * attempt failing, the full address list being unreachable, or a terminal
 * auth/budget rejection. Registered on the builder via
 * {@code LineSenderBuilder.connectionListener(SenderConnectionListener)}.
 *
 * <h2>Threading</h2>
 * Implementations are invoked on a dedicated daemon dispatcher thread, never on
 * the I/O thread or the producer thread. Slow listeners cannot stall publishing
 * or reconnect; if the bounded inbox fills up, surplus events are dropped
 * (visible via
 * {@code QwpWebSocketSender.getDroppedConnectionNotifications()}).
 *
 * <h2>Exceptions</h2>
 * Any {@link Throwable} thrown by the listener is caught and logged by the
 * dispatcher. The dispatcher and the sender continue running.
 *
 * <h2>Delivery semantics</h2>
 * Events are best-effort and rare relative to the data path. Success events
 * ({@link SenderConnectionEvent.Kind#CONNECTED},
 * {@link SenderConnectionEvent.Kind#FAILED_OVER},
 * {@link SenderConnectionEvent.Kind#RECONNECTED}) are guaranteed to fire on
 * each transition. Failure events ({@code ENDPOINT_ATTEMPT_FAILED},
 * {@code ALL_ENDPOINTS_UNREACHABLE}) may be coalesced under inbox pressure.
 * The terminal event {@code AUTH_FAILED}
 * fires before the producer-thread {@code LineSenderException} is observable on
 * the next API call -- so a listener can react sooner than the producer learns
 * via exception, but should not assume the listener fires first under heavy
 * notification load.
 *
 * @see SenderConnectionEvent
 */
@FunctionalInterface
public interface SenderConnectionListener {
    void onEvent(@NotNull SenderConnectionEvent event);
}
