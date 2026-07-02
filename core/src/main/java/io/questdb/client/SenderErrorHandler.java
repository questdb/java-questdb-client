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
 * User-supplied callback invoked when the asynchronous SF send loop observes a server-side
 * batch rejection. Registered on the builder via
 * {@code LineSenderBuilder.errorHandler(SenderErrorHandler)}.
 *
 * <h2>Threading</h2>
 * Implementations are invoked on a dedicated daemon dispatcher thread, never on the I/O
 * thread or the producer thread. Slow handlers cannot stall publishing; if the bounded
 * inbox fills up, surplus notifications are dropped (visible via
 * {@code QwpWebSocketSender.getDroppedErrorNotifications()}).
 *
 * <h2>Exceptions</h2>
 * Any {@link Throwable} thrown by the handler is caught and logged by the dispatcher.
 * The dispatcher and the sender continue running.
 *
 * <h2>What this callback is for</h2>
 * Dead-lettering rejected data, alerting, metrics. Producer-thread retry/abort logic
 * should not live here — that belongs in the {@code catch (LineSenderServerException)}
 * block on the producer thread, which fires after a {@link SenderError.Policy#TERMINAL}
 * latch on the next API call.
 *
 * @see SenderError
 * @see LineSenderServerException
 */
@FunctionalInterface
public interface SenderErrorHandler {
    void onError(@NotNull SenderError error);
}
