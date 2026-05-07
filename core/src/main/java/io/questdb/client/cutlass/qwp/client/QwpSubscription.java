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
 * Handle for one active subscription on a {@link QwpSubscribeClient}.
 * Returned by {@code subscribe()} once the server's SUBSCRIBE_ACK has been
 * decoded. Use {@link #cancel()} to release the subscription; use
 * {@link #grantCredit(long)} to replenish the byte-credit budget agreed at
 * subscribe time.
 */
public interface QwpSubscription {

    /**
     * Sends a SUB_CANCEL frame for this subscription's id. The server emits
     * SUBSCRIPTION_END (CLIENT_UNSUBSCRIBE) on receipt; the handler's
     * {@link QwpSubscriptionHandler#onEnd} fires once the next poll consumes
     * that frame.
     */
    void cancel();

    /**
     * The connection-scoped schema id the server assigned to this
     * subscription's table. The same id may also be used by query results
     * against the same table.
     */
    int getSchemaId();

    /**
     * Server-resolved sequencer txn at which streaming began.
     */
    long getStartTxn();

    /**
     * The subscription_id supplied by the client at subscribe time.
     */
    long getSubscriptionId();

    /**
     * Sends a SUB_CREDIT frame replenishing the byte-credit budget by
     * {@code additionalBytes}. Used when the subscription was created with
     * a finite credit window (see {@link QwpSubscribeClient#withInitialCredit}).
     *
     * @param additionalBytes non-negative; zero is a no-op
     */
    void grantCredit(long additionalBytes);

    /**
     * Returns {@code true} until the handler observes
     * {@link QwpSubscriptionHandler#onEnd}; {@code false} thereafter.
     */
    boolean isActive();
}
