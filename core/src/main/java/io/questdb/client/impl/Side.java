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

/**
 * Which consumer owns a connect-string key in the {@link ConfigSchema} registry.
 * It records intent and drives the per-key "honored" guard tests; it is not a
 * runtime filter. {@link ConfigView} does not gate reads by side -- each consumer
 * reads the keys it needs, and a key owned by another consumer is accepted
 * syntactically and validated by its owning consumer.
 */
public enum Side {
    /**
     * Applied by every consumer (both clients and the facade pool).
     */
    COMMON,
    /**
     * Applied by the WebSocket {@code Sender} (ingress).
     */
    INGRESS,
    /**
     * Applied by the {@code QwpQueryClient} (egress).
     */
    EGRESS,
    /**
     * Applied by the facade pool, ignored by the two clients.
     */
    POOL,
    /**
     * Accepted as a no-op by every consumer (reserved by the spec, not yet
     * wired to behavior).
     */
    RESERVED
}
