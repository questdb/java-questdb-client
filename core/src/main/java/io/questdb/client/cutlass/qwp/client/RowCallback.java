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
 * Per-row callback consumed by {@link QwpColumnBatch#forEachRow(RowCallback)}.
 * The {@link RowView} handed to {@link #onRow(RowView)} is a reusable flyweight
 * pointing at the current row; do not retain it past the call. To capture a
 * row's values, copy them out of the view.
 * <p>
 * Throwing from {@link #onRow} aborts iteration and propagates the exception
 * out of {@code forEachRow} on the caller's thread.
 */
@FunctionalInterface
public interface RowCallback {

    /**
     * Invoked once for each row in the batch, in row-index order starting at 0.
     *
     * @param row reusable view bound to the current row; valid only for the
     *            duration of this call
     */
    void onRow(RowView row);
}
