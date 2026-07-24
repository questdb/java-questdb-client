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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

/**
 * Terminal SF recovery failure. The on-disk state proves that the durable
 * segment chain is corrupt or incomplete and requires operator intervention.
 * Operational filesystem failures continue to use {@link MmapSegmentException}
 * so callers can retry them without quarantining otherwise recoverable data.
 * <p>
 * One refinement is deliberately non-terminal for unattended callers:
 * {@link SfSanitizedResidueException} marks a first-sight failure thrown
 * AFTER recovery durably healed the chain, so a single retry validates
 * clean. Catch sites that quarantine on this type must intercept the
 * refinement first.
 */
public class SfRecoveryException extends MmapSegmentException {

    public SfRecoveryException(String message) {
        super(message);
    }
}
