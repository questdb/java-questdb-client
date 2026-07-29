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
 * First-sight failure over a chain that recovery has <b>already healed</b>.
 * Thrown from the fail-closed branch of the sealed-residue sanitize: frame
 * accounting was proven complete (contiguity plus boundary checks), the
 * proven-dead suffix residue was durably zeroed ({@code msync} +
 * {@code fsync} completed — a sync failure throws
 * {@link MmapSegmentException} instead and never reaches this type), and
 * the throw exists solely to surface the incident on the startup that
 * observed it. An immediate re-open re-runs the same proofs over the
 * zeroed suffix and succeeds.
 * <p>
 * Attended callers (producer startup, where an operator or supervisor
 * restarts the process) should keep the parent's fail-closed semantics:
 * the restart proves the chain clean. Unattended callers (the orphan
 * drainer) may retry construction once instead of quarantining — dropping
 * a {@code .failed} sentinel over a just-healed slot would strand its
 * replayable backlog until an operator clears the sentinel by hand.
 */
public final class SfSanitizedResidueException extends SfRecoveryException {

    public SfSanitizedResidueException(String message) {
        super(message);
    }
}
