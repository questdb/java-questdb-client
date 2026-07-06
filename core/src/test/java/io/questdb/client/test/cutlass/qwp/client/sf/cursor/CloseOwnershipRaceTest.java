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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression guard for the close()-ownership race in {@code QwpWebSocketSender.close()}:
 * its {@code alreadyOwnedByUser} snapshot was once computed from two separate volatile
 * reads ({@code !hasUnsurfacedError() ? getLastError() : null}), so a terminal error
 * latched by the I/O thread between the reads was mis-captured as already-owned and
 * silently dropped on close(). The fix is the single read
 * {@link CursorWebSocketSendLoop#getSynchronouslySurfacedError()}.
 * <p>
 * Scope: this test races that accessor and fails if its implementation ever recomputes
 * the snapshot from two reads. It does not execute {@code QwpWebSocketSender.close()}
 * itself — old and new snapshots agree in every non-racy state, so no black-box test
 * at the sender level can detect a callsite that bypasses the accessor; that half of
 * the contract is pinned by the "must stay this single read" comments at the callsite
 * and on the accessor.
 */
public class CloseOwnershipRaceTest {

    private static final int ROUNDS = 1000;

    @Rule
    public final TemporaryFolder sfDir = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 60_000)
    public void closeOwnershipSnapshotNeverClaimsAnUnsurfacedError() {
        try (CursorSendEngine engine = new CursorSendEngine(
                sfDir.getRoot().getAbsolutePath(), 16_384)) {
            Throwable leaked = null;
            for (int i = 0; i < ROUNDS && leaked == null; i++) {
                // A null client and a reconnect factory that throws a genuine
                // terminal auth reject: start()'s real I/O thread walks the
                // production async-initial-connect path and latches a genuine
                // (SECURITY_ERROR) terminal within microseconds. One authentic
                // null->error latch transition per round. (Under Invariant B a
                // connection error / budget would retry forever and never latch;
                // only a genuine terminal like auth does.)
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0, 1_000_000L,
                        () -> {
                            throw new QwpAuthFailedException(401, "localhost", 1);
                        },
                        0,
                        1, 1);
                loop.start();
                // Race close()'s exact ownership snapshot against the latch
                // transition, stopping once the latch has landed. Nothing in
                // this test calls checkError(), so no synchronous caller ever
                // owns the error and the snapshot must stay null. On fixed
                // code it reads a field nobody here writes — it cannot flake;
                // a reintroduced two-read snapshot tears when the latch lands
                // between its reads, with near-certainty across all rounds.
                while (leaked == null && loop.getTerminalError() == null) {
                    leaked = loop.getSynchronouslySurfacedError();
                }
                loop.close();
            }
            Assert.assertNull(
                    "close() captured a terminal error as 'already owned by user' that no "
                            + "synchronous caller ever saw -- close() would silently drop it: " + leaked,
                    leaked);
        }
    }
}
