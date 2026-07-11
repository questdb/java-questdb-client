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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpWebSocketSenderCursorEngineAttachmentTest {

    private static final long SEGMENT_SIZE = 64 * 1024L;

    @Rule
    public final TemporaryFolder sfDir = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testNullDoesNotDetachAnAttachedEngine() throws Exception {
        assertMemoryLeak(() -> {
            CursorSendEngine engine = new CursorSendEngine(null, SEGMENT_SIZE);
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 1);
            try {
                sender.setCursorEngine(engine, true);
                assertSecondAttachmentRejected(sender, null, false);
                sender.close();
                Assert.assertTrue("sender must retain ownership after rejected detach",
                        engine.isCloseCompleted());
            } finally {
                sender.close();
                engine.close();
            }
        });
    }

    @Test
    public void testOwnedEngineCannotBeReplacedAndItsSlotIsReleased() throws Exception {
        assertMemoryLeak(() -> {
            File slotDir = new File(sfDir.getRoot(), "owned-slot");
            CursorSendEngine first = null;
            CursorSendEngine replacement = null;
            CursorSendEngine reacquired = null;
            QwpWebSocketSender sender = null;
            boolean replacementRejected = false;
            try {
                first = new CursorSendEngine(slotDir.getAbsolutePath(), SEGMENT_SIZE);
                replacement = new CursorSendEngine(null, SEGMENT_SIZE);
                sender = QwpWebSocketSender.createForTesting("localhost", 1);
                sender.setCursorEngine(first, true);
                try {
                    sender.setCursorEngine(replacement, true);
                } catch (LineSenderException e) {
                    replacementRejected = true;
                    Assert.assertTrue(e.getMessage().contains("already attached"));
                }

                sender.close();
                sender = null;
                try {
                    reacquired = new CursorSendEngine(slotDir.getAbsolutePath(), SEGMENT_SIZE);
                } catch (IllegalStateException e) {
                    throw new AssertionError("sender close did not release the first owned "
                            + "engine's slot after a second attachment attempt", e);
                }
                Assert.assertTrue("second attachment must be rejected", replacementRejected);
            } finally {
                if (sender != null) {
                    sender.close();
                }
                if (reacquired != null) {
                    reacquired.close();
                }
                if (replacement != null) {
                    replacement.close();
                }
                if (first != null) {
                    first.close();
                }
            }
        });
    }

    @Test
    public void testSameSharedEngineCannotTransferOwnership() throws Exception {
        assertMemoryLeak(() -> {
            CursorSendEngine engine = new CursorSendEngine(null, SEGMENT_SIZE);
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 1);
            try {
                sender.setCursorEngine(engine, false);
                assertSecondAttachmentRejected(sender, engine, true);
                sender.close();
                Assert.assertFalse("rejected attachment must not transfer ownership",
                        engine.isCloseCompleted());
            } finally {
                sender.close();
                engine.close();
            }
        });
    }

    private static void assertSecondAttachmentRejected(
            QwpWebSocketSender sender,
            CursorSendEngine engine,
            boolean takeOwnership
    ) {
        try {
            sender.setCursorEngine(engine, takeOwnership);
            Assert.fail("expected second cursor engine attachment to be rejected");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("already attached"));
        }
    }
}
