/*******************************************************************************
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

import io.questdb.client.cutlass.qwp.client.InFlightWindow;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.AbstractTest;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.temporal.ChronoUnit;

/**
 * Verifies that maxSentSymbolId and sentSchemaHashes are not updated
 * when the send fails, so the next batch's delta dictionary correctly
 * re-includes symbols the server never received.
 */
public class QwpDeltaDictRollbackTest extends AbstractTest {

    @Test
    public void testSyncFlushFailureDoesNotAdvanceMaxSentSymbolId() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Sync mode (window=1), not connected to any server
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 0, 1);
            try {
                // Bypass ensureConnected() by marking as connected.
                // Leave client null so sendBinary() will throw.
                setField(sender, "connected", true);
                setField(sender, "inFlightWindow", new InFlightWindow(1, InFlightWindow.DEFAULT_TIMEOUT_MS));

                // Buffer a row with a symbol — this registers symbol id 0
                // in the global dictionary and sets currentBatchMaxSymbolId = 0
                sender.table("t")
                        .symbol("s", "val1")
                        .at(1, ChronoUnit.MICROS);

                // maxSentSymbolId should still be -1 (nothing sent yet)
                Assert.assertEquals(-1, sender.getMaxSentSymbolId());

                // flush() -> flushSync() -> encode succeeds -> client.sendBinary() throws NPE
                // because client is null (we never actually connected)
                try {
                    sender.flush();
                    Assert.fail("Expected NullPointerException from null client");
                } catch (NullPointerException expected) {
                    // sendBinary() on null client
                }

                // The fix: maxSentSymbolId must remain -1 because the send failed.
                // Without the fix, it would have been advanced to 0 before the throw,
                // causing the next batch's delta dictionary to omit symbol "val1".
                Assert.assertEquals(
                        "maxSentSymbolId must not advance when send fails",
                        -1, sender.getMaxSentSymbolId()
                );
            } finally {
                // Mark as not connected so close() doesn't try to flush again
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }
}
