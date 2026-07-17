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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BackgroundDrainerOrphanTailTest {

    private static final int FLAG_DEFER_COMMIT = 0x01;
    private static final int HEADER_OFFSET_FLAGS = 5;
    private static final int MAGIC_MESSAGE = 0x31505751;
    private static final long SEGMENT_SIZE_BYTES = 16_384L;

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testDeferredOnlyRecoveredTailRetiresWithoutConnecting() throws Exception {
        String slotPath = temporaryFolder.newFolder("slot").getAbsolutePath();
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(slotPath, SEGMENT_SIZE_BYTES)) {
                appendDeferredFrame(engine);
                appendDeferredFrame(engine);
                appendDeferredFrame(engine);
            }

            AtomicInteger connectAttempts = new AtomicInteger();
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_SIZE_BYTES,
                    1L << 20,
                    () -> {
                        connectAttempts.incrementAndGet();
                        throw new AssertionError("deferred-only tail must retire before connect");
                    },
                    5_000L,
                    1L,
                    5L,
                    true,
                    200L);

            drainer.run();

            assertEquals("local retirement must not invoke the reconnect factory",
                    0, connectAttempts.get());
            assertEquals(BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
            assertFalse("local retirement must not quarantine the slot",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    private static void appendDeferredFrame(CursorSendEngine engine) {
        long buffer = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 16; i++) {
                Unsafe.getUnsafe().putByte(buffer + i, (byte) 0);
            }
            Unsafe.getUnsafe().putInt(buffer, MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buffer + HEADER_OFFSET_FLAGS, (byte) FLAG_DEFER_COMMIT);
            engine.appendBlocking(buffer, 16);
        } finally {
            Unsafe.free(buffer, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

}
