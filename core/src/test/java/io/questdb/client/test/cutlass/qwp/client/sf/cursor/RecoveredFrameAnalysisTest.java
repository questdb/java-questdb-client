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

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class RecoveredFrameAnalysisTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testTruncatedSymbolBytesMarkRecoveredDeltaAsGap() throws Exception {
        assertMalformedDeltaMarksGap(new byte[]{0, 1, 3, 'x'});
    }

    @Test
    public void testUnterminatedDeltaCountMarksRecoveredDeltaAsGap() throws Exception {
        assertMalformedDeltaMarksGap(new byte[]{0, (byte) 0x80});
    }

    @Test
    public void testUnterminatedDeltaStartMarksRecoveredDeltaAsGap() throws Exception {
        assertMalformedDeltaMarksGap(new byte[]{(byte) 0x80});
    }

    @Test
    public void testUnterminatedSymbolLengthMarksRecoveredDeltaAsGap() throws Exception {
        assertMalformedDeltaMarksGap(new byte[]{0, 1, (byte) 0x80});
    }

    private void assertMalformedDeltaMarksGap(byte[] deltaSection) throws Exception {
        assertMemoryLeak(() -> {
            Path slot = temporaryFolder.newFolder("qwp-malformed-recovery").toPath();
            int payloadLen = QwpConstants.HEADER_SIZE + deltaSection.length;
            long payload = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(payload, payloadLen, (byte) 0);
                Unsafe.getUnsafe().putInt(payload, QwpConstants.MAGIC_MESSAGE);
                Unsafe.getUnsafe().putByte(
                        payload + QwpConstants.HEADER_OFFSET_FLAGS,
                        QwpConstants.FLAG_DELTA_SYMBOL_DICT);
                for (int i = 0; i < deltaSection.length; i++) {
                    Unsafe.getUnsafe().putByte(payload + QwpConstants.HEADER_SIZE + i, deltaSection[i]);
                }
                try (CursorSendEngine writer = new CursorSendEngine(slot.toString(), 4_096)) {
                    Assert.assertEquals(0L, writer.appendBlocking(payload, payloadLen));
                }
            } finally {
                Unsafe.free(payload, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }

            try (CursorSendEngine recovered = new CursorSendEngine(slot.toString(), 4_096)) {
                GlobalSymbolDictionary symbols = new GlobalSymbolDictionary();
                Assert.assertEquals("malformed recovered delta must be fail-clean",
                        -1L, recovered.addRecoveredSymbolsTo(0, symbols));
                Assert.assertEquals("malformed delta must not recover partial symbols", 0, symbols.size());
                Assert.assertEquals("the malformed frame must be visited during recovery",
                        1L, recovered.recoveryFramesVisited());
            }
        });
    }
}
