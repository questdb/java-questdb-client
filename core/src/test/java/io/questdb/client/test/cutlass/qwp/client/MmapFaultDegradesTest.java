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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import org.junit.Assert;
import org.junit.Test;

public class MmapFaultDegradesTest {

    @Test
    public void testRecognisedMmapFaultIsDistinguishedFromAPlainError() {
        // commitMappedChunk uses Crc32c.updateUnsafe precisely so a SIGBUS on a sparse
        // page becomes a catchable InternalError. InternalError extends
        // VirtualMachineError extends Error, so an unqualified "if (t instanceof Error)
        // throw" hands back the one fault class the design made catchable, and the
        // sender never degrades to self-sufficient frames.
        Assert.assertTrue(MmapSegment.isMmapAccessFault(
                new InternalError("a fault occurred in an unsafe memory access operation")));
        Assert.assertFalse(MmapSegment.isMmapAccessFault(new OutOfMemoryError("heap")));
        Assert.assertFalse(MmapSegment.isMmapAccessFault(new InternalError("something else")));
    }
}
