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

package io.questdb.test.std;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class Utf8SequenceObjHashMapTest {
    @Test
    public void testHashMapUtf8() {
        final int N = 256;
        final int memSize = 2 * N;
        long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        final DirectUtf8String dus = new DirectUtf8String();
        Utf8SequenceObjHashMap<Integer> map = new Utf8SequenceObjHashMap<>();
        try {
            final String utf16Str = "ъ";
            final byte[] utf8Bytes = utf16Str.getBytes(StandardCharsets.UTF_8);
            assert utf8Bytes.length == 2;
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < 2; j++) {
                    Unsafe.getUnsafe().putByte(mem + (long) 2 * i + j, utf8Bytes[j]);
                }
            }

            for (int i = 0; i < N; i++) {
                dus.of(mem, mem + (long) 2 * i);
                Assert.assertNull(map.get(dus));

                final Utf8String bcs = Utf8String.newInstance(dus);
                map.put(Utf8String.newInstance(dus), i);
                Assert.assertEquals(i, (int) map.get(dus));
                Assert.assertEquals(i, (int) map.get(bcs));
            }
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
