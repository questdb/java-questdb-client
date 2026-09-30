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

package io.questdb.client.test.std;

import io.questdb.client.std.Misc;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class MiscTest {
    @Test
    public void testFreeSuppressingClosesResource() {
        Throwable failure = new IllegalStateException("construction failed");
        AtomicInteger closeCalls = new AtomicInteger();
        Misc.freeSuppressing(closeCalls::incrementAndGet, failure);
        Assert.assertEquals(1, closeCalls.get());
        Assert.assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    public void testFreeSuppressingHandlesNull() {
        Throwable failure = new IllegalStateException("construction failed");
        Misc.freeSuppressing(null, failure);
        Assert.assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    public void testFreeSuppressingSkipsSelfSuppression() {
        AssertionError failure = new AssertionError("construction and close failed");
        Misc.freeSuppressing(() -> {
            throw failure;
        }, failure);
        Assert.assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    public void testFreeSuppressingSuppressesError() {
        Throwable failure = new IllegalStateException("construction failed");
        AssertionError closeFailure = new AssertionError("close failed");
        Misc.freeSuppressing(() -> {
            throw closeFailure;
        }, failure);
        Assert.assertArrayEquals(new Throwable[]{closeFailure}, failure.getSuppressed());
    }

    @Test
    public void testFreeSuppressingSuppressesIOException() {
        Throwable failure = new IllegalStateException("construction failed");
        IOException closeFailure = new IOException("close failed");
        Misc.freeSuppressing(() -> {
            throw closeFailure;
        }, failure);
        Assert.assertArrayEquals(new Throwable[]{closeFailure}, failure.getSuppressed());
    }
}
