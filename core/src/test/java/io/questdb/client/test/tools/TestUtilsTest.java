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

package io.questdb.client.test.tools;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class TestUtilsTest {

    @Test
    public void testRecursiveCleanupRetainsFirstFailureAndContinuesDeleting() throws Exception {
        Path root = Files.createTempDirectory("qdb-test-utils-cleanup-");
        Path child = Files.createDirectory(root.resolve("child"));
        Path file = Files.createFile(child.resolve("blocked"));
        IOException injected = new IOException("simulated deletion failure");
        AtomicInteger attempts = new AtomicInteger();
        try {
            try {
                TestUtils.removeTmpDirRec(root, path -> {
                    attempts.incrementAndGet();
                    if (path.equals(file)) {
                        throw injected;
                    }
                    Files.deleteIfExists(path);
                });
                Assert.fail("cleanup must report the injected deletion failure");
            } catch (AssertionError e) {
                Assert.assertSame("the first deletion failure must be retained", injected, e.getCause());
                Assert.assertTrue("cleanup must continue with parent directories after a file failure",
                        attempts.get() >= 3);
                Assert.assertTrue("later parent deletion failures must remain diagnostic",
                        e.getCause().getSuppressed().length >= 2);
            }
        } finally {
            TestUtils.removeTmpDirRec(root.toString());
        }
    }
}
