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

package io.questdb.client.test.cutlass.qwp.websocket;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class TestTlsTest {
    @Test(timeout = 20_000)
    public void testFailedKeytoolReportsOutput() throws Exception {
        assertKeytoolResult("fail", "keytool failed: test-keytool-output", 10_000);
    }

    @Test(timeout = 20_000)
    public void testInterruptedKeytoolKillsTheProcessAndPreservesTheFlag() throws Exception {
        Path output = Files.createTempFile("qdb-test-keytool-interrupt", ".log");
        Process process = null;
        try {
            process = startProcess(output, "stall");
            Thread.currentThread().interrupt();
            try {
                TestTls.waitForKeytool(process, output, 10, TimeUnit.SECONDS);
                Assert.fail("the interrupted wait must fail");
            } catch (IOException expected) {
                Assert.assertEquals("interrupted while running keytool", expected.getMessage());
                Assert.assertTrue(expected.getCause() instanceof InterruptedException);
                Assert.assertTrue("the caller must retain its interrupt flag", Thread.currentThread().isInterrupted());
            }
            Assert.assertFalse("the interrupted wait must reap the process", process.isAlive());
        } finally {
            Thread.interrupted();
            stopProcess(process);
            Files.deleteIfExists(output);
        }
    }

    @Test(timeout = 20_000)
    public void testSuccessfulKeytoolExits() throws Exception {
        assertKeytoolResult("success", null, 10_000);
    }

    @Test(timeout = 20_000)
    public void testTimedOutKeytoolKillsTheProcess() throws Exception {
        assertKeytoolResult("stall", "keytool timed out", 100);
    }

    private static void assertKeytoolResult(String mode, String expectedMessage, long timeoutMillis) throws Exception {
        Path output = Files.createTempFile("qdb-test-keytool-result", ".log");
        Process process = null;
        try {
            process = startProcess(output, mode);
            try {
                TestTls.waitForKeytool(process, output, timeoutMillis, TimeUnit.MILLISECONDS);
                Assert.assertNull("the process must fail", expectedMessage);
                Assert.assertEquals(0, process.exitValue());
            } catch (IOException e) {
                Assert.assertNotNull("the process must succeed", expectedMessage);
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
            }
            Assert.assertFalse("the wait must reap the process", process.isAlive());
        } finally {
            stopProcess(process);
            Files.deleteIfExists(output);
        }
    }

    private static Process startProcess(Path output, String mode) throws Exception {
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = new File(KeytoolProcess.class.getProtectionDomain().getCodeSource()
                .getLocation().toURI()).getAbsolutePath();
        return new ProcessBuilder(java, "-cp", classpath, KeytoolProcess.class.getName(), mode)
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
    }

    private static void stopProcess(Process process) throws InterruptedException {
        if (process != null) {
            process.destroyForcibly();
            Assert.assertTrue("the test process must terminate", process.waitFor(5, TimeUnit.SECONDS));
        }
    }

    public static class KeytoolProcess {
        public static void main(String[] args) throws InterruptedException {
            System.out.println("test-keytool-output");
            switch (args[0]) {
                case "fail":
                    System.exit(7);
                    break;
                case "stall":
                    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                    break;
                case "success":
                    break;
                default:
                    throw new IllegalArgumentException(args[0]);
            }
        }
    }
}
