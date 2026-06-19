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

import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Verifies the post-connect mutation guard on every {@code with*} setter.
 * Uses reflection to flip the {@code connected} field to {@code true} --
 * avoids the cost of booting a real server just to exercise the guard, and
 * keeps the coverage dense (one assertion per setter, ~20 setters).
 * <p>
 * The test asserts two things per setter:
 * 1. The exception is {@link IllegalStateException} (so the check is
 *    reachable and typed correctly).
 * 2. The exception message names the setter, so a stack-free error surface
 *    tells the caller which setter was at fault. Downstream UIs and CLIs
 *    surface the message verbatim.
 */
public class QwpQueryClientPostConnectGuardTest {

    @Test
    public void testAllSettersRejectAfterConnect() throws Exception {
        // withBasicAuth
        assertRejects(c -> c.withBasicAuth("u", "p"), "withBasicAuth");
        // withBearerToken
        assertRejects(c -> c.withBearerToken("tok"), "withBearerToken");
        // withBufferPoolSize
        assertRejects(c -> c.withBufferPoolSize(2), "withBufferPoolSize");
        // withClientId
        assertRejects(c -> c.withClientId("id"), "withClientId");
        // withCompression
        assertRejects(c -> c.withCompression("zstd", 3), "withCompression");
        // withFailover
        assertRejects(c -> c.withFailover(false), "withFailover");
        // withFailoverBackoff
        assertRejects(c -> c.withFailoverBackoff(100L, 500L), "withFailoverBackoff");
        // withFailoverMaxAttempts
        assertRejects(c -> c.withFailoverMaxAttempts(3), "withFailoverMaxAttempts");
        // withFailoverMaxDuration
        assertRejects(c -> c.withFailoverMaxDuration(15_000L), "withFailoverMaxDuration");
        // withAuthTimeout
        assertRejects(c -> c.withAuthTimeout(5_000L), "withAuthTimeout");
        // withInitialCredit
        assertRejects(c -> c.withInitialCredit(1024L), "withInitialCredit");
        // withInsecureTls
        assertRejects(QwpQueryClient::withInsecureTls, "withInsecureTls");
        // withMaxBatchRows
        assertRejects(c -> c.withMaxBatchRows(128), "withMaxBatchRows");
        // withServerInfoTimeout
        assertRejects(c -> c.withServerInfoTimeout(500), "withServerInfoTimeout");
        // withTarget
        assertRejects(c -> c.withTarget(QwpQueryClient.TARGET_PRIMARY), "withTarget");
        // withTls
        assertRejects(QwpQueryClient::withTls, "withTls");
        // withTrustStore
        assertRejects(c -> c.withTrustStore("/tmp/ts.p12", "pw".toCharArray()), "withTrustStore");
    }

    private static QwpQueryClient fakeConnected() throws Exception {
        QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000);
        Field connected = QwpQueryClient.class.getDeclaredField("connected");
        connected.setAccessible(true);
        connected.setBoolean(c, true);
        return c;
    }

    private static void assertRejects(Setter setter, String setterName) throws Exception {
        QwpQueryClient c = fakeConnected();
        try {
            setter.apply(c);
            Assert.fail(setterName + " must throw IllegalStateException after connect()");
        } catch (IllegalStateException e) {
            Assert.assertTrue(
                    "exception message for " + setterName + " must name the setter, was: " + e.getMessage(),
                    e.getMessage() != null && e.getMessage().startsWith(setterName + " must be called before connect()")
            );
        }
    }

    @FunctionalInterface
    private interface Setter {
        void apply(QwpQueryClient client);
    }
}
