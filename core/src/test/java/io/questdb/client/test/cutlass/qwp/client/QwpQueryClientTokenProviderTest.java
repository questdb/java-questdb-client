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
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for {@link QwpQueryClient#withBearerTokenProvider}: header
 * synthesis, re-query at each resolve (a fresh token per WebSocket upgrade),
 * token validation, null rejection, and mutual exclusion with the fixed-token
 * and basic-auth setters. None of these need a live socket --
 * {@link QwpQueryClient#getAuthorizationHeaderForTest()} resolves the header the
 * same way a real upgrade does. The post-connect guard for the setter lives in
 * {@link QwpQueryClientPostConnectGuardTest}.
 */
public class QwpQueryClientTokenProviderTest {

    @Test
    public void testProviderConflictsWithBasicAuth() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerTokenProvider(() -> "tok")) {
            try {
                c.withBasicAuth("u", "p");
                Assert.fail("withBasicAuth after withBearerTokenProvider must throw");
            } catch (IllegalStateException expected) {
                // mutually exclusive
            }
        }
    }

    @Test
    public void testProviderConflictsWithBearerToken() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerTokenProvider(() -> "tok")) {
            try {
                c.withBearerToken("other");
                Assert.fail("withBearerToken after withBearerTokenProvider must throw");
            } catch (IllegalStateException expected) {
                // mutually exclusive
            }
        }
    }

    @Test
    public void testProviderNullRejected() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            try {
                c.withBearerTokenProvider(null);
                Assert.fail("a null provider must be rejected");
            } catch (IllegalArgumentException expected) {
                // expected
            }
        }
    }

    @Test
    public void testProviderQueriedAtEachResolve() {
        int[] counter = {0};
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                .withBearerTokenProvider(() -> "tok-" + (counter[0]++))) {
            // each resolve re-queries the provider, so a reconnect presents a fresh token
            Assert.assertEquals("Bearer tok-0", c.getAuthorizationHeaderForTest());
            Assert.assertEquals("Bearer tok-1", c.getAuthorizationHeaderForTest());
        }
    }

    @Test
    public void testProviderSynthesizesBearerHeader() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                .withBearerTokenProvider(() -> "abc123")) {
            Assert.assertEquals("Bearer abc123", c.getAuthorizationHeaderForTest());
        }
    }

    @Test
    public void testProviderTokenValidated() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                .withBearerTokenProvider(() -> "bad\ntoken")) {
            try {
                c.getAuthorizationHeaderForTest();
                Assert.fail("a token carrying a control character must be rejected");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("control or non-ASCII"));
            }
        }
    }

    @Test
    public void testSettingBearerTokenThenProviderConflicts() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerToken("tok")) {
            try {
                c.withBearerTokenProvider(() -> "other");
                Assert.fail("withBearerTokenProvider after withBearerToken must throw");
            } catch (IllegalStateException expected) {
                // mutually exclusive
            }
        }
    }
}
