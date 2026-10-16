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

package io.questdb.client.test.cutlass.http;

import io.questdb.client.cutlass.http.BearerChallenge;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import org.junit.Assert;
import org.junit.Test;

public class BearerChallengeTest {

    @Test
    public void testAuthFailedExceptionClassification() {
        QwpAuthFailedException plain = new QwpAuthFailedException(401, "h", 1);
        Assert.assertTrue(plain.isTokenRefreshable());
        Assert.assertNull(plain.getBearerError());
        Assert.assertEquals("auth-rejected: WebSocket upgrade rejected with HTTP 401 for h:1", plain.getMessage());

        QwpAuthFailedException invalid = new QwpAuthFailedException(401, "h", 1, "Bearer error=\"invalid_token\"");
        Assert.assertTrue(invalid.isTokenRefreshable());
        Assert.assertEquals("auth-rejected: WebSocket upgrade rejected with HTTP 401 [error=invalid_token] for h:1",
                invalid.getMessage());

        QwpAuthFailedException scope = new QwpAuthFailedException(401, "h", 1, "Bearer error=\"insufficient_scope\"");
        Assert.assertFalse(scope.isTokenRefreshable());

        // decision D2: a 403 never qualifies, challenge or not
        Assert.assertFalse(new QwpAuthFailedException(403, "h", 1).isTokenRefreshable());
        Assert.assertFalse(new QwpAuthFailedException(403, "h", 1, "Bearer error=\"invalid_token\"").isTokenRefreshable());

        // a hostile error value is sanitized and capped before it reaches the message
        QwpAuthFailedException hostile = new QwpAuthFailedException(401, "h", 1,
                "Bearer error=\"x\r\nForged: 1\u202E" + new String(new char[200]).replace('\0', 'y') + "\"");
        Assert.assertFalse(hostile.getMessage().contains("\r") || hostile.getMessage().contains("\u202E"));
        Assert.assertTrue(hostile.getBearerError().length() <= 64);
    }

    @Test
    public void testBearerError() {
        Assert.assertNull(BearerChallenge.bearerError(null));
        Assert.assertNull(BearerChallenge.bearerError(""));
        Assert.assertNull(BearerChallenge.bearerError("Bearer"));
        Assert.assertNull(BearerChallenge.bearerError("Bearer realm=\"questdb\""));
        Assert.assertEquals("invalid_token",
                BearerChallenge.bearerError("Bearer realm=\"questdb\", error=\"invalid_token\", error_description=\"The access token expired\""));
        Assert.assertEquals("insufficient_scope", BearerChallenge.bearerError("Bearer error=insufficient_scope"));
        Assert.assertEquals("invalid_token", BearerChallenge.bearerError("bearer ERROR=\"invalid_token\""));
        // a Bearer challenge after another scheme, with token68 and quoted commas in between
        Assert.assertEquals("invalid_token", BearerChallenge.bearerError(
                "Negotiate YIIB9QYGKwYBBQUCoIIB==, Basic realm=\"a, b\", Bearer error=\"invalid_token\""));
        // an error parameter of another scheme does not count
        Assert.assertNull(BearerChallenge.bearerError("Basic error=\"invalid_token\""));
        Assert.assertNull(BearerChallenge.bearerError("Basic realm=\"x\", DPoP error=\"invalid_token\""));
        // escaped quotes inside a quoted value
        Assert.assertEquals("a\"b", BearerChallenge.bearerError("Bearer error=\"a\\\"b\""));
        // only the first error counts
        Assert.assertEquals("invalid_token", BearerChallenge.bearerError("Bearer error=invalid_token, error=other"));
        // garbage does not throw
        Assert.assertNull(BearerChallenge.bearerError("\u0000\"=,,=\"Bearer"));
    }

    @Test
    public void testRefreshDecision() {
        // section 8.2: without a Bearer challenge carrying an error, the status code alone decides
        Assert.assertTrue(BearerChallenge.allowsTokenRefresh(null));
        Assert.assertTrue(BearerChallenge.allowsTokenRefresh("Basic realm=\"questdb\""));
        Assert.assertTrue(BearerChallenge.allowsTokenRefresh("Bearer realm=\"questdb\""));
        Assert.assertTrue(BearerChallenge.allowsTokenRefresh("Bearer error=\"invalid_token\""));
        Assert.assertFalse(BearerChallenge.allowsTokenRefresh("Bearer error=\"insufficient_scope\""));
        Assert.assertFalse(BearerChallenge.allowsTokenRefresh("Bearer error=\"invalid_request\""));
    }
}
