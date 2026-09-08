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

package io.questdb.client.test;

import io.questdb.client.HttpTokenProvider;
import io.questdb.client.cutlass.line.LineSenderException;
import org.junit.Assert;
import org.junit.Test;

public class HttpTokenProviderTest {

    @Test
    public void testValidateTokenAcceptsPrintableAscii() {
        // a real bearer token is printable ASCII (base64url JWT segments joined by dots); validateToken
        // must pass it through unchanged
        HttpTokenProvider.validateToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJfc3NvIn0.abc-DEF_123");
        HttpTokenProvider.validateToken("a~b"); // 0x7e (~) is the top of the allowed range
        HttpTokenProvider.validateToken("a b"); // an interior space (0x20) is allowed; only an all-blank token is rejected
    }

    @Test
    public void testValidateTokenNeverEchoesTheToken() {
        // the token is the secret this guards; it must never appear in the exception message
        try {
            HttpTokenProvider.validateToken("SUPERSECRET" + (char) 0x0d + (char) 0x0a + "TOKEN");
            Assert.fail("expected the token to be rejected");
        } catch (LineSenderException e) {
            Assert.assertFalse(e.getMessage(), e.getMessage().contains("SUPERSECRET"));
        }
    }

    @Test
    public void testValidateTokenRejectsBlank() {
        assertRejected(null, "null or empty token");
        assertRejected("", "null or empty token");
        assertRejected("   ", "null or empty token");
    }

    @Test
    public void testValidateTokenRejectsControlOrNonAscii() {
        // a control char would break out of the "Authorization: Bearer <token>" header (CR/LF injects into
        // the request line); a non-ASCII char is silently truncated to one byte by the ASCII header writer.
        // The strings are built with explicit char values to keep this source pure ASCII.
        assertRejected("abc" + (char) 0x0d + (char) 0x0a + "def", "control or non-ASCII character"); // CR/LF
        assertRejected("tok" + (char) 0x00 + "en", "control or non-ASCII character"); // NUL
        assertRejected((char) 0x1b + "[31mred", "control or non-ASCII character"); // ANSI escape (ESC)
        assertRejected("a" + (char) 0x1f + "b", "control or non-ASCII character"); // 0x1f, just below the 0x20 lower bound
        assertRejected("a" + (char) 0x7f + "b", "control or non-ASCII character"); // DEL (0x7f), just above the 0x7e upper bound
        assertRejected("tok" + (char) 0xe9 + "n", "control or non-ASCII character"); // non-ASCII (e-acute, 0xe9)
    }

    private static void assertRejected(CharSequence token, String expectedMessage) {
        try {
            HttpTokenProvider.validateToken(token);
            Assert.fail("expected token to be rejected: " + token);
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }
}
