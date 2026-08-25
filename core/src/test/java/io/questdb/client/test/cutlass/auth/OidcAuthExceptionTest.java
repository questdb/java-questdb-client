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

package io.questdb.client.test.cutlass.auth;

import io.questdb.client.cutlass.auth.OidcAuthException;
import org.junit.Assert;
import org.junit.Test;

public class OidcAuthExceptionTest {

    @Test
    public void testOauthErrorCleanCodePassesThrough() {
        OidcAuthException e = OidcAuthException.oauthError("access_denied", "the user declined");
        Assert.assertEquals("access_denied", e.getOauthError());
        Assert.assertEquals(
                "the identity provider returned an error [error=access_denied, description=the user declined]",
                e.getMessage()
        );
    }

    @Test
    public void testOauthErrorNullCodeYieldsNull() {
        OidcAuthException e = OidcAuthException.oauthError(null, null);
        Assert.assertNull(e.getOauthError());
        Assert.assertEquals("the identity provider returned an error [error=]", e.getMessage());
    }

    @Test
    public void testOauthErrorStripsControlCharsFromAccessorAndMessage() {
        // JsonLexer decodes JSON escapes, so a hostile identity provider can put a real ESC (and CR/LF) into
        // the error code. getOauthError() is public and a caller may log it verbatim, so the accessor must not
        // return raw control bytes that would inject ANSI sequences or forge log lines - the same guarantee the
        // rendered message already made. Build the control chars from their code points to keep the source ASCII.
        String raw = "access_denied" + (char) 0x1b + "[2Jx" + (char) 0x0d + (char) 0x0a + "y";
        OidcAuthException e = OidcAuthException.oauthError(raw, null);
        Assert.assertEquals("access_denied[2Jxy", e.getOauthError());
        Assert.assertEquals(
                "the identity provider returned an error [error=access_denied[2Jxy]",
                e.getMessage()
        );
    }
}
