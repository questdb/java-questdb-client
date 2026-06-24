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

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.URI;

public class BrowserLauncherTest {

    @Test
    public void testAcceptsHttpAndHttps() throws Exception {
        Assert.assertNotNull(invokeSafeHttpUri("https://idp.example.com/device?user_code=ABCD"));
        Assert.assertNotNull(invokeSafeHttpUri("http://localhost:8080/device"));
        // the scheme allowlist is case-insensitive
        Assert.assertNotNull(invokeSafeHttpUri("HTTPS://idp.example.com"));
    }

    @Test
    public void testOpenIsBestEffortForRejectedUrls() throws Exception {
        // these URLs are rejected by the scheme/parse allowlist, so open() returns at the safeHttpUri null
        // check before touching java.awt.Desktop. Assert the rejection holds so the no-op below is provably
        // the URL-rejection path (not an incidental headless no-op), then confirm open() tolerates each
        // without throwing (and never launches a real browser, so the test is safe on a desktop machine too)
        Assert.assertNull(invokeSafeHttpUri("javascript:alert(1)"));
        Assert.assertNull(invokeSafeHttpUri("not a url"));
        invokeOpen(null);
        invokeOpen("javascript:alert(1)");
        invokeOpen("not a url");
    }

    @Test
    public void testOpenRespectsDisableProperty() throws Exception {
        // a VALID http(s) URL: if open() did not short-circuit on the kill-switch it would proceed toward
        // java.awt.Desktop, so asserting safeHttpUri accepts it proves the no-op below is the kill-switch,
        // not URL rejection. This gate is also what keeps the suite from launching a real browser on a
        // developer machine.
        String validUrl = "https://idp.example.com/device?user_code=ABCD";
        Assert.assertNotNull("the URL must be one open() would otherwise launch", invokeSafeHttpUri(validUrl));
        String prop = "questdb.client.oidc.open.browser";
        String prev = System.getProperty(prop);
        System.setProperty(prop, "false");
        try {
            invokeOpen(validUrl); // kill-switch off: must return without launching and without throwing
        } finally {
            if (prev == null) {
                System.clearProperty(prop);
            } else {
                System.setProperty(prop, prev);
            }
        }
    }

    @Test
    public void testRejectsDangerousOrMalformedUrls() throws Exception {
        // an attacker-influenced verification URI must not smuggle a non-http(s) scheme to the OS handler
        Assert.assertNull(invokeSafeHttpUri("javascript:alert(1)"));
        Assert.assertNull(invokeSafeHttpUri("data:text/html,<script>alert(1)</script>"));
        Assert.assertNull(invokeSafeHttpUri("file:///etc/passwd"));
        Assert.assertNull(invokeSafeHttpUri("ftp://example.com/x"));
        Assert.assertNull(invokeSafeHttpUri("not a url"));
        Assert.assertNull(invokeSafeHttpUri("//idp.example.com/device"));
        Assert.assertNull(invokeSafeHttpUri(""));
        Assert.assertNull(invokeSafeHttpUri(null));
    }

    // BrowserLauncher is a package-private helper; the client is an open module, so reflection reaches its
    // static methods without widening production visibility for the test (mirrors invokeIsLoopbackHost).
    private static void invokeOpen(String url) throws Exception {
        Method m = Class.forName("io.questdb.client.cutlass.auth.BrowserLauncher").getDeclaredMethod("open", String.class);
        m.setAccessible(true);
        m.invoke(null, url);
    }

    private static URI invokeSafeHttpUri(String url) throws Exception {
        Method m = Class.forName("io.questdb.client.cutlass.auth.BrowserLauncher").getDeclaredMethod("safeHttpUri", String.class);
        m.setAccessible(true);
        return (URI) m.invoke(null, url);
    }
}
