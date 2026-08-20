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

/**
 * Covers {@code BrowserLauncher}, the best-effort browser launch behind the default device-code prompt.
 * <p>
 * REFLECTION, deliberately: the class and all three methods are package-private, and the only public route
 * to them is {@code DeviceCodePrompt.openBrowser().promptUser(...)}, whose whole contract is that it does
 * nothing observable - it swallows every failure and, on a headless machine, never launches anything either
 * way. There is no public path to assert on, so the choice is reflection or no coverage of the scheme
 * allowlist at all. The one behaviour that IS observable from outside is what {@code open()} does with the
 * kill-switch, and that is pinned without reflection by
 * {@link DesktopFreeModulePathTest#testTheBrowserKillSwitchIsHonouredByOpenItself}, which runs it where
 * {@code java.desktop} is absent.
 */
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
        // SCOPE: this test proves the property READ flips, and nothing more. A browser launch is
        // unobservable from here, and on a headless JVM open() is a no-op whether or not it ever consulted
        // the flag - so the invokeOpen call below would stay green against an open() that ignored the
        // kill-switch outright (verified: removing the gate from open() leaves every assertion in this class
        // passing). What open() DOES with the flag is pinned by
        // DesktopFreeModulePathTest.testTheBrowserKillSwitchIsHonouredByOpenItself, which runs it where
        // java.desktop is absent: reaching Desktop throws there, so the two directions become distinguishable
        // - quiet with the kill-switch off, LinkageError with it on. A VALID http(s) URL is used below so the
        // no-op under "false" is at least not URL rejection, and so this class never pops a browser.
        String validUrl = "https://idp.example.com/device?user_code=ABCD";
        Assert.assertNotNull("the URL must be one open() would otherwise launch", invokeSafeHttpUri(validUrl));
        String prop = "questdb.client.oidc.open.browser";
        String prev = System.getProperty(prop);
        try {
            System.clearProperty(prop);
            Assert.assertTrue("the browser launch must default to enabled", invokeIsBrowserOpenEnabled());
            System.setProperty(prop, "true");
            Assert.assertTrue("\"true\" must enable the browser launch", invokeIsBrowserOpenEnabled());
            System.setProperty(prop, "false");
            Assert.assertFalse("\"false\" must disable the browser launch (the kill-switch)", invokeIsBrowserOpenEnabled());
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
    private static boolean invokeIsBrowserOpenEnabled() throws Exception {
        Method m = Class.forName("io.questdb.client.cutlass.auth.BrowserLauncher").getDeclaredMethod("isBrowserOpenEnabled");
        m.setAccessible(true);
        return (boolean) m.invoke(null);
    }

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
