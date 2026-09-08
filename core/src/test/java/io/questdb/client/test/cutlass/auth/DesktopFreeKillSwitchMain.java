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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Proves that {@code BrowserLauncher.open} itself honours the
 * {@code questdb.client.oidc.open.browser} kill-switch, rather than merely that
 * {@code isBrowserOpenEnabled()} reads the property.
 * <p>
 * In an ordinary JVM the two are indistinguishable: a browser launch is unobservable from a test, and on a
 * headless machine {@code Desktop.isDesktopSupported()} answers false, so {@code open()} is a no-op whether
 * or not it ever consulted the property. Run it where {@code java.desktop} does NOT exist and the difference
 * becomes loud - reaching {@code Desktop} throws a {@link LinkageError}:
 * <ul>
 *     <li>kill-switch OFF: {@code open()} must return before touching {@code Desktop}, so no throw;</li>
 *     <li>kill-switch ON: {@code open()} must reach {@code Desktop} and the LinkageError must escape.</li>
 * </ul>
 * The second half is what makes the first half mean something: without it, an {@code open()} that returned
 * immediately for any reason at all would look like a working kill-switch.
 * <p>
 * Java 8 source level and no {@code java.lang.module} API, like the test that launches it - the JDK 8
 * release profile compiles this test tree.
 */
public final class DesktopFreeKillSwitchMain {

    static final int EXIT_DESKTOP_REACHABLE = 2;
    static final int EXIT_NO_LINKAGE_ERROR = 3;
    static final int EXIT_THREW_WHILE_DISABLED = 4;
    static final String SUCCESS_MARKER = "DESKTOP-FREE-KILL-SWITCH-OK";
    private static final String OPEN_BROWSER_PROPERTY = "questdb.client.oidc.open.browser";
    // a URL open() would otherwise hand to the OS: http(s), so it survives the scheme allowlist and the run
    // reaches the Desktop call. Nothing can open it here - this JVM has no java.desktop at all.
    private static final String VALID_URL = "https://verify.example/device?user_code=WDJB-MJHT";

    private DesktopFreeKillSwitchMain() {
    }

    public static void main(String[] args) throws Exception {
        try {
            Class.forName("java.awt.Desktop");
            System.out.println("java.awt.Desktop is reachable, so neither half below proves anything");
            System.exit(EXIT_DESKTOP_REACHABLE);
        } catch (ClassNotFoundException expected) {
            // desktop-free, as this run requires
        }

        final Method open = Class.forName("io.questdb.client.cutlass.auth.BrowserLauncher")
                .getDeclaredMethod("open", String.class);
        open.setAccessible(true);

        System.setProperty(OPEN_BROWSER_PROPERTY, "false");
        try {
            open.invoke(null, VALID_URL);
        } catch (InvocationTargetException e) {
            System.out.println("open() must return at the kill-switch, before java.awt.Desktop: " + e.getCause());
            System.exit(EXIT_THREW_WHILE_DISABLED);
        }

        System.setProperty(OPEN_BROWSER_PROPERTY, "true");
        try {
            open.invoke(null, VALID_URL);
            System.out.println("open() did not reach java.awt.Desktop with the kill-switch ON, so the quiet "
                    + "run above was not the kill-switch doing its job");
            System.exit(EXIT_NO_LINKAGE_ERROR);
        } catch (InvocationTargetException e) {
            if (!(e.getCause() instanceof LinkageError)) {
                System.out.println("expected a LinkageError from the missing java.desktop, got: " + e.getCause());
                System.exit(EXIT_NO_LINKAGE_ERROR);
            }
        }

        System.out.println(SUCCESS_MARKER);
    }
}
