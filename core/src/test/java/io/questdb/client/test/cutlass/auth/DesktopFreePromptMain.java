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

import io.questdb.client.cutlass.auth.DeviceAuthorizationChallenge;
import io.questdb.client.cutlass.auth.DeviceCodePrompt;

/**
 * The child half of {@link DesktopFreeModulePathTest}: run by that test in a second JVM which has
 * {@code io.questdb.client} on the MODULE path and a universe that does not contain
 * {@code java.desktop}. Started from the class path, so this class itself is in the unnamed module and
 * reads whatever the module graph resolved.
 * <p>
 * Deliberately written to Java 8 source level, like the test that launches it: the JDK 8 release profile
 * compiles this same test tree (it just excludes {@code module-info.java}), so a {@code ModuleLayer} or
 * {@code java.lang.module} reference here would break that build. Reachability of {@code java.awt.Desktop}
 * is therefore probed with {@code Class.forName}, which answers the same question.
 */
public final class DesktopFreePromptMain {

    /**
     * Exit code for "the precondition did not hold": {@code java.awt.Desktop} was reachable, so nothing
     * below would have proven anything.
     */
    static final int EXIT_DESKTOP_REACHABLE = 2;
    /**
     * Printed on success. The launching test asserts on it rather than on the exit code alone, so a JVM
     * that exited 0 without running this far cannot read as a pass.
     */
    static final String SUCCESS_MARKER = "DESKTOP-FREE-PROMPT-OK";

    private DesktopFreePromptMain() {
    }

    public static void main(String[] args) {
        // The precondition, checked FIRST and by itself fatal: this JVM must genuinely lack java.desktop.
        // It is also the regression guard. --limit-modules closes over the MANDATORY requires of the
        // module it is given, so a module-info that says "requires java.desktop" drags java.desktop back
        // into the universe and lands here - reachable - even though the run asked for a desktop-free one.
        try {
            Class.forName("java.awt.Desktop");
            System.out.println("java.awt.Desktop is reachable, so this JVM is not desktop-free: "
                    + "io.questdb.client must declare `requires static java.desktop`, not a mandatory requires");
            System.exit(EXIT_DESKTOP_REACHABLE);
        } catch (ClassNotFoundException expected) {
            // desktop-free, as intended - the module resolved without java.desktop
        }

        // The promise DeviceCodePrompt.openBrowser() documents: the browser open is best-effort and
        // "skipped on a runtime without the java.desktop module", never fatal. Reaching BrowserLauncher
        // throws a LinkageError here, which openBrowser() swallows, leaving the printed URL and code.
        DeviceCodePrompt.openBrowser().promptUser(new DeviceAuthorizationChallenge(
                "WDJB-MJHT", "https://verify.example/device", null, 300, 5));

        System.out.println(SUCCESS_MARKER);
    }
}
