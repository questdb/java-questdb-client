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

import java.lang.reflect.InvocationTargetException;

/**
 * The child half of {@link BrowserLauncherAwtErrorTest}: run by that test in a second JVM whose AWT
 * toolkit cannot initialise, so the browser launch throws a {@code java.awt.AWTError}.
 * <p>
 * Two modes, run as two separate processes because the failure is ONE-SHOT: {@code Toolkit}'s assistive
 * technology loading throws on the first {@code getDefaultToolkit()} and then completes, so a second call
 * in the same JVM succeeds. A probe and the real call therefore cannot share a process - the probe would
 * consume the only throw and leave the real call asserting nothing.
 * <p>
 * The probe reaches {@code Desktop} reflectively for the same reason {@code DesktopFreePromptMain} does:
 * this test tree compiles as module {@code io.questdb.test}, which does not read {@code java.desktop}, so a
 * direct reference would not compile. At run time the child is on the class path, where it resolves.
 * <p>
 * Deliberately Java 8 source level, like the rest of this test tree.
 */
public final class AwtErrorPromptMain {

    /**
     * Printed by {@link #MODE_PROMPT} once the default prompt returned despite the broken toolkit.
     */
    static final String DEGRADED_MARKER = "AWT-ERROR-DEGRADED-OK";
    /**
     * Exit code for "the precondition did not hold": the toolkit initialised fine, so this JVM is not the
     * hostile one the test needs and nothing it observes would prove anything.
     */
    static final int EXIT_NO_AWT_ERROR = 2;
    static final String MODE_PROBE = "probe";
    static final String MODE_PROMPT = "prompt";
    /**
     * Printed by {@link #MODE_PROBE} once it has seen the {@code AWTError} the other mode relies on.
     */
    static final String PROBE_MARKER = "AWT-ERROR-OBSERVED";

    private AwtErrorPromptMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: " + AwtErrorPromptMain.class.getName()
                    + " <" + MODE_PROBE + '|' + MODE_PROMPT + '>');
            System.exit(EXIT_NO_AWT_ERROR);
            return;
        }
        if (MODE_PROBE.equals(args[0])) {
            probe();
        } else {
            prompt();
        }
    }

    /**
     * Proves this JVM's desktop stack is genuinely broken, so the sibling process's quiet return is a
     * DEGRADE rather than a no-op. Reports the throwable's type as well, since the whole point is that it
     * is an {@code AWTError} - neither an {@code Exception} nor a {@code LinkageError}, so neither of the
     * two guards on the launch path catches it by category.
     */
    private static void probe() throws Exception {
        Throwable raised = null;
        try {
            Class.forName("java.awt.Desktop").getMethod("isDesktopSupported").invoke(null);
        } catch (InvocationTargetException e) {
            // reflection wraps whatever the toolkit raised; the cause is the throwable the launch path
            // would have met directly
            raised = e.getCause();
        }
        if (raised == null) {
            System.out.println("the AWT toolkit initialised, so this JVM is not the hostile one this test "
                    + "needs: check that java.awt.headless is off and the assistive-technology property is set");
            System.exit(EXIT_NO_AWT_ERROR);
            return;
        }
        System.out.println(PROBE_MARKER + ' ' + raised.getClass().getName()
                + " isLinkageError=" + (raised instanceof LinkageError)
                + " isException=" + (raised instanceof Exception));
    }

    /**
     * The real path, run FIRST in this process so it meets the one-shot {@code AWTError} rather than the
     * clean toolkit a probe would have left behind. The promise is that the browser launch is best-effort
     * and never fatal, so the prompt must render the challenge and return.
     */
    private static void prompt() {
        DeviceCodePrompt.openBrowser().promptUser(new DeviceAuthorizationChallenge(
                "WDJB-MJHT", "https://verify.example/device", null, 300, 5));
        System.out.println(DEGRADED_MARKER);
    }
}
