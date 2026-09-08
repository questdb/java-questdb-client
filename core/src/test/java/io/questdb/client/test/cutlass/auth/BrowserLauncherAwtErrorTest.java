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

import io.questdb.client.cutlass.auth.DeviceCodePrompt;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.security.CodeSource;

/**
 * Pins that a desktop stack which cannot initialise degrades to "no browser" instead of failing sign-in.
 * <p>
 * {@code BrowserLauncher.open()} caught {@code Exception} and {@code DeviceCodePrompt.openBrowser()}
 * catches {@code LinkageError}. {@link java.awt.AWTError} is neither: it extends {@code Error} directly, so
 * it passed through both and escaped {@code signIn()} - aborting a sign-in the human could have completed
 * from the URL already printed, as a type the caller's documented {@code catch (OidcAuthException)} does not
 * handle. {@code Toolkit} raises it whenever {@code assistive_technologies} names a class the runtime cannot
 * load, which is the stock configuration on several Linux distributions, and again when a set {@code DISPLAY}
 * points at no X server.
 * <p>
 * Two child processes rather than one, because the failure is one-shot - see {@link AwtErrorPromptMain}. The
 * probe runs first and its assertion is what keeps the other half honest: without it a prompt that returned
 * because the toolkit was FINE would read exactly like one that returned because the guard caught the error.
 * It is also what keeps this test from opening a real browser on a developer's machine - the launch is only
 * ever reached in a JVM the probe has already shown cannot get past {@code Desktop.isDesktopSupported()}.
 * <p>
 * Java 8 source level, like the rest of this test tree.
 */
public class BrowserLauncherAwtErrorTest {

    /**
     * A class no runtime can load. The real-world value here is {@code org.GNOME.Accessibility.AtkWrapper},
     * shipped in {@code accessibility.properties} by several Linux distributions without the package that
     * provides it; any absent name reaches the same {@code AWTError}.
     */
    private static final String MISSING_AT_CLASS = "io.questdb.client.test.NoSuchAssistiveTechnology";

    @Test(timeout = 60_000)
    public void testAnUninitialisableToolkitDegradesInsteadOfFailingSignIn() throws Exception {
        // The precondition, asserted BEFORE the launch half is ever forked: this JVM configuration must
        // genuinely raise an AWTError, or the assertion below proves nothing and the launch could reach a
        // real browser.
        String probe = runChild(AwtErrorPromptMain.MODE_PROBE);
        Assert.assertTrue("the assistive-technology property did not break the toolkit, so nothing below "
                + "would be exercised:\n" + probe, probe.contains(AwtErrorPromptMain.PROBE_MARKER));
        // and it must be the shape that defeats both guards by category - an Error that is not a
        // LinkageError. A JDK that started raising, say, a HeadlessException here would make this test pass
        // for the wrong reason, since the pre-existing catch (Exception) already handled that.
        Assert.assertTrue("the toolkit failure must be an Error that is neither an Exception nor a "
                + "LinkageError, or it was already caught before this fix:\n" + probe,
                probe.contains("isLinkageError=false isException=false"));

        String prompt = runChild(AwtErrorPromptMain.MODE_PROMPT);
        Assert.assertTrue("the default prompt did not survive an AWTError from the browser launch:\n"
                + prompt, prompt.contains(AwtErrorPromptMain.DEGRADED_MARKER));
        // degrading to "no browser" must not degrade to "no instructions": a user whose browser could not
        // be opened still needs the URL and the code
        Assert.assertTrue("the verification URL must still be printed:\n" + prompt,
                prompt.contains("https://verify.example/device"));
        Assert.assertTrue("the user code must still be printed:\n" + prompt, prompt.contains("WDJB-MJHT"));
    }

    private static String locationOf(Class<?> type) {
        CodeSource source = type.getProtectionDomain().getCodeSource();
        Assert.assertNotNull("no code source for " + type.getName(), source);
        return new File(source.getLocation().getPath()).getPath();
    }

    private static String readFully(InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toString("UTF-8");
    }

    /**
     * Runs {@link AwtErrorPromptMain} in a second JVM whose AWT toolkit cannot initialise, and returns its
     * merged stdout/stderr. Asserts a clean exit, so a child that failed its own precondition reports
     * through its printed reason rather than through a silent skip.
     */
    private static String runChild(String mode) throws Exception {
        // The CLASS path, deliberately, not the module path this suite itself runs on: io.questdb.test does
        // not read java.desktop, so a child in that module could not touch Desktop at all and would prove
        // nothing. On the class path everything lands in the unnamed module, which reads every resolved
        // module, and java.desktop resolves by default there.
        String classPath = locationOf(AwtErrorPromptMain.class)     // the child and this test
                + File.pathSeparator + locationOf(DeviceCodePrompt.class)  // the client under test
                + File.pathSeparator + locationOf(Logger.class);    // org.slf4j, a mandatory client requires
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                // headless=false explicitly, not merely by default: headless mode short-circuits the
                // assistive-technology loading altogether, so a CI runner that forces headless through
                // JAVA_TOOL_OPTIONS would otherwise turn this into a silent no-op. It is also what makes the
                // failure deterministic on a machine with no display, where the toolkit instead raises its
                // "cannot connect to the X11 window server" AWTError - the same category, the same guard.
                "-Djava.awt.headless=false",
                "-Djavax.accessibility.assistive_technologies=" + MISSING_AT_CLASS,
                // deliberately NOT setting questdb.client.oidc.open.browser: the kill-switch returns before
                // BrowserLauncher touches java.awt.Desktop, so a run with it set would never reach the error
                // this test exercises
                "-classpath", classPath,
                AwtErrorPromptMain.class.getName(),
                mode);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        String output = readFully(process.getInputStream());
        int exitCode = process.waitFor();
        Assert.assertEquals("the " + mode + " run of " + AwtErrorPromptMain.class.getSimpleName()
                + " failed:\n" + output, 0, exitCode);
        return output;
    }
}
