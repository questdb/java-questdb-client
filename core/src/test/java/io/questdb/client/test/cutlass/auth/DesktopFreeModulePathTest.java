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
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.security.CodeSource;

/**
 * Guards the promise {@link DeviceCodePrompt#openBrowser()} makes - that the browser launch is skipped
 * "on a runtime without the {@code java.desktop} module" and never prevents sign-in - for the one
 * configuration where the promise used to be unkeepable: a build of this source used as an EXPLICIT
 * module.
 * <p>
 * {@code module-info.java} declared a mandatory {@code requires java.desktop}. Mandatory requires are
 * satisfied during module RESOLUTION, before a single line of client code runs, so on a runtime image
 * without {@code java.desktop} the JVM failed at startup with {@code FindException} and the
 * {@code LinkageError} catch in {@code openBrowser()} - the thing that implements the promise - never got
 * to run. The published artifact is built on JDK 8 and carries no descriptor, so it is an automatic module
 * and was never affected; a build from this source is.
 * <p>
 * The test runs a second JVM with the client on the MODULE path and {@code --limit-modules} naming only
 * the client, which limits the universe to the client plus the closure of its mandatory requires. A
 * {@code requires static} is not part of that closure, so {@code java.desktop} is absent and the child
 * proves three things at once: the module resolved without it, {@code java.awt.Desktop} really is
 * unreachable (see {@link DesktopFreePromptMain}, which fails the run if it is not - this is what catches
 * a revert to a mandatory requires, since the closure would then drag {@code java.desktop} back in), and
 * the default prompt still renders the challenge and returns.
 * <p>
 * Java 8 source level, and no {@code java.lang.module} API: the JDK 8 release profile compiles this test
 * tree, only excluding {@code module-info.java}.
 */
public class DesktopFreeModulePathTest {

    @Test(timeout = 60_000)
    public void testTheBrowserKillSwitchIsHonouredByOpenItself() throws Exception {
        // BrowserLauncherTest can only assert that isBrowserOpenEnabled() reads the property: a browser
        // launch is unobservable, and on a headless JVM open() is a no-op whether or not it ever consulted
        // the flag, so that test passes even against an open() that ignores it. Without java.desktop the two
        // become distinguishable - see DesktopFreeKillSwitchMain, which drives both directions.
        String output = runInDesktopFreeJvm(DesktopFreeKillSwitchMain.class);
        Assert.assertTrue("the kill-switch is not what stopped the browser launch:\n" + output,
                output.contains(DesktopFreeKillSwitchMain.SUCCESS_MARKER));
    }

    @Test(timeout = 60_000)
    public void testTheModuleResolvesAndPromptsWithoutJavaDesktop() throws Exception {
        String output = runInDesktopFreeJvm(DesktopFreePromptMain.class);
        Assert.assertTrue("the child never reached the end of the prompt:\n" + output,
                output.contains(DesktopFreePromptMain.SUCCESS_MARKER));
        // the challenge itself must still have been shown - degrading to "no browser" must not degrade to
        // "no instructions", which would leave a user with no way to sign in at all
        Assert.assertTrue("the verification URL must still be printed:\n" + output,
                output.contains("https://verify.example/device"));
        Assert.assertTrue("the user code must still be printed:\n" + output, output.contains("WDJB-MJHT"));
    }

    private static File locationOf(Class<?> type) {
        CodeSource source = type.getProtectionDomain().getCodeSource();
        Assert.assertNotNull("no code source for " + type.getName(), source);
        return new File(source.getLocation().getPath());
    }

    /**
     * Runs {@code main} in a second JVM that has the client on the MODULE path and no {@code java.desktop},
     * and returns its merged stdout/stderr. Asserts a clean exit, so a child that failed its own
     * preconditions reports through its printed reason rather than through a silent skip.
     */
    private static String runInDesktopFreeJvm(Class<?> main) throws Exception {
        File clientLocation = locationOf(DeviceCodePrompt.class);
        Assume.assumeFalse("the module system arrived in Java 9; nothing to resolve on a Java 8 runtime",
                "1.8".equals(System.getProperty("java.specification.version")));
        // A JDK 8 build produces no module-info.class: the artifact is then an automatic module, which
        // reads every observable module and is exactly the configuration this defect never reached.
        Assume.assumeTrue("no module descriptor next to " + clientLocation + " (a JDK 8 build)",
                new File(clientLocation, "module-info.class").isFile());

        File slf4jLocation = locationOf(Logger.class); // org.slf4j is a mandatory requires of the client
        File testClasses = locationOf(DesktopFreeModulePathTest.class);
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                // Second net under each child's own desktop-free check, and independent of it: should
                // java.desktop ever be present, Desktop.isDesktopSupported() answers false in headless mode,
                // so nothing can reach a real browser on a developer's machine. It does not weaken either
                // test - headless changes what Desktop ANSWERS, not whether the class reference links.
                "-Djava.awt.headless=true",
                "--module-path", clientLocation.getPath() + File.pathSeparator + slf4jLocation.getPath(),
                // the universe: io.questdb.client and the closure of its MANDATORY requires, and nothing
                // else. This is what makes the child desktop-free - and what makes a mandatory
                // `requires java.desktop` visible, because the closure would then include it.
                "--limit-modules", "io.questdb.client",
                // the main class runs from the class path, so the client is not a root by default
                "--add-modules", "io.questdb.client",
                "-classpath", testClasses.getPath(),
                main.getName());
        // deliberately NOT setting questdb.client.oidc.open.browser here: the kill-switch returns before
        // BrowserLauncher touches java.awt.Desktop, so a run with it set would never reach the LinkageError
        // these tests exercise. What keeps a browser from opening is each child's own precondition - the
        // prompt and the launch run only in the arm that proved Desktop unreachable - plus the headless flag.
        pb.redirectErrorStream(true);
        Process process = pb.start();
        String output = readFully(process.getInputStream());
        int exitCode = process.waitFor();
        Assert.assertEquals("the desktop-free module-path run of " + main.getSimpleName() + " failed:\n"
                + output, 0, exitCode);
        return output;
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
}
