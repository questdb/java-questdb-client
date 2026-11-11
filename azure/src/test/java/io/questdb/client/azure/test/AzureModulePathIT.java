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

package io.questdb.client.azure.test;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.stream.Stream;

/**
 * Executes the packaged jars, not target/classes: the JDK 8 release needs both the core's service-uses
 * descriptor and the adapter's SDK-dependency descriptor in META-INF/versions/11. The consumer requires only
 * io.questdb.client; no --add-modules flag may supply the missing service binding or SDK resolution.
 */
public class AzureModulePathIT {
    @Test
    public void testJarLayout() throws Exception {
        try (JarFile jar = new JarFile(property("questdb.client.azure.jar"))) {
            Assert.assertEquals("io.questdb.client.azure", jar.getManifest().getMainAttributes().getValue("Automatic-Module-Name"));
            Assert.assertEquals("true", jar.getManifest().getMainAttributes().getValue("Multi-Release"));
            Assert.assertNotNull("classpath service registration missing",
                    jar.getEntry("META-INF/services/io.questdb.client.cutlass.auth.TokenProviderFactory"));
            String descriptor = "1.8".equals(System.getProperty("java.specification.version"))
                    ? "META-INF/versions/11/module-info.class" : "module-info.class";
            Assert.assertNotNull("packaged module descriptor missing: " + descriptor, jar.getEntry(descriptor));
        }
    }

    @Test(timeout = 120_000)
    public void testPackagedModuleDiscovery() throws Exception {
        Set<String> jdks = new LinkedHashSet<>();
        if ("1.8".equals(System.getProperty("java.specification.version"))) {
            String home = System.getProperty("java11.home");
            if (home == null || home.isEmpty() || home.startsWith("${")) {
                home = System.getenv("JAVA11_HOME");
            }
            Assert.assertNotNull("JDK 8 packaging needs JAVA11_HOME for the module-path smoke", home);
            jdks.add(home);
        } else {
            jdks.add(System.getProperty("java.home"));
        }
        String extraJdks = System.getenv("QUESTDB_SMOKE_JDKS");
        if (extraJdks != null && !extraJdks.trim().isEmpty()) {
            for (String home : extraJdks.split(File.pathSeparator)) {
                if (!home.trim().isEmpty()) {
                    jdks.add(home.trim());
                }
            }
        }
        for (String jdk : jdks) {
            runSmoke(jdk);
        }
    }

    private static String property(String name) {
        String value = System.getProperty(name);
        Assert.assertNotNull(name + " is required; run this test through mvn verify", value);
        Assert.assertTrue(name + " not found: " + value, new File(value).exists());
        return value;
    }

    private static String run(String jdk, String tool, Path workspace, List<String> args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(jdk + File.separator + "bin" + File.separator + tool);
        command.addAll(args);
        Path outputFile = workspace.resolve(tool + ".log");
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(outputFile.toFile());
        Map<String, String> env = builder.environment();
        env.keySet().removeIf(key -> key.startsWith("AZURE_") || key.startsWith("IDENTITY_")
                || key.startsWith("MSI_") || key.startsWith("IMDS_"));
        Process process = builder.start();
        try {
            Assert.assertTrue("module smoke timed out: " + command, process.waitFor(45, TimeUnit.SECONDS));
            String output = new String(Files.readAllBytes(outputFile), StandardCharsets.UTF_8);
            Assert.assertEquals("module smoke failed: " + command + "\n" + output, 0, process.exitValue());
            return output;
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private static void runSmoke(String jdk) throws Exception {
        Path workspace = Files.createTempDirectory("qdb-azure-module-path");
        try {
            String source = property("questdb.client.azure.module.src");
            String modulePath = property("questdb.client.azure.jar") + File.pathSeparator
                    + property("questdb.client.azure.module.deps");
            Path modules = Files.createDirectory(workspace.resolve("modules"));
            run(jdk, "javac", workspace, Arrays.asList("--release", "11", "--module-path", modulePath,
                    "-d", modules.toString(), source + File.separator + "module-info.java", source + File.separator + "Main.java"));
            String output = run(jdk, "java", workspace, Arrays.asList("--module-path",
                    modulePath + File.pathSeparator + modules, "-m", "qdb.azure.smoke/smoke.Main"));
            Assert.assertTrue(output, output.contains("azure-module-path-ok"));
            System.out.println("Azure packaged module-path smoke passed on " + jdk);
        } finally {
            try (Stream<Path> paths = Files.walk(workspace)) {
                Iterator<Path> iterator = paths.sorted(Comparator.reverseOrder()).iterator();
                while (iterator.hasNext()) {
                    Files.deleteIfExists(iterator.next());
                }
            }
        }
    }
}
