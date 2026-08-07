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

package io.questdb.client.test.std;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Guards the packaged jar against the 1.3.5-1.3.7 release regressions, which
 * unit tests cannot see because they run against target/classes where neither
 * the manifest nor Multi-Release versioned-class selection exists:
 * <ul>
 * <li>the module name silently changing from {@code io.questdb.client} to the
 * filename-derived {@code questdb.client} (no module-info.class on JDK 8
 * builds and no Automatic-Module-Name),</li>
 * <li>JDK 8 builds shipping only the {@code sun.misc.FDBigInteger} FdBig
 * bridge, which throws {@code NoClassDefFoundError} on Java 9+ without the
 * {@code META-INF/versions/11} counterpart.</li>
 * </ul>
 */
public class JarPackagingIT {
    private static final String FD_BIG_ENTRY = "io/questdb/client/std/FdBig.class";
    private static final String VERSIONED_FD_BIG_ENTRY = "META-INF/versions/11/" + FD_BIG_ENTRY;

    @Test
    public void testDoubleFormattingAgainstPackagedJar() throws Exception {
        runSmokeAgainstJar(System.getProperty("java.home"));
        if ("1.8".equals(System.getProperty("java.specification.version"))) {
            // On a JDK 8 build the run above only exercised the root sun.misc
            // classes. Run again on the JDK 11+ that compiled the versioned
            // bridge, so every packaging build -- including the release verify
            // gate -- EXECUTES the META-INF/versions/11 classes instead of only
            // checking they exist. Required, not skipped-if-absent: packaging
            // already failed earlier without a JDK 11 (see the antrun step).
            String bridgeJdkHome = System.getProperty("java11.home");
            if (bridgeJdkHome == null || bridgeJdkHome.isEmpty() || bridgeJdkHome.startsWith("${")) {
                bridgeJdkHome = System.getenv("JAVA11_HOME");
            }
            Assert.assertNotNull("JAVA11_HOME (or -Djava11.home) must point at a JDK 11+", bridgeJdkHome);
            runSmokeAgainstJar(bridgeJdkHome);
        }
    }

    private static void runSmokeAgainstJar(String jdkHome) throws Exception {
        String javaBin = jdkHome + File.separator + "bin" + File.separator + "java";
        String classpath = jarPath() + File.pathSeparator + System.getProperty("questdb.client.test.classes");
        Process process = new ProcessBuilder(javaBin, "-cp", classpath, DoubleFormatSmoke.class.getName())
                .redirectErrorStream(true)
                .start();
        String output = readFully(process.getInputStream());
        int exitCode = process.waitFor();
        Assert.assertEquals("double formatting against the packaged jar failed on " + javaBin + ":\n" + output, 0, exitCode);
    }

    @Test
    public void testJarLayout() throws Exception {
        try (JarFile jar = new JarFile(jarPath())) {
            Attributes attrs = jar.getManifest().getMainAttributes();
            Assert.assertEquals("io.questdb.client", attrs.getValue("Automatic-Module-Name"));
            Assert.assertEquals("true", attrs.getValue("Multi-Release"));

            if ("1.8".equals(System.getProperty("java.specification.version"))) {
                // JDK 8 build: the shipping layout. Root classes target Java 8,
                // the java11 bridge rides in META-INF/versions/11.
                Assert.assertTrue(
                        "root FdBig of a JDK 8 build must use sun.misc.FDBigInteger",
                        classReferences(jar, FD_BIG_ENTRY, "sun/misc/FDBigInteger")
                );
                Assert.assertTrue(
                        "META-INF/versions/11 FdBig must use jdk.internal.math.FDBigInteger",
                        classReferences(jar, VERSIONED_FD_BIG_ENTRY, "jdk/internal/math/FDBigInteger")
                );
                Assert.assertNotNull(
                        "META-INF/versions/11 must carry the java11 Compat shim",
                        jar.getEntry("META-INF/versions/11/io/questdb/client/std/Compat.class")
                );
                // every source in src/main/java11 must ship in versions/11 -- a file added
                // to the source root but missed by the packaging step would recreate the
                // NoClassDefFoundError class of bug on Java 9+
                File java11SrcRoot = new File(System.getProperty("questdb.client.java11.src"));
                Assert.assertTrue("src/main/java11 not found: " + java11SrcRoot, java11SrcRoot.isDirectory());
                for (String relativeSource : collectJavaSources(java11SrcRoot, "")) {
                    String entry = "META-INF/versions/11/" + relativeSource.replaceAll("\\.java$", ".class");
                    Assert.assertNotNull("src/main/java11/" + relativeSource + " has no packaged counterpart " + entry, jar.getEntry(entry));
                }
            } else {
                // JDK 11+ build: dev/smoke only, never shipped. Root classes are the
                // java11 variants and the real module descriptor is present.
                Assert.assertTrue(
                        "root FdBig of a JDK 11+ build must use jdk.internal.math.FDBigInteger",
                        classReferences(jar, FD_BIG_ENTRY, "jdk/internal/math/FDBigInteger")
                );
                Assert.assertNotNull("module-info.class missing", jar.getEntry("module-info.class"));
            }
        }
    }

    private static java.util.List<String> collectJavaSources(File dir, String prefix) {
        java.util.List<String> result = new java.util.ArrayList<String>();
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    result.addAll(collectJavaSources(file, prefix + file.getName() + "/"));
                } else if (file.getName().endsWith(".java")) {
                    result.add(prefix + file.getName());
                }
            }
        }
        return result;
    }

    private static boolean classReferences(JarFile jar, String entryName, String constant) throws IOException {
        JarEntry entry = jar.getJarEntry(entryName);
        Assert.assertNotNull("jar entry missing: " + entryName, entry);
        byte[] classBytes;
        try (InputStream in = jar.getInputStream(entry)) {
            classBytes = readAll(in);
        }
        // the referenced class name appears verbatim as a constant-pool UTF-8 entry
        byte[] needle = constant.getBytes("UTF-8");
        for (int i = 0; i <= classBytes.length - needle.length; i++) {
            int j = 0;
            while (j < needle.length && classBytes[i + j] == needle[j]) {
                j++;
            }
            if (j == needle.length) {
                return true;
            }
        }
        return false;
    }

    private static String jarPath() {
        String path = System.getProperty("questdb.client.jar");
        Assert.assertNotNull("questdb.client.jar system property not set", path);
        Assert.assertTrue("packaged jar not found: " + path, new File(path).exists());
        return path;
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int n;
        while ((n = in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        return out.toByteArray();
    }

    private static String readFully(InputStream in) throws IOException {
        return new String(readAll(in), "UTF-8");
    }
}
