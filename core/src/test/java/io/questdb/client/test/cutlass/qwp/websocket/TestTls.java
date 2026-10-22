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

package io.questdb.client.test.cutlass.qwp.websocket;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

/**
 * A self-signed server identity for TLS test servers, generated once per JVM with the running JDK's
 * {@code keytool} - no key material is committed to the repository. Clients connect with
 * {@code tls_verify=unsafe_off}.
 */
public final class TestTls {
    private static final char[] PASSWORD = "test-only".toCharArray();
    private static SSLServerSocketFactory factory;

    private TestTls() {
    }

    public static synchronized SSLServerSocketFactory serverSocketFactory() throws IOException {
        if (factory == null) {
            factory = create();
        }
        return factory;
    }

    static void waitForKeytool(Process process, Path output, long timeout, TimeUnit unit) throws IOException {
        try {
            if (!process.waitFor(timeout, unit)) {
                throw new IOException("keytool timed out");
            }
            if (process.exitValue() != 0) {
                throw new IOException("keytool failed: " + new String(Files.readAllBytes(output), StandardCharsets.UTF_8));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while running keytool", e);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                // Reap the child even when the caller interrupted the wait, then restore its interrupt flag.
                boolean isInterrupted = Thread.interrupted();
                try {
                    process.waitFor(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    isInterrupted = true;
                } finally {
                    if (isInterrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private static SSLServerSocketFactory create() throws IOException {
        File keystore = File.createTempFile("qdb-test-server", ".p12");
        if (!keystore.delete()) {
            throw new IOException("could not prepare " + keystore);
        }
        keystore.deleteOnExit();
        String keytool = System.getProperty("java.home") + File.separator + "bin" + File.separator + "keytool";
        ProcessBuilder pb = new ProcessBuilder(
                keytool, "-genkeypair",
                "-alias", "server",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "3650",
                "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-storetype", "PKCS12",
                "-keystore", keystore.getAbsolutePath(),
                "-storepass", new String(PASSWORD),
                "-keypass", new String(PASSWORD)
        );
        Path output = Files.createTempFile("qdb-test-keytool", ".log");
        try {
            Process process = pb.redirectErrorStream(true).redirectOutput(output.toFile()).start();
            waitForKeytool(process, output, 60, TimeUnit.SECONDS);
        } finally {
            Files.deleteIfExists(output);
        }
        try (InputStream in = new FileInputStream(keystore)) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(in, PASSWORD);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, PASSWORD);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(kmf.getKeyManagers(), null, null);
            return context.getServerSocketFactory();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("could not load the test keystore", e);
        }
    }
}
