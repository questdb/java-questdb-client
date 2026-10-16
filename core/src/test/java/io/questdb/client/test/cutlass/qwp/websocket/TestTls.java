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
        pb.redirectErrorStream(true);
        Process process = pb.start();
        byte[] output = readAll(process.getInputStream());
        try {
            if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new IOException("keytool failed: " + new String(output, StandardCharsets.UTF_8));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while running keytool", e);
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

    private static byte[] readAll(InputStream in) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }
}
