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

package io.questdb.client.test.network;

import io.questdb.client.network.TlsTrustStore;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class TlsTrustStoreTest {

    private static final String RESOURCE_PREFIX = "/io/questdb/client/test/network/";

    @Test
    public void testJksTrustStoreStillSupported() throws Exception {
        X509Certificate rootCertificate = loadCertificate("server-rootCA.pem");
        Path trustStorePath = Files.createTempFile("questdb-tls-roots", ".jks");
        char[] password = "questdb".toCharArray();
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, null);
            keyStore.setCertificateEntry("test-root", rootCertificate);
            try (OutputStream output = Files.newOutputStream(trustStorePath)) {
                keyStore.store(output, password);
            }

            X509TrustManager trustManager = x509TrustManager(
                    TlsTrustStore.load(
                            trustStorePath.toString(),
                            password,
                            TlsTrustStoreTest.class
                    )
            );
            trustManager.checkServerTrusted(
                    new X509Certificate[]{loadCertificate("server.crt")},
                    "RSA"
            );
            try {
                TlsTrustStore.load(
                        trustStorePath.toString(),
                        null,
                        TlsTrustStoreTest.class
                );
                Assert.fail("expected a JKS file without its password to be rejected as invalid PEM");
            } catch (CertificateException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains(
                                "if this is a JKS or PKCS#12 trust store, set tls_roots_password"
                        )
                );
            }
        } finally {
            Files.deleteIfExists(trustStorePath);
        }
    }

    @Test
    public void testPemBundleLoadsEveryCertificate() throws Exception {
        byte[] root = Files.readAllBytes(resourcePath("server-rootCA.pem"));
        byte[] server = Files.readAllBytes(resourcePath("server.crt"));
        byte[] bundle = new byte[root.length + server.length];
        System.arraycopy(root, 0, bundle, 0, root.length);
        System.arraycopy(server, 0, bundle, root.length, server.length);

        Path bundlePath = Files.createTempFile("questdb-tls-roots", ".pem");
        try {
            Files.write(bundlePath, bundle);
            X509TrustManager trustManager = x509TrustManager(
                    TlsTrustStore.load(
                            bundlePath.toString(),
                            null,
                            TlsTrustStoreTest.class
                    )
            );
            Assert.assertEquals(2, trustManager.getAcceptedIssuers().length);
        } finally {
            Files.deleteIfExists(bundlePath);
        }
    }

    @Test
    public void testPemClasspathRootTrustsServerCertificate() throws Exception {
        X509TrustManager trustManager = x509TrustManager(
                TlsTrustStore.load(
                        "classpath:" + RESOURCE_PREFIX + "server-rootCA.pem",
                        null,
                        TlsTrustStoreTest.class
                )
        );
        trustManager.checkServerTrusted(
                new X509Certificate[]{loadCertificate("server.crt")},
                "RSA"
        );
    }

    @Test
    public void testPemWithoutCertificatesIsRejected() throws Exception {
        Path emptyPem = Files.createTempFile("questdb-empty-tls-roots", ".pem");
        try {
            try {
                TlsTrustStore.load(
                        emptyPem.toString(),
                        null,
                        TlsTrustStoreTest.class
                );
                Assert.fail("expected an empty PEM file to be rejected");
            } catch (CertificateException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no X.509 certificates"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(emptyPem.toString()));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("set tls_roots_password"));
            }
        } finally {
            Files.deleteIfExists(emptyPem);
        }
    }

    @Test
    public void testPkcs12TrustStoreStillSupported() throws Exception {
        X509Certificate rootCertificate = loadCertificate("server-rootCA.pem");
        Path trustStorePath = Files.createTempFile("questdb-tls-roots", ".p12");
        char[] password = "questdb".toCharArray();
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, null);
            keyStore.setCertificateEntry("test-root", rootCertificate);
            try (OutputStream output = Files.newOutputStream(trustStorePath)) {
                keyStore.store(output, password);
            }

            X509TrustManager trustManager = x509TrustManager(
                    TlsTrustStore.load(
                            trustStorePath.toString(),
                            password,
                            TlsTrustStoreTest.class
                    )
            );
            trustManager.checkServerTrusted(
                    new X509Certificate[]{loadCertificate("server.crt")},
                    "RSA"
            );
        } finally {
            Files.deleteIfExists(trustStorePath);
        }
    }

    private static X509Certificate loadCertificate(String resourceName) throws Exception {
        try (InputStream input = TlsTrustStoreTest.class.getResourceAsStream(RESOURCE_PREFIX + resourceName)) {
            Assert.assertNotNull("missing test certificate: " + resourceName, input);
            return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
        }
    }

    private static Path resourcePath(String resourceName) throws Exception {
        return Paths.get(TlsTrustStoreTest.class.getResource(RESOURCE_PREFIX + resourceName).toURI());
    }

    private static X509TrustManager x509TrustManager(TrustManager[] trustManagers) {
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }
        throw new AssertionError("no X509TrustManager was created");
    }
}
