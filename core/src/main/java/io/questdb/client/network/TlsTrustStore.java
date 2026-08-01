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

package io.questdb.client.network;

import io.questdb.client.cutlass.line.LineSenderException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;

/**
 * Internal factory for client-side TLS: loads custom TLS roots and builds
 * the {@link SSLEngine} used by both the ILP TCP channel and the WS socket.
 * <p>
 * A {@code null} trust store password selects a PEM certificate bundle. A
 * non-null password retains the historical Java KeyStore behaviour.
 */
public final class TlsTrustStore {

    private static final TrustManager[] BLIND_TRUST_MANAGERS = new TrustManager[]{new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] certs, String t) {
        }

        public void checkServerTrusted(X509Certificate[] certs, String t) {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    }};

    private TlsTrustStore() {
    }

    public static SSLEngine createSslEngine(
            String trustStorePath,
            char[] trustStorePassword,
            boolean insecure,
            String peerHost,
            Class<?> resourceAnchor
    ) throws CertificateException, IOException, KeyManagementException, KeyStoreException, NoSuchAlgorithmException {
        if (trustStorePath != null && insecure) {
            throw new IllegalArgumentException("custom trust store cannot be combined with disabled TLS validation");
        }
        SSLContext sslContext;
        if (trustStorePath != null) {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, load(trustStorePath, trustStorePassword, resourceAnchor), new SecureRandom());
        } else if (insecure) {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, BLIND_TRUST_MANAGERS, new SecureRandom());
        } else {
            sslContext = SSLContext.getDefault();
        }

        // SSLEngine needs to know hostname during TLS handshake to validate a server certificate was issued
        // for the server we are connecting to. For details see the comment below.
        // Hostname validation does not use port at all hence we can get away with a dummy value -1
        SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, -1);
        if (!insecure) {
            SSLParameters sslParameters = sslEngine.getSSLParameters();
            // The https validation algorithm? That looks confusing! After all we are not using any
            // https here at so what does it mean?
            // It's actually simple: It just instructs the SSLEngine to perform the same hostname validation
            // as it does during HTTPS connections. SSLEngine does not do hostname validation by default. Without
            // this option SSLEngine would happily accept any certificate as long as it's signed by a trusted CA.
            // This option will make sure certificates are accepted only if they were issued for the
            // server we are connecting to.
            sslParameters.setEndpointIdentificationAlgorithm("https");
            sslEngine.setSSLParameters(sslParameters);
        }
        sslEngine.setUseClientMode(true);
        return sslEngine;
    }

    public static TrustManager[] load(
            String trustStorePath,
            char[] trustStorePassword,
            Class<?> resourceAnchor
    ) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        KeyStore trustStore = trustStorePassword == null
                ? loadPem(trustStorePath, resourceAnchor)
                : loadJavaKeyStore(trustStorePath, trustStorePassword, resourceAnchor);
        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        return trustManagerFactory.getTrustManagers();
    }

    private static KeyStore loadJavaKeyStore(
            String trustStorePath,
            char[] trustStorePassword,
            Class<?> resourceAnchor
    ) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        try (BufferedInputStream trustStoreStream =
                     new BufferedInputStream(open(trustStorePath, resourceAnchor))) {
            trustStoreStream.mark(1);
            int firstByte = trustStoreStream.read();
            trustStoreStream.reset();
            String keyStoreType = firstByte == 0x30 ? "PKCS12" : "JKS";
            KeyStore trustStore = KeyStore.getInstance(keyStoreType);
            trustStore.load(trustStoreStream, trustStorePassword);
            return trustStore;
        }
    }

    private static KeyStore loadPem(
            String trustStorePath,
            Class<?> resourceAnchor
    ) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Collection<? extends Certificate> certificates;
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        try (InputStream trustStoreStream = open(trustStorePath, resourceAnchor)) {
            try {
                certificates = certificateFactory.generateCertificates(trustStoreStream);
            } catch (CertificateException e) {
                throw new CertificateException(
                        "could not read PEM TLS roots [path=" + trustStorePath + "]; "
                                + "if this is a JKS or PKCS#12 trust store, set tls_roots_password",
                        e
                );
            }
        }
        if (certificates.isEmpty()) {
            throw new CertificateException(
                    "no X.509 certificates found in PEM TLS roots [path=" + trustStorePath + "]; "
                            + "if this is a JKS or PKCS#12 trust store, set tls_roots_password"
            );
        }

        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        int certificateIndex = 0;
        for (Certificate certificate : certificates) {
            trustStore.setCertificateEntry("pem-certificate-" + certificateIndex++, certificate);
        }
        return trustStore;
    }

    private static InputStream open(String trustStorePath, Class<?> resourceAnchor) throws FileNotFoundException {
        if (trustStorePath.startsWith("classpath:")) {
            String adjustedPath = trustStorePath.substring("classpath:".length());
            InputStream trustStoreStream = resourceAnchor.getResourceAsStream(adjustedPath);
            if (trustStoreStream == null) {
                throw new LineSenderException("configured trust store is unavailable ")
                        .put("[path=").put(trustStorePath).put("]");
            }
            return trustStoreStream;
        }
        return new FileInputStream(trustStorePath);
    }
}
