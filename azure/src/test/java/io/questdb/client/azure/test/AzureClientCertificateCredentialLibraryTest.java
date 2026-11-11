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

import com.azure.identity.ClientCertificateCredentialBuilder;
import io.questdb.client.Sender;
import io.questdb.client.azure.AzureTokenSource;
import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Exercises local certificate failures through the pinned Azure Identity credential without network I/O.
 */
public class AzureClientCertificateCredentialLibraryTest {
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 30_000)
    public void testMalformedCertificateIsPermanentWhenFetched() throws Exception {
        AzureTokenSource source = source(malformedCertificate());
        TokenUnavailableException e = expectFailure(source::fetchToken);
        Assert.assertFalse(e.getMessage(), e.isRetryable());
        Assert.assertEquals("credential certificate could not be parsed for api://qdb/.default", e.getMessage());
        Assert.assertEquals(TokenUnavailableException.NO_RETRY_AFTER, e.getRetryAfterMillis());
        Assert.assertNull("do not retain the SDK exception, file path or contents", e.getCause());
    }

    @Test(timeout = 30_000)
    public void testMissingCertificateKeepsItsDiagnostic() throws Exception {
        File certificate = new File(temp.newFolder(), "CERTIFICATE-PATH-SENTINEL.pem");
        TokenUnavailableException e = expectFailure(source(certificate)::fetchToken);
        Assert.assertFalse(e.getMessage(), e.isRetryable());
        Assert.assertEquals("credential file is missing for api://qdb/.default", e.getMessage());
        Assert.assertNull("do not retain the SDK exception or file path", e.getCause());
    }

    @Test(timeout = 30_000)
    public void testSyncStartupFailsFastOnMalformedCertificate() throws Exception {
        assertMemoryLeak(() -> {
            AzureTokenSource source = source(malformedCertificate());
            try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                    .coldWaitMillis(10_000).backoffInitialMillis(60_000).backoffMaxMillis(60_000).build()) {
                // Finish the first SDK attempt before startup, so the cold-wait deadline cannot hide its result.
                expectFailure(provider::getToken);
                TokenUnavailableException e = expectFailure(() -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("127.0.0.1:1")
                        .httpTokenProvider(provider)
                        .initialConnectMode(Sender.InitialConnectMode.SYNC)
                        .reconnectMaxDurationMillis(200)
                        .reconnectInitialBackoffMillis(10)
                        .reconnectMaxBackoffMillis(20)
                        .build()
                        .close());
                // Budget exhaustion throws LineSenderException instead, so this asserts the fail-fast path.
                Assert.assertFalse(e.getMessage(), e.isRetryable());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "credential certificate could not be parsed for api://qdb/.default"));
                Assert.assertFalse(provider.getLastFailure().isRetryable());
            }
        });
    }

    private static TokenUnavailableException expectFailure(Runnable action) {
        try {
            action.run();
            Assert.fail("expected the credential to fail");
            return null;
        } catch (TokenUnavailableException e) {
            return e;
        }
    }

    private static AzureTokenSource source(File certificate) {
        return new AzureTokenSource(new ClientCertificateCredentialBuilder()
                .tenantId("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
                .clientId("11111111-2222-3333-4444-555555555555")
                .pemCertificate(certificate.getAbsolutePath())
                .disableInstanceDiscovery()
                .httpClient(request -> {
                    throw new AssertionError("certificate parsing must fail before any HTTP request");
                })
                .build(), "api://qdb");
    }

    private File malformedCertificate() throws Exception {
        File certificate = temp.newFile("CERTIFICATE-PATH-SENTINEL.pem");
        Files.write(certificate.toPath(), "not a certificate: CERTIFICATE-CONTENTS-SENTINEL".getBytes(StandardCharsets.UTF_8));
        return certificate;
    }
}
