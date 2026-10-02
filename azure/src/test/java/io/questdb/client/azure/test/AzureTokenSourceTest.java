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

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.exception.ClientAuthenticationException;
import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.identity.AuthenticationRequiredException;
import com.azure.identity.CredentialUnavailableException;
import io.questdb.client.azure.AzureTokenProviderFactory;
import io.questdb.client.azure.AzureTokenSource;
import io.questdb.client.cutlass.auth.ExpiringToken;
import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenProviderRegistry;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.cutlass.auth.TokenSource;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@code azure} provider (design/qwp-token-provider-spec.md, section 7.5) against fake Azure Identity
 * credentials - no network: expiry and refresh-hint mapping, the requested scope, the classification of library
 * errors, and discovery through {@link java.util.ServiceLoader}.
 */
public class AzureTokenSourceTest {
    private static final OffsetDateTime EXPIRES = OffsetDateTime.of(2027, 1, 15, 12, 0, 0, 0, ZoneOffset.UTC);
    private static final OffsetDateTime REFRESH = EXPIRES.minusMinutes(30);

    @Test
    public void testAuthenticationFailuresEntraReportsArePermanent() {
        assertPermanent(new ClientAuthenticationException("bad request", new FakeResponse(400, null)), "HTTP 400");
        assertPermanent(new ClientAuthenticationException("unauthorized", new FakeResponse(401, null)), "HTTP 401");
        // DefaultAzureCredential often wraps MSAL without a response: recognize the Entra error code
        assertPermanent(new ClientAuthenticationException(
                "AADSTS700016: Application with identifier 'x' was not found in the directory", null, (Object) null),
                "AADSTS700016");
        assertPermanent(new RuntimeException("wrapper", new IllegalStateException(
                "AADSTS7000215: Invalid client secret provided.")), "AADSTS7000215");
    }

    @Test
    public void testConnectStringResolvesTheAzureFactory() {
        // C18 with the module present: the azure keys parse on both clients and nothing is fetched
        String cfg = "wss::addr=localhost:9000;token_provider=azure;azure_resource=api://qdb/.default;"
                + "azure_client_id=AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE;";
        TokenProviderSpec spec = TokenProviderSpec.parse(new ConfigView(ConfigString.parse(cfg)), true);
        Assert.assertNotNull(spec);
        Assert.assertTrue(spec.factory() instanceof AzureTokenProviderFactory);
        Assert.assertEquals("api://qdb", spec.params().get(TokenProviderSpec.KEY_AZURE_RESOURCE));
        Assert.assertEquals("azure[resource=api://qdb, client_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee]", spec.describe());
        QwpQueryClient.validateConfig(new ConfigView(ConfigString.parse(cfg)), true);
        try {
            TokenProviderSpec.parse(new ConfigView(ConfigString.parse("wss::addr=localhost:9000;token_provider=azure;")), true);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("requires azure_resource"));
        }
        try {
            TokenProviderSpec.parse(new ConfigView(ConfigString.parse("wss::addr=localhost:9000;token_provider=nope;")), true);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("supported values: [azure]"));
        }
    }

    @Test
    public void testCredentialUnavailableIsPermanent() {
        assertPermanent(new CredentialUnavailableException("EnvironmentCredential authentication unavailable"),
                "no credential available");
        assertPermanent(new AuthenticationRequiredException("interactive sign-in required", new TokenRequestContext()),
                "no credential available");
        assertPermanent(new RuntimeException("chain failed", new CredentialUnavailableException("none")),
                "no credential available");
    }

    @Test
    public void testExpiryAndRefreshHintAreMapped() {
        AzureTokenSource source = new AzureTokenSource(
                ctx -> Mono.just(new AccessToken("eyJ.token", EXPIRES, REFRESH)), "api://qdb");
        ExpiringToken token = source.fetchToken();
        Assert.assertEquals("eyJ.token", token.getToken());
        Assert.assertEquals(EXPIRES.toInstant().toEpochMilli(), token.getExpiresAtEpochMillis());
        Assert.assertEquals(REFRESH.toInstant().toEpochMilli(), token.getRefreshAtEpochMillis());

        AzureTokenSource noHint = new AzureTokenSource(ctx -> Mono.just(new AccessToken("t", EXPIRES)), "api://qdb");
        Assert.assertFalse(noHint.fetchToken().hasRefreshAt());
    }

    @Test
    public void testFactoryBuildsADefaultAzureCredentialSourceWithoutNetworkIo() {
        AzureTokenProviderFactory factory = new AzureTokenProviderFactory();
        Map<String, String> params = new HashMap<>();
        params.put(TokenProviderSpec.KEY_AZURE_RESOURCE, "api://qdb");
        params.put(TokenProviderSpec.KEY_AZURE_CLIENT_ID, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        TokenSource source = factory.createSource(params);
        Assert.assertTrue(source instanceof AzureTokenSource);
        Assert.assertEquals("api://qdb/.default", ((AzureTokenSource) source).getScope());
        Assert.assertTrue(source.toString(), source.toString().contains("DefaultAzureCredential"));
    }

    @Test
    public void testFactoryIsDiscoveredThroughServiceLoader() {
        Assert.assertTrue(TokenProviderRegistry.findFactory("azure") instanceof AzureTokenProviderFactory);
        List<String> supported = TokenProviderRegistry.supportedProviders();
        Assert.assertTrue(supported.toString(), supported.contains("azure"));
    }

    @Test
    public void testLibraryExceptionsAreNeverAttached() {
        // a library exception's response references the raw HTTP request, which can carry a client secret
        TokenUnavailableException e = fetchFailure(new HttpResponseException("throttled", new FakeResponse(429, "3")));
        Assert.assertNull(e.getCause());
    }

    @Test
    public void testOtherFailuresAreRetryable() {
        assertRetryable(new HttpResponseException("throttled", new FakeResponse(429, "7")), "HTTP 429", 7_000);
        assertRetryable(new HttpResponseException("unavailable", new FakeResponse(503, null)), "HTTP 503", -1);
        assertRetryable(new HttpResponseException("gone", new FakeResponse(410, "Wed, 21 Oct 2015 07:28:00 GMT")), "HTTP 410", -1);
        assertRetryable(new IllegalStateException("connection reset"), "connection reset", -1);
        assertRetryable(new ClientAuthenticationException("ManagedIdentityCredential: IMDS endpoint timed out", null,
                (Object) null), "timed out", -1);
    }

    @Test
    public void testResultsWithoutAValueOrExpiryAreRetryable() {
        TokenUnavailableException e = expectFailure(new AzureTokenSource(
                ctx -> Mono.just(new AccessToken("t", null)), "api://qdb"));
        Assert.assertTrue(e.isRetryable());
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("without an expiry"));
        e = expectFailure(new AzureTokenSource(ctx -> Mono.empty(), "api://qdb"));
        Assert.assertTrue(e.isRetryable());
        e = expectFailure(new AzureTokenSource(ctx -> Mono.just(new AccessToken("bad\r\ntoken", EXPIRES)), "api://qdb"));
        Assert.assertTrue(e.isRetryable());
        Assert.assertFalse("the token must not be echoed", e.getMessage().contains("bad"));
    }

    @Test
    public void testScopeIsResourceDefault() {
        AtomicReference<List<String>> scopes = new AtomicReference<>();
        TokenCredential credential = ctx -> {
            scopes.set(ctx.getScopes());
            return Mono.just(new AccessToken("t", EXPIRES));
        };
        new AzureTokenSource(credential, "api://qdb").fetchToken();
        Assert.assertEquals("[api://qdb/.default]", scopes.get().toString());
        new AzureTokenSource(credential, "11111111-2222-3333-4444-555555555555/.default").fetchToken();
        Assert.assertEquals("[11111111-2222-3333-4444-555555555555/.default]", scopes.get().toString());
    }

    @Test(timeout = 30_000)
    public void testSlowCredentialIsBoundedAndRetryable() {
        AzureTokenSource source = new AzureTokenSource(ctx -> Mono.never(), "api://qdb", Duration.ofMillis(200));
        long start = System.nanoTime();
        TokenUnavailableException e = expectFailure(source);
        Assert.assertTrue(e.isRetryable());
        Assert.assertTrue("the attempt must be bounded", System.nanoTime() - start < 10_000_000_000L);
    }

    @Test(timeout = 30_000)
    public void testWorksBehindTheRefreshingProvider() {
        OffsetDateTime expires = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);
        AzureTokenSource source = new AzureTokenSource(
                ctx -> Mono.just(new AccessToken("eyJ.cached", expires, expires.minusMinutes(50))), "api://qdb");
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build()) {
            Assert.assertTrue(provider.awaitReady(10_000));
            Assert.assertEquals("eyJ.cached", provider.getToken().toString());
            Assert.assertEquals(expires.toInstant().toEpochMilli(), provider.getTokenExpiresAtEpochMillis());
            Assert.assertTrue(provider.getLastSuccessEpochMillis() <= Instant.now().toEpochMilli());
        }
    }

    private static void assertPermanent(Throwable libraryFailure, String fragment) {
        TokenUnavailableException e = fetchFailure(libraryFailure);
        Assert.assertFalse("expected permanent: " + e.getMessage(), e.isRetryable());
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(fragment));
    }

    private static void assertRetryable(Throwable libraryFailure, String fragment, long retryAfterMillis) {
        TokenUnavailableException e = fetchFailure(libraryFailure);
        Assert.assertTrue("expected retryable: " + e.getMessage(), e.isRetryable());
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(fragment));
        Assert.assertEquals(retryAfterMillis, e.getRetryAfterMillis());
    }

    private static TokenUnavailableException expectFailure(AzureTokenSource source) {
        try {
            source.fetchToken();
            Assert.fail("expected the fetch to fail");
            return null;
        } catch (TokenUnavailableException e) {
            return e;
        }
    }

    private static TokenUnavailableException fetchFailure(Throwable libraryFailure) {
        return expectFailure(new AzureTokenSource(ctx -> Mono.error(libraryFailure), "api://qdb"));
    }

    private static final class FakeResponse extends HttpResponse {
        private final String retryAfter;
        private final int status;

        FakeResponse(int status, String retryAfter) {
            super(null);
            this.status = status;
            this.retryAfter = retryAfter;
        }

        @Override
        public Flux<ByteBuffer> getBody() {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray() {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString() {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset) {
            return Mono.empty();
        }

        @Override
        public String getHeaderValue(String name) {
            return "Retry-After".equalsIgnoreCase(name) ? retryAfter : null;
        }

        @Override
        public HttpHeaders getHeaders() {
            HttpHeaders headers = new HttpHeaders();
            if (retryAfter != null) {
                headers.set("Retry-After", retryAfter);
            }
            return headers;
        }

        @Override
        public int getStatusCode() {
            return status;
        }
    }
}
