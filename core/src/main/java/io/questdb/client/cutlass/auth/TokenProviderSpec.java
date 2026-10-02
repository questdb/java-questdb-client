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

package io.questdb.client.cutlass.auth;

import io.questdb.client.impl.ConfigView;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * A validated {@code token_provider} selection from a {@code ws}/{@code wss} connect string: the provider name,
 * its normalized parameters, and the factory that serves it (design/qwp-token-provider-spec.md, section 7).
 * <p>
 * {@link #parse(ConfigView, boolean)} enforces every rule of section 7.2 and never fetches a token: it only
 * looks the factory up. A client turns the spec into a provider when it connects, through
 * {@link TokenProviderRegistry#acquire(TokenProviderSpec)}.
 * <pre>
 * wss::addr=qdb1:9000,qdb2:9000;token_provider=azure;azure_resource=api://&lt;questdb-app-id&gt;;azure_client_id=&lt;uami-client-id&gt;;
 * </pre>
 */
public final class TokenProviderSpec {
    /**
     * The {@code token_provider} value of the Microsoft Entra ID provider, served by the optional
     * {@code questdb-client-azure} module.
     */
    public static final String AZURE = "azure";
    /**
     * Reserved for a zero-dependency Azure IMDS provider (decision D5); not implemented.
     */
    public static final String AZURE_IMDS = "azure_imds";
    public static final String AZURE_MODULE = "org.questdb:questdb-client-azure";
    public static final String KEY_AZURE_CLIENT_ID = "azure_client_id";
    public static final String KEY_AZURE_RESOURCE = "azure_resource";
    public static final String KEY_TOKEN_PROVIDER = "token_provider";
    private static final String AZURE_DEFAULT_SCOPE_SUFFIX = "/.default";
    private static final String[] STATIC_CREDENTIAL_KEYS = {"token", "username", "password"};
    private final TokenProviderFactory factory;
    private final String name;
    private final Map<String, String> params;
    private final String registryKey;

    private TokenProviderSpec(TokenProviderFactory factory, String name, Map<String, String> params) {
        this.factory = factory;
        this.name = name;
        this.params = Collections.unmodifiableMap(params);
        StringBuilder key = new StringBuilder(name);
        for (Map.Entry<String, String> e : params.entrySet()) {
            key.append('|').append(e.getKey()).append('=').append(e.getValue());
        }
        this.registryKey = key.toString();
    }

    /**
     * Validates and resolves the {@code token_provider} keys of a {@code ws}/{@code wss} connect string. Never
     * fetches a token and never starts a provider. Rejects, naming the offending key:
     * <ul>
     *   <li>{@code token_provider} combined with {@code token}, {@code username} or {@code password};</li>
     *   <li>an empty, unknown, reserved or uninstalled {@code token_provider}, listing the supported values;</li>
     *   <li>a provider-specific key the selected provider does not accept, such as {@code azure_resource}
     *   without {@code token_provider=azure};</li>
     *   <li>a missing required provider key;</li>
     *   <li>{@code token_provider} on {@code ws::}: the bearer token would cross the network in cleartext
     *   (decision D4).</li>
     * </ul>
     * Combining {@code token_provider} with a provider the application supplies is rejected by the builders.
     *
     * @param view the parsed connect string
     * @param tls  true for the {@code wss} schema
     * @return the spec, or null when the string selects no provider
     * @throws IllegalArgumentException on any violation
     */
    public static TokenProviderSpec parse(ConfigView view, boolean tls) {
        final String name = view.getStr(KEY_TOKEN_PROVIDER);
        final String resource = view.getStr(KEY_AZURE_RESOURCE);
        final String clientId = view.getStr(KEY_AZURE_CLIENT_ID);
        if (name == null) {
            if (resource != null) {
                throw new IllegalArgumentException(KEY_AZURE_RESOURCE + " requires " + KEY_TOKEN_PROVIDER + '=' + AZURE);
            }
            if (clientId != null) {
                throw new IllegalArgumentException(KEY_AZURE_CLIENT_ID + " requires " + KEY_TOKEN_PROVIDER + '=' + AZURE);
            }
            return null;
        }
        for (String key : STATIC_CREDENTIAL_KEYS) {
            if (view.has(key)) {
                throw new IllegalArgumentException(KEY_TOKEN_PROVIDER + " cannot be combined with " + key);
            }
        }
        if (name.trim().isEmpty()) {
            throw new IllegalArgumentException(KEY_TOKEN_PROVIDER + " must not be empty; " + supportedValues());
        }
        if (!tls) {
            throw new IllegalArgumentException(KEY_TOKEN_PROVIDER + " requires the wss:: schema: over ws:: the bearer "
                    + "token would cross the network in cleartext");
        }
        if (AZURE_IMDS.equals(name)) {
            throw new IllegalArgumentException(KEY_TOKEN_PROVIDER + '=' + AZURE_IMDS
                    + " is reserved and not implemented by this client; " + supportedValues());
        }
        final TokenProviderFactory factory = TokenProviderRegistry.findFactory(name);
        if (factory == null) {
            if (AZURE.equals(name)) {
                throw new IllegalArgumentException(KEY_TOKEN_PROVIDER + '=' + AZURE + " requires the " + AZURE_MODULE
                        + " module on the class path or module path; " + supportedValues());
            }
            throw new IllegalArgumentException("unsupported " + KEY_TOKEN_PROVIDER + ": " + safe(name) + "; "
                    + supportedValues());
        }
        if (!AZURE.equals(name)) {
            if (resource != null) {
                throw new IllegalArgumentException(KEY_AZURE_RESOURCE + " is only valid with " + KEY_TOKEN_PROVIDER + '=' + AZURE);
            }
            if (clientId != null) {
                throw new IllegalArgumentException(KEY_AZURE_CLIENT_ID + " is only valid with " + KEY_TOKEN_PROVIDER + '=' + AZURE);
            }
        }
        final Map<String, String> params = new TreeMap<>();
        if (AZURE.equals(name)) {
            if (resource == null) {
                throw new IllegalArgumentException(KEY_TOKEN_PROVIDER + '=' + AZURE + " requires " + KEY_AZURE_RESOURCE);
            }
            params.put(KEY_AZURE_RESOURCE, normalizeAzureResource(resource));
            if (clientId != null) {
                params.put(KEY_AZURE_CLIENT_ID, normalizeGuid(clientId));
            }
        }
        factory.validate(Collections.unmodifiableMap(params));
        return new TokenProviderSpec(factory, name, params);
    }

    /**
     * Lower-cases a GUID after checking its {@code 8-4-4-4-12} hex shape.
     */
    static String normalizeGuid(String value) {
        if (value.length() != 36) {
            throw invalidClientId(value);
        }
        for (int i = 0; i < 36; i++) {
            char c = value.charAt(i);
            boolean dash = i == 8 || i == 13 || i == 18 || i == 23;
            if (dash ? c != '-' : Character.digit(c, 16) < 0) {
                throw invalidClientId(value);
            }
        }
        return value.toLowerCase(Locale.ROOT);
    }

    private static IllegalArgumentException invalidClientId(String value) {
        return new IllegalArgumentException("invalid " + KEY_AZURE_CLIENT_ID
                + ": expected a GUID such as 00000000-0000-0000-0000-000000000000, got " + safe(value));
    }

    // The application ID URI (api://<app-id>) or the client ID of the QuestDB app registration. A trailing
    // "/.default" is stripped: the client always requests <azure_resource>/.default.
    private static String normalizeAzureResource(String value) {
        String resource = value;
        if (resource.endsWith(AZURE_DEFAULT_SCOPE_SUFFIX)) {
            resource = resource.substring(0, resource.length() - AZURE_DEFAULT_SCOPE_SUFFIX.length());
        }
        if (resource.isEmpty()) {
            throw new IllegalArgumentException(KEY_AZURE_RESOURCE + " must not be empty");
        }
        for (int i = 0, n = resource.length(); i < n; i++) {
            char c = resource.charAt(i);
            if (c <= 0x20 || c > 0x7e) {
                throw new IllegalArgumentException(KEY_AZURE_RESOURCE
                        + " must be an application ID URI (api://<app-id>) or a client ID without spaces or "
                        + "non-ASCII characters, got " + safe(resource));
            }
        }
        return resource;
    }

    private static String safe(String value) {
        return CredentialRedaction.sanitizeErrorText(value);
    }

    private static String supportedValues() {
        List<String> names = TokenProviderRegistry.supportedProviders();
        if (names.isEmpty()) {
            return "supported values: none installed (token_provider=azure needs " + AZURE_MODULE + ')';
        }
        return "supported values: " + names;
    }

    /**
     * A non-secret label for log lines and errors.
     */
    public String describe() {
        return factory.describe(params);
    }

    public TokenProviderFactory factory() {
        return factory;
    }

    /**
     * The provider name, the value of {@code token_provider}.
     */
    public String name() {
        return name;
    }

    /**
     * The normalized provider-specific keys; unmodifiable.
     */
    public Map<String, String> params() {
        return params;
    }

    /**
     * The registry key: the provider name plus the normalized parameters. Equivalent connect strings share it.
     */
    public String registryKey() {
        return registryKey;
    }

    @Override
    public String toString() {
        return "TokenProviderSpec{" + registryKey + '}';
    }
}
