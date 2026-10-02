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

import java.util.Map;

/**
 * Service-provider interface behind the {@code token_provider} connect-string key (design/qwp-token-provider-spec.md,
 * section 7). An implementation is found through {@link java.util.ServiceLoader}: list it in
 * {@code META-INF/services/io.questdb.client.cutlass.auth.TokenProviderFactory}, or declare it with
 * {@code provides} in a module descriptor. The optional {@code questdb-client-azure} artifact supplies the
 * {@code azure} factory this way.
 * <p>
 * A factory turns the provider-specific connect-string keys into a {@link TokenSource}. The client wraps that
 * source in a {@link RefreshingTokenProvider} that the process-wide {@link TokenProviderRegistry} shares between
 * every client built from an equivalent connect string, so a process runs one refresher per configuration.
 * <p>
 * The connect-string vocabulary is fixed - unknown keys are rejected before a factory ever sees them - so a
 * factory receives only the keys the client defines for it, already normalized: for {@code azure}, the
 * {@code azure_resource} (with any trailing {@code /.default} removed) and the optional, lower-cased
 * {@code azure_client_id}. Values in these keys are never secret: secrets come from the platform's standard
 * configuration, never from the connect string.
 */
public interface TokenProviderFactory {

    /**
     * Creates the token source for one configuration. Called once per distinct configuration, when the first
     * client using it connects. It must not perform network I/O: the provider's refresher thread calls
     * {@link TokenSource#fetchToken()} later.
     *
     * @param params the provider-specific keys, normalized; unmodifiable
     * @return the token source
     * @throws IllegalArgumentException when the parameters cannot be used
     */
    TokenSource createSource(Map<String, String> params);

    /**
     * A non-secret label for log lines and errors, e.g. {@code azure[resource=api://...]}.
     *
     * @param params the provider-specific keys, normalized
     * @return the label
     */
    default String describe(Map<String, String> params) {
        return params.isEmpty() ? name() : name() + params;
    }

    /**
     * The value of {@code token_provider} this factory serves, such as {@code azure}.
     */
    String name();

    /**
     * Validates the provider-specific keys when a connect string is parsed, before any client connects. Must be
     * pure: it runs during configuration validation that must not fetch a token (section 7.2).
     *
     * @param params the provider-specific keys, normalized; unmodifiable
     * @throws IllegalArgumentException naming the offending key
     */
    default void validate(Map<String, String> params) {
    }
}
