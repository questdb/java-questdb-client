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

package io.questdb.client.test.cutlass.auth;

import io.questdb.client.cutlass.auth.ExpiringToken;
import io.questdb.client.cutlass.auth.TokenProviderFactory;
import io.questdb.client.cutlass.auth.TokenProviderRegistry;
import io.questdb.client.cutlass.auth.TokenSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A {@link TokenProviderFactory} for tests, installed in place of {@link java.util.ServiceLoader} discovery with
 * {@link #install(TestTokenProviderFactory...)}. It counts the sources it creates and the fetches they make.
 * Registering one named {@code azure} lets core tests drive the {@code azure} connect-string keys without the
 * optional module.
 */
public final class TestTokenProviderFactory implements TokenProviderFactory {
    public final AtomicInteger created = new AtomicInteger();
    public final AtomicInteger fetches = new AtomicInteger();
    public final List<Map<String, String>> params = new CopyOnWriteArrayList<>();
    private final String name;
    private final Function<Map<String, String>, ExpiringToken> tokens;

    public TestTokenProviderFactory(String name) {
        this(name, p -> new ExpiringToken("TOKEN-" + name, System.currentTimeMillis() + 3_600_000L));
    }

    public TestTokenProviderFactory(String name, Function<Map<String, String>, ExpiringToken> tokens) {
        this.name = name;
        this.tokens = tokens;
    }

    /**
     * Makes exactly these factories visible to {@code token_provider} resolution.
     */
    public static void install(TestTokenProviderFactory... factories) {
        List<TokenProviderFactory> list = new ArrayList<>();
        Collections.addAll(list, factories);
        TokenProviderRegistry.setFactoriesForTesting(list);
    }

    /**
     * Restores {@link java.util.ServiceLoader} discovery.
     */
    public static void uninstall() {
        TokenProviderRegistry.setFactoriesForTesting(null);
    }

    @Override
    public TokenSource createSource(Map<String, String> params) {
        created.incrementAndGet();
        this.params.add(params);
        return () -> {
            fetches.incrementAndGet();
            return tokens.apply(params);
        };
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void validate(Map<String, String> params) {
        String resource = params.get("azure_resource");
        if (resource != null && resource.startsWith("invalid:")) {
            throw new IllegalArgumentException("azure_resource rejected by the factory");
        }
    }
}
