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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import io.questdb.client.azure.AzureTokenProviderFactory;
import io.questdb.client.azure.AzureTokenSource;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.cutlass.auth.TokenSource;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * The child process of {@link AzureManagedIdentityLibraryTest}: fetches one token through the {@code azure}
 * provider, with the managed-identity endpoint its environment names, and reports on stdout, one
 * {@code PROBE key=value} line per fact.
 * <ul>
 *   <li>{@code factory <azure_credential>} - the source {@link AzureTokenProviderFactory} creates for that value
 *   ({@code default} passes no key);</li>
 *   <li>{@code direct <timeout-millis>} - a {@code ManagedIdentityCredential} behind an {@link AzureTokenSource}
 *   whose bound outlasts the library's own retries.</li>
 * </ul>
 */
public final class AzureCredentialProbeMain {
    static final String RESOURCE = "api://questdb-probe";

    public static void main(String[] args) {
        final Logger factoryLogger = (Logger) LoggerFactory.getLogger(AzureTokenProviderFactory.class);
        final ListAppender<ILoggingEvent> events = new ListAppender<>();
        events.start();
        factoryLogger.addAppender(events);

        final TokenSource source;
        if ("factory".equals(args[0])) {
            final Map<String, String> params = new HashMap<>();
            params.put(TokenProviderSpec.KEY_AZURE_RESOURCE, RESOURCE);
            if (!"default".equals(args[1])) {
                params.put(TokenProviderSpec.KEY_AZURE_CREDENTIAL, args[1]);
            }
            source = new AzureTokenProviderFactory().createSource(params);
        } else {
            source = new AzureTokenSource(new ManagedIdentityCredentialBuilder().build(), RESOURCE,
                    Duration.ofMillis(Long.parseLong(args[1])));
        }
        report("fetching"); // the parent times an endpoint outage from here
        final long start = System.nanoTime();
        try {
            source.fetchToken();
            report("token=true");
        } catch (TokenUnavailableException e) {
            report("token=false");
            report("retryable=" + e.isRetryable());
            report("message=" + String.valueOf(e.getMessage()).replace('\r', ' ').replace('\n', ' '));
        }
        report("elapsedMs=" + (System.nanoTime() - start) / 1_000_000);
        int warnings = 0;
        for (ILoggingEvent event : events.list) {
            if (event.getLevel() == Level.WARN) {
                warnings++;
            }
        }
        report("factoryWarnings=" + warnings);
        // Azure Identity leaves non-daemon threads behind
        System.exit(0);
    }

    private static void report(String fact) {
        System.out.println("PROBE " + fact);
        System.out.flush();
    }
}
