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

package io.questdb.client.test.example;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.FileTokenStore;
import io.questdb.client.cutlass.auth.OidcDeviceAuth;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class OIDCAuthExample {
    public static void main(String[] args) {

        // Discover the client id, scope and endpoints from the QuestDB server's /settings:
        try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(
                "http://localhost:9000",
                new OidcDeviceAuth.DiscoveryOptions()
                        .allowInsecureTransport(true)
                        .tokenStore(FileTokenStore.atDefaultLocation())
        )) {
            // one-time interactive sign-in; caches token + refresh token
            auth.signIn();

            // ingress - ILP over HTTP
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("localhost:9000")
                    .httpTokenProvider(auth::getToken)
                    .build()) {
                sender.table("abcde")
                        .longColumn("c0", 25)
                        .atNow();
            }

            // ingress - QWP
            try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:9000")
                    .httpTokenProvider(auth::getToken)
                    .build()) {
                sender.table("abcde")
                        .longColumn("c0", 28)
                        .atNow();
            }

            // egress - QWP
            CollectingHandler handler = new CollectingHandler();
            try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)
                    .withBearerTokenProvider(auth::getToken)) {
                client.connect();
                client.execute("SELECT c0, ts FROM abcde", handler);
            }
        }
    }

    static final class CollectingHandler implements QwpColumnBatchHandler {
        public void onBatch(QwpColumnBatch batch) {
            batch.forEachRow(row -> {
                long c0 = row.getLongValue(0);
                // QuestDB TIMESTAMP columns arrive as microseconds since the Unix epoch
                Instant ts = Instant.EPOCH.plus(row.getLongValue(1), ChronoUnit.MICROS);
                System.out.printf("%d %s%n", c0, ts);
            });
        }

        public void onEnd(long totalRows) {
        }

        public void onError(byte status, String message) {
            System.err.println("query failed: status=" + status + " msg=" + message);
        }
    }
}
