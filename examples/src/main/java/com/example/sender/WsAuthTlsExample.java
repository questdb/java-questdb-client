package com.example.sender;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;

/**
 * Authenticated, TLS-secured pooled ingest over QWP ({@code wss}).
 * <p>
 * QWP runs over WebSocket, so authentication uses HTTP-style credentials sent
 * on the WebSocket upgrade -- for both ingest and egress, before any data is
 * exchanged. The credential and TLS keys are common to both directions, so a
 * single {@code QuestDB.connect(...)} string authenticates the whole handle.
 * <ul>
 *   <li>{@code token=...} -- sent as an {@code Authorization: Bearer} header
 *       (Enterprise). Mutually exclusive with {@code username}/{@code password}.</li>
 *   <li>{@code username=...;password=...} -- HTTP basic auth; both halves
 *       required together.</li>
 *   <li>{@code tls_roots=...;tls_roots_password=...} -- a custom truststore
 *       (e.g. a JKS) when the server cert isn't in the default trust store.</li>
 * </ul>
 * The {@code wss} schema turns on TLS. Contrast with the ILP TLS examples
 * ({@link AuthTlsExample}, {@link HttpsAuthExample}), which secure a dedicated
 * {@code Sender.fromConfig(...)} connection over {@code tcps}/{@code https}
 * (and, for TCP, ECDSA/JWK auth that QWP does not use).
 */
public class WsAuthTlsExample {

    public static void main(String[] args) {
        // Replace the address, token, and truststore path with your own. For
        // HTTP basic auth swap token= for username=...;password=...
        try (QuestDB db = QuestDB.connect(
                "wss::addr=db.example.com:9000;"
                        + "token=YOUR_BEARER_TOKEN;"
                        + "tls_roots=/path/to/truststore.jks;"
                        + "tls_roots_password=changeit;")) {

            try (Sender sender = db.borrowSender()) {
                sender.table("trades")
                        .symbol("symbol", "ETH-USD")
                        .symbol("side", "sell")
                        .doubleColumn("price", 2615.54)
                        .doubleColumn("amount", 0.00044)
                        .atNow();
            }
        }

        // For local development against a self-signed cert you can disable
        // certificate validation with tls_verify=unsafe_off. NEVER do this in
        // production -- it defeats TLS entirely:
        //
        //   QuestDB.connect("wss::addr=localhost:9000;tls_verify=unsafe_off;token=...;")
    }
}
