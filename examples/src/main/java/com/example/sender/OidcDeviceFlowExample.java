package com.example.sender;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.OidcDeviceAuth;

/**
 * Signs in to an OIDC-secured QuestDB Enterprise from code that has no local browser
 * (a remote notebook kernel, a container, a headless job) using the OAuth 2.0 Device
 * Authorization Grant, then shows the three ways to use the resulting token.
 * <p>
 * On first use this prints a verification URL and a short code and, on a machine with a
 * browser, opens the URL for you; otherwise open it on any device (your laptop or your
 * phone) and enter the code. The token is then cached in memory and refreshed silently,
 * so re-running this does not prompt again.
 */
public class OidcDeviceFlowExample {
    public static void main(String[] args) {
        // Discover client id, scope, endpoints and the groups-in-token mode from the server.
        // Alternatively, configure the identity provider explicitly with OidcDeviceAuth.builder().
        // The default prompt prints the URL and code AND opens the URL in your browser when one is
        // available (best-effort; skipped on a headless host). To print only, pass options:
        //   import io.questdb.client.cutlass.auth.DeviceCodePrompt;
        //   OidcDeviceAuth.fromQuestDB(url, new OidcDeviceAuth.DiscoveryOptions().prompt(DeviceCodePrompt.SYSTEM_OUT))
        // To survive a restart without prompting again, persist the token with a TokenStore - the restarted
        // process resumes from the saved refresh token instead of re-running the device flow:
        //   import io.questdb.client.cutlass.auth.FileTokenStore;
        //   OidcDeviceAuth.fromQuestDB(url, new OidcDeviceAuth.DiscoveryOptions().tokenStore(FileTokenStore.atDefaultLocation()))
        try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB("https://questdb.example.com:9000")) {
            auth.signIn(); // sign in once (prompts on first use, then caches and refreshes silently)

            // 1. Ingest with the QuestDB client over ILP-over-HTTP, presenting the token as a Bearer.
            //    Pass a provider, not the fixed token, so a long-lived sender follows silent refreshes.
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("questdb.example.com:9000")
                    .enableTls()
                    .httpTokenProvider(auth::getToken)
                    .build()) {
                sender.table("trades")
                        .symbol("symbol", "ETH-USD")
                        .doubleColumn("price", 2615.54)
                        .atNow();
            }

            // 2. Query the REST API directly: send the token in the Authorization header.
            //    String header = auth.getAuthorizationHeaderValue(); // "Bearer <token>"
            //    GET https://questdb.example.com:9000/exec?query=...  with header  Authorization: <header>

            // 3. Connect over PG-wire with any JDBC or psql client: user "_sso", password = the token
            //    (requires acl.oidc.pg.token.as.password.enabled=true on the server).
            //    jdbc:postgresql://questdb.example.com:8812/qdb   user=_sso   password=<token>
        }
    }
}
