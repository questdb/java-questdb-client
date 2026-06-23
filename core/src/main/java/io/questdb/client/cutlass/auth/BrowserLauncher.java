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

import java.awt.Desktop;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Opens a verification URL in the local default browser, best-effort. Kept separate from
 * {@link DeviceCodePrompt} so a runtime without the {@code java.desktop} module fails only when
 * {@link DeviceCodePrompt#openBrowser()} is actually used, not when the interface loads.
 */
final class BrowserLauncher {

    // System property to disable the automatic browser launch (default: enabled). Set to "false" on a
    // host that must never pop a browser - a server, automation, CI - or to keep a test run headless.
    private static final String OPEN_BROWSER_PROPERTY = "questdb.client.oidc.open.browser";

    private BrowserLauncher() {
    }

    /**
     * Opens {@code url} in the default browser when it is an http(s) URL and a desktop browser is
     * available. Does nothing on a headless JVM, for a non-http(s) URL, on a launch failure, or when the
     * {@code questdb.client.oidc.open.browser} system property is set to {@code false}. May throw a
     * {@link LinkageError} when the {@code java.desktop} module is absent from the runtime; the caller
     * treats that as "no browser available".
     */
    static void open(String url) {
        if (!Boolean.parseBoolean(System.getProperty(OPEN_BROWSER_PROPERTY, "true"))) {
            return;
        }
        URI uri = safeHttpUri(url);
        if (uri == null) {
            return;
        }
        try {
            if (Desktop.isDesktopSupported()) {
                Desktop desktop = Desktop.getDesktop();
                if (desktop.isSupported(Desktop.Action.BROWSE)) {
                    desktop.browse(uri);
                }
            }
        } catch (Exception ignore) {
            // a headless display, a missing default browser or a security restriction must never
            // break sign-in: the verification URL and code are already shown to the user
        }
    }

    /**
     * Returns {@code url} as a {@link URI} only when it parses and uses an http(s) scheme, else
     * {@code null}. The verification URL is an untrusted identity-provider response field; the
     * allowlist stops a javascript:, data: or file: scheme from reaching the OS browser handler.
     */
    static URI safeHttpUri(String url) {
        if (url == null) {
            return null;
        }
        try {
            URI uri = new URI(url);
            String scheme = uri.getScheme();
            if (scheme != null && (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
                return uri;
            }
            return null;
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
