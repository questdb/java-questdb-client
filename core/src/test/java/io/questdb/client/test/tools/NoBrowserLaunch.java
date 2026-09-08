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

package io.questdb.client.test.tools;

import org.junit.rules.ExternalResource;

/**
 * Disables the OIDC device-flow browser launch for one test class, and puts the property back afterwards.
 * <p>
 * The default {@code DeviceCodePrompt} opens a browser when one is available, which a developer machine has,
 * so any test reaching the prompt would pop a real tab. Setting
 * {@code questdb.client.oidc.open.browser=false} in a static initializer stopped that, but surefire runs the
 * whole module in one JVM: whichever class loaded first flipped the property for every class after it, and
 * nothing ever put it back. A test that means to exercise the DEFAULT - the launch enabled - then silently
 * ran against someone else's override, depending on class-load order.
 * <p>
 * Use as a class rule, so the window is exactly the class that needs it:
 * <pre>
 * &#64;ClassRule
 * public static final NoBrowserLaunch NO_BROWSER = new NoBrowserLaunch();
 * </pre>
 */
public final class NoBrowserLaunch extends ExternalResource {

    private static final String PROPERTY = "questdb.client.oidc.open.browser";
    private String previous;
    private boolean wasSet;

    @Override
    protected void after() {
        if (wasSet) {
            System.setProperty(PROPERTY, previous);
        } else {
            System.clearProperty(PROPERTY);
        }
    }

    @Override
    protected void before() {
        previous = System.getProperty(PROPERTY);
        wasSet = previous != null;
        System.setProperty(PROPERTY, "false");
    }
}
