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

package io.questdb.client.test;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * {@link Sender#fromEnv()} reads {@code QDB_CLIENT_CONF} from the JVM
 * environment. Java does not expose a public mutator for the environment
 * map, so these tests reach into the unmodifiable wrapper that
 * {@code System.getenv()} returns and mutate the underlying map. Requires
 * {@code --add-opens=java.base/java.util=ALL-UNNAMED} on the surefire
 * argLine to defeat strong encapsulation on Java 17+.
 */
public class SenderFromEnvTest {
    private static final String CONFIG_VAR = "QDB_CLIENT_CONF";
    private String snapshot;

    @Before
    public void snapshotEnv() {
        snapshot = System.getenv(CONFIG_VAR);
        removeEnv(CONFIG_VAR);
    }

    @After
    public void restoreEnv() {
        if (snapshot == null) {
            removeEnv(CONFIG_VAR);
        } else {
            setEnv(CONFIG_VAR, snapshot);
        }
    }

    @Test
    public void testThrowsWhenEnvBlank() {
        setEnv(CONFIG_VAR, "   ");
        try {
            Sender.fromEnv().close();
            Assert.fail("expected LineSenderException for blank env var");
        } catch (LineSenderException e) {
            Assert.assertTrue(
                    "message should name the env var, was: " + e.getMessage(),
                    e.getMessage().contains(CONFIG_VAR));
        }
    }

    @Test
    public void testThrowsWhenEnvUnset() {
        // @Before already removed CONFIG_VAR.
        try {
            Sender.fromEnv().close();
            Assert.fail("expected LineSenderException for unset env var");
        } catch (LineSenderException e) {
            Assert.assertTrue(
                    "message should name the env var, was: " + e.getMessage(),
                    e.getMessage().contains(CONFIG_VAR));
        }
    }

    @Test
    public void testValidEnvDelegatesToFromConfig() {
        // UDP is connectionless, so the builder constructs the sender without
        // any network I/O. A successful build proves fromEnv() forwarded the
        // env-var contents to fromConfig() verbatim.
        setEnv(CONFIG_VAR, "udp::addr=127.0.0.1:1;");
        try (Sender sender = Sender.fromEnv()) {
            Assert.assertNotNull(sender);
        }
    }

    @Test
    public void testValidEnvFailsAtParseWhenSchemeUnknown() {
        // Delegation to fromConfig() must surface the parse failure verbatim --
        // confirms fromEnv() is a pass-through, not a swallow-and-rethrow.
        setEnv(CONFIG_VAR, "bogus::addr=127.0.0.1:1;");
        try {
            Sender.fromEnv().close();
            Assert.fail("expected LineSenderException from underlying fromConfig parse");
        } catch (LineSenderException e) {
            // The exact message is owned by fromConfig; we only assert it is NOT
            // the env-not-set message, proving we got past the env-read step.
            Assert.assertFalse(
                    "should have surfaced parse error, not env-unset error: " + e.getMessage(),
                    e.getMessage().contains("environment variable is not set"));
        }
    }

    private static void removeEnv(String key) {
        mutableEnv().remove(key);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> mutableEnv() {
        Map<String, String> env = System.getenv();
        try {
            Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            return (Map<String, String>) field.get(env);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(
                    "cannot mutate JVM env map; surefire needs "
                            + "--add-opens=java.base/java.util=ALL-UNNAMED",
                    e);
        }
    }

    private static void setEnv(String key, String value) {
        mutableEnv().put(key, value);
    }
}
