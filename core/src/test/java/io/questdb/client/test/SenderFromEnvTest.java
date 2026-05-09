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
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * The delegation half of {@link Sender#fromEnv()} is just one line through to
 * {@link Sender#fromConfig(CharSequence)}, which {@code fromConfig}'s own
 * suite covers thoroughly. The interesting bit is the env-read + null/blank
 * check, exercised here without mutating {@link System#getenv()} (which is
 * non-portable: Windows splits the case-sensitive and case-insensitive maps).
 */
public class SenderFromEnvTest {

    @Test
    public void testThrowsWhenEnvUnset() {
        // CI runs and developer machines typically don't have QDB_CLIENT_CONF
        // set. Skip rather than fail in the rare case it is, so this test
        // doesn't tell a lie about what's broken.
        Assume.assumeTrue(
                "QDB_CLIENT_CONF must be unset for this test",
                System.getenv("QDB_CLIENT_CONF") == null);
        try {
            Sender.fromEnv();
            Assert.fail("expected LineSenderException for unset env var");
        } catch (LineSenderException e) {
            Assert.assertTrue(
                    "message should name the env var, was: " + e.getMessage(),
                    e.getMessage().contains("QDB_CLIENT_CONF"));
        }
    }
}
