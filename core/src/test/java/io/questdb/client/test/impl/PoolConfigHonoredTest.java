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

package io.questdb.client.test.impl;

import io.questdb.client.QuestDB;
import io.questdb.client.QuestDBBuilder;
import io.questdb.client.impl.ConfigSchema;
import io.questdb.client.impl.Side;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Proves every POOL key carried in a {@code ws}/{@code wss} connect string is
 * resolved into the facade's pool config -- not merely accepted. Uses
 * {@code min=0} so {@code build()} runs resolution without connecting.
 * {@link #testHonoredCasesCoverEveryPoolRegistryKey} guards against drift.
 */
public class PoolConfigHonoredTest {

    private static final Set<String> COVERED = new HashSet<>(Arrays.asList(
            "sender_pool_min", "sender_pool_max", "query_pool_min", "query_pool_max",
            "acquire_timeout_ms", "idle_timeout_ms", "max_lifetime_ms", "housekeeper_interval_ms"
    ));

    @Test
    public void testEveryPoolKeyIsHonored() {
        String cfg = "ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=7;query_pool_min=0;query_pool_max=5;"
                + "acquire_timeout_ms=1234;idle_timeout_ms=4321;max_lifetime_ms=98765;housekeeper_interval_ms=222;";
        QuestDBBuilder b = QuestDB.builder().fromConfig(cfg);
        // min=0 -> build() resolves the pool keys but pre-warms/connects nothing.
        b.build().close();
        Map<String, Object> snap = b.poolConfigSnapshotForTest();
        Assert.assertEquals(0, snap.get("sender_pool_min"));
        Assert.assertEquals(7, snap.get("sender_pool_max"));
        Assert.assertEquals(0, snap.get("query_pool_min"));
        Assert.assertEquals(5, snap.get("query_pool_max"));
        Assert.assertEquals(1234L, snap.get("acquire_timeout_ms"));
        Assert.assertEquals(4321L, snap.get("idle_timeout_ms"));
        Assert.assertEquals(98765L, snap.get("max_lifetime_ms"));
        Assert.assertEquals(222L, snap.get("housekeeper_interval_ms"));
    }

    @Test
    public void testHonoredCasesCoverEveryPoolRegistryKey() {
        for (ConfigSchema.KeySpec spec : ConfigSchema.all()) {
            if (spec.side() == Side.POOL) {
                Assert.assertTrue("registry pool key '" + spec.name() + "' has no honored case",
                        COVERED.contains(spec.name()));
            }
        }
    }
}
