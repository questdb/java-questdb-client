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
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Proves every POOL key carried in a {@code ws}/{@code wss} connect string is
 * resolved into the facade's pool config -- not merely accepted. Uses
 * {@code min=0} so {@code build()} runs resolution without connecting.
 * {@link #testHonoredCasesCoverEveryPoolRegistryKey} guards against drift.
 */
public class PoolConfigHonoredTest {

    @Test
    public void testEveryPoolKeyIsHonored() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Drive both the value assertions and the drift guard from one map, so the
            // coverage check cannot drift from what is actually asserted. min=0 keys
            // let build() resolve the pool keys without pre-warming/connecting. Pool
            // sizes resolve to int, the timeouts to long (the snapshot's boxed types).
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("sender_pool_min", 0);
            expected.put("sender_pool_max", 7);
            expected.put("query_pool_min", 0);
            expected.put("query_pool_max", 5);
            expected.put("acquire_timeout_ms", 1234L);
            expected.put("query_close_timeout_ms", 2468L);
            expected.put("idle_timeout_ms", 4321L);
            expected.put("max_lifetime_ms", 98765L);
            expected.put("housekeeper_interval_ms", 222L);

            StringBuilder cfg = new StringBuilder("ws::addr=127.0.0.1:1;");
            for (Map.Entry<String, Object> e : expected.entrySet()) {
                cfg.append(e.getKey()).append('=').append(e.getValue()).append(';');
            }
            QuestDBBuilder b = QuestDB.builder().fromConfig(cfg.toString());
            b.build().close();

            Map<String, Object> snap = b.poolConfigSnapshotForTest();
            for (Map.Entry<String, Object> e : expected.entrySet()) {
                Assert.assertEquals("pool key '" + e.getKey() + "' not honored", e.getValue(), snap.get(e.getKey()));
            }

            // Drift guard: every POOL registry key must appear in the map that drove
            // the assertions above, so a new pool key with no assertion trips this.
            for (ConfigSchema.KeySpec spec : ConfigSchema.all()) {
                if (spec.side() == Side.POOL) {
                    // lazy_connect is a facade flag (build()'s tolerant-startup
                    // branch, covered by QuestDBLazyConnectTest), not a numeric
                    // pool-sizing knob resolved into the snapshot.
                    if ("lazy_connect".equals(spec.name())) {
                        continue;
                    }
                    Assert.assertTrue("registry pool key '" + spec.name() + "' has no honored assertion",
                            expected.containsKey(spec.name()));
                }
            }
        });
    }
}
