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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.QwpIngressRoleRejectedException;
import org.junit.Assert;
import org.junit.Test;

public class QwpIngressRoleRejectedExceptionTest {

    @Test
    public void testReplicaIsTopological() {
        QwpIngressRoleRejectedException ex = new QwpIngressRoleRejectedException("REPLICA", "h", 9);
        Assert.assertTrue(ex.isTopological());
        Assert.assertFalse(ex.isTransient());
    }

    @Test
    public void testPrimaryCatchupIsTransient() {
        QwpIngressRoleRejectedException ex = new QwpIngressRoleRejectedException("PRIMARY_CATCHUP", "h", 9);
        Assert.assertTrue(ex.isTransient());
        Assert.assertFalse(ex.isTopological());
    }

    @Test
    public void testStandaloneIsNeither() {
        QwpIngressRoleRejectedException ex = new QwpIngressRoleRejectedException("STANDALONE", "h", 9);
        Assert.assertFalse(ex.isTransient());
        Assert.assertFalse(ex.isTopological());
    }

    @Test
    public void testFieldsRetained() {
        QwpIngressRoleRejectedException ex = new QwpIngressRoleRejectedException("REPLICA", "db", 9000);
        Assert.assertEquals("REPLICA", ex.getRole());
        Assert.assertEquals("db", ex.getHost());
        Assert.assertEquals(9000, ex.getPort());
        Assert.assertTrue(ex.getMessage().contains("REPLICA"));
        Assert.assertTrue(ex.getMessage().contains("db:9000"));
    }
}
