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

import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pure data-class coverage for the {@link QwpServerInfo} accessors,
 * the {@link QwpServerInfo#roleName(byte)} lookup, and the
 * {@link QwpRoleMismatchException} getters. No I/O.
 */
public class QwpServerInfoTest {

    @Test
    public void testAccessorsReturnConstructorArgs() {
        QwpServerInfo info = new QwpServerInfo(
                QwpEgressMsgKind.ROLE_PRIMARY,
                42L,
                0xCAFEBABE,
                1_700_000_000_000_000_000L,
                "cluster-x",
                "node-7",
                "eu-west-1a"
        );
        Assert.assertEquals(QwpEgressMsgKind.ROLE_PRIMARY, info.getRole());
        Assert.assertEquals(42L, info.getEpoch());
        Assert.assertEquals(0xCAFEBABE, info.getCapabilities());
        Assert.assertEquals(1_700_000_000_000_000_000L, info.getServerWallNs());
        Assert.assertEquals("cluster-x", info.getClusterId());
        Assert.assertEquals("node-7", info.getNodeId());
        Assert.assertEquals("eu-west-1a", info.getZoneId());
    }

    @Test
    public void testNullZoneIdAccessor() {
        QwpServerInfo info = new QwpServerInfo(
                QwpEgressMsgKind.ROLE_STANDALONE, 0L, 0, 0L, "c", "n", null);
        Assert.assertNull(info.getZoneId());
    }

    @Test
    public void testRoleNameAllKnownValues() {
        Assert.assertEquals("STANDALONE", QwpServerInfo.roleName(QwpEgressMsgKind.ROLE_STANDALONE));
        Assert.assertEquals("PRIMARY", QwpServerInfo.roleName(QwpEgressMsgKind.ROLE_PRIMARY));
        Assert.assertEquals("REPLICA", QwpServerInfo.roleName(QwpEgressMsgKind.ROLE_REPLICA));
        Assert.assertEquals("PRIMARY_CATCHUP", QwpServerInfo.roleName(QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP));
    }

    @Test
    public void testRoleNameUnknownReturnsTaggedDecimal() {
        // 0xFE = 254 unsigned -- exercises the (role & 0xFF) widening so the
        // diagnostic doesn't print -2.
        Assert.assertEquals("UNKNOWN(254)", QwpServerInfo.roleName((byte) 0xFE));
        Assert.assertEquals("UNKNOWN(99)", QwpServerInfo.roleName((byte) 99));
    }

    @Test
    public void testToStringContainsAllFields() {
        QwpServerInfo info = new QwpServerInfo(
                QwpEgressMsgKind.ROLE_REPLICA,
                7L,
                QwpEgressMsgKind.CAP_ZONE,
                1234L,
                "myCluster",
                "myNode",
                "myZone"
        );
        String s = info.toString();
        Assert.assertTrue("expected role name in toString: " + s, s.contains("REPLICA"));
        Assert.assertTrue("expected epoch in toString: " + s, s.contains("epoch=7"));
        Assert.assertTrue("expected clusterId in toString: " + s, s.contains("myCluster"));
        Assert.assertTrue("expected nodeId in toString: " + s, s.contains("myNode"));
        Assert.assertTrue("expected zoneId in toString: " + s, s.contains("myZone"));
        Assert.assertTrue("expected hex capabilities in toString: " + s, s.contains("0x1"));
        Assert.assertTrue("expected serverWallNs in toString: " + s, s.contains("1234"));
    }

    @Test
    public void testToStringHandlesUnknownRole() {
        QwpServerInfo info = new QwpServerInfo((byte) 99, 0L, 0, 0L, "", "", null);
        Assert.assertTrue(info.toString().contains("UNKNOWN(99)"));
    }

    @Test
    public void testRoleMismatchExceptionGetters() {
        QwpServerInfo lastObserved = new QwpServerInfo(
                QwpEgressMsgKind.ROLE_REPLICA, 1L, 0, 0L, "c", "n", null);
        QwpRoleMismatchException ex = new QwpRoleMismatchException(
                "primary", lastObserved, "no primary endpoint available");

        Assert.assertEquals("primary", ex.getTargetRole());
        Assert.assertSame(lastObserved, ex.getLastObserved());
        Assert.assertTrue(ex.getMessage().contains("no primary endpoint available"));
    }

    @Test
    public void testRoleMismatchExceptionIsHttpClientException() {
        QwpRoleMismatchException ex = new QwpRoleMismatchException("any", null, "msg");
        // The static-typed assignment below would fail to compile if
        // QwpRoleMismatchException stopped extending HttpClientException --
        // a load-bearing relationship since callers catch the parent type.
        // A runtime instanceof check would always be true here, so it adds
        // nothing; this compile-time check is the meaningful guard.
        @SuppressWarnings("unused") HttpClientException base = ex;
        Assert.assertNull("nullable lastObserved must round-trip", ex.getLastObserved());
        Assert.assertEquals("any", ex.getTargetRole());
    }
}
