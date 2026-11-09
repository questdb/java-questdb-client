/*******************************************************************************
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

import io.questdb.client.cutlass.qwp.client.DurableAckTiers;
import org.junit.Test;

import static io.questdb.client.cutlass.qwp.client.DurableAckTiers.LEGACY_TRUE;
import static io.questdb.client.cutlass.qwp.client.DurableAckTiers.LOCAL;
import static io.questdb.client.cutlass.qwp.client.DurableAckTiers.NONE;
import static io.questdb.client.cutlass.qwp.client.DurableAckTiers.REPLICATED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the {@link DurableAckTiers} bitmask helpers: config-value
 * parsing and printing, the request/confirmation wire tokens, and the
 * trim-trigger predicate.
 */
public class DurableAckTiersTest {

    @Test
    public void testConfigValueInverseOfParse() {
        // configValue . parseConfigValue is identity for every accepted value
        // (modulo case and the replicated,local ordering alias).
        String[] canonical = {"off", "on", "local", "replicated", "local,replicated"};
        for (String v : canonical) {
            assertEquals(v, DurableAckTiers.configValue(DurableAckTiers.parseConfigValue(v)));
        }
        // The reversed alias normalizes to the canonical order.
        assertEquals("local,replicated",
                DurableAckTiers.configValue(DurableAckTiers.parseConfigValue("replicated,local")));
    }

    @Test
    public void testConfigValuePrintsTierSets() {
        assertEquals("off", DurableAckTiers.configValue(NONE));
        assertEquals("local", DurableAckTiers.configValue(LOCAL));
        assertEquals("replicated", DurableAckTiers.configValue(REPLICATED));
        assertEquals("local,replicated", DurableAckTiers.configValue(LOCAL | REPLICATED));
        assertEquals("on", DurableAckTiers.configValue(REPLICATED | LEGACY_TRUE));
    }

    @Test
    public void testExpectedConfirmToken() {
        // Explicit requests are confirmed by their own token echoed back;
        // the legacy request is confirmed by "enabled"; no request expects
        // no confirmation.
        assertNull(DurableAckTiers.expectedConfirmToken(NONE));
        assertEquals("local", DurableAckTiers.expectedConfirmToken(LOCAL));
        assertEquals("replicated", DurableAckTiers.expectedConfirmToken(REPLICATED));
        assertEquals("local,replicated", DurableAckTiers.expectedConfirmToken(LOCAL | REPLICATED));
        assertEquals("enabled", DurableAckTiers.expectedConfirmToken(REPLICATED | LEGACY_TRUE));
    }

    @Test
    public void testHasLocalHasReplicated() {
        assertFalse(DurableAckTiers.hasLocal(NONE));
        assertFalse(DurableAckTiers.hasReplicated(NONE));
        assertTrue(DurableAckTiers.hasLocal(LOCAL));
        assertFalse(DurableAckTiers.hasReplicated(LOCAL));
        assertFalse(DurableAckTiers.hasLocal(REPLICATED));
        assertTrue(DurableAckTiers.hasReplicated(REPLICATED));
        assertTrue(DurableAckTiers.hasLocal(LOCAL | REPLICATED));
        assertTrue(DurableAckTiers.hasReplicated(LOCAL | REPLICATED));
        assertTrue(DurableAckTiers.hasReplicated(REPLICATED | LEGACY_TRUE));
    }

    @Test
    public void testIsTrimOnLocalAck() {
        // Only a local-without-replicated request trims on the local ack;
        // any set that includes the replicated tier trims on the replicated
        // ack (strongest requested guarantee wins).
        assertTrue(DurableAckTiers.isTrimOnLocalAck(LOCAL));
        assertFalse(DurableAckTiers.isTrimOnLocalAck(NONE));
        assertFalse(DurableAckTiers.isTrimOnLocalAck(REPLICATED));
        assertFalse(DurableAckTiers.isTrimOnLocalAck(LOCAL | REPLICATED));
        assertFalse(DurableAckTiers.isTrimOnLocalAck(REPLICATED | LEGACY_TRUE));
    }

    @Test
    public void testParseConfigValueAccepted() {
        assertEquals(NONE, DurableAckTiers.parseConfigValue("off"));
        assertEquals(REPLICATED | LEGACY_TRUE, DurableAckTiers.parseConfigValue("on"));
        assertEquals(LOCAL, DurableAckTiers.parseConfigValue("local"));
        assertEquals(REPLICATED, DurableAckTiers.parseConfigValue("replicated"));
        assertEquals(LOCAL | REPLICATED, DurableAckTiers.parseConfigValue("local,replicated"));
        assertEquals(LOCAL | REPLICATED, DurableAckTiers.parseConfigValue("replicated,local"));
    }

    @Test
    public void testParseConfigValueCaseInsensitiveAndTrimmed() {
        assertEquals(REPLICATED | LEGACY_TRUE, DurableAckTiers.parseConfigValue("ON"));
        assertEquals(LOCAL, DurableAckTiers.parseConfigValue("Local"));
        assertEquals(LOCAL | REPLICATED, DurableAckTiers.parseConfigValue("LOCAL,REPLICATED"));
        assertEquals(NONE, DurableAckTiers.parseConfigValue("  off  "));
    }

    @Test
    public void testParseConfigValueRejected() {
        // The whole value must match one of the accepted spellings; spaces
        // inside a list, empty and unknown tokens, and null all read as -1.
        assertEquals(-1, DurableAckTiers.parseConfigValue(null));
        assertEquals(-1, DurableAckTiers.parseConfigValue(""));
        assertEquals(-1, DurableAckTiers.parseConfigValue("true"));
        assertEquals(-1, DurableAckTiers.parseConfigValue("yes"));
        assertEquals(-1, DurableAckTiers.parseConfigValue("enabled"));
        assertEquals(-1, DurableAckTiers.parseConfigValue("local, replicated"));
        assertEquals(-1, DurableAckTiers.parseConfigValue("local,"));
        assertEquals(-1, DurableAckTiers.parseConfigValue("local,local"));
        assertEquals(-1, DurableAckTiers.parseConfigValue("remote"));
    }

    @Test
    public void testRequestHeaderValue() {
        // The legacy set is sent as the literal "true"; explicit sets send
        // their token list; no request means no header.
        assertNull(DurableAckTiers.requestHeaderValue(NONE));
        assertEquals("local", DurableAckTiers.requestHeaderValue(LOCAL));
        assertEquals("replicated", DurableAckTiers.requestHeaderValue(REPLICATED));
        assertEquals("local,replicated", DurableAckTiers.requestHeaderValue(LOCAL | REPLICATED));
        assertEquals("true", DurableAckTiers.requestHeaderValue(REPLICATED | LEGACY_TRUE));
    }
}
