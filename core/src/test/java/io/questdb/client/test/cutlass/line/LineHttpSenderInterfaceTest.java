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

package io.questdb.client.test.cutlass.line;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.bytes.DirectByteSlice;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Exercises {@link Sender#reset()} and {@link Sender#bufferView()} on the
 * HTTP transport. The HTTP sender connects lazily on the first
 * {@link Sender#flush()}, so building it against a port that nobody listens
 * on lets us drive the public buffer-management API without a server.
 */
public class LineHttpSenderInterfaceTest {

    @Test
    public void testBufferViewReflectsAccumulatedRows() {
        try (Sender sender = Sender.fromConfig("http::addr=127.0.0.1:1;auto_flush=off;protocol_version=1;")) {
            DirectByteSlice empty = sender.bufferView();
            Assert.assertEquals(0, empty.size());

            sender.table("t").longColumn("v", 7L).atNow();
            DirectByteSlice afterRow = sender.bufferView();
            Assert.assertTrue(
                    "buffer should grow after appending a row, was " + afterRow.size(),
                    afterRow.size() > 0);
        }
    }

    @Test
    public void testRejectedExplicitTimestampWritesNothing() {
        // at(timestamp) validates BEFORE it writes, and that ordering is the whole reason it does not simply
        // delegate to atNow(): a row rejected after the timestamp went into the buffer would leave those bytes
        // for the NEXT row to inherit, splicing a stray " 1700000000000000000" into an otherwise valid line.
        // Both rejected states are covered - no table name, and a table with no symbols or columns - over V1
        // and V2 (V3 inherits V2's at()), for both overloads.
        for (int version = 1; version <= 2; version++) {
            for (int overload = 0; overload < 2; overload++) {
                String config = "http::addr=127.0.0.1:1;auto_flush=off;protocol_version=" + version + ';';
                String where = "[version=" + version + " overload=" + overload + ']';

                try (Sender sender = Sender.fromConfig(config)) {
                    try {
                        at(sender, overload);
                        Assert.fail("at() with no table name must be rejected " + where);
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("no table name was provided"));
                    }
                    Assert.assertEquals("a rejected at() must not write a timestamp " + where,
                            0, sender.bufferView().size());
                }

                try (Sender sender = Sender.fromConfig(config)) {
                    sender.table("t");
                    int afterTableName = sender.bufferView().size();
                    Assert.assertTrue("preconditions: table() writes " + where, afterTableName > 0);
                    try {
                        at(sender, overload);
                        Assert.fail("at() with no symbols or columns must be rejected " + where);
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(),
                                e.getMessage().contains("no symbols or columns were provided"));
                    }
                    Assert.assertEquals("a rejected at() must not write a timestamp " + where,
                            afterTableName, sender.bufferView().size());

                    // and the half-built row is still intact: finishing it properly must work
                    sender.longColumn("v", 1L);
                    sender.atNow();
                    Assert.assertTrue("the row must still complete after the rejection " + where,
                            sender.bufferView().size() > afterTableName);
                }
            }
        }
    }

    @Test
    public void testResetClearsBufferAndAllowsNewRows() {
        try (Sender sender = Sender.fromConfig("http::addr=127.0.0.1:1;auto_flush=off;protocol_version=1;")) {
            sender.table("t").longColumn("v", 1L).atNow();
            sender.table("t").longColumn("v", 2L).atNow();
            int filled = sender.bufferView().size();
            Assert.assertTrue("preconditions: buffer should be non-empty", filled > 0);

            sender.reset();
            Assert.assertEquals(
                    "reset() must zero the buffer",
                    0, sender.bufferView().size());

            // Sender must remain usable post-reset: append a fresh row and
            // see the buffer grow again.
            sender.table("t").longColumn("v", 3L).atNow();
            Assert.assertTrue(
                    "sender must keep accepting rows after reset",
                    sender.bufferView().size() > 0);
        }
    }

    private static void at(Sender sender, int overload) {
        if (overload == 0) {
            sender.at(1_700_000_000_000_000_000L, ChronoUnit.NANOS);
        } else {
            sender.at(Instant.ofEpochMilli(1_700_000_000_000L));
        }
    }
}
