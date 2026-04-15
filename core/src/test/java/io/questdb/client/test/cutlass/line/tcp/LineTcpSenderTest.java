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

package io.questdb.client.test.cutlass.line.tcp;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineChannel;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.LineTcpSenderV2;
import io.questdb.client.cutlass.line.LineTcpSenderV3;
import io.questdb.client.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.client.test.AbstractTest;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;

import static io.questdb.client.test.tools.TestUtils.assertContains;
import static org.junit.Assert.*;

/**
 * Unit tests for LineTcpSender.
 * <p>
 * These tests use DummyLineChannel/ByteChannel (no server needed).
 * Integration tests have been migrated to core module's LineTcpBootstrapTest.
 */
public class LineTcpSenderTest extends AbstractTest {
    protected static final Consumer<Sender> SET_TABLE_NAME_ACTION = s -> s.table("test_mytable");

    @Test
    public void testCannotStartNewRowBeforeClosingTheExistingAfterValidationError() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("test_mytable");
            try {
                sender.boolColumn("col\n", true);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "name contains an illegal char");
            }
            try {
                sender.table("test_mytable");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "duplicated table");
            }
        }
        assertFalse(channel.contain(new byte[]{(byte) '\n'}));
    }

    @Test
    public void testCloseIdempotent() {
        DummyLineChannel channel = new DummyLineChannel();
        LineTcpSenderV2 sender = new LineTcpSenderV2(channel, 1000, 127);
        sender.close();
        sender.close();
        assertEquals(1, channel.closeCounter);
    }

    @Test
    public void testControlCharInColumnName() {
        assertControlCharacterException();
    }

    @Test
    public void testControlCharInTableName() {
        assertControlCharacterException();
    }

    @Test
    public void testInsertDecimalTextFormatInvalid() {
        try (Sender sender = new LineTcpSenderV3(new DummyLineChannel(), 4096, 127)) {
            sender.table("test");
            // Test invalid characters
            try {
                sender.decimalColumn("value", "abc");
                fail("Letters should throw exception");
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test multiple dots
            try {
                sender.decimalColumn("value", "12.34.56");
                fail("Multiple dots should throw exception");
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test multiple signs
            try {
                sender.decimalColumn("value", "+-123");
                fail("Multiple signs should throw exception");
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test special characters
            try {
                sender.decimalColumn("value", "12$34");
                fail("Special characters should throw exception");
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test empty decimal
            try {
                sender.decimalColumn("value", "");
                fail("Empty string should throw exception");
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test invalid exponent
            try {
                sender.decimalColumn("value", "1.23eABC");
                fail("Invalid exponent should throw exception");
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test incomplete exponent
            try {
                sender.decimalColumn("value", "1.23e");
                fail("Incomplete exponent should throw exception");
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }
        }
    }

    @Test
    public void testUnfinishedRowDoesNotContainNewLine() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("test_mytable");
            sender.boolColumn("col\n", true);
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "name contains an illegal char");
        }
        assertFalse(channel.contain(new byte[]{(byte) '\n'}));
    }

    @Test
    public void testUseAfterClose_atMicros() {
        assertExceptionOnClosedSender(s -> {
            s.table("test_mytable");
            s.longColumn("col", 42);
        }, s -> s.at(MicrosecondClockImpl.INSTANCE.getTicks(), ChronoUnit.MICROS));
    }

    @Test
    public void testUseAfterClose_atNow() {
        assertExceptionOnClosedSender(s -> {
            s.table("test_mytable");
            s.longColumn("col", 42);
        }, Sender::atNow);
    }

    @Test
    public void testUseAfterClose_boolColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.boolColumn("col", true));
    }

    @Test
    public void testUseAfterClose_doubleColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.doubleColumn("col", 42.42));
    }

    @Test
    public void testUseAfterClose_flush() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, Sender::flush);
    }

    @Test
    public void testUseAfterClose_longColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.longColumn("col", 42));
    }

    @Test
    public void testUseAfterClose_stringColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.stringColumn("col", "val"));
    }

    @Test
    public void testUseAfterClose_symbol() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.symbol("sym", "val"));
    }

    @Test
    public void testUseAfterClose_table() {
        assertExceptionOnClosedSender();
    }

    @Test
    public void testUseAfterClose_tsColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.timestampColumn("col", 0, ChronoUnit.MICROS));
    }

    private static void assertControlCharacterException() {
        DummyLineChannel channel = new DummyLineChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("test_mytable");
            sender.boolColumn("col\u0001", true);
            fail("control character in column or table name must throw exception");
        } catch (LineSenderException e) {
            String m = e.getMessage();
            assertContains(m, "name contains an illegal char");
            assertNoControlCharacter(m);
        }
    }

    private static void assertExceptionOnClosedSender() {
        assertExceptionOnClosedSender(s -> {
        }, LineTcpSenderTest.SET_TABLE_NAME_ACTION);
    }

    private static void assertExceptionOnClosedSender(Consumer<Sender> beforeCloseAction,
                                                      Consumer<Sender> afterCloseAction) {
        DummyLineChannel channel = new DummyLineChannel();
        Sender sender = new LineTcpSenderV2(channel, 1000, 127);
        beforeCloseAction.accept(sender);
        sender.close();
        try {
            afterCloseAction.accept(sender);
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "sender already closed");
        }
    }

    private static void assertNoControlCharacter(CharSequence m) {
        for (int i = 0, n = m.length(); i < n; i++) {
            assertFalse(Character.isISOControl(m.charAt(i)));
        }
    }

    private static class DummyLineChannel implements LineChannel {
        private int closeCounter;

        @Override
        public void close() {
            closeCounter++;
        }

        @Override
        public int errno() {
            return 0;
        }

        @Override
        public int receive(long ptr, int len) {
            return 0;
        }

        @Override
        public void send(long ptr, int len) {

        }
    }
}
