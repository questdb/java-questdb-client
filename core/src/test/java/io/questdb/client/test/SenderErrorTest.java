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

import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class SenderErrorTest {

    @Test
    public void testAllCategoriesEnumerable() {
        // Pin the public enum values — adding/removing requires a deliberate spec change
        // (and an update to wire-classification mapping in the I/O loop).
        SenderError.Category[] cats = SenderError.Category.values();
        Assert.assertEquals(8, cats.length);
        Assert.assertEquals(SenderError.Category.SCHEMA_MISMATCH, SenderError.Category.valueOf("SCHEMA_MISMATCH"));
        Assert.assertEquals(SenderError.Category.PARSE_ERROR, SenderError.Category.valueOf("PARSE_ERROR"));
        Assert.assertEquals(SenderError.Category.INTERNAL_ERROR, SenderError.Category.valueOf("INTERNAL_ERROR"));
        Assert.assertEquals(SenderError.Category.SECURITY_ERROR, SenderError.Category.valueOf("SECURITY_ERROR"));
        Assert.assertEquals(SenderError.Category.WRITE_ERROR, SenderError.Category.valueOf("WRITE_ERROR"));
        Assert.assertEquals(SenderError.Category.NOT_WRITABLE, SenderError.Category.valueOf("NOT_WRITABLE"));
        Assert.assertEquals(SenderError.Category.PROTOCOL_VIOLATION, SenderError.Category.valueOf("PROTOCOL_VIOLATION"));
        Assert.assertEquals(SenderError.Category.UNKNOWN, SenderError.Category.valueOf("UNKNOWN"));
    }

    @Test
    public void testAllPoliciesEnumerable() {
        SenderError.Policy[] policies = SenderError.Policy.values();
        Assert.assertEquals(3, policies.length);
        Assert.assertEquals(SenderError.Policy.RETRIABLE, SenderError.Policy.valueOf("RETRIABLE"));
        Assert.assertEquals(SenderError.Policy.RETRIABLE_OTHER, SenderError.Policy.valueOf("RETRIABLE_OTHER"));
        Assert.assertEquals(SenderError.Policy.TERMINAL, SenderError.Policy.valueOf("TERMINAL"));
    }

    @Test
    public void testFieldsExposedViaGetters() {
        long t = System.nanoTime();
        SenderError e = new SenderError(
                SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.RETRIABLE,
                0x03,
                "column 'price' missing",
                42L,
                100L,
                104L,
                "trades",
                t
        );

        Assert.assertEquals(SenderError.Category.SCHEMA_MISMATCH, e.getCategory());
        Assert.assertEquals(SenderError.Policy.RETRIABLE, e.getAppliedPolicy());
        Assert.assertEquals(0x03, e.getServerStatusByte());
        Assert.assertEquals("column 'price' missing", e.getServerMessage());
        Assert.assertEquals(42L, e.getMessageSequence());
        Assert.assertEquals(100L, e.getFromFsn());
        Assert.assertEquals(104L, e.getToFsn());
        Assert.assertEquals("trades", e.getTableName());
        Assert.assertEquals(t, e.getDetectedAtNanos());
    }

    @Test
    public void testHandlerIsFunctionalInterface() {
        AtomicReference<SenderError> received = new AtomicReference<>();
        SenderErrorHandler h = received::set;
        SenderError e = new SenderError(
                SenderError.Category.UNKNOWN,
                SenderError.Policy.TERMINAL,
                0x7F,
                "weird",
                0L, 0L, 0L, null, 0L
        );
        h.onError(e);
        Assert.assertSame(e, received.get());
    }

    @Test
    public void testNullableFieldsAccepted() {
        SenderError e = new SenderError(
                SenderError.Category.PROTOCOL_VIOLATION,
                SenderError.Policy.TERMINAL,
                SenderError.NO_STATUS_BYTE,
                null, // serverMessage
                SenderError.NO_MESSAGE_SEQUENCE,
                10L,
                20L,
                null, // tableName: multi-table batch
                0L
        );
        Assert.assertNull(e.getServerMessage());
        Assert.assertNull(e.getTableName());
        Assert.assertEquals(SenderError.NO_STATUS_BYTE, e.getServerStatusByte());
        Assert.assertEquals(SenderError.NO_MESSAGE_SEQUENCE, e.getMessageSequence());
    }

    @Test
    public void testServerExceptionIsLineSenderException() {
        SenderError e = new SenderError(
                SenderError.Category.PARSE_ERROR,
                SenderError.Policy.TERMINAL,
                0x05,
                "bad frame",
                1L, 1L, 1L, null, 0L
        );
        // Ensures existing catch blocks for LineSenderException continue to work.
        LineSenderException ex = new LineSenderServerException(e);
        //noinspection ConstantValue
        Assert.assertTrue(ex instanceof LineSenderServerException);
    }

    @Test
    public void testServerExceptionMessageMentionsCategoryStatusFsn() {
        SenderError e = new SenderError(
                SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.TERMINAL,
                0x03,
                "no such column 'foo'",
                7L,
                10L,
                10L,
                "trades",
                0L
        );
        String msg = new LineSenderServerException(e).getMessage();
        Assert.assertTrue(msg, msg.contains("SCHEMA_MISMATCH"));
        Assert.assertTrue(msg, msg.contains("0x3"));
        Assert.assertTrue(msg, msg.contains("[10,10]"));
        Assert.assertTrue(msg, msg.contains("trades"));
        Assert.assertTrue(msg, msg.contains("seq=7"));
        Assert.assertTrue(msg, msg.contains("no such column 'foo'"));
    }

    @Test
    public void testServerExceptionMessageOmitsSentinelFields() {
        SenderError e = new SenderError(
                SenderError.Category.PROTOCOL_VIOLATION,
                SenderError.Policy.TERMINAL,
                SenderError.NO_STATUS_BYTE,
                "ws-close[1002]: bad frame",
                SenderError.NO_MESSAGE_SEQUENCE,
                100L,
                105L,
                null,
                0L
        );
        String msg = new LineSenderServerException(e).getMessage();
        Assert.assertTrue(msg, msg.contains("PROTOCOL_VIOLATION"));
        Assert.assertTrue(msg, msg.contains("[100,105]"));
        Assert.assertTrue(msg, msg.contains("ws-close[1002]"));
        Assert.assertFalse("status= should be elided when no status byte present: " + msg,
                msg.contains("status="));
        Assert.assertFalse("seq= should be elided when no sequence present: " + msg,
                msg.contains("seq="));
        Assert.assertFalse("table= should be elided when no table attribution: " + msg,
                msg.contains("table="));
    }

    @Test
    public void testServerExceptionWrapsSenderError() {
        SenderError e = new SenderError(
                SenderError.Category.SECURITY_ERROR,
                SenderError.Policy.TERMINAL,
                0x08,
                "permission denied",
                12L,
                200L,
                200L,
                "secure_table",
                0L
        );
        LineSenderServerException ex = new LineSenderServerException(e);
        Assert.assertSame(e, ex.getServerError());
    }

    @Test
    public void testToStringContainsLoadBearingFields() {
        SenderError e = new SenderError(
                SenderError.Category.WRITE_ERROR,
                SenderError.Policy.RETRIABLE,
                0x09,
                "table not accepting writes",
                7L,
                500L,
                500L,
                "events",
                0L
        );
        String s = e.toString();
        Assert.assertTrue(s, s.contains("WRITE_ERROR"));
        Assert.assertTrue(s, s.contains("RETRIABLE"));
        Assert.assertTrue(s, s.contains("0x9"));
        Assert.assertTrue(s, s.contains("[500,500]"));
        Assert.assertTrue(s, s.contains("events"));
        Assert.assertTrue(s, s.contains("table not accepting writes"));
    }

    @Test
    public void testToStringRendersMultiTableTableNameAsMulti() {
        SenderError e = new SenderError(
                SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.RETRIABLE,
                0x03,
                "msg",
                1L, 1L, 1L,
                null,
                0L
        );
        Assert.assertTrue(e.toString().contains("table=(multi)"));
    }
}
