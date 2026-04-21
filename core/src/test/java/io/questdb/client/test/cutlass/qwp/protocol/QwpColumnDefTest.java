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

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QwpColumnDefTest {

    @Test
    public void testValidateAcceptsAllValidTypes() {
        byte[] validTypes = {
                QwpConstants.TYPE_BOOLEAN,
                QwpConstants.TYPE_BYTE,
                QwpConstants.TYPE_SHORT,
                QwpConstants.TYPE_INT,
                QwpConstants.TYPE_LONG,
                QwpConstants.TYPE_FLOAT,
                QwpConstants.TYPE_DOUBLE,
                QwpConstants.TYPE_SYMBOL,
                QwpConstants.TYPE_TIMESTAMP,
                QwpConstants.TYPE_DATE,
                QwpConstants.TYPE_UUID,
                QwpConstants.TYPE_LONG256,
                QwpConstants.TYPE_GEOHASH,
                QwpConstants.TYPE_VARCHAR,
                QwpConstants.TYPE_TIMESTAMP_NANOS,
                QwpConstants.TYPE_DOUBLE_ARRAY,
                QwpConstants.TYPE_LONG_ARRAY,
                QwpConstants.TYPE_DECIMAL64,
                QwpConstants.TYPE_DECIMAL128,
                QwpConstants.TYPE_DECIMAL256,
                QwpConstants.TYPE_CHAR,
        };
        for (byte type : validTypes) {
            QwpColumnDef col = new QwpColumnDef("col", type);
            col.validate(); // must not throw
        }
    }

    @Test
    public void testValidateCharType() {
        // TYPE_CHAR (0x16) must pass validation
        QwpColumnDef col = new QwpColumnDef("ch", QwpConstants.TYPE_CHAR);
        col.validate();
        assertEquals("CHAR", col.getTypeName());
        assertEquals(QwpConstants.TYPE_CHAR, col.getTypeCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateRejectsHighBit() {
        // The high bit is not a valid part of the type code
        byte badType = (byte) (QwpConstants.TYPE_CHAR | 0x80);
        QwpColumnDef col = new QwpColumnDef("ch", badType);
        col.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateRejectsInvalidType() {
        QwpColumnDef col = new QwpColumnDef("bad", (byte) 0x17);
        col.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateRejectsZeroType() {
        QwpColumnDef col = new QwpColumnDef("bad", (byte) 0x00);
        col.validate();
    }
}
