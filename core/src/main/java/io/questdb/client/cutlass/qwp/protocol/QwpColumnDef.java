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

package io.questdb.client.cutlass.qwp.protocol;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_BOOLEAN;
import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_CHAR;

/**
 * Represents a column definition in a QWP v1 schema.
 * <p>
 * This class is immutable and safe for caching.
 */
public final class QwpColumnDef {
    private final String name;
    private final byte typeCode;

    /**
     * Creates a column definition.
     *
     * @param name     the column name (UTF-8)
     * @param typeCode the QWP v1 type code (0x01-0x16)
     */
    public QwpColumnDef(String name, byte typeCode) {
        this.name = name;
        this.typeCode = typeCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QwpColumnDef that = (QwpColumnDef) o;
        return typeCode == that.typeCode &&
                name.equals(that.name);
    }

    /**
     * Gets the column name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the base type code.
     *
     * @return type code 0x01-0x16
     */
    public byte getTypeCode() {
        return typeCode;
    }

    /**
     * Gets the type name for display purposes.
     */
    public String getTypeName() {
        return QwpConstants.getTypeName(typeCode);
    }

    /**
     * Gets the wire type code.
     *
     * @return type code as sent on wire
     */
    public byte getWireTypeCode() {
        return typeCode;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + typeCode;
        return result;
    }

    @Override
    public String toString() {
        return name + ':' + getTypeName();
    }

    /**
     * Validates that this column definition has a valid type code.
     *
     * @throws IllegalArgumentException if type code is invalid
     */
    public void validate() {
        // Valid type codes: TYPE_BOOLEAN (0x01) through TYPE_CHAR (0x16)
        // This includes all basic types, arrays, decimals, and char
        boolean valid = (typeCode >= TYPE_BOOLEAN && typeCode <= TYPE_CHAR);
        if (!valid) {
            throw new IllegalArgumentException(
                    "invalid column type code: 0x" + Integer.toHexString(typeCode)
            );
        }
    }
}
