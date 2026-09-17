/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.std.LowerCaseCharSequenceIntHashMap;

/**
 * Immutable response to a QWP schema DESCRIBE control request. The snapshot
 * owns the case-insensitive column-name index that the decoder builds while
 * it validates name uniqueness, so consumers never rebuild it.
 */
public final class QwpSchemaResponse {
    private final LowerCaseCharSequenceIntHashMap columnIndexes;
    private final Column[] columns;
    private final int designatedIndex;
    private final int result;
    private final int tableId;
    private final long metadataVersion;
    private final long requestId;

    QwpSchemaResponse(
            long requestId,
            int result,
            int tableId,
            long metadataVersion,
            int designatedIndex,
            Column[] columns,
            LowerCaseCharSequenceIntHashMap columnIndexes
    ) {
        this.requestId = requestId;
        this.result = result;
        this.tableId = tableId;
        this.metadataVersion = metadataVersion;
        this.designatedIndex = designatedIndex;
        this.columns = columns;
        this.columnIndexes = columnIndexes;
    }

    public int getColumnCount() {
        return columns.length;
    }

    /** Returns the index of the case-insensitively named column, or -1. */
    public int getColumnIndex(CharSequence name) {
        return columnIndexes.get(name);
    }

    public String getColumnName(int index) {
        return columns[index].name;
    }

    public int getColumnType(int index) {
        return columns[index].type;
    }

    /**
     * Returns whether the server supplied an extension parameter block for
     * this column's type. The current client safely skips those bytes but must
     * not interpret a parameterized type as an unparameterized one.
     */
    public boolean hasColumnExtensionParameters(int index) {
        return columns[index].hasTypeParameters;
    }

    public int getDesignatedIndex() {
        return designatedIndex;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    /** Returns the positive DESCRIBE id, or zero for named write feedback. */
    public long getRequestId() {
        return requestId;
    }

    public int getResult() {
        return result;
    }

    public int getTableId() {
        return tableId;
    }

    /**
     * Returns whether {@code name} resolves to an equivalent conversion target
     * in both snapshots: same presence, designated-timestamp role, column type,
     * and extension-parameter flag. A null {@code name} selects the designated
     * timestamp. Column position is irrelevant.
     */
    public boolean hasSameRelevantTarget(QwpSchemaResponse other, CharSequence name) {
        int current = name == null ? designatedIndex : getColumnIndex(name);
        int next = name == null ? other.designatedIndex : other.getColumnIndex(name);
        if (current < 0 || next < 0) {
            return current < 0 && next < 0;
        }
        return (current == designatedIndex) == (next == other.designatedIndex)
                && columns[current].type == other.columns[next].type
                && columns[current].hasTypeParameters == other.columns[next].hasTypeParameters;
    }

    public boolean hasSchema() {
        return result == QwpSchemaProtocol.RESULT_KNOWN;
    }

    static final class Column {
        private final boolean hasTypeParameters;
        private final String name;
        private final int type;

        Column(String name, int type, boolean hasTypeParameters) {
            this.name = name;
            this.type = type;
            this.hasTypeParameters = hasTypeParameters;
        }
    }
}
