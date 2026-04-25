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

package io.questdb.client.cutlass.qwp.client;

/**
 * Per-column metadata recorded when a schema is registered on a client connection.
 * Internal to the QWP client; callers read column attributes through the forwarder
 * methods on {@link QwpColumnBatch} (for example {@link QwpColumnBatch#getColumnName}).
 */
class QwpEgressColumnInfo {
    String name;
    int precisionBits; // valid only for GEOHASH; set per-batch by QwpResultBatchDecoder.parseColumn
    int scale;         // valid only for DECIMAL*;  set per-batch by QwpResultBatchDecoder.parseColumn
    byte wireType;

    void of(String name, byte wireType) {
        this.name = name;
        this.wireType = wireType;
        // scale / precisionBits come from the per-batch column data section, not the schema.
        // Reset here in case a schema slot is reused for a different schema.
        this.scale = 0;
        this.precisionBits = 0;
    }
}
