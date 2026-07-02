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

import io.questdb.client.std.LongList;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Utf8SequenceObjHashMap;
import io.questdb.client.std.str.DirectUtf8String;
import io.questdb.client.std.str.Utf8s;
import org.jetbrains.annotations.TestOnly;

import java.nio.charset.StandardCharsets;

/**
 * Binary response format for WebSocket QWP v1 protocol.
 * <p>
 * STATUS_OK response format (little-endian):
 * <pre>
 * +--------+----------+------------+--------------------------------------+
 * | status | sequence | tableCount | table entries                         |
 * | 1 byte | 8 bytes  | 2 bytes    | [nameLen(2)+name(N)+seqTxn(8)] * cnt |
 * +--------+----------+------------+--------------------------------------+
 * </pre>
 * <p>
 * STATUS_DURABLE_ACK response format:
 * <pre>
 * +--------+------------+--------------------------------------+
 * | status | tableCount | table entries                         |
 * | 1 byte | 2 bytes    | [nameLen(2)+name(N)+seqTxn(8)] * cnt |
 * +--------+------------+--------------------------------------+
 * </pre>
 * <p>
 * Error response format:
 * <pre>
 * +--------+----------+------------------+
 * | status | sequence | error            |
 * | 1 byte | 8 bytes  | 2 bytes + UTF-8  |
 * +--------+----------+------------------+
 * </pre>
 */
public class WebSocketResponse {

    public static final int MAX_ERROR_MESSAGE_LENGTH = 1024;
    public static final int MIN_DURABLE_ACK_SIZE = 3; // status + tableCount
    public static final int MIN_ERROR_RESPONSE_SIZE = 11; // status + sequence + error length
    public static final int MIN_OK_RESPONSE_SIZE = 11; // status + sequence + tableCount
    /**
     * Per-table durable-upload acknowledgment. Emitted by servers where
     * primary replication is enabled and the connection opted in via
     * X-QWP-Request-Durable-Ack. Payload: status + tableCount + per-table
     * entries (nameLen + name + seqTxn).
     */
    public static final byte STATUS_DURABLE_ACK = 0x02;
    public static final byte STATUS_INTERNAL_ERROR = 0x06;
    /**
     * Node cannot serve writes (read-only replica / demoting primary). Reserved:
     * current servers signal this with a reconnect-eligible close instead of a
     * NACK; mapped so a future server that NACKs it mid-stream gets
     * retriable-with-rotation treatment instead of falling into UNKNOWN.
     */
    public static final byte STATUS_NOT_WRITABLE = 0x0C;
    // Status codes (must match https://questdb.com/docs/connect/wire-protocols/qwp-ingress-websocket/)
    public static final byte STATUS_OK = 0x00;
    public static final byte STATUS_PARSE_ERROR = 0x05;
    public static final byte STATUS_SCHEMA_MISMATCH = 0x03;
    public static final byte STATUS_SECURITY_ERROR = 0x08;
    public static final byte STATUS_WRITE_ERROR = 0x09;
    private final DirectUtf8String lookupKey = new DirectUtf8String();
    // Caches decoded table names so the same native-memory byte sequence resolves to
    // the same interned String across frames. First sight of a name allocates a String
    // (decoded via Utf8s.stringFromUtf8Bytes) and an owned Utf8String map key; every
    // subsequent sight is allocation-free.
    private final Utf8SequenceObjHashMap<String> tableNameCache = new Utf8SequenceObjHashMap<>();
    private final ObjList<String> tableNames = new ObjList<>();
    private final LongList tableSeqTxns = new LongList();
    private String errorMessage;
    private int errorMessageUtf8Length;
    private long sequence;
    private byte status;

    public WebSocketResponse() {
        this.status = STATUS_OK;
        this.sequence = 0;
        this.errorMessage = null;
        this.errorMessageUtf8Length = -1;
    }

    /**
     * Creates a durable-upload ACK response with a single table entry.
     */
    @TestOnly
    public static WebSocketResponse durableAck(String tableName, long seqTxn) {
        WebSocketResponse response = new WebSocketResponse();
        response.status = STATUS_DURABLE_ACK;
        response.sequence = -1;
        response.tableNames.add(tableName);
        response.tableSeqTxns.add(seqTxn);
        return response;
    }

    /**
     * Creates an error response.
     */
    @TestOnly
    public static WebSocketResponse error(long sequence, byte status, String errorMessage) {
        WebSocketResponse response = new WebSocketResponse();
        response.status = status;
        response.sequence = sequence;
        response.errorMessage = errorMessage;
        response.errorMessageUtf8Length = -1;
        return response;
    }

    /**
     * Validates binary response framing without allocating.
     *
     * @param ptr    response buffer pointer
     * @param length response frame payload length
     * @return true if payload structure is valid
     */
    public static boolean isStructurallyValid(long ptr, int length) {
        if (length < 1) {
            return false;
        }

        byte status = Unsafe.getUnsafe().getByte(ptr);
        if (status == STATUS_OK) {
            if (length < MIN_OK_RESPONSE_SIZE) {
                return false;
            }
            return validateTableEntries(ptr + 9, length - 9);
        }

        if (status == STATUS_DURABLE_ACK) {
            if (length < MIN_DURABLE_ACK_SIZE) {
                return false;
            }
            return validateTableEntries(ptr + 1, length - 1);
        }

        // Error response
        if (length < MIN_ERROR_RESPONSE_SIZE) {
            return false;
        }
        int msgLen = Unsafe.getUnsafe().getShort(ptr + 9) & 0xFFFF;
        return length == MIN_ERROR_RESPONSE_SIZE + msgLen;
    }

    /**
     * Creates a success response.
     */
    @TestOnly
    public static WebSocketResponse success(long sequence) {
        WebSocketResponse response = new WebSocketResponse();
        response.status = STATUS_OK;
        response.sequence = sequence;
        response.errorMessageUtf8Length = -1;
        return response;
    }

    /**
     * Returns the error message, or null for success responses.
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns the client batch sequence number (STATUS_OK and error frames),
     * or -1 for STATUS_DURABLE_ACK frames.
     */
    public long getSequence() {
        return sequence;
    }

    /**
     * Returns the raw status byte.
     */
    public byte getStatus() {
        return status;
    }

    /**
     * Returns a human-readable status name.
     */
    public String getStatusName() {
        switch (status) {
            case STATUS_OK:
                return "OK";
            case STATUS_DURABLE_ACK:
                return "DURABLE_ACK";
            case STATUS_PARSE_ERROR:
                return "PARSE_ERROR";
            case STATUS_SCHEMA_MISMATCH:
                return "SCHEMA_MISMATCH";
            case STATUS_WRITE_ERROR:
                return "WRITE_ERROR";
            case STATUS_SECURITY_ERROR:
                return "SECURITY_ERROR";
            case STATUS_INTERNAL_ERROR:
                return "INTERNAL_ERROR";
            case STATUS_NOT_WRITABLE:
                return "NOT_WRITABLE";
            default:
                return "UNKNOWN(" + (status & 0xFF) + ")";
        }
    }

    public int getTableEntryCount() {
        return tableNames.size();
    }

    public String getTableName(int index) {
        return tableNames.getQuick(index);
    }

    public long getTableSeqTxn(int index) {
        return tableSeqTxns.getQuick(index);
    }

    /**
     * Returns true when this is a per-table durable-upload ACK (STATUS_DURABLE_ACK).
     */
    public boolean isDurableAck() {
        return status == STATUS_DURABLE_ACK;
    }

    /**
     * Returns true if this is a success response (STATUS_OK).
     */
    public boolean isSuccess() {
        return status == STATUS_OK;
    }

    /**
     * Reads a response from native memory.
     *
     * @param ptr    source address
     * @param length available bytes
     * @return true if successfully parsed, false if not enough data
     */
    public boolean readFrom(long ptr, int length) {
        tableNames.clear();
        tableSeqTxns.clear();

        if (length < 1) {
            return false;
        }

        status = Unsafe.getUnsafe().getByte(ptr);

        if (status == STATUS_OK) {
            if (length < MIN_OK_RESPONSE_SIZE) {
                return false;
            }
            sequence = Unsafe.getUnsafe().getLong(ptr + 1);
            errorMessage = null;
            errorMessageUtf8Length = -1;
            return readTableEntries(ptr + 9, length - 9);
        }

        if (status == STATUS_DURABLE_ACK) {
            if (length < MIN_DURABLE_ACK_SIZE) {
                return false;
            }
            sequence = -1;
            errorMessage = null;
            errorMessageUtf8Length = -1;
            return readTableEntries(ptr + 1, length - 1);
        }

        // Error response
        if (length < MIN_ERROR_RESPONSE_SIZE) {
            return false;
        }
        sequence = Unsafe.getUnsafe().getLong(ptr + 1);
        int offset = 9;
        int msgLen = Unsafe.getUnsafe().getShort(ptr + offset) & 0xFFFF;
        offset += 2;
        if (length < offset + msgLen) {
            return false;
        }
        if (msgLen > 0) {
            byte[] msgBytes = new byte[msgLen];
            for (int i = 0; i < msgLen; i++) {
                msgBytes[i] = Unsafe.getUnsafe().getByte(ptr + offset + i);
            }
            errorMessage = new String(msgBytes, StandardCharsets.UTF_8);
            errorMessageUtf8Length = -1;
        } else {
            errorMessage = null;
            errorMessageUtf8Length = 0;
        }
        return true;
    }

    /**
     * Calculates the serialized size of this response.
     */
    @TestOnly
    public int serializedSize() {
        if (status == STATUS_OK) {
            return MIN_OK_RESPONSE_SIZE + tableEntriesSize();
        }
        if (status == STATUS_DURABLE_ACK) {
            return MIN_DURABLE_ACK_SIZE + tableEntriesSize();
        }
        return MIN_ERROR_RESPONSE_SIZE + getErrorMessageUtf8Length();
    }

    @Override
    public String toString() {
        if (isSuccess()) {
            return "WebSocketResponse{status=OK, seq=" + sequence + ", tables=" + tableNames.size() + "}";
        } else if (isDurableAck()) {
            return "WebSocketResponse{status=DURABLE_ACK, tables=" + tableNames.size() + "}";
        } else {
            return "WebSocketResponse{status=" + getStatusName() + ", seq=" + sequence +
                    ", error=" + errorMessage + "}";
        }
    }

    /**
     * Writes this response to native memory.
     *
     * @param ptr destination address
     * @return number of bytes written
     */
    @TestOnly
    public int writeTo(long ptr) {
        int offset = 0;

        Unsafe.getUnsafe().putByte(ptr + offset, status);
        offset += 1;

        if (status == STATUS_OK) {
            Unsafe.getUnsafe().putLong(ptr + offset, sequence);
            offset += 8;
            offset += writeTableEntries(ptr + offset);
        } else if (status == STATUS_DURABLE_ACK) {
            offset += writeTableEntries(ptr + offset);
        } else {
            Unsafe.getUnsafe().putLong(ptr + offset, sequence);
            offset += 8;

            int msgLen = getErrorMessageUtf8Length();
            // Length prefix (2 bytes, little-endian)
            Unsafe.getUnsafe().putShort(ptr + offset, (short) msgLen);
            offset += 2;

            // Message bytes
            if (msgLen > 0) {
                offset += Utf8s.strCpyUtf8(errorMessage, ptr + offset, msgLen);
            }
        }
        return offset;
    }

    // Validates inline as it parses; returns false on truncation, lying-length
    // entries, empty table names, or trailing garbage. On false, tableNames /
    // tableSeqTxns may hold partial state, but the caller (readFrom) clears
    // both lists at the start of every call so partial state never leaks.
    private boolean readTableEntries(long ptr, int remaining) {
        if (remaining < 2) {
            return false;
        }
        int tableCount = Unsafe.getUnsafe().getShort(ptr) & 0xFFFF;
        int offset = 2;
        for (int i = 0; i < tableCount; i++) {
            if (remaining < offset + 2) {
                return false;
            }
            int nameLen = Unsafe.getUnsafe().getShort(ptr + offset) & 0xFFFF;
            offset += 2;
            // Empty table names are rejected as structurally invalid - a valid
            // table name is never zero bytes, and accepting empty names would
            // let a misbehaving server poison the per-table tracker with "" entries.
            if (nameLen == 0 || remaining < offset + nameLen + 8) {
                return false;
            }
            long nameLo = ptr + offset;
            long nameHi = nameLo + nameLen;
            offset += nameLen;
            long seqTxn = Unsafe.getUnsafe().getLong(ptr + offset);
            offset += 8;
            tableNames.add(internTableName(nameLo, nameHi));
            tableSeqTxns.add(seqTxn);
        }
        return remaining == offset;
    }

    private String internTableName(long lo, long hi) {
        lookupKey.of(lo, hi);
        int keyIndex = tableNameCache.keyIndex(lookupKey);
        if (keyIndex < 0) {
            return tableNameCache.valueAtQuick(keyIndex);
        }
        String decoded = Utf8s.stringFromUtf8Bytes(lo, hi);
        tableNameCache.putAt(keyIndex, lookupKey, decoded);
        return decoded;
    }

    private int tableEntriesSize() {
        int size = 0;
        for (int i = 0, n = tableNames.size(); i < n; i++) {
            size += 2 + tableNames.getQuick(i).getBytes(StandardCharsets.UTF_8).length + 8;
        }
        return size;
    }

    private static boolean validateTableEntries(long ptr, int remaining) {
        if (remaining < 2) {
            return false;
        }
        int tableCount = Unsafe.getUnsafe().getShort(ptr) & 0xFFFF;
        int offset = 2;
        for (int i = 0; i < tableCount; i++) {
            if (remaining < offset + 2) {
                return false;
            }
            int nameLen = Unsafe.getUnsafe().getShort(ptr + offset) & 0xFFFF;
            offset += 2;
            // Empty table names are rejected as structurally invalid - a valid
            // table name is never zero bytes, and accepting empty names would
            // let a misbehaving server poison the per-table tracker with "" entries.
            if (nameLen == 0 || remaining < offset + nameLen + 8) {
                return false;
            }
            offset += nameLen + 8;
        }
        return remaining == offset;
    }

    private int writeTableEntries(long ptr) {
        int offset = 0;
        int count = tableNames.size();
        Unsafe.getUnsafe().putShort(ptr + offset, (short) count);
        offset += 2;
        for (int i = 0; i < count; i++) {
            byte[] nameBytes = tableNames.getQuick(i).getBytes(StandardCharsets.UTF_8);
            Unsafe.getUnsafe().putShort(ptr + offset, (short) nameBytes.length);
            offset += 2;
            for (int j = 0; j < nameBytes.length; j++) {
                Unsafe.getUnsafe().putByte(ptr + offset + j, nameBytes[j]);
            }
            offset += nameBytes.length;
            Unsafe.getUnsafe().putLong(ptr + offset, tableSeqTxns.getQuick(i));
            offset += 8;
        }
        return offset;
    }

    private int getErrorMessageUtf8Length() {
        if (status == STATUS_OK || status == STATUS_DURABLE_ACK || errorMessage == null || errorMessage.isEmpty()) {
            errorMessageUtf8Length = 0;
            return 0;
        }
        if (errorMessageUtf8Length < 0) {
            errorMessageUtf8Length = Utf8s.utf8Bytes(errorMessage, MAX_ERROR_MESSAGE_LENGTH);
        }
        return errorMessageUtf8Length;
    }
}
