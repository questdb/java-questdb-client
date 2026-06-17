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

import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaDecoder;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Zstd;

import java.nio.charset.StandardCharsets;

/**
 * Zero-alloc (after warmup) decoder for inbound QWP egress {@code RESULT_BATCH} frames.
 * <p>
 * The decoder parses the payload in-place -- no values are copied out of the
 * WebSocket receive buffer. It maintains pooled {@link QwpColumnLayout} slots,
 * per-column {@code int[]} index arrays, and a native SYMBOL dictionary
 * (UTF-8 heap + packed offset/length entries) that is reused across batches.
 * After the connection has seen its peak schema width and row count, decoding
 * a batch allocates nothing on the JVM heap.
 * <p>
 * The produced {@link QwpColumnBatch} is valid only during the surrounding
 * {@code onBatch} callback because its pointers refer into the caller's native
 * payload buffer.
 */
public class QwpResultBatchDecoder implements QuietCloseable {

    private static final int CONN_DICT_INITIAL_BYTES = 4096;
    private static final int CONN_DICT_INITIAL_ENTRIES = 512;
    /**
     * Hard cap on a single ARRAY dimension's length, mirroring the server's
     * {@code ArrayView.DIM_MAX_LEN}. It is the largest length the server can
     * place on any one dimension when it builds an array, so the decoder accepts
     * every dimension up to it. The cap bounds each dimension independently of
     * {@link #MAX_ARRAY_ELEMENTS}: an empty array carries a 0-length dimension
     * that drives the element-count product to 0, leaving the product guard
     * unable to bound a sibling dimension -- this per-dimension cap bounds it.
     */
    private static final int MAX_ARRAY_DIM_LEN = (1 << 28) - 1;
    /**
     * Hard cap on per-row ARRAY element count. 8 bytes per element x this ~ 256 MB max payload,
     * which fits in {@code int} once {@code rowEnd - p} is computed. A malicious or buggy
     * server cannot push a negative or wrap-around length past this guard.
     */
    private static final long MAX_ARRAY_ELEMENTS = (Integer.MAX_VALUE - 1024) / 8L;
    /**
     * Hard cap on the connection-scoped SYMBOL dict's UTF-8 heap size in bytes.
     * Well below {@link Integer#MAX_VALUE} to prevent {@code connDictHeapPos + len}
     * from wrapping negative (which would bypass {@link #ensureConnDictHeapCapacity}
     * and let {@code copyMemory} write past the allocated heap). Servers that
     * approach this cap are expected to emit {@code CACHE_RESET}; crossing it
     * means either a broken server or a hostile frame, so we fail hard.
     */
    private static final int MAX_CONN_DICT_HEAP_BYTES = 256 * 1024 * 1024;
    /**
     * Hard cap on the connection-scoped SYMBOL dict entry count. Matches the
     * rationale for {@link #MAX_CONN_DICT_HEAP_BYTES}: a hostile server that
     * never emits {@code CACHE_RESET} cannot drive {@code connDictSize} to
     * {@link Integer#MAX_VALUE} without being rejected first.
     */
    private static final int MAX_CONN_DICT_SIZE = 8_388_608;
    /**
     * Hard cap on {@code row_count} per batch. Matches the server's MAX_ROWS_PER_BATCH.
     * A hostile server could otherwise encode row_count = Integer.MAX_VALUE; ensureIntArray
     * would then try to allocate an {@code int[Integer.MAX_VALUE]} (~8 GB) before any
     * wire-length bounds check fires. Cap two orders of magnitude above the batch size
     * to leave head-room for future server-side batch enlargement without breaking clients.
     */
    private static final int MAX_ROWS_PER_BATCH = 1_048_576;
    // Reusable scratch for decoding column names from the first batch's inline
    // schema (batch_seq == 0).
    // Sized to the wire-level cap so {@link #readColumnName} can copy bytes
    // without allocating a fresh {@code byte[]} per column. The returned
    // {@code String} is still a fresh allocation (immutable + retained
    // long-term on {@link QwpEgressColumnInfo#name}), but dropping the
    // throwaway byte[] is measurable on schema-heavy workloads and post
    // CACHE_RESET re-registration.
    private final byte[] colNameScratch = new byte[QwpConstants.MAX_COLUMN_NAME_LENGTH];
    // Reused for Gorilla-encoded TIMESTAMP / TIMESTAMP_NANOS / DATE columns.
    // Stateful per-column -- {@link #parseTimestampColumn} calls {@code reset}
    // before decoding each column so residue from a previous column never bleeds
    // in.
    private final QwpGorillaDecoder gorillaDecoder = new QwpGorillaDecoder();
    // Connection-scoped state (safe to share across buffers -- reused across batches
    // of the same query and across queries on the same connection).
    // Connection-scoped SYMBOL dictionary. Populated by {@link #parseDeltaSymbolDict}
    // from the per-message delta section. Two native buffers:
    // - {@link #connDictHeapAddr} holds the concatenated UTF-8 bytes of every entry.
    // - {@link #connDictEntriesAddr} holds one packed 8-byte pair per entry:
    //   {@code (offset:i32 | length:i32<<32)} relative to the heap base.
    // Storing offsets (rather than absolute addresses) means heap reallocs don't
    // invalidate entries; the hot-path accessor does one 64-bit load + base add,
    // no object dereferences. Grows but never shrinks; freed on {@link #close}.
    // Per-query column schema. The full column list rides only the first
    // RESULT_BATCH of a query (batch_seq == 0); continuation batches carry rows
    // against it. Reused across queries (slots pooled); the IoThread invalidates
    // it via resetQuerySchema() when a new query starts, so a stray continuation
    // frame can't bind rows to a stale schema.
    //
    // Dropping the old connection-scoped schema registry means every query's
    // batch_seq == 0 re-parses the inline schema and allocates a fresh
    // column-name String per column, even when the same query shape repeats --
    // the trade-off for not carrying schema ids on the wire.
    //
    // A single slot is correct only because the IoThread runs one query at a
    // time (one pendingRequest, one currentRequestId). If client-side
    // pipelining is ever enabled so batches from different request_ids
    // interleave, this must become a map keyed by request_id; the single
    // querySchemaValid flag would otherwise mis-bind rows across queries.
    private final ObjList<QwpEgressColumnInfo> querySchema = new ObjList<>();
    private long connDictEntriesAddr;
    private int connDictEntriesCapacity;
    // Monotonic generation counter for the connection-scoped dict. Incremented
    // whenever {@link #applyCacheReset} wipes the dict, so per-layout SYMBOL
    // String caches (which key by dict id) can detect a reset and wipe lazily
    // instead of paying an eager clear on every batch.
    private long connDictGeneration;
    private long connDictHeapAddr;
    private int connDictHeapCapacity;
    private int connDictHeapPos;
    private int connDictSize;
    // Native ZSTD_DCtx pointer, lazy-allocated on the first {@code FLAG_ZSTD}
    // batch. One per decoder instance (which in turn is one per IoThread), reused
    // for every subsequent compressed batch on the same connection.
    private long dctx;
    // Growable scratch holding the decompressed body between the zstd call and
    // the downstream parse. Starts small and doubles on demand when the decoder
    // reports a destination-too-small error.
    private long decompressScratchAddr;
    private int decompressScratchCapacity;
    // True when the current message carries {@code FLAG_DELTA_SYMBOL_DICT}. Read by
    // {@link #parseSymbolColumn} to decide whether to consume a per-column dict.
    private boolean deltaMode;
    // True when the current message carries {@code FLAG_GORILLA}. When set,
    // TIMESTAMP / TIMESTAMP_NANOS / DATE columns are prefixed by a 1-byte
    // encoding discriminator (0x00 raw, 0x01 Gorilla).
    private boolean gorillaMode;
    // True once the schema-bearing batch_seq == 0 of the current query has been
    // parsed into {@link #querySchema}. Reset by {@link #resetQuerySchema()} when
    // a new query starts, so a continuation batch can never bind to a stale schema.
    private boolean querySchemaValid;
    // Reusable varint decode state: value in varintValue, new position in varintPos.
    // Instance-level so no {@code long[2]} scratch is allocated per call.
    private long varintPos;
    private long varintValue;

    /**
     * Clears the caches indicated by {@code resetMask}. The only bit defined is
     * {@link QwpEgressMsgKind#RESET_MASK_DICT}: the server emits a
     * {@code CACHE_RESET} frame after hitting the connection-level SYMBOL dict
     * soft cap, and dropping the dict here keeps the next batch's
     * {@code deltaStart} aligned with the server's fresh counter.
     * <p>
     * The native dict buffers are reused (positions reset to 0, capacity
     * retained) so a workload that churns just above the cap does not reallocate
     * on every reset.
     */
    public void applyCacheReset(byte resetMask) {
        if ((resetMask & QwpEgressMsgKind.RESET_MASK_DICT) != 0) {
            connDictSize = 0;
            connDictHeapPos = 0;
            // Bump generation so any per-layout SYMBOL String cache populated
            // against the pre-reset dict detects the change on its next lookup
            // and wipes before reading.
            connDictGeneration++;
        }
    }

    @Override
    public void close() {
        if (connDictHeapAddr != 0) {
            Unsafe.free(connDictHeapAddr, connDictHeapCapacity, MemoryTag.NATIVE_DEFAULT);
            connDictHeapAddr = 0;
            connDictHeapCapacity = 0;
            connDictHeapPos = 0;
        }
        if (connDictEntriesAddr != 0) {
            Unsafe.free(connDictEntriesAddr, connDictEntriesCapacity, MemoryTag.NATIVE_DEFAULT);
            connDictEntriesAddr = 0;
            connDictEntriesCapacity = 0;
        }
        if (decompressScratchAddr != 0) {
            Unsafe.free(decompressScratchAddr, decompressScratchCapacity, MemoryTag.NATIVE_DEFAULT);
            decompressScratchAddr = 0;
            decompressScratchCapacity = 0;
        }
        if (dctx != 0) {
            Zstd.freeDCtx(dctx);
            dctx = 0;
        }
        connDictSize = 0;
        // Invalidate any per-query schema captured before close(). The decoder is
        // single-use per connection today, but resetting here keeps a future
        // reuse from binding a continuation batch to a prior connection's schema.
        querySchemaValid = false;
    }

    /**
     * Decodes the RESULT_BATCH frame whose payload has been copied into {@code buffer}.
     * Populates {@code buffer.batch} and {@code buffer.layoutPool}. The resulting
     * batch view stays valid as long as the buffer is not reused.
     */
    public void decode(QwpBatchBuffer buffer) throws QwpDecodeException {
        decodePayload(buffer, buffer.getScratchAddr(), buffer.getPayloadLen());
    }

    /**
     * In-place decode: parses the frame whose bytes live at {@code payloadPtr} (e.g. the
     * WebSocket recv buffer) without copying into {@code buffer}'s native scratch.
     * {@code buffer} contributes only its reusable layout pool and batch view; all
     * column pointers produced reference {@code payloadPtr}, so the caller must keep
     * those bytes stable until it's done reading the {@link QwpColumnBatch}.
     */
    public void decode(QwpBatchBuffer buffer, long payloadPtr, int payloadLen) throws QwpDecodeException {
        decodePayload(buffer, payloadPtr, payloadLen);
    }

    /**
     * Invalidates the per-query schema captured from the last {@code batch_seq == 0}.
     * The IoThread calls this when a new query starts so the next query's
     * continuation batches can't bind rows to a stale schema; the underlying
     * {@link QwpEgressColumnInfo} slots are retained for reuse.
     */
    public void resetQuerySchema() {
        querySchemaValid = false;
    }

    // Pool helpers

    private static long advanceFixed(QwpColumnLayout layout, long p, long limit, int sizeBytes) throws QwpDecodeException {
        layout.valuesAddr = p;
        long total = (long) sizeBytes * layout.nonNullCount;
        if (p + total > limit) throw new QwpDecodeException("truncated fixed-width column");
        return p + total;
    }

    private static QwpColumnLayout borrowLayout(ObjList<QwpColumnLayout> layoutPool, int colIdx) {
        while (layoutPool.size() <= colIdx) {
            layoutPool.add(new QwpColumnLayout());
        }
        return layoutPool.getQuick(colIdx);
    }

    private static int[] ensureIntArray(int[] current, int size) {
        if (current != null && current.length >= size) return current;
        return new int[Math.max(size, current == null ? 16 : current.length * 2)];
    }

    private static long[] ensureLongArray(long[] current, int size) {
        if (current != null && current.length >= size) return current;
        return new long[Math.max(size, current == null ? 16 : current.length * 2)];
    }

    /**
     * STRING / VARCHAR: the offsets array is (nonNullCount+1) x uint32 starting at {@code p},
     * followed by the concatenated UTF-8 bytes.
     */
    private static long parseStringColumn(QwpColumnLayout layout, long p, long limit) throws QwpDecodeException {
        int nonNull = layout.nonNullCount;
        long offsetsSize = 4L * (nonNull + 1);
        if (p + offsetsSize > limit) throw new QwpDecodeException("truncated string offsets");
        layout.valuesAddr = p;
        layout.stringBytesAddr = p + offsetsSize;
        int totalBytes = nonNull == 0 ? 0 : Unsafe.getUnsafe().getInt(p + 4L * nonNull);
        // totalBytes is signed int32 read from the wire. A negative value passes the
        // "addr + totalBytes > limit" check (the sum stays below limit) and would
        // return a position before stringBytesAddr -- subsequent column parsing would
        // then read native memory backwards. Reject it explicitly.
        if (totalBytes < 0 || layout.stringBytesAddr + totalBytes > limit) {
            throw new QwpDecodeException("invalid string column total bytes: " + totalBytes);
        }
        // Validate per-row offsets: each must be in [0, totalBytes] and
        // monotonically non-decreasing. Without this loop only offset[nonNull]
        // is checked; a hostile server can supply a negative or out-of-order
        // intermediate offset that QwpColumnBatch.lookupBinaryBytes /
        // lookupStringBytes will then turn into a negative-length view (which
        // surfaces as NegativeArraySizeException in getBinary) or a native
        // address before stringBytesAddr (out-of-bounds read of unrelated
        // native memory). Cost is one int load per non-null row over bytes
        // already in cache.
        int prev = 0;
        for (int i = 0; i < nonNull; i++) {
            int off = Unsafe.getUnsafe().getInt(p + 4L * i);
            if (off < prev || off > totalBytes) {
                throw new QwpDecodeException("invalid string column offset[" + i + "]="
                        + off + " (prev=" + prev + ", total=" + totalBytes + ")");
            }
            prev = off;
        }
        return layout.stringBytesAddr + totalBytes;
    }

    private void decodePayload(QwpBatchBuffer buffer, long payload, int payloadLen) throws QwpDecodeException {
        if (payloadLen < QwpConstants.HEADER_SIZE + 10) {
            throw new QwpDecodeException("RESULT_BATCH payload too short: " + payloadLen);
        }
        // Message header
        int magic = Unsafe.getUnsafe().getInt(payload);
        if (magic != QwpConstants.MAGIC_MESSAGE) {
            throw new QwpDecodeException("bad magic 0x" + Integer.toHexString(magic));
        }
        byte version = Unsafe.getUnsafe().getByte(payload + 4);
        if (version != QwpConstants.VERSION) {
            throw QwpProtocolVersionException.unsupported(version & 0xFF);
        }
        byte flags = Unsafe.getUnsafe().getByte(payload + QwpConstants.HEADER_OFFSET_FLAGS);
        deltaMode = (flags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0;
        gorillaMode = (flags & QwpConstants.FLAG_GORILLA) != 0;
        long p = payload + QwpConstants.HEADER_SIZE;
        long limit = payload + payloadLen;

        byte msgKind = Unsafe.getUnsafe().getByte(p++);
        if (msgKind != (byte) 0x11) {
            throw new QwpDecodeException("expected RESULT_BATCH (0x11), got 0x" + Integer.toHexString(msgKind & 0xFF));
        }
        if (p + 8 > limit) throw new QwpDecodeException("truncated request_id");
        long requestId = Unsafe.getUnsafe().getLong(p);
        p += 8;
        decodeVarint(p, limit);
        long batchSeq = varintValue;
        p = varintPos;

        // Zstd-compressed body: the region from here to {@code limit} is a
        // single zstd frame covering the delta section + table block. Decode
        // into the decoder-owned scratch buffer and rebind {@code p} /
        // {@code limit} so downstream parsers see the plain bytes without any
        // structural awareness of compression. {@code payloadAddr} exposed
        // on the batch view also follows {@code p} here so size-measuring
        // callers ({@code batch.payloadLimit() - batch.payloadAddr()}) report
        // the uncompressed-equivalent body size rather than an arbitrary
        // pointer delta between two native allocations.
        long batchViewAddr = payload;
        if ((flags & QwpConstants.FLAG_ZSTD) != 0) {
            long srcLen = limit - p;
            if (dctx == 0) {
                try {
                    dctx = Zstd.createDCtx();
                } catch (UnsatisfiedLinkError e) {
                    // The early probe in QwpQueryClient.connect() should have
                    // caught this already. If we end up here, something went
                    // around that probe (custom client wiring, direct use of
                    // QwpResultBatchDecoder, etc.) -- surface a message that
                    // names the actual cause instead of letting the JNI symbol
                    // name leak into the user callback.
                    throw new QwpDecodeException("server sent a zstd-compressed batch but this "
                            + "client build does not include zstd -- libquestdb was built without "
                            + "the zstd submodule. Rebuild the native library with the submodule "
                            + "initialised, or set compression=raw on the connection string "
                            + "[cause=" + e.getMessage() + "]");
                }
                if (dctx == 0) {
                    throw new QwpDecodeException("failed to allocate zstd decompression context");
                }
            }
            // Read the declared content size from the frame header BEFORE any
            // allocation. This both lets us size the scratch in a single malloc
            // (no decompress-fail-grow-retry loop) and -- crucially -- rejects
            // corrupt or truncated frames upfront. Without the up-front check,
            // a single hostile or malformed frame would have driven scratch
            // doubling all the way to MAX_SCRATCH (~127 MiB of malloc/free
            // churn per bad frame, plus 64 MiB pinned for the connection's
            // lifetime).
            final int MAX_SCRATCH = 64 * 1024 * 1024;
            final int MIN_SCRATCH = 1024 * 1024;
            long expectedSize = Zstd.getFrameContentSize(p, srcLen);
            if (expectedSize == -2) {
                throw new QwpDecodeException("invalid zstd frame header (truncated, bad magic, or content size > Long.MAX_VALUE)");
            }
            if (expectedSize == -1) {
                // The server's encoder leaves ZSTD_c_contentSizeFlag at its
                // default (on), so every frame it produces declares content
                // size. An "unknown" reading is therefore a protocol violation
                // -- refuse rather than guess a buffer size and reopen the
                // amplification window.
                throw new QwpDecodeException("zstd frame missing content size (protocol violation)");
            }
            if (expectedSize > MAX_SCRATCH) {
                throw new QwpDecodeException("zstd frame content size " + expectedSize
                        + " exceeds client cap " + MAX_SCRATCH);
            }
            if (decompressScratchCapacity < expectedSize) {
                long newCap = Math.max((long) decompressScratchCapacity * 2L, expectedSize);
                if (newCap < MIN_SCRATCH) newCap = MIN_SCRATCH;
                if (newCap > MAX_SCRATCH) newCap = MAX_SCRATCH;
                // Reset to 0 before free + malloc so a throwing malloc cannot
                // leave a dangling address + non-zero capacity behind; the next
                // decode would otherwise skip the first-alloc branch and
                // use-after-free.
                if (decompressScratchAddr != 0) {
                    Unsafe.free(decompressScratchAddr, decompressScratchCapacity, MemoryTag.NATIVE_DEFAULT);
                    decompressScratchAddr = 0;
                    decompressScratchCapacity = 0;
                }
                decompressScratchAddr = Unsafe.malloc(newCap, MemoryTag.NATIVE_DEFAULT);
                decompressScratchCapacity = (int) newCap;
            }
            long decLen = Zstd.decompress(dctx, p, srcLen, decompressScratchAddr, decompressScratchCapacity);
            if (decLen < 0) {
                throw new QwpDecodeException("zstd decompression failed [code=" + (-decLen) + "]");
            }
            if (decLen != expectedSize) {
                throw new QwpDecodeException("zstd decompressed size " + decLen
                        + " does not match frame content size " + expectedSize);
            }
            p = decompressScratchAddr;
            limit = decompressScratchAddr + decLen;
            batchViewAddr = decompressScratchAddr;
        }

        // Delta section (if enabled) sits right after the prelude and before the
        // table block. We consume it first so that SYMBOL columns inside the
        // table block resolve indices against the freshly-updated connection dict.
        if (deltaMode) {
            p = parseDeltaSymbolDict(p, limit);
        }

        // Table block. The schema (column_count + inline column descriptors)
        // rides only the first batch of a query (batch_seq == 0); continuation
        // batches carry rows against the schema parsed there:
        //   batch_seq == 0: name_length, name, row_count, column_count, columns
        //   batch_seq  > 0: name_length, name, row_count
        decodeVarint(p, limit);
        long nameLen = varintValue;
        p = varintPos;
        // Negative nameLen would make p + nameLen wrap below the bound check
        // and let the next decode rewind into already-consumed bytes.
        if (nameLen < 0 || nameLen > QwpConstants.MAX_TABLE_NAME_LENGTH) {
            throw new QwpDecodeException("table name length out of range: " + nameLen);
        }
        if (p + nameLen > limit) throw new QwpDecodeException("truncated table name");
        p += nameLen;

        decodeVarint(p, limit);
        // Reject row counts that would force multi-GB allocations in ensureIntArray/ensureLongArray
        // before the per-column bounds checks fire. A hostile varint with the high bit set also
        // casts negative, which would silently flip bitmapBytes = (rowCount + 7) >>> 3 into a huge
        // positive int via unsigned shift.
        if (varintValue < 0 || varintValue > MAX_ROWS_PER_BATCH) {
            throw new QwpDecodeException("row_count out of range: " + varintValue);
        }
        int rowCount = (int) varintValue;
        p = varintPos;

        ObjList<QwpEgressColumnInfo> columns;
        int columnCount;
        if (batchSeq == 0) {
            // First batch carries the full schema: column_count then one
            // (name_length, name, wire_type) descriptor per column.
            decodeVarint(p, limit);
            if (varintValue < 0 || varintValue > QwpConstants.MAX_COLUMNS_PER_TABLE) {
                throw new QwpDecodeException("column_count out of range: " + varintValue);
            }
            columnCount = (int) varintValue;
            p = varintPos;
            columns = ensureQuerySchema(columnCount);
            for (int i = 0; i < columnCount; i++) {
                decodeVarint(p, limit);
                // Same negative / out-of-range guard as nameLen above; without
                // it, a hostile colNameLen wraps p + colNameLen below limit
                // and lets the decoder rewind into already-consumed bytes.
                if (varintValue < 0 || varintValue > QwpConstants.MAX_COLUMN_NAME_LENGTH) {
                    throw new QwpDecodeException("column name length out of range: " + varintValue);
                }
                int colNameLen = (int) varintValue;
                p = varintPos;
                if (p + colNameLen + 1 > limit) throw new QwpDecodeException("truncated column def");
                String colName = readColumnName(p, colNameLen);
                p += colNameLen;
                byte wireType = Unsafe.getUnsafe().getByte(p++);
                columns.getQuick(i).of(colName, wireType);
            }
            querySchemaValid = true;
        } else {
            // Continuation batch: reuse the schema delivered on batch_seq == 0.
            // A continuation that arrives before any schema-bearing batch (a
            // malformed or hostile server) must not bind rows to a stale schema.
            if (!querySchemaValid) {
                throw new QwpDecodeException("RESULT_BATCH batch_seq=" + batchSeq
                        + " arrived before the schema-bearing batch_seq=0");
            }
            columns = querySchema;
            columnCount = querySchema.size();
        }

        // Reset batch view and parse columns into per-column layouts owned by the buffer.
        resetBatch(buffer, requestId, batchSeq, rowCount, columnCount, columns, batchViewAddr, limit);
        for (int ci = 0; ci < columnCount; ci++) {
            QwpColumnLayout layout = borrowLayout(buffer.layoutPool, ci);
            layout.clear();
            layout.info = columns.getQuick(ci);
            p = parseColumn(layout, rowCount, p, limit);
        }
    }

    /**
     * Decodes a varint starting at {@code p}. Stores the decoded value in
     * {@link #varintValue} and the position just past the varint in
     * {@link #varintPos}. Caller reads both before issuing the next varint call.
     * <p>
     * The encoding is the standard 7-bit varint: each byte contributes 7 data
     * bits and signals continuation via the MSB. With ten data-carrying bytes
     * the last byte's high data bits would spill past bit 63 of the result --
     * in Java the {@code <<} operator silently masks the shift count to 6 bits,
     * which would wrap the high bits back into the low end and produce a value
     * the caller couldn't tell from a legitimate small one. Reject those.
     */
    private void decodeVarint(long p, long limit) throws QwpDecodeException {
        long value = 0;
        int shift = 0;
        long cur = p;
        while (true) {
            if (cur >= limit) throw new QwpDecodeException("truncated varint");
            byte b = Unsafe.getUnsafe().getByte(cur++);
            // On byte 10 (shift == 63) only bit 0 of the data nibble can fit
            // in the result without overflowing bit 63 (the sign bit of long).
            // Any other data bit on byte 10 means the encoded value would not
            // round-trip; refuse to claim it decoded successfully. The server
            // has the same guard in QwpVarint.decodeMultiByte.
            if (shift == 63 && (b & 0x7E) != 0) {
                throw new QwpDecodeException("varint overflow");
            }
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) throw new QwpDecodeException("varint overflow");
        }
        varintValue = value;
        varintPos = cur;
    }

    // Per-column parse: advances through wire bytes, populates layout pointers,
    // precomputes nonNullIdx for O(1) per-row access.

    private void ensureConnDictEntriesCapacity(int requiredEntries) {
        int requiredBytes = requiredEntries * 8;
        if (connDictEntriesCapacity >= requiredBytes) return;
        int newCap = Math.max(connDictEntriesCapacity * 2, Math.max(CONN_DICT_INITIAL_ENTRIES * 8, requiredBytes));
        connDictEntriesAddr = Unsafe.realloc(connDictEntriesAddr, connDictEntriesCapacity, newCap, MemoryTag.NATIVE_DEFAULT);
        connDictEntriesCapacity = newCap;
    }

    private void ensureConnDictHeapCapacity(int required) {
        if (connDictHeapCapacity >= required) return;
        int newCap = Math.max(connDictHeapCapacity * 2, Math.max(CONN_DICT_INITIAL_BYTES, required));
        connDictHeapAddr = Unsafe.realloc(connDictHeapAddr, connDictHeapCapacity, newCap, MemoryTag.NATIVE_DEFAULT);
        connDictHeapCapacity = newCap;
        // No re-pointing needed: entries store offsets into the heap, not absolute
        // addresses, so the hot-path accessor resolves against the current heap base.
    }

    private ObjList<QwpEgressColumnInfo> ensureQuerySchema(int columnCount) {
        int currentPos = querySchema.size();
        if (columnCount > currentPos) {
            querySchema.setPos(columnCount);
            for (int i = currentPos; i < columnCount; i++) {
                if (querySchema.getQuick(i) == null) {
                    querySchema.setQuick(i, new QwpEgressColumnInfo());
                }
            }
        } else {
            querySchema.setPos(columnCount);
        }
        return querySchema;
    }

    private long parseArrayColumn(QwpColumnLayout layout, int rowCount, long p, long limit) throws QwpDecodeException {
        layout.arrayRowAddr = ensureLongArray(layout.arrayRowAddr, rowCount);
        layout.arrayRowLen = ensureIntArray(layout.arrayRowLen, rowCount);
        layout.valuesAddr = p;
        // Hoist the no-nulls discriminator out of the per-row loop -- when the
        // column has no nulls in this batch, every row is non-null and the
        // null-skip branch is dead.
        boolean noNulls = layout.nullBitmapAddr == 0;
        int[] nonNullIdx = layout.nonNullIdx;
        for (int i = 0; i < rowCount; i++) {
            if (!noNulls && nonNullIdx[i] < 0) {
                layout.arrayRowAddr[i] = 0;
                layout.arrayRowLen[i] = 0;
                continue;
            }
            if (p + 1 > limit) throw new QwpDecodeException("truncated ARRAY header");
            int nDims = Unsafe.getUnsafe().getByte(p) & 0xFF;
            if (nDims < 1 || nDims > ColumnType.ARRAY_NDIMS_LIMIT) {
                throw new QwpDecodeException("invalid array dimensions: " + nDims
                        + " (must be 1-" + ColumnType.ARRAY_NDIMS_LIMIT + ")");
            }
            long headerEnd = p + 1 + 4L * nDims;
            if (headerEnd > limit) throw new QwpDecodeException("truncated ARRAY dims");
            long elements = 1;
            for (int d = 0; d < nDims; d++) {
                int dl = Unsafe.getUnsafe().getInt(p + 1 + 4L * d);
                // A 0-length dimension is a valid empty array (cardinality 0),
                // distinct from a NULL array (which the null bitmap carries). A
                // dimension is a non-negative int up to MAX_ARRAY_DIM_LEN, the
                // same per-dimension ceiling the server applies when it builds an
                // array, so every shape the server can emit decodes here. The cap
                // also bounds an empty array's other dimensions: a single 0-length
                // dimension collapses {@code elements} to 0, leaving the product
                // guard below unable to catch an oversized sibling.
                if (dl < 0 || dl > MAX_ARRAY_DIM_LEN) {
                    throw new QwpDecodeException("ARRAY dim " + d + " out of range [0, "
                            + MAX_ARRAY_DIM_LEN + "]: " + dl);
                }
                elements *= dl;
                if (elements > MAX_ARRAY_ELEMENTS) {
                    throw new QwpDecodeException("ARRAY element count exceeds limit ("
                            + elements + " > " + MAX_ARRAY_ELEMENTS + ")");
                }
            }
            long rowEnd = headerEnd + 8L * elements;
            if (rowEnd > limit) throw new QwpDecodeException("truncated ARRAY payload");
            layout.arrayRowAddr[i] = p;
            layout.arrayRowLen[i] = (int) (rowEnd - p);
            p = rowEnd;
        }
        return p;
    }

    private long parseColumn(QwpColumnLayout layout, int rowCount, long p, long limit) throws QwpDecodeException {
        p = parseNullSection(layout, rowCount, p, limit);
        byte wt = layout.info.wireType;
        if (wt == QwpConstants.TYPE_BOOLEAN) {
            layout.valuesAddr = p;
            int bytes = (layout.nonNullCount + 7) >>> 3;
            if (p + bytes > limit) throw new QwpDecodeException("truncated BOOLEAN");
            return p + bytes;
        }
        if (wt == QwpConstants.TYPE_BYTE) return advanceFixed(layout, p, limit, 1);
        if (wt == QwpConstants.TYPE_SHORT || wt == QwpConstants.TYPE_CHAR) return advanceFixed(layout, p, limit, 2);
        if (wt == QwpConstants.TYPE_INT || wt == QwpConstants.TYPE_FLOAT
                || wt == QwpConstants.TYPE_IPv4) return advanceFixed(layout, p, limit, 4);
        if (wt == QwpConstants.TYPE_LONG || wt == QwpConstants.TYPE_DOUBLE) {
            return advanceFixed(layout, p, limit, 8);
        }
        if (wt == QwpConstants.TYPE_DATE
                || wt == QwpConstants.TYPE_TIMESTAMP
                || wt == QwpConstants.TYPE_TIMESTAMP_NANOS) {
            return parseTimestampColumn(layout, p, limit);
        }
        if (wt == QwpConstants.TYPE_DECIMAL64) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL64 scale");
            layout.info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return advanceFixed(layout, p, limit, 8);
        }
        if (wt == QwpConstants.TYPE_UUID) return advanceFixed(layout, p, limit, 16);
        if (wt == QwpConstants.TYPE_DECIMAL128) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL128 scale");
            layout.info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return advanceFixed(layout, p, limit, 16);
        }
        if (wt == QwpConstants.TYPE_LONG256) return advanceFixed(layout, p, limit, 32);
        if (wt == QwpConstants.TYPE_DECIMAL256) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL256 scale");
            layout.info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return advanceFixed(layout, p, limit, 32);
        }
        if (wt == QwpConstants.TYPE_VARCHAR || wt == QwpConstants.TYPE_BINARY) {
            // VARCHAR/BINARY share the (N+1) x uint32 offsets + concatenated bytes layout.
            // BINARY differs only in that the bytes are opaque (no UTF-8 contract).
            return parseStringColumn(layout, p, limit);
        }
        if (wt == QwpConstants.TYPE_SYMBOL) {
            return parseSymbolColumn(layout, rowCount, p, limit);
        }
        if (wt == QwpConstants.TYPE_GEOHASH) {
            decodeVarint(p, limit);
            // The server enforces [1, 60] on GEOLONG precision; mirror the
            // check here so a hostile varint that decodes out of range (or
            // negative once cast to int) fails fast rather than driving a
            // nonsense bytesPerValue into the length calculation below.
            if (varintValue < 1 || varintValue > 60) {
                throw new QwpDecodeException("GEOHASH precision bits out of range (1..60): " + varintValue);
            }
            layout.info.precisionBits = (int) varintValue;
            p = varintPos;
            int bytesPerValue = (layout.info.precisionBits + 7) >>> 3;
            layout.valuesAddr = p;
            long total = (long) bytesPerValue * layout.nonNullCount;
            if (p + total > limit) throw new QwpDecodeException("truncated GEOHASH");
            return p + total;
        }
        if (wt == QwpConstants.TYPE_DOUBLE_ARRAY || wt == QwpConstants.TYPE_LONG_ARRAY) {
            return parseArrayColumn(layout, rowCount, p, limit);
        }
        throw new QwpDecodeException("unsupported wire type 0x" + Integer.toHexString(wt & 0xFF));
    }

    /**
     * Parses the message-level delta symbol dictionary section present when
     * {@code FLAG_DELTA_SYMBOL_DICT} is set. Copies newly-seen symbol bytes into
     * the decoder's connection-scoped native heap and appends packed
     * {@code (offset:i32 | length:i32<<32)} entries to {@link #connDictEntriesAddr}.
     * <pre>
     *   [deltaStartId: varint]
     *   [deltaCount:   varint]
     *   for each new entry: [length: varint][UTF-8 bytes]
     * </pre>
     * The server is required to emit {@code deltaStartId == connDictSize}
     * (otherwise the two ends are out of sync and we bail rather than silently
     * corrupt the dict). Returns the wire position just past the section.
     */
    private long parseDeltaSymbolDict(long p, long limit) throws QwpDecodeException {
        decodeVarint(p, limit);
        long deltaStart = varintValue;
        p = varintPos;
        decodeVarint(p, limit);
        long deltaCount = varintValue;
        p = varintPos;
        // Validate operands separately rather than via the sum: a hostile
        // (deltaStart, deltaCount) pair where deltaStart matches connDictSize
        // and deltaCount is large enough to make deltaStart + deltaCount wrap
        // negative would otherwise bypass the cap check, then the (int)
        // deltaCount cast in newSize below collapses to a small/negative int
        // and the for-loop iterates while writing past connDictEntriesAddr.
        // After the deltaStart upper-bound, MAX_CONN_DICT_SIZE - deltaStart is
        // non-negative and the deltaCount comparison is well-defined.
        if (deltaStart < 0 || deltaCount < 0
                || deltaStart > MAX_CONN_DICT_SIZE
                || deltaCount > MAX_CONN_DICT_SIZE - deltaStart) {
            throw new QwpDecodeException("delta symbol section out of range: start="
                    + deltaStart + ", count=" + deltaCount);
        }
        if (deltaStart != connDictSize) {
            throw new QwpDecodeException("delta symbol dict out of sync: expected start="
                    + connDictSize + ", got=" + deltaStart);
        }
        int newSize = connDictSize + (int) deltaCount;
        ensureConnDictEntriesCapacity(newSize);
        for (long i = 0; i < deltaCount; i++) {
            decodeVarint(p, limit);
            long entryLen = varintValue;
            p = varintPos;
            if (entryLen < 0 || entryLen > Integer.MAX_VALUE || p + entryLen > limit) {
                throw new QwpDecodeException("truncated delta symbol entry");
            }
            int len = (int) entryLen;
            // Long arithmetic so a hostile (connDictHeapPos + len) that would
            // wrap int-negative gets caught by the cap check instead of
            // silently skipping ensureConnDictHeapCapacity (which would let
            // copyMemory write past the heap).
            long newHeapPos = (long) connDictHeapPos + (long) len;
            if (newHeapPos > MAX_CONN_DICT_HEAP_BYTES) {
                throw new QwpDecodeException("connection SYMBOL dict heap exceeds cap ("
                        + MAX_CONN_DICT_HEAP_BYTES + " bytes); server must emit CACHE_RESET");
            }
            ensureConnDictHeapCapacity((int) newHeapPos);
            int offset = connDictHeapPos;
            Unsafe.getUnsafe().copyMemory(p, connDictHeapAddr + offset, len);
            // Pack (offset, length) into one 8-byte entry. Low 32 bits = offset,
            // high 32 bits = length. Single 64-bit load + two int extractions in
            // the hot-path accessor.
            long packed = (offset & 0xFFFFFFFFL) | ((long) len << 32);
            Unsafe.getUnsafe().putLong(connDictEntriesAddr + 8L * connDictSize, packed);
            connDictSize++;
            connDictHeapPos = (int) newHeapPos;
            p += len;
        }
        return p;
    }

    /**
     * Reads the null flag and bitmap, populates {@code layout.nullBitmapAddr} and
     * {@code layout.nonNullCount}, and fills {@code layout.nonNullIdx[0..rowCount)}
     * with dense indices (or -1 for NULL rows). Returns the position just past
     * the null section.
     */
    private long parseNullSection(QwpColumnLayout layout, int rowCount, long p, long limit) throws QwpDecodeException {
        if (p >= limit) throw new QwpDecodeException("truncated null flag");
        byte flag = Unsafe.getUnsafe().getByte(p++);
        if (flag == 0) {
            // No nulls in this column -- skip the per-row "nonNullIdx[i] = i"
            // array fill entirely. Accessors detect the no-nulls case via
            // {@code nullBitmapAddr == 0} and treat dense-index == row directly
            // (see {@link QwpColumnLayout#denseIndex}), so the array is unread
            // on this path. For a 16K-row x 100-column wide result this saves
            // 1.6M trivial assignments per batch. nonNullIdx is nulled so a
            // raw-API caller of {@code QwpColumnBatch.nonNullIndex(col)} can
            // distinguish "needs identity-fill" from "fully populated by a
            // prior with-nulls batch"; that path lazy-materialises on demand.
            layout.nullBitmapAddr = 0;
            layout.nonNullIdx = null;
            layout.nonNullCount = rowCount;
            return p;
        }
        int bitmapBytes = (rowCount + 7) >>> 3;
        if (p + bitmapBytes > limit) throw new QwpDecodeException("truncated null bitmap");
        layout.nullBitmapAddr = p;
        layout.nonNullIdx = ensureIntArray(layout.nonNullIdx, rowCount);
        int denseIdx = 0;
        for (int i = 0; i < rowCount; i++) {
            int bi = i >>> 3;
            int bit = i & 7;
            byte bm = Unsafe.getUnsafe().getByte(p + bi);
            if ((bm & (1 << bit)) != 0) {
                layout.nonNullIdx[i] = -1;
            } else {
                layout.nonNullIdx[i] = denseIdx++;
            }
        }
        layout.nonNullCount = denseIdx;
        return p + bitmapBytes;
    }

    /**
     * SYMBOL: in delta mode (always, with the current server) there's no per-column
     * dictionary -- indices reference the connection-scoped dict built up by
     * {@link #parseDeltaSymbolDict}. In non-delta mode the column carries its own
     * dict: (dict_size varint, then len+bytes per entry), followed by per-non-null-row
     * varint indices.
     */
    private long parseSymbolColumn(QwpColumnLayout layout, int rowCount, long p, long limit) throws QwpDecodeException {
        final int dictSize;
        if (deltaMode) {
            // Point the column's dict at the connection-scoped native arrays; size
            // is whatever the dict has grown to across all batches on this connection.
            layout.symbolDictHeapAddr = connDictHeapAddr;
            layout.symbolDictEntriesAddr = connDictEntriesAddr;
            dictSize = connDictSize;
            // Stamp the cache version with an even-tagged generation so the
            // String cache populated on a previous delta-mode batch stays
            // valid until CACHE_RESET bumps the generation. Low bit 0 marks
            // delta-mode tokens; non-delta uses bit 0 = 1 (see else branch).
            layout.symbolDictVersion = connDictGeneration << 1;
        } else {
            decodeVarint(p, limit);
            // A column can have at most rowCount distinct symbols, and a
            // hostile varint can decode negative once cast to int. Reject both
            // before the int-multiply below, which would otherwise overflow
            // silently and let {@code ensureOwnedEntriesAddr} return without
            // growing the buffer.
            if (varintValue < 0 || varintValue > rowCount) {
                throw new QwpDecodeException("SYMBOL dict size out of range: " + varintValue
                        + " (rowCount=" + rowCount + ")");
            }
            dictSize = (int) varintValue;
            p = varintPos;
            // Non-delta: dict entries point directly into the payload buffer. Build
            // a per-layout packed-entries array keyed by offset-from-the-dict-base.
            long dictBase = p;
            long entriesAddr = layout.ensureOwnedEntriesAddr(dictSize * 8);
            for (int e = 0; e < dictSize; e++) {
                decodeVarint(p, limit);
                long entryLen = varintValue;
                p = varintPos;
                // entryLen must be non-negative and fit in int. Without this,
                // a hostile varint can drive `p + entryLen` negative (int-cast
                // sign-extend on the long addend wraps), slipping the
                // `> limit` check; the subsequent `p += entryLen` then
                // advances backwards through previously-consumed bytes.
                if (entryLen < 0 || entryLen > Integer.MAX_VALUE || p + entryLen > limit) {
                    throw new QwpDecodeException("truncated symbol entry");
                }
                int lenBytes = (int) entryLen;
                int offset = (int) (p - dictBase);
                long packed = (offset & 0xFFFFFFFFL) | ((long) lenBytes << 32);
                Unsafe.getUnsafe().putLong(entriesAddr + 8L * e, packed);
                p += lenBytes;
            }
            layout.symbolDictHeapAddr = dictBase;
            layout.symbolDictEntriesAddr = entriesAddr;
            // Non-delta: the dict lives inside the payload and changes every
            // batch, so the cache must invalidate on every parse. Using
            // (dictBase | 1) picks up the per-batch address change and also
            // sets the low bit so a non-delta token can never collide with a
            // delta generation token.
            layout.symbolDictVersion = dictBase | 1L;
        }
        layout.symbolDictSize = dictSize;
        // Materialise per-row IDs into int[rowCount] so random access is O(1).
        layout.symbolRowIds = ensureIntArray(layout.symbolRowIds, rowCount);
        // Hoist the no-nulls discriminator out of the per-row loop -- when the
        // column has no nulls in this batch, every row carries an id and the
        // null-skip branch is dead.
        boolean noNulls = layout.nullBitmapAddr == 0;
        int[] nonNullIdx = layout.nonNullIdx;
        for (int i = 0; i < rowCount; i++) {
            if (!noNulls && nonNullIdx[i] < 0) continue; // NULL row; leave slot stale
            decodeVarint(p, limit);
            p = varintPos;
            int id = (int) varintValue;
            if (id < 0 || id >= dictSize) {
                throw new QwpDecodeException("symbol index out of range: " + id);
            }
            layout.symbolRowIds[i] = id;
        }
        layout.valuesAddr = 0; // Not applicable; accessors use symbolRowIds + symbolDictEntriesAddr.
        return p;
    }

    /**
     * TIMESTAMP / TIMESTAMP_NANOS / DATE: with {@code FLAG_GORILLA} set on the
     * message, the column is prefixed with a 1-byte encoding discriminator
     * ({@code 0x00} uncompressed, {@code 0x01} Gorilla bitstream). Without the
     * flag the column is plain 8-byte fixed-width -- the pre-Gorilla format.
     * <p>
     * Uncompressed: {@link QwpColumnLayout#valuesAddr} points directly at the
     * wire bytes. Gorilla: we decode into a per-layout native i64 buffer and
     * point {@code valuesAddr} at it, so the caller's {@code getLong(col, row)}
     * path is identical in both cases.
     */
    private long parseTimestampColumn(QwpColumnLayout layout, long p, long limit) throws QwpDecodeException {
        int nonNull = layout.nonNullCount;
        if (!gorillaMode) {
            return advanceFixed(layout, p, limit, 8);
        }
        if (p >= limit) throw new QwpDecodeException("truncated TIMESTAMP encoding byte");
        byte encoding = Unsafe.getUnsafe().getByte(p++);
        if (encoding == 0x00) {
            // Uncompressed: bytes sit inline just like advanceFixed.
            long total = (long) nonNull * 8L;
            if (p + total > limit) throw new QwpDecodeException("truncated TIMESTAMP raw values");
            layout.valuesAddr = p;
            return p + total;
        }
        if (encoding != 0x01) {
            throw new QwpDecodeException("unknown TIMESTAMP encoding 0x" + Integer.toHexString(encoding & 0xFF));
        }
        // Gorilla: first two timestamps are uncompressed (16 bytes), the rest is
        // a bitstream. Server shortcuts nonNull<3 to the uncompressed branch, so
        // we always have at least 3 values here.
        if (nonNull < 3) throw new QwpDecodeException("Gorilla-encoded column with nonNull<3: " + nonNull);
        if (p + 16L > limit) throw new QwpDecodeException("truncated Gorilla prefix");
        long firstTs = Unsafe.getUnsafe().getLong(p);
        long secondTs = Unsafe.getUnsafe().getLong(p + 8L);
        long bitstreamStart = p + 16L;
        long decodeAddr = layout.ensureTimestampDecodeAddr(nonNull * 8);
        Unsafe.getUnsafe().putLong(decodeAddr, firstTs);
        Unsafe.getUnsafe().putLong(decodeAddr + 8L, secondTs);
        gorillaDecoder.reset(firstTs, secondTs, bitstreamStart, limit - bitstreamStart);
        for (int i = 2; i < nonNull; i++) {
            Unsafe.getUnsafe().putLong(decodeAddr + (long) i * 8L, gorillaDecoder.decodeNext());
        }
        layout.valuesAddr = decodeAddr;
        long bitPos = gorillaDecoder.getBitPosition();
        long bitstreamBytes = (bitPos + 7L) >>> 3;
        long end = bitstreamStart + bitstreamBytes;
        if (end > limit) throw new QwpDecodeException("truncated Gorilla bitstream");
        return end;
    }

    /**
     * Copies {@code len} bytes from native address {@code p} into the reusable
     * {@link #colNameScratch} buffer and materialises them as a UTF-8 {@code
     * String}. Callers must hold {@code len <= MAX_COLUMN_NAME_LENGTH}; the
     * varint-length parse ({@link QwpConstants#MAX_COLUMN_NAME_LENGTH} guard)
     * upstream in {@link #decodePayload} already ensures that.
     */
    private String readColumnName(long p, int len) {
        for (int i = 0; i < len; i++) {
            colNameScratch[i] = Unsafe.getUnsafe().getByte(p + i);
        }
        return new String(colNameScratch, 0, len, StandardCharsets.UTF_8);
    }

    private void resetBatch(
            QwpBatchBuffer buffer,
            long requestId,
            long batchSeq,
            int rowCount,
            int columnCount,
            ObjList<QwpEgressColumnInfo> columns,
            long payloadAddr,
            long payloadLimit
    ) {
        QwpColumnBatch batch = buffer.batch;
        batch.requestId = requestId;
        batch.batchSeq = batchSeq;
        batch.rowCount = rowCount;
        batch.columnCount = columnCount;
        batch.columns = columns;
        batch.payloadAddr = payloadAddr;
        batch.payloadLimit = payloadLimit;
        // Surface the buffer-owned layouts to the batch view
        while (batch.columnLayouts.size() < columnCount) {
            batch.columnLayouts.add(null);
        }
        for (int i = 0; i < columnCount; i++) {
            batch.columnLayouts.setQuick(i, borrowLayout(buffer.layoutPool, i));
        }
    }
}
