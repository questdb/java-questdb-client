/*
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
 */

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;

final class SegmentedNativeBufferWriter implements QwpBufferWriter, QuietCloseable {
    private final ObjList<NativeBufferWriter> chunks = new ObjList<>();
    private final NativeSegmentList segments;

    private NativeBufferWriter currentChunk;
    private long flushedBytes;
    private int nextChunkIndex;

    SegmentedNativeBufferWriter() {
        NativeSegmentList segs = new NativeSegmentList();
        try {
            currentChunk = new NativeBufferWriter();
        } catch (Throwable t) {
            segs.close();
            throw t;
        }
        this.segments = segs;
        chunks.add(currentChunk);
    }

    @Override
    public void close() {
        for (int i = 0, n = chunks.size(); i < n; i++) {
            chunks.getQuick(i).close();
        }
        chunks.clear();
        segments.close();
    }

    void finish() {
        flushCurrentChunk();
    }

    NativeSegmentList getSegments() {
        return segments;
    }

    @Override
    public void ensureCapacity(int additionalBytes) {
        currentChunk.ensureCapacity(additionalBytes);
    }

    @Override
    public long getBufferPtr() {
        return currentChunk.getBufferPtr();
    }

    @Override
    public int getCapacity() {
        return currentChunk.getCapacity();
    }

    @Override
    public int getPosition() {
        return (int) (flushedBytes + currentChunk.getPosition());
    }

    @Override
    public long getWriteAddress() {
        return currentChunk.getWriteAddress();
    }

    @Override
    public int getWritableBytes() {
        return currentChunk.getWritableBytes();
    }

    @Override
    public void patchByte(int offset, byte value) {
        if (offset < flushedBytes || offset + 1 > flushedBytes + currentChunk.getPosition()) {
            throw new UnsupportedOperationException("cannot patch flushed segment data");
        }
        currentChunk.patchByte((int) (offset - flushedBytes), value);
    }

    @Override
    public void patchInt(int offset, int value) {
        if (offset < flushedBytes || offset + Integer.BYTES > flushedBytes + currentChunk.getPosition()) {
            throw new UnsupportedOperationException("cannot patch flushed segment data");
        }
        currentChunk.patchInt((int) (offset - flushedBytes), value);
    }

    @Override
    public void putBlockOfBytes(long from, long len) {
        flushCurrentChunk();
        segments.add(from, len);
        flushedBytes += len;
    }

    @Override
    public void putByte(byte value) {
        currentChunk.putByte(value);
    }

    @Override
    public void putDouble(double value) {
        currentChunk.putDouble(value);
    }

    @Override
    public void putFloat(float value) {
        currentChunk.putFloat(value);
    }

    @Override
    public void putInt(int value) {
        currentChunk.putInt(value);
    }

    @Override
    public void putLong(long value) {
        currentChunk.putLong(value);
    }

    @Override
    public void putShort(short value) {
        currentChunk.putShort(value);
    }

    @Override
    public void putString(CharSequence value) {
        currentChunk.putString(value);
    }

    @Override
    public void putUtf8(CharSequence value) {
        currentChunk.putUtf8(value);
    }

    @Override
    public void putVarint(long value) {
        currentChunk.putVarint(value);
    }

    @Override
    public void reset() {
        segments.reset();
        flushedBytes = 0;
        nextChunkIndex = 0;
        for (int i = 0, n = chunks.size(); i < n; i++) {
            chunks.getQuick(i).reset();
        }
        currentChunk = chunks.getQuick(0);
    }

    @Override
    public void skip(int bytes) {
        currentChunk.skip(bytes);
    }

    private void flushCurrentChunk() {
        int chunkSize = currentChunk.getPosition();
        if (chunkSize == 0) {
            return;
        }

        segments.add(currentChunk.getBufferPtr(), chunkSize);
        flushedBytes += chunkSize;
        currentChunk = nextChunk();
    }

    private NativeBufferWriter nextChunk() {
        nextChunkIndex++;
        if (nextChunkIndex < chunks.size()) {
            NativeBufferWriter chunk = chunks.getQuick(nextChunkIndex);
            chunk.reset();
            return chunk;
        }

        NativeBufferWriter chunk = new NativeBufferWriter();
        chunks.add(chunk);
        return chunk;
    }
}
