/*+*****************************************************************************
 * ___                 _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
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
 *
 ******************************************************************************/

package io.questdb.client.cutlass.http.client;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Vect;
import io.questdb.client.std.str.DirectUtf8String;

/**
 * Abstract base class for chunked HTTP response handling.
 */
public abstract class AbstractChunkedResponse implements Response, Fragment {
    private final static int CRLF_LEN = 2;
    private static final int STATE_CHUNK_DATA = 1;
    private static final int STATE_CHUNK_DATA_END = 2;
    private static final int STATE_CHUNK_SIZE = 0;
    private final long bufHi;
    private final long bufLo;
    private final DirectUtf8String chunkSize = new DirectUtf8String();
    private final int defaultTimeout;
    long available;
    long consumed = 0;
    long dataAddr;
    long size;
    private long dataHi;
    private long dataLo;
    private boolean receive = true;
    private int state = STATE_CHUNK_SIZE;

    /**
     * Constructs a new chunked response handler.
     *
     * @param bufLo          the low address of the buffer
     * @param bufHi          the high address of the buffer
     * @param defaultTimeout the default timeout in milliseconds
     */
    public AbstractChunkedResponse(long bufLo, long bufHi, int defaultTimeout) {
        this.bufLo = bufLo;
        this.bufHi = bufHi;
        this.defaultTimeout = defaultTimeout;
    }

    /**
     * Begins processing a new chunk of response data.
     *
     * @param lo the low address of the data
     * @param hi the high address of the data
     */
    public void begin(long lo, long hi) {
        this.dataLo = lo;
        this.dataHi = hi;
        this.state = STATE_CHUNK_SIZE;
        this.receive = hi == lo;
        size = 0;
        available = 0;
        consumed = 0;
    }

    @Override
    public long hi() {
        return dataAddr + available;
    }

    @Override
    public long lo() {
        return dataAddr;
    }

    /**
     * Reads the next fragment of chunked response data, blocking on I/O as needed.
     * <p>
     * This method drives a small state machine with three states: reading the
     * chunk-size header, consuming chunk data, and consuming the chunk terminator.
     * Each state's logic is delegated to a dedicated helper method to keep this
     * driver method simple and easy to follow.
     */
    public Fragment recv(int timeout) {
        while (true) {
            if (receive || dataLo == dataHi) {
                compactBuffer();
                dataHi += recvOrDie(dataHi, bufHi, timeout);
            }

            StateOutcome outcome = processState();
            if (outcome == StateOutcome.RETURN_FRAGMENT) {
                return this;
            }
            if (outcome == StateOutcome.RETURN_NULL) {
                return null;
            }
        }
    }

    private StateOutcome processState() {
        switch (state) {
            case STATE_CHUNK_SIZE:
                if (!tryParseChunkSize()) {
                    receive = true;
                    return StateOutcome.CONTINUE;
                }
                // fall through: chunk size parsed, state is now STATE_CHUNK_DATA

            case STATE_CHUNK_DATA:
                ChunkDataOutcome dataOutcome = tryConsumeChunkData();
                if (dataOutcome == ChunkDataOutcome.FRAGMENT_READY) {
                    return StateOutcome.RETURN_FRAGMENT;
                }
                if (dataOutcome == ChunkDataOutcome.NEED_MORE_DATA) {
                    return StateOutcome.CONTINUE;
                }
                // dataOutcome == CHUNK_EXHAUSTED: fall through to consume the terminator

            case STATE_CHUNK_DATA_END:
                if (tryConsumeChunkEnd() == ChunkEndOutcome.END_OF_STREAM) {
                    return StateOutcome.RETURN_NULL;
                }
                return StateOutcome.CONTINUE;

            default:
                throw new HttpClientException("internal error [state=" + state + ']');
        }
    }

    @Override
    public Fragment recv() {
        return recv(defaultTimeout);
    }

    /**
     * Attempts to locate and parse the hex-encoded chunk-size header terminated by CRLF.
     *
     * @return {@code true} if the chunk size was fully parsed and {@code state} advanced
     * to {@code STATE_CHUNK_DATA}; {@code false} if more data must be received
     * before the header can be completed
     */
    private boolean tryParseChunkSize() {
        long terminatorPos = findChunkSizeTerminator();
        if (terminatorPos == -1) {
            return false;
        }

        chunkSize.of(dataLo, terminatorPos + 1);
        try {
            size = Numbers.parseHexLong(chunkSize.asAsciiCharSequence());
        } catch (NumericException e) {
            throw new HttpClientException("malformed chunk size");
        }
        consumed = 0;
        state = STATE_CHUNK_DATA;
        dataLo = terminatorPos + CRLF_LEN + 1;
        return true;
    }

    /**
     * Scans the buffer for the CRLF that terminates the chunk-size header.
     *
     * @return the index of the CRLF start position, or -1 if the buffer does not
     * yet contain a complete chunk-size header
     */
    private long findChunkSizeTerminator() {
        long p = dataLo;
        while (p < dataHi) {
            if (getByte(p) == '\r') {
                p++;
                if (p >= dataHi) {
                    // incomplete CRLF, must wait for more data
                    return -1;
                }
                if (getByte(p) != '\n') {
                    throw new HttpClientException("malformed chunk size");
                }
                return p - CRLF_LEN;
            }
            p++;
        }
        return -1;
    }

    /**
     * Attempts to consume as much chunk data as is currently available in the buffer.
     *
     * @return {@link ChunkDataOutcome#FRAGMENT_READY} if a data fragment is ready to
     * be returned to the caller; {@link ChunkDataOutcome#NEED_MORE_DATA} if the
     * buffer must be refilled; {@link ChunkDataOutcome#CHUNK_EXHAUSTED} if the
     * current chunk has zero remaining bytes and the terminator should be read
     */
    private ChunkDataOutcome tryConsumeChunkData() {
        if (size > 0 && dataLo < dataHi) {
            long chunkBytesRemaining = size - consumed;
            long bufBytesRemaining = dataHi - dataLo;
            dataAddr = dataLo;

            if (chunkBytesRemaining <= bufBytesRemaining) {
                available = chunkBytesRemaining;
                consumed += chunkBytesRemaining;
                dataLo += chunkBytesRemaining;
                state = STATE_CHUNK_DATA_END;
                receive = false;
            } else {
                available = bufBytesRemaining;
                consumed += bufBytesRemaining;
                dataLo = dataHi;
                receive = true;
            }
            return ChunkDataOutcome.FRAGMENT_READY;
        }

        if (size != 0) {
            return ChunkDataOutcome.NEED_MORE_DATA;
        }

        return ChunkDataOutcome.CHUNK_EXHAUSTED;
    }

    /**
     * Attempts to consume the CRLF that terminates a chunk's data.
     *
     * @return {@link ChunkEndOutcome#END_OF_STREAM} if the zero-length terminating
     * chunk was consumed; {@link ChunkEndOutcome#NEED_MORE_DATA} otherwise,
     * meaning either more data is required or the next chunk is ready to process
     */
    private ChunkEndOutcome tryConsumeChunkEnd() {
        if (dataLo >= dataHi || (dataHi - dataLo) < CRLF_LEN) {
            receive = true;
            return ChunkEndOutcome.NEED_MORE_DATA;
        }

        if (getByte(dataLo) != '\r' || getByte(dataLo + 1) != '\n') {
            throw new HttpClientException("malformed chunk");
        }

        state = STATE_CHUNK_SIZE;
        dataLo += CRLF_LEN;
        receive = false;
        return size == 0 ? ChunkEndOutcome.END_OF_STREAM : ChunkEndOutcome.NEED_MORE_DATA;
    }

    private void compactBuffer() {
        // move unprocessed data to the front of the buffer
        // to maximise
        if (dataLo > bufLo) {
            final long len = dataHi - dataLo;
            assert len > -1;
            if (len > 0) {
                Vect.memmove(bufLo, dataLo, len);
            }
            dataLo = bufLo;
            dataHi = bufLo + len;
        }
    }

    private byte getByte(long addr) {
        assert addr != 0;
        assert addr >= bufLo;
        assert addr < bufHi;
        return Unsafe.getUnsafe().getByte(addr);
    }

    /**
     * Receives data into the buffer or throws an exception.
     *
     * @param bufLo   the low address of the buffer
     * @param bufHi   the high address of the buffer
     * @param timeout the timeout in milliseconds
     * @return the number of bytes received
     */
    protected abstract int recvOrDie(long bufLo, long bufHi, int timeout);

    private enum ChunkDataOutcome {
        FRAGMENT_READY,
        NEED_MORE_DATA,
        CHUNK_EXHAUSTED
    }

    private enum ChunkEndOutcome {
        END_OF_STREAM,
        NEED_MORE_DATA
    }

    private enum StateOutcome {
        CONTINUE,
        RETURN_FRAGMENT,
        RETURN_NULL
    }
}