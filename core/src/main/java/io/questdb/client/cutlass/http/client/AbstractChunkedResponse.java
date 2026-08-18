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

    public Fragment recv(int timeout) {
        // A positive timeout bounds the whole call, not each socket read. This loop re-reads while a
        // chunk-size line (or the chunk-data-end CRLF) is incomplete, so without one shared deadline a server
        // dribbling those bytes - one per timeout window - would run a single recv() for (line length) x
        // timeout and defeat a caller's wall-clock bound (e.g. OidcDeviceAuth.parseBody). A non-positive
        // timeout keeps the legacy "no bound" behaviour.
        final boolean bounded = timeout > 0;
        final long startNanos = bounded ? System.nanoTime() : 0L;
        while (true) {
            // Consult the deadline on EVERY pass, not only on the passes that read. A pass that neither
            // reads nor advances the state machine re-enters the loop with receive == false and
            // dataLo < dataHi, which skips the read gate below - so a deadline checked only inside that
            // gate is never reached, and the loop spins without the bound this method promises. Keeping
            // the check above the gate makes the bound hold for every pass, however the state machine got
            // there.
            int callTimeout = timeout;
            if (bounded) {
                callTimeout = timeout - (int) ((System.nanoTime() - startNanos) / 1_000_000L);
                if (callTimeout <= 0) {
                    throw new HttpClientException("timed out reading the chunked response body");
                }
            }
            if (receive || dataLo == dataHi) {
                compactBuffer();
                dataHi += recvOrDie(dataHi, bufHi, callTimeout);
            }
            long p; // moving data pointer for scanning buffer
            switch (state) {
                case STATE_CHUNK_SIZE:
                    p = dataLo;
                    // chunk size is hex encoded integer terminated with CRLF
                    long res = -1;

                    // this loop is looking at the CRLF after chunk size
                    while (p < dataHi) {
                        if (getByte(p) == '\r') {
                            p++;
                            if (p < dataHi) {
                                if (getByte(p) == '\n') {
                                    res = p - CRLF_LEN;
                                    break;
                                } else {
                                    throw new HttpClientException("malformed chunk size");
                                }
                            } else {
                                // CRLF at chunk size is incomplete, we have to
                                // wait until we receive the full thing
                                break;
                            }
                        }
                        p++;
                    }

                    if (res != -1) {
                        // at this stage we consumed the chunk size end (CRLF)
                        chunkSize.of(dataLo, res + 1);
                        try {
                            // parseHexLong rejects an overflowing size rather than wrapping it, so nothing
                            // is needed here beyond catching NumericException below. Each residue used to
                            // break framing its own way: a negative one (8000000000000000 is the smallest)
                            // matched neither the "size > 0" data branch nor the "size == 0" terminator
                            // below, so the state machine looped on it forever; zero (10000000000000000)
                            // read as the TERMINAL chunk, truncating the response and losing framing for
                            // the next keep-alive response on the connection; a positive residue framed a
                            // short data chunk and mis-read everything after it. The size line is chosen by
                            // the server, which for an OIDC discovery or token response is untrusted.
                            size = Numbers.parseHexLong(chunkSize.asAsciiCharSequence());
                            consumed = 0;
                            // consume data buffer ignoring chunk size value and its furniture
                            state = STATE_CHUNK_DATA;
                            dataLo = res + CRLF_LEN + 1;
                        } catch (NumericException e) {
                            throw new HttpClientException("malformed chunk size");
                        }

                        // fall thru the switch to process remaining data buffer
                    } else {
                        // we have not received complete chunk size value yet
                        receive = true;
                        break;
                    }

                case STATE_CHUNK_DATA:
                    // there is data in the buffer
                    if (size > 0 && dataLo < dataHi) {
                        long chunkBytesRemaining = size - consumed;
                        long bufBytesRemaining = dataHi - dataLo;

                        // chunk data starts with dataLo address
                        dataAddr = dataLo;

                        if (chunkBytesRemaining <= bufBytesRemaining) {
                            // chunk data fits in the buffer
                            available = chunkBytesRemaining;
                            consumed += chunkBytesRemaining;
                            // skip chunk data to begin processing chunk end
                            dataLo += chunkBytesRemaining;
                            state = STATE_CHUNK_DATA_END;
                            receive = false;
                        } else {
                            available = bufBytesRemaining;
                            consumed += bufBytesRemaining;
                            // we consumed the entire buffer for chunk data
                            // we must recv more data
                            dataLo = dataHi;
                            receive = true;
                        }
                        return this;
                    }

                    if (size != 0) {
                        // no chunk data in the buffer
                        break;
                    }
                    // fall thru to read chunk end

                case STATE_CHUNK_DATA_END:
                    // we are here to consume CRLF
                    // we have to have two bytes here
                    if (dataLo < dataHi && (dataHi - dataLo) >= CRLF_LEN) {
                        if (getByte(dataLo) == '\r' && getByte(dataLo + 1) == '\n') {
                            state = STATE_CHUNK_SIZE;
                            dataLo += CRLF_LEN;
                            receive = false;
                            // we had to consume the tail CRLF after the last chunk
                            // not to leave garbage in the recv buffer
                            if (size == 0) {
                                return null;
                            }
                            break;
                        } else {
                            throw new HttpClientException("malformed chunk");
                        }
                    } else {
                        receive = true;
                    }
                    break;
                default:
                    throw new HttpClientException("internal error [state=" + state + ']');
            }
        }
    }

    @Override
    public Fragment recv() {
        return recv(defaultTimeout);
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
}
