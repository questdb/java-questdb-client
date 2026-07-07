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

package io.questdb.client.network;

import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.tcp.DelegatingTlsChannel;
import io.questdb.client.std.Chars;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Vect;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public final class JavaTlsClientSocket implements Socket {

    private static final long ADDRESS_FIELD_OFFSET;
    private static final TrustManager[] BLIND_TRUST_MANAGERS = new TrustManager[]{new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] certs, String t) {
        }

        public void checkServerTrusted(X509Certificate[] certs, String t) {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    }};
    private static final long CAPACITY_FIELD_OFFSET;
    private static final int INITIAL_BUFFER_CAPACITY_BYTES = 256 * 1024;
    private static final long LIMIT_FIELD_OFFSET;
    private static final int STATE_CLOSING = 3;
    private static final int STATE_EMPTY = 0;
    private static final int STATE_PLAINTEXT = 1;
    private static final int STATE_TLS = 2;
    private final ByteBuffer callerOutputBuffer;
    private final Socket delegate;
    private final Logger log;
    private final ClientTlsConfiguration tlsConfig;
    private final ByteBuffer unwrapInputBuffer;
    private final ByteBuffer unwrapOutputBuffer;
    private final ByteBuffer wrapInputBuffer;
    private final ByteBuffer wrapOutputBuffer;
    private SSLEngine sslEngine;
    private int state = STATE_EMPTY;
    private long unwrapInputBufferPtr;
    private long unwrapOutputBufferPtr;
    private long wrapOutputBufferPtr;

    JavaTlsClientSocket(NetworkFacade nf, Logger log, ClientTlsConfiguration tlsConfig) {
        this.delegate = new PlainSocket(nf, log);
        this.log = log;
        this.tlsConfig = tlsConfig;

        // wrapInputBuffer and callerOutputBuffer are just placeholders: their address, capacity and
        // limit are reset to point at the caller's buffer in send() and recv() respectively, so the
        // SSLEngine can read/write the caller's memory directly without an extra copy. They have
        // capacity = 0 during handshake because handshake does not touch them.
        this.wrapInputBuffer = ByteBuffer.allocateDirect(0);
        this.callerOutputBuffer = ByteBuffer.allocateDirect(0);

        // wrapOutputBuffer, unwrapInputBuffer and unwrapOutputBuffer all back internal allocations
        // that are only created when starting a new TLS session, so the ByteBuffer instances can be
        // reused across sessions.
        this.wrapOutputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapInputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapOutputBuffer = ByteBuffer.allocateDirect(0);
    }

    @Override
    public void close() {
        log.debug("closing TLS socket [fd={}]", delegate.getFd());
        switch (state) {
            case STATE_CLOSING: // intentional fall through
            case STATE_EMPTY:
                return;
            case STATE_TLS: {
                assert sslEngine != null;
                state = STATE_CLOSING;
                sslEngine.closeOutbound();
                try {
                    // we don't care about the result. wrap() is just to generate a close_notify TLS record
                    // if that fails, we don't care, we are closing anyway
                    sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                    while (wantsTlsWrite()) {
                        int n = tlsIO(Socket.WRITE_FLAG);
                        if (n < 0) {
                            log.debug("could not send TLS close_notify");
                            break;
                        }
                    }
                } catch (SSLException e) {
                    log.debug("could not send TLS close_notify", e);
                }
                sslEngine = null;
            } // fall through
            case STATE_PLAINTEXT:
                state = STATE_CLOSING;
                // it could be that we allocated buffers but failed to start a TLS session
                // so we need to free the buffers even in the STATE_PLAINTEXT state
                freeInternalBuffers();
                delegate.close();
                state = STATE_EMPTY;
                break;
        }
    }

    @Override
    public int getFd() {
        return delegate.getFd();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public void of(int fd) {
        assert state == STATE_EMPTY;
        delegate.of(fd);
        state = STATE_PLAINTEXT;
    }

    @Override
    public int recv(long bufferPtr, int bufferLen) {
        assert sslEngine != null;

        try {
            // Pending plaintext from a previous spill is held in the internal buffer. Drain it first.
            if (unwrapOutputBuffer.position() != 0) {
                return drainUnwrapOutputBuffer(bufferPtr, bufferLen);
            }

            // Fast path: have the SSLEngine decrypt straight into the caller's buffer. We only fall
            // back to the internal spill buffer if a single TLS record does not fit in the caller's
            // buffer, in which case we drain the spill buffer to the caller and return.
            resetBufferToPointer(callerOutputBuffer, bufferPtr, bufferLen);
            ByteBuffer output = callerOutputBuffer;
            boolean spilling = false;

            for (; ; ) {
                int n = readFromSocket();
                assert unwrapInputBuffer.position() == 0 : "unwrapInputBuffer is not compacted";
                int bytesAvailable = unwrapInputBuffer.limit();
                if (n < 0 && bytesAvailable == 0) {
                    if (output.position() != 0) {
                        return spilling ? drainUnwrapOutputBuffer(bufferPtr, bufferLen) : output.position();
                    }
                    return n;
                }
                if (bytesAvailable == 0) {
                    if (output.position() != 0) {
                        return spilling ? drainUnwrapOutputBuffer(bufferPtr, bufferLen) : output.position();
                    }
                    return 0;
                }

                SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, output);

                // compact the TLS buffer
                int bytesConsumed = result.bytesConsumed();
                int bytesRemaining = bytesAvailable - bytesConsumed;
                if (bytesRemaining > 0) {
                    Vect.memcpy(unwrapInputBufferPtr, unwrapInputBufferPtr + bytesConsumed, bytesRemaining);
                }
                unwrapInputBuffer.position(0);
                unwrapInputBuffer.limit(bytesRemaining);

                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        if (output.position() != 0) {
                            return spilling ? drainUnwrapOutputBuffer(bufferPtr, bufferLen) : output.position();
                        }
                        return 0;
                    case BUFFER_OVERFLOW:
                        if (output.position() != 0) {
                            // Output already has plaintext: hand it to the caller and let the
                            // unprocessed record be picked up on the next recv() call.
                            return spilling ? drainUnwrapOutputBuffer(bufferPtr, bufferLen) : output.position();
                        }
                        if (spilling) {
                            // Internal buffer cannot fit a single record either: grow and retry.
                            growUnwrapOutputBuffer();
                        } else {
                            // Caller's buffer cannot fit a single record. Switch to the internal
                            // spill buffer for this record (and any further records that fit), then
                            // drain to the caller.
                            output = unwrapOutputBuffer;
                            spilling = true;
                        }
                        break;
                    case OK:
                        // Plaintext (if any) is accumulating in `output`. Keep looping so we either
                        // batch more records or hit BUFFER_UNDERFLOW / BUFFER_OVERFLOW.
                        break;
                    case CLOSED:
                        log.debug("SSL engine closed");
                        // We received a TLS close notification from the server. We don't expect any further data from this connection.
                        // If we have some previously unwrapped data then let's return it so the caller has a chance to process them.
                        // If a caller calls recv() again and we have no remaining plaintext to return, we will return -1 so the
                        // caller learned that the connection is closed.
                        // If we have no plaintext data to return now then we can immediately indicate that we are done with the connection.
                        if (output.position() != 0) {
                            return spilling ? drainUnwrapOutputBuffer(bufferPtr, bufferLen) : output.position();
                        }
                        return -1;
                }
            }
        } catch (SSLException e) {
            log.error("could not unwrap SSL packet", e);
            return -1;
        }
    }

    @Override
    public int send(long bufferPtr, int bufferLen) {
        try {
            resetBufferToPointer(wrapInputBuffer, bufferPtr, bufferLen);
            wrapInputBuffer.position(0);
            int plainBytesConsumed = 0;
            for (; ; ) {
                // try to send whatever we have in the encrypted buffer
                int bytesToSend = wrapOutputBuffer.position();
                if (bytesToSend > 0) {
                    int sent = writeToSocket(bytesToSend);
                    if (sent < 0) {
                        return sent;
                    } else if (sent < bytesToSend) {
                        // we didn't manage to send everything we wanted, the socket is full
                        return plainBytesConsumed;
                    }
                }

                if (wrapInputBuffer.remaining() == 0) {
                    // we sent whatever we could and there is nothing left to be wrapped
                    return plainBytesConsumed;
                }

                SSLEngineResult result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                plainBytesConsumed += result.bytesConsumed();
                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        throw new AssertionError("Underflow while reading a plain text. This should not happen, please report as a bug");
                    case BUFFER_OVERFLOW:
                        if (wrapOutputBuffer.position() == 0) {
                            // not even a single byte was written to the output buffer even the buffer is empty
                            // apparently the output buffer cannot fit even a single TLS record. let's grow it and try again!
                            growWrapOutputBuffer();
                        }
                        break;
                    case OK:
                        break;
                    case CLOSED:
                        log.error("Attempt to send to a closed SSLEngine");
                        return -1;
                }
            }
        } catch (SSLException e) {
            log.error("could not wrap SSL packet", e);
            return -1;
        }
    }

    @Override
    public void startTlsSession(CharSequence peerName, SocketReadinessWaiter waiter) throws TlsSessionInitFailedException {
        assert state == STATE_PLAINTEXT;
        prepareInternalBuffers();
        try {
            this.sslEngine = createSslEngine(peerName);
            this.sslEngine.beginHandshake();
            runHandshake(waiter);
            // unwrap input buffer: read mode and empty
            unwrapInputBuffer.position(0);
            unwrapInputBuffer.limit(0);

            // write mode and empty
            unwrapOutputBuffer.position(0);
            unwrapOutputBuffer.limit(unwrapOutputBuffer.capacity());
            wrapOutputBuffer.clear();
            state = STATE_TLS;
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | IOException |
                 CertificateException e) {
            throw TlsSessionInitFailedException.instance("TLS session creation failed [error=").put(e.getMessage()).put(']');
        }
    }

    @Override
    public boolean supportsTls() {
        return true;
    }

    @Override
    public int tlsIO(int readinessFlags) {
        if ((readinessFlags & WRITE_FLAG) != 0) {
            int bytesToSend = wrapOutputBuffer.position();
            if (bytesToSend > 0) {
                int n = writeToSocket(bytesToSend);
                return Math.min(n, 0);
            }
        }
        return 0;
    }

    @Override
    public boolean wantsTlsWrite() {
        // we want to write if we have TLS data to send
        return wrapOutputBuffer.position() > 0;
    }

    private static long allocateMemoryAndResetBuffer(ByteBuffer buffer, int capacity) {
        long newAddress = Unsafe.malloc(capacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, capacity);
        return newAddress;
    }

    private static long expandBuffer(ByteBuffer buffer, long oldAddress) {
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity * 2;
        long newAddress = Unsafe.realloc(oldAddress, oldCapacity, newCapacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, newCapacity);
        return newAddress;
    }

    private static InputStream openTrustStoreStream(String trustStorePath) throws FileNotFoundException {
        InputStream trustStoreStream;
        if (trustStorePath.startsWith("classpath:")) {
            String adjustedPath = trustStorePath.substring("classpath:".length());
            trustStoreStream = DelegatingTlsChannel.class.getResourceAsStream(adjustedPath);
            if (trustStoreStream == null) {
                throw new LineSenderException("configured trust store is unavailable ")
                        .put("[path=").put(trustStorePath).put("]");
            }
            return trustStoreStream;
        }
        return new FileInputStream(trustStorePath);
    }

    private static void resetBufferToPointer(ByteBuffer buffer, long ptr, int len) {
        assert buffer.isDirect();
        Unsafe.getUnsafe().putLong(buffer, ADDRESS_FIELD_OFFSET, ptr);
        Unsafe.getUnsafe().putLong(buffer, LIMIT_FIELD_OFFSET, len);
        Unsafe.getUnsafe().putLong(buffer, CAPACITY_FIELD_OFFSET, len);
        buffer.position(0);
    }

    private SSLEngine createSslEngine(CharSequence serverName) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException {
        SSLContext sslContext;
        String trustStorePath = tlsConfig.trustStorePath();
        int tlsValidationMode = tlsConfig.tlsValidationMode();
        if (trustStorePath != null) {
            sslContext = SSLContext.getInstance("TLS");
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore jks = KeyStore.getInstance("JKS");
            try (InputStream trustStoreStream = openTrustStoreStream(trustStorePath)) {
                jks.load(trustStoreStream, tlsConfig.trustStorePassword());
            }
            tmf.init(jks);
            TrustManager[] trustManagers = tmf.getTrustManagers();
            sslContext.init(null, trustManagers, new SecureRandom());
        } else if (tlsValidationMode == ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE) {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, BLIND_TRUST_MANAGERS, new SecureRandom());
        } else {
            sslContext = SSLContext.getDefault();
        }

        SSLEngine sslEngine = sslContext.createSSLEngine(Chars.toString(serverName), -1);
        if (tlsValidationMode != ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE) {
            SSLParameters sslParameters = sslEngine.getSSLParameters();
            // The https validation algorithm? That looks confusing! After all we are not using any
            // https here at so what does it mean?
            // It's actually simple: It just instructs the SSLEngine to perform the same hostname validation
            // as it does during HTTPS connections. SSLEngine does not do hostname validation by default. Without
            // this option SSLEngine would happily accept any certificate as long as it's signed by a trusted CA.
            // This option will make sure certificates are accepted only if they were issued for the
            // server we are connecting to.
            sslParameters.setEndpointIdentificationAlgorithm("https");
            sslEngine.setSSLParameters(sslParameters);
        }

        sslEngine.setUseClientMode(true);
        return sslEngine;
    }

    private void freeInternalBuffers() {
        long ptrToFree = wrapOutputBufferPtr;
        if (ptrToFree != 0) {
            int capacity = wrapOutputBuffer.capacity();
            assert capacity != 0;
            // reset to dummy buffer so a bug in the code will not dereference a dangling pointer
            // if a bug results in a dereference of null then a subsequent crash produces a better diagnostics
            resetBufferToPointer(wrapOutputBuffer, 0, 0);
            wrapOutputBufferPtr = 0;
            Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);

            // if the first buffer was initialized then the 2nd buffer must have been initialized too
            assert unwrapInputBufferPtr != 0;
            capacity = unwrapInputBuffer.capacity();
            assert capacity != 0;
            resetBufferToPointer(unwrapInputBuffer, 0, 0);
            ptrToFree = unwrapInputBufferPtr;
            unwrapInputBufferPtr = 0;
            Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);

            capacity = unwrapOutputBuffer.capacity();
            assert capacity != 0;
            resetBufferToPointer(unwrapOutputBuffer, 0, 0);
            ptrToFree = unwrapOutputBufferPtr;
            unwrapOutputBufferPtr = 0;
            Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);
        }
    }

    private void growWrapOutputBuffer() {
        wrapOutputBufferPtr = expandBuffer(wrapOutputBuffer, wrapOutputBufferPtr);
    }

    private void growUnwrapOutputBuffer() {
        unwrapOutputBufferPtr = expandBuffer(unwrapOutputBuffer, unwrapOutputBufferPtr);
    }

    private void prepareInternalBuffers() {
        int initialCapacity = Integer.getInteger("questdb.experimental.tls.buffersize", INITIAL_BUFFER_CAPACITY_BYTES);
        this.wrapOutputBufferPtr = allocateMemoryAndResetBuffer(wrapOutputBuffer, initialCapacity);
        this.unwrapInputBufferPtr = allocateMemoryAndResetBuffer(unwrapInputBuffer, initialCapacity);
        unwrapInputBuffer.flip(); // read mode
        this.unwrapOutputBufferPtr = allocateMemoryAndResetBuffer(unwrapOutputBuffer, initialCapacity);
    }

    private int drainUnwrapOutputBuffer(long bufferPtr, int bufferLen) {
        unwrapOutputBuffer.flip();
        int oldPosition = unwrapOutputBuffer.position();
        int len = Math.min(bufferLen, unwrapOutputBuffer.remaining());
        if (len > 0) {
            Vect.memcpy(bufferPtr, unwrapOutputBufferPtr + oldPosition, len);
        }
        unwrapOutputBuffer.position(oldPosition + len);
        unwrapOutputBuffer.compact();
        return len;
    }

    private int readFromSocket() {
        // unwrap input buffer: read mode

        int writerPos = unwrapInputBuffer.limit(); // we are in the read mode, so limit (for reader) = position for writer
        int freeSpace = unwrapInputBuffer.capacity() - writerPos;
        if (freeSpace == 0) {
            // no point in reading if we have no space left
            return 0;
        }

        assert Unsafe.getUnsafe().getLong(unwrapInputBuffer, ADDRESS_FIELD_OFFSET) == unwrapInputBufferPtr;
        long adjustedPtr = unwrapInputBufferPtr + writerPos;

        int n = delegate.recv(adjustedPtr, freeSpace);
        if (n < 0) {
            return n;
        }
        unwrapInputBuffer.limit(writerPos + n);
        return n;
    }

    /**
     * Drives the TLS handshake state machine to completion. When the
     * non-blocking socket would block, hands control to {@code waiter} (which
     * parks on the event loop bounded by the connect deadline) instead of
     * busy-spinning on read/write. Extracted from {@link #startTlsSession} so a
     * stub {@code sslEngine} can exercise the wait paths in isolation.
     */
    private void runHandshake(SocketReadinessWaiter waiter) throws SSLException, TlsSessionInitFailedException {
        SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
        // Exit on NOT_HANDSHAKING as well as FINISHED: getHandshakeStatus() (used by the NEED_TASK
        // branch) never returns FINISHED per the JSSE contract -- it returns NOT_HANDSHAKING once the
        // handshake completes. Without this, a delegated task that is the terminal step would leave the
        // loop on NOT_HANDSHAKING, match no case, and busy-spin forever with no deadline escape.
        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
                && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (handshakeStatus) {
                case NEED_TASK:
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;
                case NEED_WRAP: {
                    SSLEngineResult result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                    handshakeStatus = result.getHandshakeStatus();
                    switch (result.getStatus()) {
                        case BUFFER_UNDERFLOW:
                            // there cannot be underflow since wrap() during handshake does not read from the input buffer at all
                            throw new AssertionError("Buffer underflow during TLS handshake. This should not happen. please report as a bug");
                        case BUFFER_OVERFLOW:
                            if (wrapOutputBuffer.position() != 0) {
                                // wrap() left bytes behind without producing a complete record. The OK
                                // branch is the only place that drains and clears, so a non-empty
                                // buffer here means we would re-enter NEED_WRAP with identical state
                                // and spin forever. Fail loudly instead.
                                throw new AssertionError("Buffer overflow during TLS handshake with non-empty output buffer. This should not happen, please report as a bug");
                            }
                            // in theory, this can happen if the output buffer is too small to fit a single TLS handshake record,
                            // but that would indicate our starting buffer is too small.
                            growWrapOutputBuffer();
                            break;
                        case OK:
                            // wrapOutputBuffer: write mode
                            int written = 0;
                            int bufferLimit = wrapOutputBuffer.position();
                            while (written < bufferLimit) {
                                int n = delegate.send(wrapOutputBufferPtr + written, bufferLimit - written);
                                if (n < 0) {
                                    throw TlsSessionInitFailedException.instance("socket write error");
                                }
                                if (n == 0) {
                                    // The non-blocking socket's send buffer is full. Wait for it to
                                    // become writable -- bounded by the connect deadline -- instead of
                                    // busy-spinning on send().
                                    waiter.awaitReady(IOOperation.WRITE);
                                }
                                written += n;
                            }
                            wrapOutputBuffer.clear();
                            break;
                        case CLOSED:
                            throw TlsSessionInitFailedException.instance("server closed connection unexpectedly");
                    }
                    break;
                }
                case NEED_UNWRAP: {
                    int n = readFromSocket();
                    if (n < 0) {
                        throw TlsSessionInitFailedException.instance("socket read error");
                    }
                    SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
                    handshakeStatus = result.getHandshakeStatus();
                    switch (result.getStatus()) {
                        case BUFFER_UNDERFLOW:
                            // Not enough bytes for a complete TLS record yet. If the last read
                            // drained the socket (n == 0, would-block on the non-blocking fd), wait
                            // for it to become readable -- bounded by the connect deadline -- instead
                            // of busy-spinning. A positive n means we read a partial record, so loop
                            // immediately and read the rest.
                            if (n == 0) {
                                waiter.awaitReady(IOOperation.READ);
                            }
                            break;
                        case BUFFER_OVERFLOW:
                            if (unwrapOutputBuffer.position() != 0) {
                                // unwrap() produced plaintext but signalled overflow without consuming
                                // the next record. Nothing in the handshake loop drains this buffer,
                                // so re-entering NEED_UNWRAP would spin forever. Fail loudly.
                                throw new AssertionError("Buffer overflow during TLS handshake with non-empty output buffer. This should not happen, please report as a bug");
                            }
                            // in theory, this can happen if the output buffer is too small to fit a single TLS handshake record,
                            // but that would indicate our starting buffer is too small.
                            growUnwrapOutputBuffer();
                            break;
                        case OK:
                            // good, let's see what we need to do next
                            break;
                        case CLOSED:
                            throw TlsSessionInitFailedException.instance("server closed connection unexpectedly");
                    }
                }
                break;
            }
        }
    }

    private int writeToSocket(int bytesToSend) {
        // wrapOutputBuffer is in the write mode
        int n = delegate.send(wrapOutputBufferPtr, bytesToSend);
        if (n < 0) {
            // ops, something went wrong
            return n;
        }

        int bytesRemaining = bytesToSend - n;
        // compact the buffer
        Vect.memmove(wrapOutputBufferPtr, wrapOutputBufferPtr + n, bytesRemaining);
        wrapOutputBuffer.position(bytesRemaining);
        return n;
    }

    static {
        Field addressField;
        Field limitField;
        Field capacityField;
        try {
            addressField = Buffer.class.getDeclaredField("address");
            limitField = Buffer.class.getDeclaredField("limit");
            capacityField = Buffer.class.getDeclaredField("capacity");
        } catch (NoSuchFieldException e) {
            // possible improvement: implement a fallback strategy when reflection is unavailable for any reason.
            throw new ExceptionInInitializerError(e);
        }
        ADDRESS_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(addressField);
        LIMIT_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(limitField);
        CAPACITY_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(capacityField);
    }
}
