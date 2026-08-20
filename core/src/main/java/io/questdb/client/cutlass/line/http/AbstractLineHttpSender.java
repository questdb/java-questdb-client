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

package io.questdb.client.cutlass.line.http;

import io.questdb.client.BuildInformationHolder;
import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.HttpTokenProvider;
import io.questdb.client.Sender;
import io.questdb.client.cairo.TableUtils;
import io.questdb.client.cutlass.http.HttpConstants;
import io.questdb.client.cutlass.http.HttpException;
import io.questdb.client.cutlass.http.HttpKeywords;
import io.questdb.client.cutlass.http.client.Fragment;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.cutlass.http.client.Response;
import io.questdb.client.cutlass.json.JsonException;
import io.questdb.client.cutlass.json.JsonLexer;
import io.questdb.client.cutlass.json.JsonParser;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.Chars;
import io.questdb.client.std.IntList;
import io.questdb.client.std.Misc;
import io.questdb.client.std.Mutable;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Os;
import io.questdb.client.std.Rnd;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.client.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8Sequence;
import io.questdb.client.std.str.Utf8s;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public abstract class AbstractLineHttpSender implements Sender {
    private static final String PATH = "/write?precision=n";
    private static final int RETRY_BACKOFF_MULTIPLIER = 2;
    private static final int RETRY_INITIAL_BACKOFF_MS = 10;
    private static final int RETRY_MAX_JITTER_MS = 10;
    private final String authToken;
    private final int autoFlushRows;
    private final int baseTimeoutMillis;
    private final DirectByteSlice bufferView = new DirectByteSlice();
    private final long flushIntervalNanos;
    private final ObjList<String> hosts;
    private final boolean isTls;
    private final int maxBackoffMillis;
    private final int maxNameLength;
    private final long maxRetriesNanos;
    private final long minRequestThroughput;
    private final String password;
    private final String path;
    private final IntList ports;
    private final CharSequence questDBVersion;
    private final Rnd rnd;
    private final StringSink sink = new StringSink();
    private final String userAgent;
    private final String username;
    protected HttpClient.Request request;
    private HttpClient client;
    private boolean closed;
    private int currentAddressIndex;
    private long flushAfterNanos = Long.MAX_VALUE;
    private HttpTokenProvider httpTokenProvider;
    private boolean isTokenPending;
    private JsonErrorParser jsonErrorParser;
    private boolean lastFlushFailed;
    private long pendingRows;
    private int rowBookmark;
    private RequestState state = RequestState.EMPTY;

    protected AbstractLineHttpSender(
            String host,
            int port,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            int maxNameLength,
            long maxRetriesNanos,
            int maxBackoffMillis,
            long minRequestThroughput,
            long flushIntervalNanos,
            Rnd rnd
    ) {
        this(
                host,
                port,
                PATH,
                clientConfiguration,
                tlsConfig,
                null,
                autoFlushRows,
                authToken,
                username,
                password,
                maxNameLength,
                maxRetriesNanos,
                maxBackoffMillis,
                minRequestThroughput,
                flushIntervalNanos,
                rnd
        );
    }

    protected AbstractLineHttpSender(
            String host,
            int port,
            String path,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            HttpClient client,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            int maxNameLength,
            long maxRetriesNanos,
            int maxBackoffMillis,
            long minRequestThroughput,
            long flushIntervalNanos,
            Rnd rnd
    ) {
        this(new ObjList<>(host), IntList.createWithValues(port), path, clientConfiguration, tlsConfig, client, autoFlushRows, authToken, username, password, maxNameLength, maxRetriesNanos, maxBackoffMillis, minRequestThroughput,
                flushIntervalNanos,
                0,
                rnd
        );
    }

    @SuppressWarnings("ReplaceNullCheck")
    protected AbstractLineHttpSender(
            ObjList<String> hosts,
            IntList ports,
            String path,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            HttpClient client,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            int maxNameLength,
            long maxRetriesNanos,
            int maxBackoffMillis,
            long minRequestThroughput,
            long flushIntervalNanos,
            int currentAddressIndex,
            Rnd rnd
    ) {
        assert authToken == null || (username == null && password == null);
        this.maxRetriesNanos = maxRetriesNanos;
        this.maxBackoffMillis = maxBackoffMillis;
        this.hosts = hosts;
        this.ports = ports;
        this.currentAddressIndex = currentAddressIndex;
        this.path = path != null ? path : PATH;
        this.autoFlushRows = autoFlushRows;
        this.authToken = authToken;
        this.username = username;
        this.password = password;
        this.minRequestThroughput = minRequestThroughput;
        this.flushIntervalNanos = flushIntervalNanos;
        this.baseTimeoutMillis = clientConfiguration.getTimeout();

        this.isTls = tlsConfig != null;

        if (client != null) {
            this.client = client;
        } else {
            this.client = isTls ?
                    HttpClientFactory.newTlsInstance(clientConfiguration, tlsConfig)
                    : HttpClientFactory.newPlainTextInstance(clientConfiguration);
        }
        this.questDBVersion = new BuildInformationHolder().getSwVersion();
        // precompute the User-Agent header value once: newRequest() runs on every flush, so concatenating it
        // there would allocate a String each time
        this.userAgent = "QuestDB/java/" + questDBVersion;
        this.request = newRequest();
        this.maxNameLength = maxNameLength;
        this.rnd = rnd;
    }

    @SuppressWarnings("unused")
    public static AbstractLineHttpSender createLineSender(
            String host,
            int port,
            String path,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            int maxNameLength,
            long maxRetriesNanos,
            int maxBackoffMillis,
            long minRequestThroughput,
            long flushIntervalNanos,
            int protocolVersion
    ) {
        return createLineSender(new ObjList<>(host), IntList.createWithValues(port), path, clientConfiguration, tlsConfig, autoFlushRows, authToken, username, password, maxNameLength, maxRetriesNanos, maxBackoffMillis, minRequestThroughput,
                flushIntervalNanos,
                protocolVersion,
                null
        );
    }

    /**
     * Provider-less form of the overload below, kept so callers compiled against the pre-{@code
     * httpTokenProvider} signature keep linking. Mirrors the single-host overload above, which delegates
     * with the same {@code null} provider.
     */
    @SuppressWarnings("unused")
    public static AbstractLineHttpSender createLineSender(
            ObjList<String> hosts,
            IntList ports,
            String path,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            int maxNameLength,
            long maxRetriesNanos,
            int maxBackoffMillis,
            long minRequestThroughput,
            long flushIntervalNanos,
            int protocolVersion
    ) {
        return createLineSender(hosts, ports, path, clientConfiguration, tlsConfig, autoFlushRows,
                authToken, username, password, maxNameLength, maxRetriesNanos, maxBackoffMillis,
                minRequestThroughput, flushIntervalNanos, protocolVersion, null);
    }

    public static AbstractLineHttpSender createLineSender(
            ObjList<String> hosts,
            IntList ports,
            String path,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            int maxNameLength,
            long maxRetriesNanos,
            int maxBackoffMillis,
            long minRequestThroughput,
            long flushIntervalNanos,
            int protocolVersion,
            HttpTokenProvider httpTokenProvider
    ) {
        HttpClient cli = null;
        Rnd rnd = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
        int currentAddressIndex = 0;

        // if user does not set protocol version explicit, client will try to detect it from server
        StringSink lastErrorSink = null;
        if (protocolVersion == PROTOCOL_VERSION_NOT_SET_EXPLICIT) {
            if (tlsConfig != null) {
                cli = HttpClientFactory.newTlsInstance(clientConfiguration, tlsConfig);
            } else {
                cli = HttpClientFactory.newPlainTextInstance(clientConfiguration);
            }
            try (JsonSettingsParser parser = new JsonSettingsParser()) {
                if (hosts.size() < 1 || ports.size() < 1 || hosts.size() != ports.size()) {
                    throw new LineSenderException(
                            "addresses have been improperly configured [hostCount=").put(hosts.size())
                            .put(", portCount=").put(ports.size()).put(']');
                }
                long retryingDeadlineNanos = Long.MIN_VALUE; // we want to start retry timer only after a first failure
                int retryBackoff = Math.min(maxBackoffMillis, RETRY_INITIAL_BACKOFF_MS);
                for (int i = 0; ; i++) {
                    currentAddressIndex = i % hosts.size();

                    final String host = hosts.getQuick(currentAddressIndex);
                    final int port = ports.getQuick(currentAddressIndex);
                    try {
                        HttpClient.Request req = cli.newRequest(host, port).GET().url(clientConfiguration.getSettingsPath());
                        HttpClient.ResponseHeaders response = req.send();
                        response.await();
                        DirectUtf8Sequence statusCode = response.getStatusCode();
                        if (isSuccessResponse(statusCode)) {
                            parser.clear();
                            parser.parse(response.getResponse());
                            protocolVersion = parser.getDefaultProtocolVersion();
                            if (parser.getMaxNameLen() != 0) {
                                maxNameLength = parser.getMaxNameLen();
                            }
                            if (parser.isAcceptingWrites()) {
                                break;
                            }
                        } else if (isNotFound(statusCode)) {
                            // The client is unable to differentiate between a server shutdown and connecting to an older version.
                            // So, the protocol is set to PROTOCOL_VERSION_V1 here for both scenarios.
                            protocolVersion = PROTOCOL_VERSION_V1;
                            break;
                        }
                        if (lastErrorSink == null) {
                            lastErrorSink = new StringSink();
                        } else {
                            lastErrorSink.clear();
                        }
                        // the construct-time probe retries on any read abort (caught below), so its own
                        // configured request timeout is the right bound here
                        chunkedResponseToSink(response, lastErrorSink, clientConfiguration.getTimeout());
                    } catch (HttpClientException e) {
                        if (lastErrorSink == null) {
                            lastErrorSink = new StringSink();
                        } else {
                            lastErrorSink.clear();
                        }
                        lastErrorSink.put(e.getMessage());
                        // ignore, we will retry
                    }
                    long nowNanos = System.nanoTime();
                    retryingDeadlineNanos = (retryingDeadlineNanos == Long.MIN_VALUE)
                            ? nowNanos + maxRetriesNanos
                            : retryingDeadlineNanos;
                    if (nowNanos >= retryingDeadlineNanos) {
                        break;
                    }
                    cli.disconnect(); // forces reconnect
                    retryBackoff = backoff(rnd, retryBackoff, maxBackoffMillis);
                }
            } catch (LineSenderException e) {
                Misc.free(cli);
                throw e;
            } catch (Throwable e) {
                Misc.free(cli);
                throw new LineSenderException("Failed to detect server line protocol version", e);
            }
        }

        if (protocolVersion == PROTOCOL_VERSION_NOT_SET_EXPLICIT) {
            Misc.free(cli);
            if (lastErrorSink != null) {
                // sanitize the raw server body before it reaches the exception message (and any log/terminal):
                // a hostile or proxied endpoint must not splice control, ANSI or bidi chars into the render
                throw new LineSenderException("Failed to detect server line protocol version: ").putAsPrintable(lastErrorSink);
            }
            throw new LineSenderException("Failed to detect server line protocol version");
        }

        final AbstractLineHttpSender sender;
        switch (protocolVersion) {
            case PROTOCOL_VERSION_V1:
                sender = new LineHttpSenderV1(
                        hosts,
                        ports,
                        path,
                        clientConfiguration,
                        tlsConfig,
                        cli,
                        autoFlushRows,
                        authToken,
                        username,
                        password,
                        maxNameLength,
                        maxRetriesNanos,
                        maxBackoffMillis,
                        minRequestThroughput,
                        flushIntervalNanos,
                        currentAddressIndex,
                        rnd
                );
                break;
            case PROTOCOL_VERSION_V2:
                sender = new LineHttpSenderV2(
                        hosts,
                        ports,
                        path,
                        clientConfiguration,
                        tlsConfig,
                        cli,
                        autoFlushRows,
                        authToken,
                        username,
                        password,
                        maxNameLength,
                        maxRetriesNanos,
                        maxBackoffMillis,
                        minRequestThroughput,
                        flushIntervalNanos,
                        currentAddressIndex,
                        rnd
                );
                break;
            case PROTOCOL_VERSION_V3:
                sender = new LineHttpSenderV3(
                        hosts,
                        ports,
                        path,
                        clientConfiguration,
                        tlsConfig,
                        cli,
                        autoFlushRows,
                        authToken,
                        username,
                        password,
                        maxNameLength,
                        maxRetriesNanos,
                        maxBackoffMillis,
                        minRequestThroughput,
                        flushIntervalNanos,
                        currentAddressIndex,
                        rnd
                );
                break;
            default:
                throw new LineSenderException("Unsupported protocol version: " + protocolVersion);
        }
        if (httpTokenProvider != null) {
            // The constructor built the initial request before the provider was wired (httpTokenProvider was
            // still null, so it took the no-auth path with withContent). Rebuild it via the deferred path now
            // that the provider is set: this leaves the request at the header stage with the token pending,
            // matching the reset() path, so the first row's stampTokenIfPending() finishes it (appends the auth
            // header + withContent()) without a second client.newRequest(). Deferring the first getToken() off
            // the build path also lets a lazily-signing-in provider (e.g. OidcDeviceAuth::getToken) be wired
            // before sign-in completes, keeping the token pull on the use/flush path the provider documents.
            sender.httpTokenProvider = httpTokenProvider;
            sender.request = sender.newRequest();
        }
        return sender;
    }

    public static boolean isNotFound(DirectUtf8Sequence statusCode) {
        if (statusCode == null || statusCode.size() != 3) {
            return false;
        }
        return statusCode.byteAt(0) == '4' && statusCode.byteAt(1) == '0' && statusCode.byteAt(2) == '4';
    }

    @Override
    public void atNow() {
        // validateRowStarted() rejects EMPTY and TABLE_NAME_SET, so only ADDING_SYMBOLS and ADDING_COLUMNS
        // reach the terminator write
        validateRowStarted();
        terminateRow();
    }

    @Override
    public Sender boolColumn(CharSequence name, boolean value) {
        writeFieldName(name);
        request.put(value ? 't' : 'f');
        return this;
    }

    public DirectByteSlice bufferView() {
        return bufferView.of(request.getContentStart(), request.getContentLength());
    }

    @Override
    public void cancelRow() {
        validateNotClosed();
        if (isTokenPending) {
            // newRequest() left the request at the header stage with the provider token deferred, so
            // withContent() has not run and contentStart is still -1 (getContentLength() reads 0): no row
            // bytes were written, so there is nothing to trim. trimContentToLen(0) would set the write
            // pointer to contentStart + 0 == -1 and the next buffer write would segfault. Just reset the
            // row state and leave the token pending for the next row.
            state = RequestState.EMPTY;
            return;
        }
        request.trimContentToLen(rowBookmark);
        state = RequestState.EMPTY;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            if (autoFlushRows != 0 || flushIntervalNanos != Long.MAX_VALUE) {
                // either row-based or time-based auto flushing is enabled
                // => let's auto-flush on close
                flush0(true);
            }
        } finally {
            jsonErrorParser = Misc.free(jsonErrorParser);
            closed = true;
            client = Misc.free(client);
        }
    }

    @Override
    public void flush() {
        flush0(false);
    }

    public boolean isMisdirectedRequest(DirectUtf8Sequence statusCode) {
        if (statusCode == null || statusCode.size() != 3) {
            return false;
        }
        return statusCode.byteAt(0) == '4' && statusCode.byteAt(1) == '2' && statusCode.byteAt(2) == '1';
    }

    @Override
    public Sender longColumn(CharSequence name, long value) {
        writeFieldName(name);
        request.put(value);
        request.put('i');
        return this;
    }

    @TestOnly
    public void putRawMessage(Utf8Sequence msg) {
        // stamp the deferred provider token (like table() does) so a raw message sent as the first row
        // carries it; a no-op when no provider is configured
        stampTokenIfPending();
        request.put(msg); // message must include trailing \n
        state = RequestState.EMPTY;
        if (rowAdded()) {
            flush();
        }
    }

    @Override
    public void reset() {
        reset(Long.MAX_VALUE);
    }

    @Override
    public Sender stringColumn(CharSequence name, CharSequence value) {
        writeFieldName(name);
        request.put('"');
        escapeString(value);
        request.put('"');
        return this;
    }

    @Override
    public Sender symbol(CharSequence name, CharSequence value) {
        switch (state) {
            case EMPTY:
                throw new LineSenderException("table name must be set first");
            case ADDING_COLUMNS:
                throw new LineSenderException("symbols must be written before any other column types");
            case TABLE_NAME_SET:
                state = RequestState.ADDING_SYMBOLS;
                // fall through
            case ADDING_SYMBOLS:
                validateColumnName(name);
                request.putAscii(',');
                escapeQuotedString(name);
                request.putAscii('=');
                escapeQuotedString(value);
                state = RequestState.ADDING_SYMBOLS;
                break;
            default:
                throw new LineSenderException("unexpected state: ").put(state.name());
        }
        return this;
    }

    @Override
    public Sender table(CharSequence table) {
        assert request != null;
        validateNotClosed();
        validateTableName(table);
        if (state != RequestState.EMPTY) {
            throw new LineSenderException("duplicated table. call sender.at() or sender.atNow() to finish the current row first");
        }
        if (table.length() == 0) {
            throw new LineSenderException("table name cannot be empty");
        }
        // stamp the deferred provider token before the first row of this request, so the send carries it;
        // a no-op once the token has been stamped or when no provider is configured
        stampTokenIfPending();
        // set bookmark at start of the line.
        rowBookmark = request.getContentLength();
        state = RequestState.TABLE_NAME_SET;
        escapeQuotedString(table);
        return this;
    }

    private static int backoff(Rnd rnd, int retryBackoff, int retryMaxBackoffMs) {
        int jitter = rnd.nextInt(RETRY_MAX_JITTER_MS);
        int backoff = retryBackoff + jitter;
        Os.sleep(backoff);
        return Math.min(retryMaxBackoffMs, backoff * RETRY_BACKOFF_MULTIPLIER);
    }

    private static void chunkedResponseToSink(HttpClient.ResponseHeaders response, StringSink sink, int timeoutMillis) {
        if (!response.isChunked()) {
            return;
        }
        Response chunkedRsp = response.getResponse();
        Fragment fragment;
        while ((fragment = chunkedRsp.recv(timeoutMillis)) != null) {
            sink.putNonAscii(fragment.lo(), fragment.hi());
        }
    }

    private static boolean isRetryableHttpStatus(DirectUtf8Sequence statusCode) {
        if (statusCode == null || statusCode.size() != 3 || statusCode.byteAt(0) != '5') {
            return false;
        }

        /*
        We are retrying on the following response codes (copied from the Rust client):
        500:  Internal Server Error
        503:  Service Unavailable
        504:  Gateway Timeout

        // Unofficial extensions
        507:  Insufficient Storage
        509:  Bandwidth Limit Exceeded
        523:  Origin is Unreachable
        524:  A Timeout Occurred
        529:  Site is overloaded
        599:  Network Connect Timeout Error
        */

        byte middle = statusCode.byteAt(1);
        byte last = statusCode.byteAt(2);
        return (middle == '0' && (last == '0' || last == '3' || last == '4' || last == '7' || last == '9'))
                || (middle == '2' && (last == '3' || last == '4' || last == '9'))
                || (middle == '9' && last == '9');
    }

    private static boolean isSuccessResponse(DirectUtf8Sequence statusCode) {
        return statusCode != null && statusCode.size() == 3 && statusCode.byteAt(0) == '2';
    }

    private static boolean keepAliveDisabled(HttpClient.ResponseHeaders response) {
        DirectUtf8Sequence connectionHeader = response.getHeader(HttpConstants.HEADER_CONNECTION);
        return HttpKeywords.isClose(connectionHeader);
    }

    private void consumeChunkedResponse(HttpClient.ResponseHeaders response, int timeoutMillis) {
        if (!response.isChunked()) {
            return;
        }
        Response chunkedRsp = response.getResponse();
        //noinspection StatementWithEmptyBody
        while ((chunkedRsp.recv(timeoutMillis)) != null) {
            // we don't care about the response, just consume it, so it won't stay in the socket receive buffer
        }
    }

    private CharSequence currentHost() {
        return hosts.get(currentAddressIndex);
    }

    private int currentPort() {
        return ports.get(currentAddressIndex);
    }

    private void escapeString(CharSequence value) {
        for (int i = 0, n = value.length(); i < n; i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\n':
                case '\r':
                case '"':
                case '\\':
                    request.put((byte) '\\').put((byte) c);
                    break;
                default:
                    request.put(c);
                    break;
            }
        }
    }

    private void flush0(boolean closing) {
        if (state != RequestState.EMPTY && !closing) {
            throw new LineSenderException(
                    "Cannot flush buffer while row is in progress. " +
                            "Use sender.at() or sender.atNow() to finish the current row first.");
        }
        if (pendingRows == 0 || (closing && lastFlushFailed)) {
            return;
        }

        long retryingDeadlineNanos = Long.MIN_VALUE;
        int retryBackoff = RETRY_INITIAL_BACKOFF_MS;
        int contentLen = request.getContentLength();
        int actualTimeoutMillis = baseTimeoutMillis;
        if (minRequestThroughput > 0) {
            long throughputTimeoutBonusMillis = (contentLen * 1_000L / minRequestThroughput);
            if (throughputTimeoutBonusMillis + actualTimeoutMillis > Integer.MAX_VALUE) {
                actualTimeoutMillis = Integer.MAX_VALUE;
            } else {
                actualTimeoutMillis += (int) throughputTimeoutBonusMillis;
            }
        }
        for (; ; ) {
            try {
                long beforeRequest = System.nanoTime();
                HttpClient.ResponseHeaders response = request.send(currentHost(), currentPort(), actualTimeoutMillis);
                long elapsedNanos = System.nanoTime() - beforeRequest;
                int remainingMillis = actualTimeoutMillis - (int) (elapsedNanos / 1_000_000L);
                if (remainingMillis <= 0) {
                    throw new HttpClientException("Request timed out");
                }

                response.await(remainingMillis);
                DirectUtf8Sequence statusCode = response.getStatusCode();
                if (isSuccessResponse(statusCode)) {
                    // pass the whole per-flush budget (base + throughput extension) as EACH recv() read's
                    // timeout, NOT the raw request_timeout: recv() otherwise inherits defaultTimeout, so a
                    // tuned-low request_timeout paired with request_min_throughput would abort a large,
                    // still-progressing chunked body. This bounds each read, not the whole body cumulatively -
                    // fine here because the ILP server is trusted (unlike OidcDeviceAuth.parseBody, which also
                    // caps total bytes and elapsed time against an untrusted identity provider).
                    // A 2xx IS the commit: the server already has these rows. Draining its response body
                    // afterwards is only bookkeeping to keep the connection reusable, so a failure there must
                    // not escape into the catch below, which treats HttpClientException as a transport error
                    // and re-sends the whole batch -- duplicate rows on data the server accepted. Base could
                    // not reach this, because recv() re-armed its timeout on every socket read and a
                    // dribbling-but-progressing body never aborted; bounding the whole call means it now can.
                    // On abort the body is left unconsumed, which would mis-frame the next response on this
                    // connection, so drop the connection and report the flush as what it was: a success.
                    boolean drained = true;
                    try {
                        consumeChunkedResponse(response, actualTimeoutMillis); // if any
                    } catch (HttpClientException e) {
                        drained = false;
                    }
                    // Server has HTTP keep-alive disabled, and it's closing this TCP connection.
                    if (!drained || keepAliveDisabled(response)) {
                        client.disconnect();
                    }
                    lastFlushFailed = false;
                    break;
                }
                assert response.isChunked();
                lastFlushFailed = true;
                if (isRetryableHttpStatus(statusCode) || isMisdirectedRequest(statusCode) || isNotFound(statusCode)) {
                    if (isMisdirectedRequest(statusCode) || isNotFound(statusCode)) {
                        rotateAddress();
                    }

                    long nowNanos = System.nanoTime();
                    retryingDeadlineNanos = (retryingDeadlineNanos == Long.MIN_VALUE && !closing)
                            ? nowNanos + maxRetriesNanos
                            : retryingDeadlineNanos;
                    if (nowNanos >= retryingDeadlineNanos) {
                        // throw, but do not reset - a caller can try to flush later
                        throwOnHttpErrorResponse(statusCode, response, true, actualTimeoutMillis);
                    }
                    client.disconnect(); // forces reconnect, just in case
                    retryBackoff = backoff(rnd, retryBackoff, maxBackoffMillis);
                    continue;
                }
                throwOnHttpErrorResponse(statusCode, response, false, actualTimeoutMillis);
            } catch (HttpClientException | HttpException e) {
                // this is a network error, we can retry.
                //
                // HttpException too: response.await() above hands the response head to HttpHeaderParser,
                // which rejects one it cannot parse - a header block past its fixed 4096-byte buffer (an
                // intermediary stacking Set-Cookie/CSP), a malformed Content-Length, a status line that is
                // not HTTP/1.x - by throwing HttpException. That is a SIBLING of HttpClientException, not a
                // subclass, so it escaped this catch and with it the retry, the address rotation and the
                // client.disconnect() that keeps the next flush off a connection holding a half-read
                // response. It also left flush() throwing a raw HttpException rather than the
                // LineSenderException its contract promises, past every caller's catch. An unparseable head
                // is the response being unusable, which is exactly what this arm already handles.
                lastFlushFailed = true;
                client.disconnect(); // forces reconnect
                long nowNanos = System.nanoTime();
                retryingDeadlineNanos = (retryingDeadlineNanos == Long.MIN_VALUE && !closing)
                        ? nowNanos + maxRetriesNanos
                        : retryingDeadlineNanos;
                if (nowNanos >= retryingDeadlineNanos) {
                    // we did our best, give up, but do not reset the sender
                    // a caller can try to flush later
                    LineSenderException ex = new LineSenderException("Could not flush buffer: http", true);
                    if (isTls) {
                        ex.put('s');
                    }
                    ex.put("://");
                    ex.put(currentHost()).put(':').put(currentPort()).put(this.path);
                    ex.put(" Connection Failed").put(": ").put(e.getMessage());
                    throw ex;
                }
                rotateAddress();
                retryBackoff = backoff(rnd, retryBackoff, maxBackoffMillis);
            }
        }
        reset(System.nanoTime() + flushIntervalNanos);
    }

    private HttpClient.Request newRequest() {
        HttpClient.Request r = client.newRequest(currentHost(), currentPort())
                .POST()
                .url(path)
                .header("User-Agent", userAgent);
        if (username != null) {
            r.authBasic(username, password);
        } else if (httpTokenProvider != null) {
            // Do NOT pull the token here (the construct / flush-completion path): getToken() can throw (not
            // signed in yet, or a failed silent refresh), and a throw after client.newRequest() reset the
            // shared request would corrupt the sender, turning an already-successful flush into an exception.
            // Leave the request at the header stage (no withContent() yet) with the token pending, so the first
            // row's stampTokenIfPending() appends the Authorization header + withContent() on THIS request
            // WITHOUT a second client.newRequest() - the request line and headers are written once per flush,
            // not twice. bufferView() reads empty meanwhile (contentStart is -1, so getContentLength() is 0).
            isTokenPending = true;
            rowBookmark = r.getContentLength();
            state = RequestState.EMPTY;
            return r;
        } else if (authToken != null) {
            r.authToken(authToken);
        }
        r.withContent();
        rowBookmark = r.getContentLength();
        state = RequestState.EMPTY;
        return r;
    }

    private void reset(long newFlushAfterNanos) {
        pendingRows = 0;
        flushAfterNanos = newFlushAfterNanos;
        request = newRequest();
    }

    private void rotateAddress() {
        currentAddressIndex = (currentAddressIndex + 1) % hosts.size();
    }

    /**
     * @return true if flush is required
     */
    private boolean rowAdded() {
        pendingRows++;
        long nowNanos = System.nanoTime();
        if (flushAfterNanos == Long.MAX_VALUE) {
            flushAfterNanos = nowNanos + flushIntervalNanos;
        } else if (flushAfterNanos - nowNanos < 0) {
            return true;
        }
        return pendingRows == autoFlushRows;
    }

    private void stampTokenIfPending() {
        if (isTokenPending) {
            // The construct/flush path deferred the token so a lazily-signing-in provider (e.g.
            // OidcDeviceAuth::getToken) could be wired before sign-in completed, and so a provider failure
            // never strikes after a successful send. The caller is now starting the first row, so finish the
            // request newRequest() left at the header stage: pull a fresh token (so a long-lived sender
            // follows token rotation), then append the Authorization header + withContent() on THIS request -
            // no second client.newRequest(), so the request line and headers are written once, not twice.
            //
            // The throwing operations run BEFORE the request is mutated: a getToken()/validateToken() throw
            // (not signed in yet, a failed refresh, or a rejected token) leaves isTokenPending set and the
            // request untouched at the header stage, so the next row retries cleanly - the sender is never left
            // corrupted. Validate EVERY pulled token, not just a changed instance: HttpTokenProvider.getToken()
            // makes no immutability promise, so a provider that reuses one CharSequence buffer (the idiomatic
            // zero-alloc style) and mutates its content between flushes must be re-checked, or a mutated token
            // could splice a CR/LF into the "Authorization: Bearer" header (request.authToken writes it verbatim,
            // with no CR/LF filtering). The scan is O(token length) and is dwarfed by the flush's network
            // round-trip; the WebSocket auth path validates on every pull for the same reason.
            CharSequence pulled;
            try {
                pulled = httpTokenProvider.getToken();
            } catch (LineSenderException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new LineSenderException(
                        e.getMessage() == null
                                ? "token provider failed to supply a credential"
                                : e.getMessage(),
                        e);
            }
            // Snapshot BEFORE validating, so the bytes that are checked are the bytes that are sent. Without
            // it validateToken scans the provider's sequence and authToken then re-reads it - two reads of a
            // buffer the provider owns and, per the paragraph above, is invited to reuse. A mutation landing
            // between them passes the check and splices the mutated content, CR/LF included, into the
            // Authorization header. One String per FLUSH (not per row), dwarfed by the round-trip that
            // follows. Null-safe: a null pull must still reach validateToken's "null or empty" message
            // rather than NPE here.
            CharSequence token = pulled == null ? null : pulled.toString();
            HttpTokenProvider.validateToken(token);
            request.authToken(token);
            request.withContent();
            rowBookmark = request.getContentLength();
            state = RequestState.EMPTY;
            isTokenPending = false;
        }
    }

    private void throwOnHttpErrorResponse(DirectUtf8Sequence statusCode, HttpClient.ResponseHeaders response, boolean retryable, int timeoutMillis) {
        // The STATUS is the verdict; the body is detail for the message. A body read that aborts must not
        // escape into flush0's catch, which treats HttpClientException as a transport failure: a definitive
        // 401/403/405 would be reclassified as a network error, retried for the whole retry budget, and
        // finally surfaced as "Connection Failed: timed out reading the chunked response body" with the real
        // status nowhere in it. Report the status we already have instead, and say the body was unreadable
        // rather than inventing detail. LineSenderException is a sibling of HttpClientException, not a
        // subclass, so the intended throw passes through this catch untouched.
        try {
            throwOnHttpErrorResponse0(statusCode, response, retryable, timeoutMillis);
        } catch (HttpClientException e) {
            client.disconnect();
            throw new LineSenderException("Could not flush buffer: could not read the error response body", retryable)
                    .put(" [http-status=").putAsPrintable(statusCode.asAsciiCharSequence()).put(']');
        }
    }

    private void throwOnHttpErrorResponse0(DirectUtf8Sequence statusCode, HttpClient.ResponseHeaders response, boolean retryable, int timeoutMillis) {
        CharSequence statusAscii = statusCode.asAsciiCharSequence();
        if (Chars.equals("405", statusAscii)) {
            consumeChunkedResponse(response, timeoutMillis);
            client.disconnect();
            throw new LineSenderException("Could not flush buffer: HTTP endpoint does not support ILP. [http-status=405]", retryable);
        }
        if (Chars.equals("401", statusAscii) || Chars.equals("403", statusAscii)) {
            sink.clear();
            chunkedResponseToSink(response, sink, timeoutMillis);
            LineSenderException ex = new LineSenderException("Could not flush buffer: HTTP endpoint authentication error", retryable);
            if (sink.length() > 0) {
                // sanitize the raw server body before it reaches the exception message (and any log/terminal):
                // an untrusted or proxied endpoint must not splice control, ANSI or bidi chars into the render
                ex = ex.put(": ").putAsPrintable(sink);
            }
            ex.put(" [http-status=").putAsPrintable(statusAscii).put(']');
            client.disconnect();
            throw ex;
        }
        DirectUtf8Sequence contentType = response.getContentType();
        if (contentType != null && Utf8s.equalsAscii("application/json", contentType)) {
            if (jsonErrorParser == null) {
                jsonErrorParser = new JsonErrorParser();
            }
            jsonErrorParser.reset();
            LineSenderException ex = jsonErrorParser.toException(response.getResponse(), statusCode, retryable, timeoutMillis);
            client.disconnect();
            throw ex;
        }
        // ok, no JSON, let's do something more generic
        sink.clear();
        chunkedResponseToSink(response, sink, timeoutMillis);
        // sanitize the raw server body before it reaches the exception message (and any log/terminal):
        // an untrusted or proxied endpoint must not splice control, ANSI or bidi chars into the render
        LineSenderException ex = new LineSenderException("Could not flush buffer: ", retryable)
                .putAsPrintable(sink)
                .put(" [http-status=").putAsPrintable(statusCode.asAsciiCharSequence()).put(']');
        client.disconnect();
        throw ex;
    }

    private void validateNotClosed() {
        if (closed) {
            throw new LineSenderException("sender already closed");
        }
    }

    private void validateTableName(CharSequence name) {
        if (!TableUtils.isValidTableName(name, maxNameLength)) {
            if (name.length() > maxNameLength) {
                throw new LineSenderException("table name is too long: [name = ")
                        .putAsPrintable(name)
                        .put(", maxNameLength=")
                        .put(maxNameLength)
                        .put(']');
            }
            throw new LineSenderException("table name contains an illegal char: '\\n', '\\r', '?', ',', ''', " +
                    "'\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: ")
                    .putAsPrintable(name);
        }
    }

    protected void escapeQuotedString(CharSequence name) {
        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
            switch (c) {
                case ' ':
                case ',':
                case '=':
                case '\n':
                case '\r':
                case '\\':
                    request.put((byte) '\\').put((byte) c);
                    break;
                default:
                    request.put(c);
                    break;
            }
        }
    }

    protected void validateColumnName(CharSequence name) {
        if (!TableUtils.isValidColumnName(name, maxNameLength)) {
            if (name.length() > maxNameLength) {
                throw new LineSenderException("column name is too long: [name = ")
                        .putAsPrintable(name)
                        .put(", maxNameLength=")
                        .put(maxNameLength)
                        .put(']');
            }
            throw new LineSenderException("column name contains an illegal char: '\\n', '\\r', '?', '.', ','" +
                    ", ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: ")
                    .putAsPrintable(name);
        }
    }

    /**
     * Rejects a row terminator that no row precedes. Subclasses MUST call this before writing the first byte
     * of a terminator, not after: with an httpTokenProvider configured, newRequest() leaves the request at the
     * header stage (withContent() deferred until the first row stamps the Authorization header), so a write
     * that lands here while the state is EMPTY goes into the HTTP HEADER block, not the request body. Those
     * bytes then start a line that folds the following "Authorization: Bearer ..." into the previous header
     * (RFC 7230 obs-fold), and the request ships with no credential at all. cancelRow() cannot undo it either:
     * trimContentToLen only rewinds within the content section.
     */
    /**
     * Writes the row terminator and closes the row, WITHOUT re-checking that a row was started - the caller
     * has already done it.
     * <p>
     * {@link #at(long, java.time.temporal.ChronoUnit)} and {@link #at(java.time.Instant)} must validate
     * before they write the timestamp, not after: a rejected row would otherwise leave a stray timestamp in
     * the request buffer for the next row to inherit. They used to follow that write with {@code atNow()},
     * which validated the very same state a second time - nothing between the two calls can change it, since
     * only {@code table()}, a column write and this method touch {@code state} - so every explicit-timestamp
     * row paid for a second switch on the hot ingestion path. They call this instead.
     */
    protected void terminateRow() {
        request.put('\n');
        state = RequestState.EMPTY;
        if (rowAdded()) {
            flush();
        }
    }

    protected void validateRowStarted() {
        switch (state) {
            case EMPTY:
                throw new LineSenderException("no table name was provided");
            case TABLE_NAME_SET:
                throw new LineSenderException("no symbols or columns were provided");
            default:
                break;
        }
    }

    protected HttpClient.Request writeFieldName(CharSequence name) {
        validateColumnName(name);
        switch (state) {
            case EMPTY:
                throw new LineSenderException("table name must be set first");
            case ADDING_SYMBOLS:
                // fall through
            case TABLE_NAME_SET:
                request.putAscii(' ');
                state = RequestState.ADDING_COLUMNS;
                break;
            case ADDING_COLUMNS:
                request.putAscii(',');
                break;
        }
        escapeQuotedString(name);
        request.put('=');
        return request;
    }

    enum RequestState {
        EMPTY,
        TABLE_NAME_SET,
        ADDING_SYMBOLS,
        ADDING_COLUMNS,
    }

    private static class JsonErrorParser implements JsonParser, Closeable {
        private final StringSink codeSink = new StringSink();
        private final StringSink errorIdSink = new StringSink();
        private final StringSink jsonSink = new StringSink();
        private final JsonLexer lexer = new JsonLexer(1024, 1024);
        private final StringSink lineSink = new StringSink();
        private final StringSink messageSink = new StringSink();
        private State state = State.INIT;

        @Override
        public void close() {
            Misc.free(lexer);
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) throws JsonException {
            switch (state) {
                case INIT:
                    if (code == JsonLexer.EVT_OBJ_START) {
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected '{'");
                    }
                    break;
                case NEXT_KEY_NAME:
                    if (code == JsonLexer.EVT_OBJ_END) {
                        state = State.INIT;
                    } else if (code == JsonLexer.EVT_NAME) {
                        if (Chars.equals("code", tag)) {
                            state = State.NEXT_CODE_VALUE;
                        } else if (Chars.equals("message", tag)) {
                            state = State.NEXT_MESSAGE_VALUE;
                        } else if (Chars.equals("line", tag)) {
                            state = State.NEXT_LINE_NUMBER_VALUE;
                        } else if (Chars.equals("errorId", tag)) {
                            state = State.NEXT_ERROR_ID_VALUE;
                        } else {
                            throw JsonException.$(position, "expected 'code', 'message', 'line' or 'error'");
                        }
                    } else {
                        throw JsonException.$(position, "expected 'error' or 'message'");
                    }
                    break;
                case NEXT_CODE_VALUE:
                    if (code == JsonLexer.EVT_VALUE) {
                        codeSink.put(tag);
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected number");
                    }
                    break;
                case NEXT_MESSAGE_VALUE:
                    if (code == JsonLexer.EVT_VALUE) {
                        messageSink.put(tag);
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected string");
                    }
                    break;
                case NEXT_LINE_NUMBER_VALUE:
                    if (code == JsonLexer.EVT_VALUE) {
                        lineSink.put(tag);
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected number");
                    }
                    break;
                case NEXT_ERROR_ID_VALUE:
                    if (code == JsonLexer.EVT_VALUE) {
                        errorIdSink.put(tag);
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected string");
                    }
                    break;
                case DONE:
                    break;
            }
        }

        private void drainAndReset(LineSenderException sink, DirectUtf8Sequence httpStatus) {
            assert state == State.INIT;

            sink.putAsPrintable(messageSink).put(" [http-status=").putAsPrintable(httpStatus.asAsciiCharSequence());
            if (codeSink.length() != 0 || errorIdSink.length() != 0 || lineSink.length() != 0) {
                if (errorIdSink.length() != 0) {
                    sink.put(", id: ").putAsPrintable(errorIdSink);
                }
                if (codeSink.length() != 0) {
                    sink.put(", code: ").putAsPrintable(codeSink);
                }
                if (lineSink.length() != 0) {
                    sink.put(", line: ").putAsPrintable(lineSink);
                }
            }
            sink.put(']');
            reset();
        }

        private void reset() {
            state = State.INIT;
            codeSink.clear();
            errorIdSink.clear();
            lineSink.clear();
            messageSink.clear();
            lexer.clear();
            jsonSink.clear();
        }

        LineSenderException toException(Response chunkedRsp, DirectUtf8Sequence httpStatus, boolean retryable, int timeoutMillis) {
            Fragment fragment;
            LineSenderException exception = new LineSenderException("Could not flush buffer: ", retryable);
            while ((fragment = chunkedRsp.recv(timeoutMillis)) != null) {
                try {
                    jsonSink.putNonAscii(fragment.lo(), fragment.hi());
                    lexer.parse(fragment.lo(), fragment.hi(), this);
                } catch (JsonException e) {
                    // we failed to parse JSON, but we still want to show the error message.
                    // if we cannot parse it then we show the whole response as is.
                    // let's make sure we have the whole message - there might be more chunks
                    while ((fragment = chunkedRsp.recv(timeoutMillis)) != null) {
                        jsonSink.putNonAscii(fragment.lo(), fragment.hi());
                    }
                    // sanitize the raw server body before it reaches the exception message (and any log/terminal):
                    // an untrusted or proxied endpoint must not splice control, ANSI or bidi chars into the render
                    exception.putAsPrintable(jsonSink).put(" [http-status=").putAsPrintable(httpStatus.asAsciiCharSequence()).put(']');
                    reset();
                    return exception;
                }
            }
            drainAndReset(exception, httpStatus);
            return exception;
        }

        enum State {
            INIT,
            NEXT_KEY_NAME,
            NEXT_CODE_VALUE,
            NEXT_MESSAGE_VALUE,
            NEXT_LINE_NUMBER_VALUE,
            NEXT_ERROR_ID_VALUE,
            DONE
        }
    }

    public static class JsonSettingsParser implements JsonParser, Closeable, Mutable {
        private final static byte ACCEPTING_WRITES = 3;
        private final static byte LINE_PROTO_SUPPORT_VERSIONS = 1;
        private final static byte MAX_NAME_LEN = 2;
        private final JsonLexer lexer = new JsonLexer(1024, 1024);
        private final IntList supportVersions = new IntList(8);
        private boolean acceptingWrites = true;
        private int maxNameLen = 0;
        private byte nextJsonValueFlag = 0;

        @Override
        public void clear() {
            supportVersions.clear();
            acceptingWrites = true;
            maxNameLen = 0;
            nextJsonValueFlag = 0;
            lexer.clear();
        }

        @Override
        public void close() {
            Misc.free(lexer);
        }

        public int getDefaultProtocolVersion() {
            if (supportVersions.size() == 0) {
                return PROTOCOL_VERSION_V1;
            }
            if (supportVersions.contains(PROTOCOL_VERSION_V3)) {
                return PROTOCOL_VERSION_V3;
            } else if (supportVersions.contains(PROTOCOL_VERSION_V2)) {
                return PROTOCOL_VERSION_V2;
            } else if (supportVersions.contains(PROTOCOL_VERSION_V1)) {
                return PROTOCOL_VERSION_V1;
            } else {
                throw new LineSenderException("Server does not support current client");
            }
        }

        public int getMaxNameLen() {
            return maxNameLen;
        }

        public boolean isAcceptingWrites() {
            return acceptingWrites;
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_NAME:
                    if (tag.equals("line.proto.support.versions")) {
                        nextJsonValueFlag = LINE_PROTO_SUPPORT_VERSIONS;
                    } else if (tag.equals("cairo.max.file.name.length")) {
                        nextJsonValueFlag = MAX_NAME_LEN;
                    } else if (tag.equals("accepting.writes")) {
                        nextJsonValueFlag = ACCEPTING_WRITES;
                        // server supports sending accepting.writes arrays,
                        // thus it has to explicitly allow HTTP otherwise
                        // the server is considered read-only
                        acceptingWrites = false;
                    } else {
                        nextJsonValueFlag = 0;
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    if (nextJsonValueFlag == MAX_NAME_LEN) {
                        try {
                            maxNameLen = Numbers.parseInt(tag);
                        } catch (NumericException ignored) {
                        }
                    }
                    break;
                case JsonLexer.EVT_ARRAY_VALUE:
                    if (nextJsonValueFlag == LINE_PROTO_SUPPORT_VERSIONS) {
                        try {
                            supportVersions.add(Numbers.parseInt(tag));
                        } catch (NumericException e) {
                            // ignore it
                        }
                    } else if (nextJsonValueFlag == ACCEPTING_WRITES) {
                        if (Chars.equals("http", tag)) {
                            acceptingWrites = true;
                        }
                    }
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    if (nextJsonValueFlag == LINE_PROTO_SUPPORT_VERSIONS) {
                        nextJsonValueFlag = 0;
                    }
            }
        }

        public void parse(Response chunkedRsp) throws JsonException {
            Fragment fragment;
            while ((fragment = chunkedRsp.recv()) != null) {
                lexer.parse(fragment.lo(), fragment.hi(), this);
            }
        }
    }
}
