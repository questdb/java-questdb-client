/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.std.Chars;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.LineSenderSchemaException.Reason.ACCESS_DENIED;
import static io.questdb.client.LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE;
import static io.questdb.client.LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE;

final class QwpSchemaCoordinator {
    static final int MAX_CACHE_ENTRIES = 1_000_000;
    private LinkedHashMap<String, QwpSchemaResponse> cache;
    private volatile boolean closed;
    private volatile boolean hasPendingRequest;
    private long nextRequestId = 1;
    private Request request;

    QwpSchemaResponse resolve(CharSequence tableName, long timeoutMillis, boolean refresh) {
        final long startNanos = System.nanoTime();
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis must be non-negative");
        }
        if (tableName == null || tableName.length() == 0
                || tableName.length() > QwpSchemaProtocol.MAX_NAME_UTF16_LENGTH) {
            throw new IllegalArgumentException("table name must contain 1 to "
                    + QwpSchemaProtocol.MAX_NAME_UTF16_LENGTH + " UTF-16 units");
        }
        final String key = normalize(tableName);
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (this) {
            if (closed) {
                throw failure(SCHEMA_UNAVAILABLE, key, "schema coordinator is closed");
            }
            if (!refresh) {
                QwpSchemaResponse cached = cache == null ? null : cache.get(key);
                if (cached != null) {
                    return cached;
                }
            } else {
                remove(key);
            }
            if (timeoutNanos - (System.nanoTime() - startNanos) <= 0) {
                throw failure(SCHEMA_UNAVAILABLE, key, "schema lookup timed out");
            }
            if (request != null) {
                throw failure(SCHEMA_UNAVAILABLE, key, "another schema lookup is already in progress");
            }
            if (nextRequestId <= 0 || nextRequestId == Long.MAX_VALUE) {
                throw failure(UNSUPPORTED_FEATURE, key, "schema request id space is exhausted");
            }
            long id = nextRequestId;
            byte[] message = QwpSchemaProtocol.encodeDescribe(id, tableName);
            nextRequestId++;
            Request own = new Request(id, key, tableName.toString(), startNanos, timeoutNanos, message);
            request = own;
            // The I/O loop polls hasPendingRequest on every pass; waking it would
            // cut its reconnect backoff short while the wire is down.
            hasPendingRequest = true;
            while (!own.done) {
                try {
                    await(own, key);
                } catch (LineSenderSchemaException e) {
                    if (request == own) {
                        request = null;
                        hasPendingRequest = false;
                        notifyAll();
                    }
                    throw e;
                }
            }
            if (own.error != null) {
                throw own.error;
            }
            return own.response;
        }
    }

    synchronized Request requestToSend(WebSocketClient client) {
        if (request == null || request.done || request.sentClient != null) {
            return null;
        }
        if (request.remainingNanos() <= 0) {
            complete(request, null, failure(SCHEMA_UNAVAILABLE, request.key, "schema lookup timed out"));
            return null;
        }
        if (request.id == 0) {
            if (nextRequestId <= 0 || nextRequestId == Long.MAX_VALUE) {
                complete(request, null, failure(UNSUPPORTED_FEATURE, request.key, "schema request id space is exhausted"));
                return null;
            }
            request.id = nextRequestId++;
            request.message = QwpSchemaProtocol.encodeDescribe(request.id, request.tableName);
        }
        request.sentClient = client;
        return request;
    }

    boolean hasPendingRequest() {
        return hasPendingRequest;
    }

    synchronized boolean isLiveResponse(WebSocketClient client, long requestId) {
        Request current = request;
        return current != null && !current.done && current.sentClient == client
                && current.id == requestId && current.remainingNanos() > 0;
    }

    synchronized void completeResponse(WebSocketClient client, QwpSchemaResponse response) {
        Request current = request;
        if (current == null || current.done || current.sentClient != client
                || current.id != response.getRequestId()) {
            return;
        }
        if (current.remainingNanos() <= 0) {
            complete(current, null, failure(SCHEMA_UNAVAILABLE, current.key, "schema lookup timed out"));
            return;
        }
        if (isCacheable(response.getResult())) {
            put(current.key, response);
            complete(current, response, null);
            return;
        }
        switch (response.getResult()) {
            case QwpSchemaProtocol.RESULT_DENIED:
                remove(current.key);
                complete(current, null, failure(ACCESS_DENIED, current.key, "schema access denied"));
                return;
            case QwpSchemaProtocol.RESULT_UNAVAILABLE:
                remove(current.key);
                complete(current, null, failure(SCHEMA_UNAVAILABLE, current.key, "schema is unavailable"));
                return;
            default:
                remove(current.key);
                complete(current, null, failure(UNSUPPORTED_FEATURE, current.key, "unsupported schema result"));
        }
    }

    synchronized void fail(Request own, LineSenderSchemaException.Reason reason, String detail) {
        if (request == own && !own.done) {
            complete(own, null, failure(reason, own.key, detail));
        }
    }

    void applyFeedback(WebSocketResponse feedback) {
        if (feedback.isSchemaInvalidation()) {
            clearCache();
        }
        for (int i = 0, n = feedback.getSchemaUpdateCount(); i < n; i++) {
            QwpSchemaResponse schema = feedback.getSchemaUpdate(i);
            String key = normalize(feedback.getSchemaUpdateTableName(i));
            synchronized (this) {
                if (closed) {
                    return;
                }
                if (isCacheable(schema.getResult())) {
                    put(key, schema);
                } else {
                    remove(key);
                }
            }
        }
    }

    /**
     * Records that {@code client} died. With {@code abandon} false the producer's
     * request keeps its deadline and the replacement connection re-sends it; with
     * {@code abandon} true the request fails right away, so a producer that can
     * fall back to the legacy contract does not sit out the rest of its budget on
     * a wire that is known to be down.
     */
    synchronized void connectionLost(WebSocketClient client, boolean abandon) {
        if (request == null || request.done) {
            return;
        }
        if (abandon) {
            complete(request, null, failure(SCHEMA_UNAVAILABLE, request.key, "connection lost during schema lookup"));
            return;
        }
        if (request.sentClient == client) {
            // Keep the producer's request and deadline; only its wire attempt ended.
            // A fresh ID also rejects old replies delivered on the replacement connection.
            request.sentClient = null;
            request.id = 0;
        }
    }

    /**
     * Cache-only lookup: the schema this coordinator already holds for the table,
     * or {@code null}. Never sends a request and never waits.
     */
    synchronized QwpSchemaResponse peek(CharSequence tableName) {
        if (closed || cache == null) {
            return null;
        }
        return cache.get(normalize(tableName));
    }

    synchronized boolean invalidResponse(WebSocketClient client, long requestId) {
        // Zero means the malformed header could not identify a request. For an
        // identifiable reply, cancellation must share the liveness check's lock.
        if (requestId != 0 && !isLiveResponse(client, requestId)) {
            return false;
        }
        if (request != null && (request.sentClient == null || request.sentClient == client)) {
            complete(request, null, failure(SCHEMA_UNAVAILABLE, request.key, "invalid schema control response"));
        }
        return true;
    }

    synchronized void clearCache() {
        if (!closed) {
            cache = null;
        }
    }

    synchronized void close() {
        closed = true;
        cache = null;
        if (request != null) {
            complete(request, null, failure(SCHEMA_UNAVAILABLE, request.key, "schema coordinator is closed"));
        }
        notifyAll();
    }

    /**
     * Results that stay true until the table's schema version changes, which
     * the server then reports through ACK feedback. TOO_LARGE is a negative entry,
     * like MISSING: without it, AUTO would pay a DESCRIBE round trip every batch
     * for a table too wide to describe. DENIED and UNAVAILABLE may clear at any
     * time, so they are never cached.
     */
    private static boolean isCacheable(int result) {
        return result == QwpSchemaProtocol.RESULT_KNOWN
                || result == QwpSchemaProtocol.RESULT_MISSING
                || result == QwpSchemaProtocol.RESULT_TOO_LARGE;
    }

    private static LineSenderSchemaException failure(LineSenderSchemaException.Reason reason, String table, String detail) {
        return new LineSenderSchemaException(reason, "schema lookup failed [reason=" + reason
                + ", table=" + table + ", detail=" + detail + ']');
    }

    private static String normalize(CharSequence tableName) {
        return Chars.toLowerCase(tableName);
    }

    private void await(Request request, String key) {
        long remaining = request.remainingNanos();
        if (remaining <= 0) {
            throw failure(SCHEMA_UNAVAILABLE, key, "schema lookup timed out");
        }
        try {
            long millis = remaining / 1_000_000L;
            int nanos = (int) (remaining % 1_000_000L);
            wait(millis, nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failure(SCHEMA_UNAVAILABLE, key, "schema lookup was interrupted");
        }
    }

    private void complete(Request own, QwpSchemaResponse response, LineSenderSchemaException error) {
        own.response = response;
        own.error = error;
        own.done = true;
        if (request == own) {
            request = null;
            hasPendingRequest = false;
        }
        notifyAll();
    }

    private void put(String key, QwpSchemaResponse schema) {
        if (cache == null) {
            cache = new LinkedHashMap<>();
        }
        cache.put(key, schema);
        while (cache.size() > MAX_CACHE_ENTRIES) {
            Iterator<Map.Entry<String, QwpSchemaResponse>> iterator = cache.entrySet().iterator();
            iterator.next();
            iterator.remove();
        }
    }

    private void remove(String key) {
        if (cache != null) {
            cache.remove(key);
        }
    }

    static final class Request {
        final long startNanos;
        final long timeoutNanos;
        long id;
        final String key;
        final String tableName;
        byte[] message;
        boolean done;
        LineSenderSchemaException error;
        QwpSchemaResponse response;
        WebSocketClient sentClient;

        Request(long id, String key, String tableName, long startNanos, long timeoutNanos, byte[] message) {
            this.id = id;
            this.key = key;
            this.tableName = tableName;
            this.startNanos = startNanos;
            this.timeoutNanos = timeoutNanos;
            this.message = message;
        }

        long remainingNanos() {
            return timeoutNanos - (System.nanoTime() - startNanos);
        }
    }
}
