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
import java.util.concurrent.locks.LockSupport;

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

    QwpSchemaResponse resolve(CharSequence tableName, long timeoutMillis, boolean refresh, Thread ioThread) {
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
        final long timeoutNanos = timeoutNanos(timeoutMillis);
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
            Request own = new Request(id, key, startNanos, timeoutNanos, message);
            request = own;
            hasPendingRequest = true;
            LockSupport.unpark(ioThread);
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
        switch (response.getResult()) {
            case QwpSchemaProtocol.RESULT_KNOWN:
            case QwpSchemaProtocol.RESULT_MISSING:
                put(current.key, response);
                complete(current, response, null);
                return;
            case QwpSchemaProtocol.RESULT_DENIED:
                remove(current.key);
                complete(current, null, failure(ACCESS_DENIED, current.key, "schema access denied"));
                return;
            case QwpSchemaProtocol.RESULT_UNAVAILABLE:
                remove(current.key);
                complete(current, null, failure(SCHEMA_UNAVAILABLE, current.key, "schema is unavailable"));
                return;
            case QwpSchemaProtocol.RESULT_TOO_LARGE:
            default:
                remove(current.key);
                complete(current, null, failure(UNSUPPORTED_FEATURE, current.key, "schema response is too large or unsupported"));
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
                if (schema.getResult() == QwpSchemaProtocol.RESULT_KNOWN
                        || schema.getResult() == QwpSchemaProtocol.RESULT_MISSING) {
                    put(key, schema);
                } else {
                    remove(key);
                }
            }
        }
    }

    synchronized void connectionLost(WebSocketClient client) {
        if (request != null && (request.sentClient == null || request.sentClient == client)) {
            complete(request, null, failure(SCHEMA_UNAVAILABLE, request.key, "connection lost during schema lookup"));
        }
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

    private static long timeoutNanos(long timeoutMillis) {
        return timeoutMillis > Long.MAX_VALUE / 1_000_000L ? Long.MAX_VALUE : timeoutMillis * 1_000_000L;
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
        final long id;
        final String key;
        final byte[] message;
        boolean done;
        LineSenderSchemaException error;
        QwpSchemaResponse response;
        WebSocketClient sentClient;

        Request(long id, String key, long startNanos, long timeoutNanos, byte[] message) {
            this.id = id;
            this.key = key;
            this.startNanos = startNanos;
            this.timeoutNanos = timeoutNanos;
            this.message = message;
        }

        long remainingNanos() {
            return timeoutNanos - (System.nanoTime() - startNanos);
        }
    }
}
