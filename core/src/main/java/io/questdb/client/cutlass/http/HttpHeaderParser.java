/*******************************************************************************
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

package io.questdb.client.cutlass.http;

import io.questdb.client.cairo.Reopenable;
import io.questdb.client.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Mutable;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.ObjectPool;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Utf8SequenceObjHashMap;
import io.questdb.client.std.Vect;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.client.std.str.DirectUtf8Sink;
import io.questdb.client.std.str.DirectUtf8String;
import io.questdb.client.std.str.Utf8Sequence;
import io.questdb.client.std.str.Utf8String;
import io.questdb.client.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.client.cutlass.http.HttpConstants.HEADER_CONTENT_LENGTH;

public class HttpHeaderParser implements Mutable, QuietCloseable, HttpRequestHeader {
    private final BoundaryAugmenter boundaryAugmenter = new BoundaryAugmenter();
    private final ObjList<HttpCookie> cookieList = new ObjList<>();
    private final ObjectPool<HttpCookie> cookiePool;
    private final Utf8SequenceObjHashMap<HttpCookie> cookies = new Utf8SequenceObjHashMap<>();
    private final ObjectPool<DirectUtf8String> csPool;
    private final LowerCaseUtf8SequenceObjHashMap<DirectUtf8String> headers = new LowerCaseUtf8SequenceObjHashMap<>();
    private final DirectUtf8Sink sink = new DirectUtf8Sink(0);
    private final DirectUtf8String temp = new DirectUtf8String();
    private final Utf8SequenceObjHashMap<DirectUtf8String> urlParams = new Utf8SequenceObjHashMap<>();
    protected boolean incomplete;
    protected DirectUtf8String url;
    private long _lo;
    private long _wptr;
    private long contentLength;
    private DirectUtf8String contentType;
    private DirectUtf8String headerName;
    private long headerPtr;
    private long hi;
    private boolean isMethod = true;
    private boolean isProtocol = true;
    private boolean isQueryParams = false;
    private boolean isStatusCode = true;
    private boolean isStatusText = true;
    private boolean isUrl = true;
    private DirectUtf8String method;
    private boolean needMethod;
    private boolean needProtocol = true;
    private DirectUtf8String protocol;

    private DirectUtf8String query;
    private DirectUtf8String statusCode;

    public HttpHeaderParser(int bufferSize, ObjectPool<DirectUtf8String> csPool) {
        this.headerPtr = this._wptr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_HTTP_CONN);
        this.hi = headerPtr + bufferSize;
        this.csPool = csPool;
        this.cookiePool = new ObjectPool<>(HttpCookie::new, 16);
        clear();
    }

    @Override
    public void clear() {
        this.needMethod = true;
        this._wptr = this._lo = this.headerPtr;
        this.incomplete = true;
        this.headers.clear();
        this.method = null;
        this.url = null;
        this.query = null;
        this.headerName = null;
        this.contentType = null;
        this.urlParams.clear();
        this.isMethod = true;
        this.isUrl = true;
        this.isQueryParams = false;
        this.protocol = null;
        this.statusCode = null;
        this.isProtocol = true;
        this.isStatusCode = true;
        this.isStatusText = true;
        this.needProtocol = true;
        this.contentLength = -1;
        this.cookieList.clear();
        this.cookiePool.clear();
        // do not clear the pool
        // this.pool.clear();
    }

    @Override
    public void close() {
        clear();
        if (headerPtr != 0) {
            headerPtr = _wptr = hi = Unsafe.free(headerPtr, hi - headerPtr, MemoryTag.NATIVE_HTTP_CONN);
            boundaryAugmenter.close();
        }
        sink.close();
        csPool.clear();
    }

    @Override
    public long getContentLength() {
        return contentLength;
    }

    @Override
    public DirectUtf8Sequence getContentType() {
        return contentType;
    }

    public HttpCookie getCookie(Utf8Sequence cookieName) {
        return cookies.get(cookieName);
    }

    public @NotNull ObjList<HttpCookie> getCookieList() {
        return cookieList;
    }

    @Override
    public DirectUtf8Sequence getHeader(Utf8Sequence name) {
        return headers.get(name);
    }

    @Override
    public DirectUtf8Sequence getMethod() {
        return method;
    }

    public @Nullable DirectUtf8String getQuery() {
        return query;
    }

    public DirectUtf8Sequence getStatusCode() {
        return statusCode;
    }

    public DirectUtf8String getUrl() {
        return url;
    }

    public DirectUtf8Sequence getUrlParam(Utf8Sequence name) {
        return urlParams.get(name);
    }

    public boolean isIncomplete() {
        return incomplete;
    }

    public long parse(long ptr, long hi, boolean _method, boolean _protocol) {
        long p;
        if (_method && needMethod) {
            int l = parseMethod(ptr, hi);
            p = ptr + l;
        } else if (_protocol && needProtocol) {
            int l = parseProtocol(ptr, hi);
            p = ptr + l;
        } else {
            p = ptr;
        }

        while (p < hi) {
            if (_wptr == this.hi) {
                throw HttpException.instance("header is too large");
            }

            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == '\r') {
                continue;
            }

            Unsafe.getUnsafe().putByte(_wptr++, (byte) b);

            switch (b) {
                case ':':
                    if (headerName == null) {
                        headerName = csPool.next().of(_lo, _wptr - 1);
                        _lo = _wptr + 1;
                    }
                    break;
                case '\n':
                    if (headerName == null) {
                        incomplete = false;
                        parseKnownHeaders();
                        return p;
                    }
                    headers.putImmutable(headerName, csPool.next().of(_lo, _wptr - 1));
                    headerName = null;
                    _lo = _wptr;
                    break;
                default:
                    break;
            }
        }
        return p;
    }

    private void parseContentLength() {
        contentLength = -1;
        DirectUtf8Sequence seq = getHeader(HEADER_CONTENT_LENGTH);
        if (seq == null) {
            return;
        }

        try {
            contentLength = Numbers.parseLong(seq);
        } catch (NumericException ignore) {
            throw HttpException.instance("Malformed ").put(HEADER_CONTENT_LENGTH).put(" header");
        }
    }

    private void parseKnownHeaders() {
        parseContentLength();
    }

    private int parseMethod(long lo, long hi) {
        long p = lo;
        while (p < hi) {
            if (_wptr == this.hi) {
                throw HttpException.instance("url is too long");
            }

            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == '\r') {
                continue;
            }

            switch (b) {
                case ' ':
                    if (isMethod) {
                        method = csPool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        isMethod = false;
                    } else if (isUrl) {
                        url = csPool.next().of(_lo, _wptr);
                        isUrl = false;
                        _lo = _wptr + 1;
                    } else if (isQueryParams) {
                        query = csPool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        isQueryParams = false;
                        break;
                    }
                    break;
                case '?':
                    url = csPool.next().of(_lo, _wptr);
                    isUrl = false;
                    isQueryParams = true;
                    _lo = _wptr + 1;
                    break;
                case '\n':
                    if (method == null) {
                        throw HttpException.instance("bad method");
                    }
                    needMethod = false;

                    // parse and decode query string
                    if (query != null) {
                        final int querySize = query.size();
                        final long newBoundary = _wptr + querySize;
                        if (querySize > 0 && newBoundary < this.hi) {
                            Vect.memcpy(_wptr, query.ptr(), querySize);
                            int o = urlDecode(_wptr, newBoundary, urlParams);
                            _wptr = newBoundary - o;
                        } else {
                            throw HttpException.instance("URL query string is too long");
                        }
                    }
                    this._lo = _wptr;
                    return (int) (p - lo);
                default:
                    break;
            }
            Unsafe.getUnsafe().putByte(_wptr++, (byte) b);
        }
        return (int) (p - lo);
    }

    private int parseProtocol(long lo, long hi) {
        long p = lo;
        while (p < hi) {
            if (_wptr == this.hi) {
                throw HttpException.instance("protocol line is too long");
            }

            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == '\r') {
                continue;
            }

            switch (b) {
                case ' ':
                    if (isProtocol) {
                        protocol = csPool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        isProtocol = false;
                    } else if (isStatusCode) {
                        statusCode = csPool.next().of(_lo, _wptr);
                        isStatusCode = false;
                        _lo = _wptr + 1;
                    }
                    break;
                case '\n':
                    if (isStatusText) {
                        isStatusText = false;
                    }
                    if (protocol == null) {
                        throw HttpException.instance("bad protocol");
                    }
                    needProtocol = false;
                    this._lo = _wptr;
                    return (int) (p - lo);
                default:
                    break;
            }
            Unsafe.getUnsafe().putByte(_wptr++, (byte) b);
        }
        return (int) (p - lo);
    }

    private int urlDecode(long lo, long hi, Utf8SequenceObjHashMap<DirectUtf8String> map) {
        long _lo = lo;
        long rp = lo;
        long wp = lo;
        int offset = 0;

        DirectUtf8String name = null;
        while (rp < hi) {
            char b = (char) Unsafe.getUnsafe().getByte(rp++);
            switch (b) {
                case '=':
                    if (_lo < wp) {
                        name = csPool.next().of(_lo, wp);
                    }
                    _lo = rp - offset;
                    break;
                case '&':
                    if (name != null) {
                        map.put(name, csPool.next().of(_lo, wp));
                        name = null;
                    }
                    _lo = rp - offset;
                    break;
                case '+':
                    Unsafe.getUnsafe().putByte(wp++, (byte) ' ');
                    continue;
                case '%':
                    try {
                        if (rp + 1 < hi) {
                            byte bb = (byte) Numbers.parseHexInt(temp.of(rp, rp += 2).asAsciiCharSequence());
                            Unsafe.getUnsafe().putByte(wp++, bb);
                            offset += 2;
                            continue;
                        }
                    } catch (NumericException ignore) {
                    }
                    throw HttpException.instance("invalid query encoding");
                default:
                    break;
            }
            Unsafe.getUnsafe().putByte(wp++, (byte) b);
        }

        if (_lo < wp && name != null) {
            map.put(name, csPool.next().of(_lo, wp));
        }

        return offset;
    }

    public static class BoundaryAugmenter implements Reopenable, QuietCloseable {
        private static final Utf8String BOUNDARY_PREFIX = new Utf8String("\r\n--");
        private final DirectUtf8String export = new DirectUtf8String();
        private long _wptr;
        private long lim;
        private long lo;

        @Override
        public void close() {
            if (lo > 0) {
                lo = _wptr = Unsafe.free(lo, lim, MemoryTag.NATIVE_HTTP_CONN);
            }
        }

        public DirectUtf8String of(Utf8Sequence value) {
            int len = value.size() + BOUNDARY_PREFIX.size();
            if (len > lim) {
                resize(len);
            }
            _wptr = lo + BOUNDARY_PREFIX.size();
            of0(value);
            return export.of(lo, _wptr);
        }

        private void of0(Utf8Sequence value) {
            int len = value.size();
            Utf8s.strCpy(value, len, _wptr);
            _wptr += len;
        }

        private void resize(int lim) {
            final long prevLim = this.lim;
            this.lim = Numbers.ceilPow2(lim);
            this.lo = this._wptr = Unsafe.realloc(this.lo, prevLim, this.lim, MemoryTag.NATIVE_HTTP_CONN);
            of0(BOUNDARY_PREFIX);
        }
    }
}