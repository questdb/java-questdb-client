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

package io.questdb.cutlass.http;

import io.questdb.cairo.Reopenable;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

import static io.questdb.cutlass.http.HttpConstants.HEADER_CONTENT_LENGTH;

public class HttpHeaderParser implements Mutable, QuietCloseable, HttpRequestHeader {
    private static final Comparator<HttpCookie> COOKIE_COMPARATOR = HttpHeaderParser::cookieComparator;
    private static final Logger LOG = LoggerFactory.getLogger(HttpHeaderParser.class);
    private final BoundaryAugmenter boundaryAugmenter = new BoundaryAugmenter();
    private final ObjList<HttpCookie> cookieList = new ObjList<>();
    private final ObjectPool<HttpCookie> cookiePool;
    private final Utf8SequenceObjHashMap<HttpCookie> cookies = new Utf8SequenceObjHashMap<>();
    private final ObjectPool<DirectUtf8String> csPool;
    private final LowerCaseUtf8SequenceObjHashMap<DirectUtf8String> headers = new LowerCaseUtf8SequenceObjHashMap<>();
    private final HttpHeaderParameterValue parameterValue = new HttpHeaderParameterValue();
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

    private static int cookieComparator(HttpCookie o1, HttpCookie o2) {
        int pathLen1 = o1.path == null ? 0 : o1.path.size();
        int pathLen2 = o2.path == null ? 0 : o2.path.size();
        int diff = pathLen2 - pathLen1;
        return diff != 0 ? diff : Long.compare(o2.expires, o1.expires);
    }

    private static long cookieSkipBytes(long p, long hi) {
        while (p < hi && Unsafe.getUnsafe().getByte(p) != ';') {
            p++;
        }
        return p;
    }

    private static boolean isEquals(long p) {
        return Unsafe.getUnsafe().getByte(p) == '=';
    }

    private static int lowercaseByte(long p) {
        return Unsafe.getUnsafe().getByte(p) | 0x20;
    }

    private static int swarLowercaseInt(long p) {
        return Unsafe.getUnsafe().getInt(p) | 0x20202020;
    }

    private static long swarLowercaseLong(long p) {
        return Unsafe.getUnsafe().getLong(p) | 0x2020202020202020L;
    }

    private static int swarLowercaseShort(long p) {
        return Unsafe.getUnsafe().getShort(p) | 0x2020;
    }

    private static DirectUtf8String unquote(CharSequence key, DirectUtf8String that) {
        int len = that.size();
        if (len == 0) {
            throw HttpException.instance("missing value [key=").put(key).put(']');
        }

        if (that.byteAt(0) == '"') {
            if (that.byteAt(len - 1) == '"') {
                return that.of(that.lo() + 1, that.hi() - 1);
            } else {
                throw HttpException.instance("unclosed quote [key=").put(key).put(']');
            }
        } else {
            return that;
        }
    }

    public DirectUtf8Sequence getUrlParam(Utf8Sequence name) {
        return urlParams.get(name);
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

    public @NotNull ObjList<HttpCookie> getCookieList() {
        return cookieList;
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

    public @Nullable DirectUtf8String getQuery() {
        return query;
    }

    @Override
    public long getContentLength() {
        return contentLength;
    }

    @Override
    public DirectUtf8Sequence getContentType() {
        return contentType;
    }

    @Override
    public DirectUtf8Sequence getHeader(Utf8Sequence name) {
        return headers.get(name);
    }

    @Override
    public DirectUtf8Sequence getMethod() {
        return method;
    }

    public DirectUtf8Sequence getStatusCode() {
        return statusCode;
    }

    public HttpCookie getCookie(Utf8Sequence cookieName) {
        return cookies.get(cookieName);
    }

    public DirectUtf8String getUrl() {
        return url;
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
                    if (HttpKeywords.isHeaderSetCookie(headerName)) {
                        cookieParse(_lo, _wptr - 1);
                    } else {
                        headers.putImmutable(headerName, csPool.next().of(_lo, _wptr - 1));
                    }
                    headerName = null;
                    _lo = _wptr;
                    break;
                default:
                    break;
            }
        }
        return p;
    }

    private long cookieLogUnknownAttributeError(long p, long lo, long hi) {
        long pnext = cookieSkipBytes(p, hi);
        LOG.error("unknown cookie attribute [attribute={}, cookie={}]", csPool.next().of(p, pnext), csPool.next().of(lo, hi));
        return pnext;
    }

    private void cookieParse(long lo, long hi) {

        // let's be pessimistic in case the switch exists early

        HttpCookie cookie = null;
        boolean attributeArea = false;
        long p0 = lo;
        int nonSpaceCount = 0; // non-space character count since p0
        for (long p = lo; p < hi; p++) {
            char c = (char) Unsafe.getUnsafe().getByte(p);
            switch (c | 32) {
                case '=':
                    if (p0 == p) {
                        LOG.error("cookie name is missing");
                        return;
                    }
                    if (cookie != null) {
                        // this means that we have an attribute with name, which we did not
                        // recognize.
                        p = cookieLogUnknownAttributeError(p0, lo, hi);
                    } else {
                        cookie = cookiePool.next();
                        cookie.cookieName = csPool.next().of(p0, p);
                    }
                    p0 = p + 1;
                    nonSpaceCount = 0;
                    break;
                case ';':
                    if (cookie == null) {
                        LOG.error("cookie name is missing");
                        return;
                    }
                    cookie.value = csPool.next().of(p0, p);
                    attributeArea = true;

                    p0 = p + 1;
                    nonSpaceCount = 0;
                    break;
                case 'd':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Domain=<domain-value>
                        // 0x69616d6f = "omai" from Domain
                        if (
                                p + 6 < hi
                                        && swarLowercaseInt(p + 1) == 0x69616d6f
                                        && lowercaseByte(p + 5) == 'n'
                                        && isEquals(p + 6)
                        ) {
                            p += 7;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            cookie.domain = csPool.next().of(p0, p);
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 'p':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Path=<path-value>
                        if (p + 4 < hi && swarLowercaseInt(p) == 0x68746170 && isEquals(p + 4)) {
                            p += 5;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            cookie.path = csPool.next().of(p0, p);
                        } else if (p + 10 < hi && swarLowercaseLong(p + 1) == 0x6e6f697469747261L && swarLowercaseShort(p + 9) == 0x6465) {
                            // Partitioned, len = 11
                            p += 11;
                            p = cookieSkipBytes(p, hi);
                            cookie.partitioned = true;
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 's':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Secure, len = 6, 'S' + 0x72756365 + 'e'
                        if (p + 5 < hi && swarLowercaseInt(p + 1) == 0x72756365 && lowercaseByte(p + 5) == 'e') {
                            // Secure
                            p += 6;
                            p = cookieSkipBytes(p, hi);
                            cookie.secure = true;
                        } else if (p + 8 < hi && swarLowercaseLong(p) == 0x65746973656d6173L && isEquals(p + 8)) {
                            // SameSite=<value>
                            p += 9;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            cookie.sameSite = csPool.next().of(p0, p);
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 'h':
                    if (attributeArea && nonSpaceCount == 0) {
                        // HttpOnly, len = 8, as long 0x796c6e4f70747448L
                        if (p + 7 < hi && swarLowercaseLong(p) == 0x796c6e6f70747468L) {
                            // HttpOnly
                            p += 8;
                            p = cookieSkipBytes(p, hi);
                            cookie.httpOnly = true;
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 'm':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Max-Age=<number>, key len = 7
                        if (p + 7 < hi && swarLowercaseInt(p + 1) == 0x612d7861 && swarLowercaseShort(p + 5) == 0x6567 && isEquals(p + 7)) {
                            p += 8;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            Utf8Sequence v = csPool.next().of(p0, p);
                            try {
                                cookie.maxAge = Numbers.parseLong(v);
                            } catch (NumericException e) {
                                LOG.error("invalid cookie Max-Age value [value={}]", v);
                            }
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 'e':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Expires=<date>
                        // 0x69727078 = "irpx" from Expires
                        if (
                                p + 7 < hi
                                        && swarLowercaseInt(p + 1) == 0x72697078
                                        && lowercaseByte(p + 6) == 's'
                                        && isEquals(p + 7)
                        ) {
                            p += 8;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            Utf8Sequence v = csPool.next().of(p0, p);
                            try {
                                cookie.expires = MicrosFormatUtils.parseHTTP(v.asAsciiCharSequence());
                            } catch (NumericException e) {
                                LOG.error("invalid cookie Expires value [value={}]", v);
                            }
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case ' ':
                    if (nonSpaceCount == 0) {
                        break;
                    }
                    // fallthrough
                default:
                    nonSpaceCount++;
                    break;
            }
        }
        if (cookie == null) {
            LOG.error("malformed cookie [value={}]", csPool.next().of(lo, hi));
            return;
        }
        if (cookie.cookieName != null && cookie.value == null) {
            cookie.value = csPool.next().of(p0, hi);
        }
        cookieList.add(cookie);
    }

    private void cookieSortAndMap() {
        cookieList.sort(COOKIE_COMPARATOR);
        for (int i = 0, n = cookieList.size(); i < n; i++) {
            HttpCookie cookie = cookieList.getQuick(i);
            int index = cookies.keyIndex(cookie.cookieName);
            if (index > -1) {
                cookies.putAt(index, cookie.cookieName, cookie);
            }
        }
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
        cookieSortAndMap();
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