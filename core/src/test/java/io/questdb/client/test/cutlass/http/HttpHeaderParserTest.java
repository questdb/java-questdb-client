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

package io.questdb.client.test.cutlass.http;

import io.questdb.client.cutlass.http.HttpException;
import io.questdb.client.cutlass.http.HttpHeaderParser;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjectPool;
import io.questdb.client.std.Rnd;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.DirectUtf8String;
import io.questdb.client.std.str.Utf8String;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class HttpHeaderParserTest {

    private static final ObjectPool<DirectUtf8String> pool = new ObjectPool<>(DirectUtf8String.FACTORY, 64);
    private static final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
            "Host: localhost:9000\r\n" +
            "Connection: keep-alive\r\n" +
            "Cache-Control: max-age=0\r\n" +
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
            "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
            "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML\r\n" +
            "Accept-Encoding: gzip,deflate,sdch\r\n" +
            "Accept-Language: en-US,en;q=0.8\r\n" +
            "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
            "\r\n";

    @Test
    public void testContentLengthLarge() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Length: 81136060058\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false, false);
                Assert.assertEquals(81136060058L, hp.getContentLength());
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDanglingUrlParamWithoutValue() {
        String request = "GET /status?accept HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "\r\n";
        try (HttpHeaderParser hp = new HttpHeaderParser(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                hp.parse(p, p + request.length(), true, false);
                Assert.assertNull(hp.getUrlParam(new Utf8String("accept")));
            } finally {
                Unsafe.free(p, request.length(), MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testMethodTooLarge() {
        String v = "GET /xyzadadadjlkjqeljqasdqweqeasdasdasdawqeadadsqweqeweqdadsasdadadasdadasdqadqw HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(64, pool)) {
            hp.parse(p, p + v.length(), true, false);
            Assert.fail();
        } catch (HttpException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "url is too long");
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testProtocolLine() {
        String v = "HTTP/3 200 OK\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), false, true);
            TestUtils.assertEquals("200", hp.getStatusCode());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testProtocolLineFuzz() {
        String v = "HTTP/3 200 OK\r\n";
        long p = TestUtils.toMemory(v);
        long s1 = System.currentTimeMillis();
        long s2 = System.nanoTime();
        Rnd rnd = new Rnd(s1, s2);
        int steps = rnd.nextInt(v.length()) + 1;
        int stepMax = rnd.nextInt(v.length() / steps + 1) + 1;
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            int total = 0;
            for (int i = 0; i < steps; i++) {
                int n = rnd.nextInt(stepMax);
                hp.parse(p + total, p + total + n, false, true);
                total += n;
            }
            if (total < v.length()) {
                hp.parse(p + total, p + v.length(), false, true);
            }
            TestUtils.assertEquals("200", hp.getStatusCode());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testQueryDanglingEncoding() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "GET /status?x=1&a=% HTTP/1.1\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), true, false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid query encoding");
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testQueryInvalidEncoding() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "GET /status?x=1&a=%i6b&c&d=x HTTP/1.1\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), true, false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid query encoding");
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSplitWrite() {
        try (HttpHeaderParser hp = new HttpHeaderParser(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                for (int i = 0, n = request.length(); i < n; i++) {
                    hp.clear();
                    hp.parse(p, p + i, true, false);
                    Assert.assertTrue(hp.isIncomplete());
                    hp.parse(p + i, p + n, true, false);
                    assertHeaders(hp);
                }
            } finally {
                Unsafe.free(p, request.length(), MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testUrlParamsDecode() {
        String v = "GET /test?x=a&y=b+c%26&z=ab%20ba&w=2 HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b c&", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("ab ba", hp.getUrlParam(new Utf8String("z")));
            TestUtils.assertEquals("2", hp.getUrlParam(new Utf8String("w")));
            TestUtils.assertEquals("x=a&y=b+c%26&z=ab%20ba&w=2", hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsDecodeSpace() {
        String v = "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b c", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("123", hp.getUrlParam(new Utf8String("z")));
            TestUtils.assertEquals("x=a&y=b+c&z=123", hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsDecodeTrailingSpace() {
        String v = "GET /xyz?x=a&y=b+c HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b c", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("x=a&y=b+c", hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsDuplicateAmp() {
        String v = "GET /query?x=a&&y==b HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("x=a&&y==b", hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsSimple() {
        String v = "GET /query?x=a&y=b HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsTrailingEmpty() {
        String v = "GET /ip?x=a&y=b&z= HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
            Assert.assertNull(hp.getUrlParam(new Utf8String("z")));
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsTrailingNull() {
        String v = "GET /opi?x=a&y=b& HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWrite() {
        try (HttpHeaderParser hp = new HttpHeaderParser(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                hp.parse(p, p + request.length(), true, false);
                assertHeaders(hp);
            } finally {
                Unsafe.free(p, request.length(), MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private void assertHeaders(HttpHeaderParser hp) {
        Assert.assertFalse(hp.isIncomplete());
        TestUtils.assertEquals("GET", hp.getMethod());
        TestUtils.assertEquals("/status", hp.getUrl());
        TestUtils.assertEquals("x=1&a=%26b&c&d=x", hp.getQuery());
        TestUtils.assertEquals("localhost:9000", hp.getHeader(new Utf8String("Host")));
        TestUtils.assertEquals("keep-alive", hp.getHeader(new Utf8String("Connection")));
        TestUtils.assertEquals("max-age=0", hp.getHeader(new Utf8String("Cache-Control")));
        TestUtils.assertEquals("text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", hp.getHeader(new Utf8String("Accept")));
        TestUtils.assertEquals("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36", hp.getHeader(new Utf8String("User-Agent")));
        TestUtils.assertEquals("multipart/form-data; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML", hp.getHeader(new Utf8String("Content-Type")));
        TestUtils.assertEquals("gzip,deflate,sdch", hp.getHeader(new Utf8String("Accept-Encoding")));
        TestUtils.assertEquals("en-US,en;q=0.8", hp.getHeader(new Utf8String("Accept-Language")));
        TestUtils.assertEquals("textwrapon=false; textautoformat=false; wysiwyg=textarea", hp.getHeader(new Utf8String("Cookie")));
        TestUtils.assertEquals("1", hp.getUrlParam(new Utf8String("x")));
        TestUtils.assertEquals("&b", hp.getUrlParam(new Utf8String("a")));
        Assert.assertNull(hp.getUrlParam(new Utf8String("c")));
        Assert.assertNull(hp.getHeader(new Utf8String("merge_copy_var_column")));
    }
}
