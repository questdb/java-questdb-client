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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.cutlass.http.client.AbstractChunkedResponse;
import io.questdb.client.cutlass.http.client.Fragment;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Os;
import io.questdb.client.std.Rnd;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ChunkedResponseTest {

    @Test
    public void testChunkSizeSplit() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd\r\n" +
                        "0a",
                "\r\n" +
                        "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit2() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd\r\n" +
                        "0a\r",
                "\n" +
                        "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit3() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd\r\n" +
                        "0a\r\n",
                "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit4() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd\r",
                "\n" +
                        "0a\r\n" +
                        "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit5() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd",
                "\r\n" +
                        "0a\r\n" +
                        "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit6() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd\r\n" +
                        "0",
                "a\r\n" +
                        "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit7() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd",
                "\r",
                "\n" +
                        "0a\r\n" +
                        "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSplit() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd\r\n",
                "0a\r\n" +
                        "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        int strLen = Math.max(1, rnd.nextInt(1024));
        int chunkCount = Math.max(1, rnd.nextInt(strLen));
        int fragCount = Math.max(1, rnd.nextInt(strLen));
        String input = rnd.nextString(strLen);
        String[] chunks = createChunks(rnd, input, chunkCount);

        // verify that we split chunks correctly
        StringSink actual = new StringSink();
        for (String c : chunks) {
            actual.put(c);
        }
        TestUtils.assertEquals(input, actual);

        StringSink encoded = new StringSink();
        for (String c : chunks) {
            int len = c.length();
            if (len > 0) {
                Numbers.appendHex(encoded, len, false);
                encoded.put("\r\n");
                encoded.put(c);
                encoded.put("\r\n");
            }
        }
        encoded.put("00\r\n");
        encoded.put("\r\n");

        assertResponse(
                input,
                createChunks(rnd, encoded.toString(), fragCount));
    }

    @Test(timeout = 30_000)
    public void testNoArgRecvHonoursPositiveDefaultTimeout() {
        // The ILP flush path reads a chunked response via the no-arg recv(), which delegates to
        // recv(defaultTimeout). With a positive defaultTimeout (the production HttpClient timeout) the
        // whole-call bound applies on that path too, so a server dribbling a never-terminated chunk size
        // cannot wedge a single recv() past the timeout. The explicit recv(int) path is covered by
        // testRecvHonoursTotalTimeoutWhileChunkSizeDribbles.
        final long memSize = 64;
        final long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        try {
            final AbstractChunkedResponse rsp = new AbstractChunkedResponse(mem, mem + memSize, 50) { // positive default
                @Override
                protected int recvOrDie(long bufLo, long bufHi, int timeout) {
                    if (bufLo >= bufHi) {
                        return 0; // buffer full of a CRLF-less chunk size: no forward progress
                    }
                    Unsafe.getUnsafe().putByte(bufLo, (byte) '0'); // a hex digit, never the terminating CR
                    return 1;
                }
            };
            rsp.begin(mem, mem);
            try {
                rsp.recv(); // no-arg: delegates to recv(defaultTimeout=50)
                Assert.fail("expected the no-arg recv to time out on a dribbled, never-terminated chunk size");
            } catch (HttpClientException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
            }
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test(timeout = 30_000)
    public void testOverflowingChunkSizeIsRejectedRatherThanSpun() {
        // A chunk-size line of 16 or more hex digits overflows an unchecked val << 4 accumulation, and the
        // residue decides how the damage shows up. All three must be rejected; only the first one was.
        //
        //   8000000000000000  -> NEGATIVE. Matches neither the "size > 0" data branch nor the "size == 0"
        //                        terminator, so the state machine breaks straight back to the top of the
        //                        loop. The preceding chunk left receive == false with bytes still buffered,
        //                        so the read gate is skipped too and the loop spins with nothing to stop it.
        //   10000000000000000 -> ZERO, and 10000000000000001 -> 1; both report success with the wrong
        //                        bytes, and get their own tests below. Rejecting only the negative case
        //                        left those two, which are the dangerous ones: a spin is at least visible,
        //                        whereas truncated JSON parses.
        //
        // defaultTimeout is -1 on purpose, so no deadline can rescue the spinning case and the test passes
        // only because the size itself is rejected. The server chooses that size line, and for a discovery
        // or token response that server is untrusted.
        // a trailing byte keeps dataLo < dataHi so the read gate stays shut and the spin is reachable
        assertChunkSizeRejected("8000000000000000", "X", "negative overflow residue");
    }

    @Test(timeout = 30_000)
    public void testZeroWrappingChunkSizeIsRejectedRatherThanTruncating() {
        // 10000000000000000 wraps to ZERO, which the state machine reads as the terminal chunk: recv()
        // returns null and the caller sees a complete-looking body that is actually truncated. Worse than
        // the spin the negative residue causes, because nothing looks wrong -- truncated JSON parses, and
        // the connection's framing is lost for the next keep-alive response on it. The size line is chosen
        // by the server, untrusted for an OIDC discovery or token response.
        // A proper CRLF terminator here, so the pre-fix parser really does complete the body rather than
        // stall waiting for one.
        assertChunkSizeRejected("10000000000000000", "\r\n", "zero overflow residue");
    }

    @Test(timeout = 30_000)
    public void testPositiveWrappingChunkSizeIsRejectedRatherThanMisframing() {
        // 10000000000000001 wraps to 1: a one-byte data chunk that frames the following bytes as chunk
        // furniture. Like the zero residue this reports success, just with the wrong bytes.
        assertChunkSizeRejected("10000000000000001", "X", "positive overflow residue");
    }

    @Test
    public void testZeroPaddedChunkSizeIsStillAccepted() {
        // The guard counts SIGNIFICANT hex digits, so a server that pads its size line is not mistaken for
        // one overflowing it. A raw length check would reject this - it is 20 characters against a 15-digit
        // bound - and would break framing against a perfectly conformant peer, which is a worse failure
        // than the one the bound exists to prevent.
        final long memSize = 128;
        final long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        try {
            final String wire = "00000000000000000001\r\nZ\r\n0\r\n\r\n";
            final AbstractChunkedResponse rsp = new AbstractChunkedResponse(mem, mem + memSize, -1) {
                boolean delivered;

                @Override
                protected int recvOrDie(long bufLo, long bufHi, int timeout) {
                    if (delivered) {
                        return 0;
                    }
                    delivered = true;
                    for (int i = 0; i < wire.length(); i++) {
                        Unsafe.getUnsafe().putByte(bufLo + i, (byte) wire.charAt(i));
                    }
                    return wire.length();
                }
            };
            rsp.begin(mem, mem);
            Fragment first = rsp.recv();
            Assert.assertNotNull("a zero-padded size line must frame its chunk normally", first);
            Assert.assertEquals('Z', (char) Unsafe.getUnsafe().getByte(first.lo()));
            Assert.assertEquals(1, first.hi() - first.lo());
            Assert.assertNull("and the terminator must still terminate", rsp.recv());
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertChunkSizeRejected(String sizeLine, String tail, String what) {
        final long memSize = 128;
        final long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        try {
            // one well-formed chunk (leaves receive == false), then the overflowing size line, then a
            // trailing byte so dataLo < dataHi holds the read gate shut
            final String wire = "1\r\nA\r\n" + sizeLine + "\r\n" + tail;
            final AbstractChunkedResponse rsp = new AbstractChunkedResponse(mem, mem + memSize, -1) {
                boolean delivered;

                @Override
                protected int recvOrDie(long bufLo, long bufHi, int timeout) {
                    if (delivered) {
                        return 0;
                    }
                    delivered = true;
                    for (int i = 0; i < wire.length(); i++) {
                        Unsafe.getUnsafe().putByte(bufLo + i, (byte) wire.charAt(i));
                    }
                    return wire.length();
                }
            };
            rsp.begin(mem, mem);
            Fragment first = rsp.recv();
            Assert.assertNotNull(what + ": the first chunk must still be delivered", first);
            Assert.assertEquals('A', (char) Unsafe.getUnsafe().getByte(first.lo()));
            try {
                Fragment second = rsp.recv();
                Assert.fail(what + ": expected the overflowing chunk size to be rejected as malformed, got "
                        + (second == null ? "a terminal chunk (a truncated body reported as complete)"
                        : "a data chunk"));
            } catch (HttpClientException e) {
                Assert.assertTrue(what + ": " + e.getMessage(),
                        e.getMessage().contains("malformed chunk size"));
            }
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test(timeout = 30_000)
    public void testRecvHonoursTotalTimeoutWhileChunkSizeDribbles() {
        // a server that dribbles the chunk-size line and never sends its terminating CRLF must not keep a
        // single recv() running past its timeout. recv(timeout) bounds the whole call (not each socket read),
        // so the loop scanning the never-terminated chunk size aborts once the timeout elapses. Without the
        // bound this recv() never returns and the @Test timeout fires instead.
        final long memSize = 64;
        final long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        try {
            final AbstractChunkedResponse rsp = new AbstractChunkedResponse(mem, mem + memSize, -1) {
                @Override
                protected int recvOrDie(long bufLo, long bufHi, int timeout) {
                    if (bufLo >= bufHi) {
                        return 0; // buffer full of a CRLF-less chunk size: no forward progress
                    }
                    Unsafe.getUnsafe().putByte(bufLo, (byte) '0'); // a hex digit, never the terminating CR
                    return 1;
                }
            };
            rsp.begin(mem, mem);
            try {
                rsp.recv(50);
                Assert.fail("expected recv to time out on a dribbled, never-terminated chunk size");
            } catch (HttpClientException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
            }
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSingleFragment() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd\r\n" +
                        "0a\r\n" +
                        "0123456789\r\n" +
                        "00\r\n" +
                        "\r\n"
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    private static void assertResponse(CharSequence expected, String[] fragments) {
        long memSize = 4096;
        long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        try {
            final AbstractChunkedResponse rsp = new AbstractChunkedResponse(mem, mem + memSize, -1) {
                int fragIndex = 0;
                int fragOffset = 0;

                @Override
                protected int recvOrDie(long bufLo, long bufHi, int timeout) {
                    String frag = fragments[fragIndex];
                    int fragLen = frag.length() - fragOffset;
                    int bufRemaining = (int) (bufHi - bufLo);

                    final int n;
                    final int o = fragOffset;
                    if (fragLen <= bufRemaining) {
                        fragIndex++;
                        fragOffset = 0;
                        n = fragLen;
                    } else {
                        fragOffset += bufRemaining;
                        n = bufRemaining;
                    }
                    for (int i = 0; i < n; i++) {
                        Unsafe.getUnsafe().putByte(bufLo + i, (byte) frag.charAt(o + i));
                    }
                    return n;
                }
            };

            StringSink sink = new StringSink();
            Fragment fragment;
            rsp.begin(mem, mem);
            while ((fragment = rsp.recv()) != null) {
                for (long p = fragment.lo(); p < fragment.hi(); p++) {
                    sink.put((char) Unsafe.getUnsafe().getByte(p));
                }
            }
            TestUtils.assertEquals(expected, sink);
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static String[] createChunks(Rnd rnd, String str, int chunkCount) {
        final ObjList<String> chunks = new ObjList<>();
        int len = str.length();
        int[] splits = new int[chunkCount + 1];
        splits[0] = 0;
        for (int i = 1; i < chunkCount; i++) {
            splits[i] = rnd.nextInt(len);
        }
        splits[chunkCount] = len;
        Arrays.sort(splits);
        for (int i = 0; i < chunkCount; i++) {
            int lo = splits[i];
            int hi = splits[i + 1];
            if (lo < hi) {
                chunks.add(str.substring(lo, hi));
            }
        }

        // copy non-zero len chunks
        String[] array = new String[chunks.size()];
        for (int i = 0, n = chunks.size(); i < n; i++) {
            array[i] = chunks.getQuick(i);
            Assert.assertNotEquals(0, array[i].length());
        }
        return array;
    }

    static {
        Os.init();
    }
}