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

import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Pins {@code Request.trimContentToLen}'s sentinel guard, whose absence is a SIGSEGV rather than a failed
 * assertion.
 * <p>
 * {@code newRequest()} sets {@code contentStart = -1} and only {@code withContent()} replaces it with a real
 * address, so a request still at the header stage has no content section. Trimming one anyway computes
 * {@code -1 + contentLen} as the write pointer, and the next write to the buffer takes the process down -
 * surefire reports "The forked VM terminated without properly saying goodbye", with no test named.
 * <p>
 * That state is ordinary rather than exotic: an ILP request built with an {@code httpTokenProvider} defers
 * {@code withContent()} until the first row stamps the Authorization header, so it sits at the header stage
 * between every flush and the next row - which is when {@code cancelRow()} can arrive. {@code cancelRow()}
 * used to carry a second {@code isTokenPending} check of its own that returned before the trim. The two were
 * mutually masking: removing either alone left the whole suite green, and only removing both crashed, so
 * neither was pinned and either could have been dropped by a refactor with CI green. The caller-side one is
 * gone; this pins the one that remains, which is also the only protection an external caller of this
 * exported method has.
 * <p>
 * Asserted on the pointer rather than by writing through it: an assertion names what broke, where a write
 * would just kill the fork.
 */
public class HttpClientRequestTrimTest {

    @Test
    public void testTrimContentToLenOnAHeaderStageRequestLeavesThePointerValid() throws Exception {
        assertMemoryLeak(() -> {
            try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                // header stage only - no withContent(), so contentStart is still the -1 sentinel. No socket
                // is involved: newRequest() just resets the buffer and the request state.
                HttpClient.Request request = client.newRequest("127.0.0.1", 9000);
                request.GET().url("/write").header("Authorization", "Bearer GOODTOKEN");

                Assert.assertEquals("precondition: no content section, so no content length",
                        0, request.getContentLength());
                Assert.assertEquals("precondition: and getContentStart() reports 0, not the sentinel",
                        0, request.getContentStart());
                final long ptrAfterHeaders = request.getPtr();
                Assert.assertTrue("precondition: the headers advanced the write pointer",
                        ptrAfterHeaders > 0);

                // what cancelRow() does on a row that never started
                request.trimContentToLen(0);
                Assert.assertEquals("trimming a request with no content section must not move the write "
                                + "pointer - contentStart is -1, so the arithmetic yields an invalid "
                                + "pointer the next write segfaults on",
                        ptrAfterHeaders, request.getPtr());

                // and with a stale non-zero bookmark, which is what rowBookmark holds from the previous
                // request until stampTokenIfPending resets it
                request.trimContentToLen(37);
                Assert.assertEquals("a stale non-zero bookmark must not move it either",
                        ptrAfterHeaders, request.getPtr());

                // the request is still usable afterwards: the content section opens where it should, and
                // writing through it does not touch a rewound pointer
                request.withContent();
                final long contentStart = request.getContentStart();
                Assert.assertTrue("withContent() must open a real content section", contentStart > 0);
                request.putAscii("t v=1i\n");
                Assert.assertEquals(7, request.getContentLength());

                // now that a content section exists, the trim is a real rewind rather than a no-op
                request.trimContentToLen(0);
                Assert.assertEquals("with a content section, trimming must actually rewind",
                        0, request.getContentLength());
                Assert.assertEquals(contentStart, request.getPtr());
            }
        });
    }
}
