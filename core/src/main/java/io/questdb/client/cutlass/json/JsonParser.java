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

package io.questdb.client.cutlass.json;

/**
 * Receives the events {@link JsonLexer} emits as it parses. Implementations assemble whatever they need
 * from the event stream; the lexer keeps no document.
 */
@FunctionalInterface
public interface JsonParser {
    /**
     * Called once per parse event, on the thread driving {@link JsonLexer#parse}.
     *
     * <p><b>{@code tag} is JSON-UNESCAPED.</b> A value written {@code "a\\nb"} in the document arrives as
     * the four characters {@code a \ n b}, not as the five raw ones. An implementation must NOT unescape it
     * again: doing so decodes the {@code \n} a second time and yields {@code a}, LF, {@code b}. Earlier
     * releases handed back the raw bytes and left the decoding to the listener, so a parser carried over
     * from one of those has exactly that second decode to remove.
     *
     * <p><b>{@code tag} is a reused buffer, and not necessarily the same instance twice.</b> It is valid
     * only for the duration of this call - copy it to keep it. The lexer assembles an escape-free value in
     * one sink and an escaped one in another, so which object arrives depends on whether that particular
     * value contained a backslash. An implementation must therefore never compare {@code tag} by identity
     * or cache the reference: either works across escape-free input and then fails on the first value that
     * carries an escape.
     *
     * <p>{@code tag} is {@code null} for the structural events - {@link JsonLexer#EVT_OBJ_START},
     * {@link JsonLexer#EVT_OBJ_END}, {@link JsonLexer#EVT_ARRAY_START} and {@link JsonLexer#EVT_ARRAY_END}
     * - and non-null only for {@link JsonLexer#EVT_NAME}, {@link JsonLexer#EVT_VALUE} and
     * {@link JsonLexer#EVT_ARRAY_VALUE}.
     *
     * @param code     the event, one of {@code JsonLexer.EVT_*}
     * @param tag      the name or value the event carries, unescaped, or {@code null} for a structural
     *                 event; borrowed for the duration of the call only
     * @param position byte offset of the event within the whole parsed stream, accumulated across
     *                 {@link JsonLexer#parse} calls rather than an index into {@code tag}
     * @throws JsonException to abort the parse
     */
    void onEvent(int code, CharSequence tag, int position) throws JsonException;
}