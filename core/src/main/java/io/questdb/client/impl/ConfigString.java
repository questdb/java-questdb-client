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

package io.questdb.client.impl;

import io.questdb.client.std.ObjList;
import io.questdb.client.std.str.StringSink;

/**
 * Layer 1 of the QWP connect-string parser: a thin tokenizer over
 * {@link ConfStringParser}. It splits {@code schema::k=v;k=v} into the schema
 * and an ordered list of key/value pairs, preserving repeats so a multivalue
 * {@code addr} survives. Non-multi keys are resolved last-write-wins by
 * {@link ConfigView}.
 * <p>
 * Tokenizing (the {@code ;;} escaping, empty values, trailing {@code ;},
 * control-character rejection) is exactly {@link ConfStringParser}'s, so the
 * QWP consumers parse identically to the hand-rolled loops they replace.
 */
public final class ConfigString {

    private final ObjList<String> keys = new ObjList<>();
    private final String schema;
    private final ObjList<String> values = new ObjList<>();

    private ConfigString(String schema) {
        this.schema = schema;
    }

    /**
     * Tokenizes {@code input}. Throws {@link IllegalArgumentException} with a
     * colon-dialect message on a malformed string.
     */
    public static ConfigString parse(CharSequence input) {
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("configuration string cannot be empty");
        }
        StringSink sink = new StringSink();
        int pos = ConfStringParser.of(input, sink);
        if (pos < 0) {
            throw new IllegalArgumentException("invalid configuration string: " + sink);
        }
        ConfigString cs = new ConfigString(sink.toString());
        while (ConfStringParser.hasNext(input, pos)) {
            pos = ConfStringParser.nextKey(input, pos, sink);
            if (pos < 0) {
                throw new IllegalArgumentException("invalid configuration string: " + sink);
            }
            String key = sink.toString();
            pos = ConfStringParser.value(input, pos, sink);
            if (pos < 0) {
                throw new IllegalArgumentException("invalid configuration string: " + sink);
            }
            cs.keys.add(key);
            cs.values.add(sink.toString());
        }
        return cs;
    }

    public String key(int i) {
        return keys.getQuick(i);
    }

    public String schema() {
        return schema;
    }

    public int size() {
        return keys.size();
    }

    public String value(int i) {
        return values.getQuick(i);
    }
}
