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

package io.questdb.client.test.std.fastdouble;

public final class TestData {
    private final String input;
    private final String title;
    private final boolean valid;

    public TestData(String input) {
        this(input, input, true);
    }

    public TestData(String title, String input) {
        this(title.contains(input) ? title : title + " " + input, input, true);
    }

    public TestData(String title, String input, boolean valid) {
        this.title = title;
        this.input = input;
        this.valid = valid;
    }

    public String input() {
        return input;
    }

    public String title() {
        return title;
    }

    public boolean valid() {
        return valid;
    }
}
