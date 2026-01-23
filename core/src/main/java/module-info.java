/*******************************************************************************
 * ___ _ ____ ____
 * / _ \ _ _ ___ ___| |_| _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | | _ \
 * | |_| | |_| | __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

open module io.questdb.client {
    requires transitive jdk.unsupported;
    requires static org.jetbrains.annotations;
    requires static java.management;
    requires jdk.management;
    requires java.desktop;
    requires java.sql;
    requires org.slf4j;

    exports io.questdb.client;
    exports io.questdb.client.cairo;

    exports io.questdb.client.cutlass.http;
    exports io.questdb.client.cutlass.json;
    exports io.questdb.client.cutlass.line;
    exports io.questdb.client.cutlass.line.tcp;
    exports io.questdb.client.cutlass.line.http;

    exports io.questdb.client.std;
    exports io.questdb.client.std.datetime;
    exports io.questdb.client.std.datetime.microtime;
    exports io.questdb.client.std.datetime.nanotime;
    exports io.questdb.client.std.str;
    exports io.questdb.client.std.ex;
    exports io.questdb.client.std.fastdouble;
    exports io.questdb.client.network;
    exports io.questdb.client.cairo.vm.api;
    exports io.questdb.client.cutlass.http.client;
    exports io.questdb.client.cutlass.auth;
    exports io.questdb.client.std.bytes;
    exports io.questdb.client.impl;
    exports io.questdb.client.cairo.arr;
    exports io.questdb.client.cutlass.line.array;
    exports io.questdb.client.cutlass.line.udp;
}
