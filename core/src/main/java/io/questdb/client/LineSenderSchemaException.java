/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package io.questdb.client;

import io.questdb.client.cutlass.line.LineSenderException;

/** A schema-bound row construction failure with a stable reason. */
public class LineSenderSchemaException extends LineSenderException {
    public enum Reason {
        /**
         * The value cannot go into the target column: it does not parse, it is
         * outside the column's range, or the column's type does not accept the
         * setter's input type.
         */
        INVALID_VALUE,
        /**
         * The sender could not obtain the table's schema: the lookup timed out,
         * the connection dropped under it, or the server reported the schema as
         * temporarily unavailable. A later attempt may succeed.
         */
        SCHEMA_UNAVAILABLE,
        /**
         * The server denied this connection access to the table's schema.
         */
        ACCESS_DENIED,
        /**
         * The client does not implement this conversion or target, or the
         * server's schema is too large to return in a discovery response.
         */
        UNSUPPORTED_FEATURE
    }

    private final Reason reason;

    public LineSenderSchemaException(Reason reason, CharSequence message) {
        super(message, false);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}
