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

package io.questdb.client.cutlass.line;

import io.questdb.client.network.Net;
import io.questdb.client.std.str.StringSink;

/**
 * Exception thrown by QuestDB's ILP (InfluxDB Line Protocol) sender implementations to indicate
 * failures during data transmission or protocol violations.
 *
 * <h2>Buffer Preservation on Error</h2>
 * <strong>Important:</strong> When a Sender throws a {@code LineSenderException}, it does NOT clear
 * its internal buffer. This design allows for retry strategies:
 * <ul>
 *   <li>For transient errors: Retry by calling {@code flush()} again on the same Sender instance</li>
 *   <li>For permanent errors: Either close and recreate the Sender, or call {@code reset()} to clear
 *       the buffer and continue with new data</li>
 * </ul>
 *
 * <h2>Retryability</h2>
 * The {@link #isRetryable()} method provides a best-effort indication of whether the error
 * might be resolved by retrying at the application level. This is particularly important
 * because this exception is only thrown after the sender has exhausted its own internal
 * retry attempts. The retryability flag helps applications decide whether to implement
 * additional retry logic with longer delays or different strategies.
 *
 * @see io.questdb.client.Sender
 * @see io.questdb.client.Sender#flush()
 * @see io.questdb.client.Sender#reset()
 */
public class LineSenderException extends RuntimeException {

    private final StringSink message = new StringSink();

    private int errno = Integer.MIN_VALUE;

    public LineSenderException(CharSequence message) {
        this.message.put(message);
    }

    public LineSenderException(CharSequence message, boolean retryable) {
        this.message.put(message);
    }

    public LineSenderException(Throwable t) {
        super(t);
    }

    public LineSenderException(String message, Throwable cause) {
        super(message, cause);
        this.message.put(message);
    }

    public LineSenderException appendIPv4(int ip) {
        Net.appendIP4(message, ip);
        return this;
    }

    public LineSenderException errno(int errno) {
        this.errno = errno;
        return this;
    }

    @Override
    public String getMessage() {
        if (errno == Integer.MIN_VALUE) {
            return message.toString();
        }
        String errNoRender = "[" + errno + "]";
        if (message.length() == 0) {
            return errNoRender;
        }
        return errNoRender + " " + message;
    }

    public LineSenderException put(char ch) {
        message.put(ch);
        return this;
    }

    public LineSenderException put(long value) {
        message.put(value);
        return this;
    }

    public LineSenderException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public LineSenderException putAsPrintable(CharSequence nonPrintable) {
        message.putAsPrintable(nonPrintable);
        return this;
    }
}