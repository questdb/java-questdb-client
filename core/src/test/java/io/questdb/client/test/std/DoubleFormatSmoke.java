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

package io.questdb.client.test.std;

import io.questdb.client.std.str.StringSink;

/**
 * Standalone (no JUnit) smoke check that double formatting works against the
 * packaged jar on the running JDK. The extreme-exponent values exercise the
 * FdBig fallback of {@code Numbers.appendDouble0}, which resolves
 * {@code FDBigInteger} from a different JDK-internal package on Java 8 vs 9+;
 * a jar packaged with the wrong bridge dies here with
 * {@code NoClassDefFoundError: sun/misc/FDBigInteger} (the 1.3.5-1.3.7
 * regression). Run by {@link JarPackagingIT} on the build JDK and by CI on
 * JDK 25 against the JDK 8-built jar.
 */
public final class DoubleFormatSmoke {

    public static void main(String[] args) {
        double[] values = {
                0.0d,
                123.456d,
                // FdBig slow-path values
                1.0E-300,
                Double.MIN_VALUE,
                Double.MAX_VALUE,
                -2.225073858507201E-308,
                1.1317400099603851E308
        };
        for (int i = 0; i < values.length; i++) {
            double d = values[i];
            StringSink sink = new StringSink();
            sink.put(d);
            String formatted = sink.toString();
            if (Double.doubleToLongBits(Double.parseDouble(formatted)) != Double.doubleToLongBits(d)) {
                System.err.println("FAIL: " + d + " formatted as \"" + formatted + "\" does not round-trip");
                System.exit(1);
            }
        }
        System.out.println("OK: double formatting works on Java " + System.getProperty("java.version"));
    }

    private DoubleFormatSmoke() {
    }
}
