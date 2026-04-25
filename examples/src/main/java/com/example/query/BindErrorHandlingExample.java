package com.example.query;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

/**
 * How bind-encoding errors surface.
 * <p>
 * The setters validate values at call time. If something's wrong -- scale
 * out of range, geohash precision out of range, indexes out of order, too
 * many binds, or an unsupported NULL wire-type code -- the setter throws.
 * When the throw happens inside the {@code binds -> ...} lambda passed to
 * {@code execute}, the client catches it and dispatches through
 * {@code handler.onError} with a "bind encoding failed: ..." message. No
 * query is sent and the client stays healthy for the next call.
 * <p>
 * This example hits each failure mode intentionally. The calls that succeed
 * show what valid usage looks like against the same scenarios.
 */
public class BindErrorHandlingExample {

    public static void main(String[] args) {
        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            // 1. Scale out of range. Max scale is 76 for every DECIMAL form.
            System.out.println("bad: DECIMAL scale = 200");
            client.execute(
                    "SELECT $1::DECIMAL(18, 4) AS v FROM long_sequence(1)",
                    binds -> binds.setDecimal64(0, 200, 1L),  // scale 200 > 76
                    printErrorHandler()
            );

            // 2. Geohash precision out of range. Valid precisions: 1..60 bits.
            System.out.println("bad: GEOHASH precision = 99");
            client.execute(
                    "SELECT $1::GEOHASH(60b) AS v FROM long_sequence(1)",
                    binds -> binds.setGeohash(0, 99, 0L),
                    printErrorHandler()
            );

            // 3. Out-of-order index. Indexes must be 0, 1, 2, ... dense.
            System.out.println("bad: skip index 0");
            client.execute(
                    "SELECT $1::INT + $2::INT AS sum FROM long_sequence(1)",
                    binds -> binds.setInt(1, 10),  // should be 0 first
                    printErrorHandler()
            );

            // 4. Duplicate index. Same index twice is rejected.
            System.out.println("bad: duplicate index 0");
            client.execute(
                    "SELECT $1::INT AS v FROM long_sequence(1)",
                    binds -> binds.setInt(0, 1).setInt(0, 2),
                    printErrorHandler()
            );

            // 5. Unsupported NULL type code. The server doesn't accept
            //    BINARY, IPv4, or ARRAY as bind types -- the client mirrors
            //    that by rejecting them at the setter.
            System.out.println("bad: setNull for BINARY (unsupported as bind)");
            client.execute(
                    "SELECT $1 AS v FROM long_sequence(1)",
                    binds -> binds.setNull(0, QwpConstants.TYPE_BINARY),
                    printErrorHandler()
            );

            // 6. For comparison: a good call. The client stays healthy
            //    after the errors above, so this still works.
            System.out.println("good: valid DECIMAL64 bind");
            client.execute(
                    "SELECT $1::DECIMAL(18, 4) AS v FROM long_sequence(1)",
                    binds -> binds.setDecimal64(0, 4, 123_456L),
                    new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            System.out.println("  v scale=" + batch.getDecimalScale(0));
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            System.err.println("  unexpected failure: " + message);
                        }
                    }
            );
        }
    }

    private static QwpColumnBatchHandler printErrorHandler() {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                System.err.println("  unexpected batch -- encode was supposed to fail");
            }

            @Override
            public void onEnd(long totalRows) {
                System.err.println("  unexpected onEnd -- encode was supposed to fail");
            }

            @Override
            public void onError(byte status, String message) {
                System.out.println("  caught: " + message);
            }
        };
    }
}
