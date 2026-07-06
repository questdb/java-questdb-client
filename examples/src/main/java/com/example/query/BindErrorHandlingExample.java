package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

/**
 * How bind-encoding errors surface.
 * <p>
 * The setters validate values at call time. If something's wrong -- scale
 * out of range, geohash precision out of range, indexes out of order, too
 * many binds, or an unsupported NULL wire-type code -- the setter throws.
 * When the throw happens inside the {@code binds -> ...} lambda, the pooled
 * query worker catches it and dispatches through {@code handler.onError} with
 * a "bind encoding failed: ..." message; {@code Completion.await()} rethrows
 * it as a {@link QueryException}. No query is sent and the pooled worker
 * returns clean for the next {@link Query#submit()}.
 * <p>
 * This example hits each failure mode intentionally. The call that succeeds
 * shows what valid usage looks like against the same scenario.
 */
public class BindErrorHandlingExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {

            // 1. Scale out of range. Max scale is 76 for every DECIMAL form.
            System.out.println("bad: DECIMAL scale = 200");
            runExpectingBindError(db,
                    "SELECT $1::DECIMAL(18, 4) AS v FROM long_sequence(1)",
                    binds -> binds.setDecimal64(0, 200, 1L));  // scale 200 > 76

            // 2. Geohash precision out of range. Valid precisions: 1..60 bits.
            System.out.println("bad: GEOHASH precision = 99");
            runExpectingBindError(db,
                    "SELECT $1::GEOHASH(60b) AS v FROM long_sequence(1)",
                    binds -> binds.setGeohash(0, 99, 0L));

            // 3. Out-of-order index. Indexes must be 0, 1, 2, ... dense.
            System.out.println("bad: skip index 0");
            runExpectingBindError(db,
                    "SELECT $1::INT + $2::INT AS sum FROM long_sequence(1)",
                    binds -> binds.setInt(1, 10));  // should be 0 first

            // 4. Duplicate index. Same index twice is rejected.
            System.out.println("bad: duplicate index 0");
            runExpectingBindError(db,
                    "SELECT $1::INT AS v FROM long_sequence(1)",
                    binds -> binds.setInt(0, 1).setInt(0, 2));

            // 5. Unsupported NULL type code. The server doesn't accept
            //    BINARY, IPv4, or ARRAY as bind types -- the client mirrors
            //    that by rejecting them at the setter.
            System.out.println("bad: setNull for BINARY (unsupported as bind)");
            runExpectingBindError(db,
                    "SELECT $1 AS v FROM long_sequence(1)",
                    binds -> binds.setNull(0, QwpConstants.TYPE_BINARY));

            // 6. For comparison: a good call. The pool stays healthy after the
            //    errors above, so this still works.
            System.out.println("good: valid DECIMAL64 bind");
            q.sql("SELECT $1::DECIMAL(18, 4) AS v FROM long_sequence(1)")
                    .binds(binds -> binds.setDecimal64(0, 4, 123_456L))
                    .handler(new QwpColumnBatchHandler() {
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
                    })
                    .submit()
                    .await();
        }
    }

    private static void runExpectingBindError(QuestDB db, String sql, QwpBindSetter binds)
            throws InterruptedException {
        try (Query q = db.borrowQuery()) {
            q.sql(sql).binds(binds).handler(printErrorHandler()).submit().await();
        } catch (QueryException e) {
            // await() rethrows the bind-encode failure the handler already
            // reported in onError; swallowed here so the demo continues.
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
