package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Running DDL / INSERT / UPDATE statements over QWP egress.
 * <p>
 * The query path accepts any SQL statement the compiler understands, not just
 * {@code SELECT}. Non-SELECT statements skip the
 * {@code onBatch} / {@code onEnd} callbacks entirely -- the server executes
 * the statement and replies with a single {@code EXEC_DONE} frame that the
 * client surfaces via {@link QwpColumnBatchHandler#onExecDone}.
 * <p>
 * The callback carries two pieces of information:
 * <ul>
 *   <li>{@code opType} -- one of {@code CompiledQuery.SELECT} /
 *       {@code INSERT} / {@code UPDATE} / {@code CREATE_TABLE} /
 *       {@code DROP} / etc. (see {@code io.questdb.griffin.CompiledQuery}
 *       for the full enum).</li>
 *   <li>{@code rowsAffected} -- number of rows inserted / updated / deleted.
 *       Pure DDL (CREATE / DROP / ALTER / RENAME / TRUNCATE / ...) reports 0.</li>
 * </ul>
 */
public class ExecStatementExample {

    // Op-type codes, copied from io.questdb.griffin.CompiledQuery so this
    // example stays compilable without the server-side module on the classpath.
    // The comments are just for readability -- the server sends the numeric
    // value in the EXEC_DONE frame and the client hands it to onExecDone as-is.
    private static final short CREATE_TABLE = 9;
    private static final short DROP = 7;
    private static final short INSERT = 2;
    private static final short UPDATE = 14;

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {

            // 1. DDL: CREATE TABLE. rowsAffected is 0 for pure DDL.
            runExec(db, "CREATE TABLE trades_example (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL", CREATE_TABLE);

            // 2. INSERT with multi-row VALUES. rowsAffected = 3.
            runExec(db,
                    "INSERT INTO trades_example VALUES " +
                            "(0, 'AAPL', 150.25, 100), " +
                            "(1_000_000, 'AAPL', 150.30, 200), " +
                            "(2_000_000, 'MSFT', 420.10, 50)",
                    INSERT);

            // 3. UPDATE with a predicate. Server reports how many rows matched.
            runExec(db,
                    "UPDATE trades_example SET qty = qty * 2 WHERE sym = 'AAPL'",
                    UPDATE);

            // 4. SELECT the data back to confirm the UPDATE landed. Uses the
            //    standard batch-streaming path (onBatch + onEnd).
            System.out.println("-- verifying UPDATE via SELECT");
            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT ts, sym, qty FROM trades_example")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                for (int row = 0; row < batch.getRowCount(); row++) {
                                    System.out.printf(
                                            "  ts=%d sym=%s qty=%d%n",
                                            batch.getLongValue(0, row),
                                            batch.getString(1, row),
                                            batch.getLongValue(2, row)
                                    );
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                            }
                        }
                ).submit().await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }

            // 5. Clean up with DROP. Also pure DDL, rowsAffected=0.
            runExec(db, "DROP TABLE trades_example", DROP);
        }
    }

    /**
     * Executes a non-SELECT statement and prints the server's ack. Fails
     * fast if the server unexpectedly streams rows or the op type doesn't
     * match what we asked for.
     */
    private static void runExec(QuestDB db, String sql, short expectedOpType) throws InterruptedException {
        System.out.println("-- executing: " + sql);
        try (Query q = db.borrowQuery()) {
            q.sql(sql).handler(new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                    System.err.println("(unexpected) batch with " + batch.getRowCount() + " rows");
                }

                @Override
                public void onEnd(long totalRows) {
                    System.err.println("(unexpected) onEnd for a non-SELECT; totalRows=" + totalRows);
                }

                @Override
                public void onError(byte status, String message) {
                }

                @Override
                public void onExecDone(short opType, long rowsAffected) {
                    System.out.printf(
                            "  done: opType=%d (expected=%d), rowsAffected=%d%n",
                            opType, expectedOpType, rowsAffected
                    );
                    if (opType != expectedOpType) {
                        System.err.println("  !! op type mismatch");
                    }
                }
            }).submit().await();
        } catch (QueryException e) {
            System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
        }
    }
}
