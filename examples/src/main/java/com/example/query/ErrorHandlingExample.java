package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Surfacing server-side errors back to application code.
 * <p>
 * When the server rejects a query (syntax error, missing table, unsupported
 * statement, permission denied), it sends a {@code QUERY_ERROR} frame rather
 * than data. Through the pooled {@link QuestDB} facade this surfaces two ways:
 * the handler's {@link QwpColumnBatchHandler#onError} fires (skipping
 * {@code onBatch} / {@code onEnd} entirely), and {@code Completion.await()}
 * rethrows the failure as a {@link QueryException} carrying the same status.
 * <p>
 * Status codes mirror the ingress namespace. For egress the common ones are:
 * <ul>
 *   <li>{@code 0x03 SCHEMA_MISMATCH}  — bind parameter type doesn't match the placeholder</li>
 *   <li>{@code 0x05 PARSE_ERROR}      — SQL syntax error OR unsupported statement on the endpoint</li>
 *   <li>{@code 0x06 INTERNAL_ERROR}   — unexpected server-side failure</li>
 *   <li>{@code 0x08 SECURITY_ERROR}   — authorization failure</li>
 *   <li>{@code 0x0A CANCELLED}        — query terminated in response to CANCEL</li>
 *   <li>{@code 0x0B LIMIT_EXCEEDED}   — a protocol limit was hit</li>
 * </ul>
 * SQL-level errors carry the position embedded in the message, using QuestDB's
 * standard "{@code [pos] text}" format, so you can point the user directly at
 * the offending token.
 * <p>
 * Note: DDL / INSERT / UPDATE are <em>not</em> errors on egress -- the server
 * executes them and replies with {@code EXEC_DONE}, surfaced via
 * {@link QwpColumnBatchHandler#onExecDone}. See {@link ExecStatementExample}.
 */
public class ErrorHandlingExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {

            // Malformed SQL — triggers a parse error at position 14 (just past "FROM").
            runAndReport(db, "SELECT * FROM");

            // Nonexistent table — also reported as PARSE_ERROR with a "does not exist" message.
            runAndReport(db, "SELECT * FROM nowhere");

            // Reference to a column that doesn't exist — a validation error
            // (PARSE_ERROR) rather than a row stream.
            runAndReport(db, "SELECT no_such_column FROM long_sequence(1)");
        }
    }

    private static void runAndReport(QuestDB db, final String sql) throws InterruptedException {
        System.out.println("-- executing: " + sql);
        try (Query q = db.borrowQuery()) {
            q.sql(sql).handler(new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                    System.out.println("(unexpected) received " + batch.getRowCount() + " rows");
                }

                @Override
                public void onEnd(long totalRows) {
                    System.out.println("query succeeded: rows=" + totalRows);
                }

                @Override
                public void onError(byte status, String message) {
                    System.out.printf("query failed: status=0x%02X, message=%s%n", status & 0xFF, message);
                }

                @Override
                public void onExecDone(short opType, long rowsAffected) {
                    System.out.printf("(unexpected) exec ok: opType=%d, rows=%d%n", opType, rowsAffected);
                }
            }).submit().await();
        } catch (QueryException e) {
            // await() rethrows the same failure the handler already reported in
            // onError above; caught here so the loop moves on to the next query.
        }
    }
}
