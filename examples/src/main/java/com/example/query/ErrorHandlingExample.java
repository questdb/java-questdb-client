package com.example.query;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

/**
 * Surfacing server-side errors back to application code.
 * <p>
 * When the server rejects a query (syntax error, missing table, unsupported
 * statement, permission denied), it sends a {@code QUERY_ERROR} frame rather
 * than data. The client delivers this via {@link QwpColumnBatchHandler#onError},
 * skipping {@code onBatch} / {@code onEnd} entirely.
 * <p>
 * Status codes mirror the ingress namespace. For egress the common ones are:
 * <ul>
 *   <li>{@code 0x03 SCHEMA_MISMATCH}  — bind parameter type doesn't match the placeholder</li>
 *   <li>{@code 0x05 PARSE_ERROR}      — SQL syntax error OR non-SELECT statement</li>
 *   <li>{@code 0x06 INTERNAL_ERROR}   — unexpected server-side failure</li>
 *   <li>{@code 0x08 SECURITY_ERROR}   — authorization failure</li>
 *   <li>{@code 0x0A CANCELLED}        — query terminated in response to CANCEL</li>
 *   <li>{@code 0x0B LIMIT_EXCEEDED}   — a protocol limit was hit</li>
 * </ul>
 * SQL-level errors carry the position embedded in the message, using QuestDB's
 * standard "{@code [pos] text}" format, so you can point the user directly at
 * the offending token.
 */
public class ErrorHandlingExample {

    public static void main(String[] args) {
        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            // Malformed SQL — triggers a parse error at position 14 (just past "FROM").
            runAndReport(client, "SELECT * FROM");

            // Nonexistent table — also reported as PARSE_ERROR with a "does not exist" message.
            runAndReport(client, "SELECT * FROM nowhere");

            // DDL sent over the read endpoint — Phase 1 restricts /read/v1 to SELECT.
            runAndReport(client, "DROP TABLE trades");
        }
    }

    private static void runAndReport(QwpQueryClient client, final String sql) {
        System.out.println("-- executing: " + sql);
        client.execute(sql, new QwpColumnBatchHandler() {
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
        });
    }
}
