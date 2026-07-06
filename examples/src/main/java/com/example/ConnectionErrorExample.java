package com.example;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;

/**
 * Connection-level failures on the pooled facade.
 * <p>
 * Some failures happen when the pool opens a WebSocket, before any row or query
 * is exchanged. The pool warms up its minimum connections at {@code connect()}
 * and opens more on demand, so a connect-level error can surface either from
 * {@link QuestDB#connect} or from the first {@link QuestDB#borrowSender()} /
 * query -- wrap both.
 * <ul>
 *   <li><b>Authentication failure</b> (401/403 on the upgrade) is terminal
 *       across all endpoints and surfaces as {@link LineSenderException} on the
 *       ingest side.</li>
 *   <li><b>Role mismatch</b>: when every listed endpoint reports a role that
 *       doesn't match the query-side {@code target=} filter, the client raises
 *       {@link QwpRoleMismatchException}.</li>
 *   <li><b>Unreachable / unresolvable host</b>: a generic connect failure
 *       surfaces as {@link LineSenderException} on the ingest side and as
 *       {@link HttpClientException} (the {@code QwpRoleMismatchException}
 *       supertype) on the query side.</li>
 * </ul>
 */
public class ConnectionErrorExample {

    public static void main(String[] args) throws InterruptedException {

        // 1. Bad credentials -> auth failure on the ingest connection.
        try (QuestDB db = QuestDB.connect(
                "wss::addr=db.example.com:9000;token=INVALID_TOKEN;")) {
            try (Sender sender = db.borrowSender()) {
                sender.table("trades").doubleColumn("price", 1.0).atNow();
            }
        } catch (LineSenderException | HttpClientException e) {
            System.err.println("ingest connection failed (auth/unreachable?): " + e.getMessage());
        }

        // 2. target=replica but no listed endpoint is a replica -> role mismatch.
        try (QuestDB db = QuestDB.connect(
                "wss::addr=db-primary:9000;token=YOUR_TOKEN;target=replica;")) {
            try (Query q = db.borrowQuery()) {
                q.sql("SELECT 1").handler(new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                    }
                }).submit().await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        } catch (QwpRoleMismatchException e) {
            System.err.println("no endpoint matched target=replica: " + e.getMessage());
        } catch (LineSenderException | HttpClientException e) {
            System.err.println("connection failed: " + e.getMessage());
        }
    }
}
