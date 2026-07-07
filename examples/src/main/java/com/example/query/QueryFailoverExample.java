package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;

/**
 * Query-side failover across replicas (Enterprise).
 * <p>
 * List several hosts in {@code addr}; the query client tries them in order and
 * reconnects to the next reachable one when a connection drops. Unlike ingest
 * (which must reach the primary), reads can target replicas:
 * <ul>
 *   <li>{@code target=any|primary|replica} -- filter endpoints by node role.</li>
 *   <li>{@code zone=...} -- prefer a replica in a given availability zone
 *       (applies when {@code target} is {@code any} or {@code replica}).</li>
 * </ul>
 * When the client transparently fails over to a different node mid-stream, the
 * handler's {@link QwpColumnBatchHandler#onFailoverReset(QwpServerInfo)} fires:
 * any partially-streamed result was discarded and the query is being re-run on
 * the new node. Multi-host failover requires QuestDB Enterprise.
 */
public class QueryFailoverExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect(
                "wss::addr=db-replica-1:9000,db-replica-2:9000,db-primary:9000;"
                        + "token=YOUR_TOKEN;"
                        + "target=replica;"      // prefer read replicas for egress
                        + "zone=us-east-1a;")) {  // prefer this availability zone

            try (Query q = db.borrowQuery()) {
                q.sql("SELECT count() FROM trades").handler(new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        batch.forEachRow(row -> System.out.println("count = " + row.getLongValue(0)));
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                    }

                    @Override
                    public void onFailoverReset(QwpServerInfo newNode) {
                        // Reconnected to a different node mid-query; any partial
                        // result was discarded and the query re-runs from scratch.
                        System.out.println("failed over to node=" + newNode.getNodeId()
                                + " zone=" + newNode.getZoneId());
                    }
                }).submit().await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        }
    }
}
