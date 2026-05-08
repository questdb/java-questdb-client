package com.example.subscribe;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpSubscribeClient;
import io.questdb.client.cutlass.qwp.client.QwpSubscribeMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpSubscription;
import io.questdb.client.cutlass.qwp.client.QwpSubscriptionHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Self-contained QWP table-subscription demo. Creates the {@code quotes}
 * table on startup if needed, subscribes over the WebSocket egress, and
 * spawns a background writer thread that inserts a unique row every second
 * over the HTTP REST endpoint. Each iteration prints both ends of the
 * round-trip:
 * <pre>
 *   -&gt;&gt; INSERT n=1 price=101.00
 *   &lt;-- SUB txn=2 seq=0 row=0 sym=AAPL price=101.0 ts=1000000
 *   -&gt;&gt; INSERT n=2 price=102.00
 *   &lt;-- SUB txn=3 seq=0 row=0 sym=AAPL price=102.0 ts=2000000
 * </pre>
 * <p>
 * Subscriptions are an Enterprise-only feature on the server side. Against
 * an OSS server the SUBSCRIBE_REQUEST is rejected and the example exits.
 * <p>
 * Stops on Ctrl-C: the JVM shutdown propagates to the try-with-resources,
 * the subscribe client sends SUB_CANCEL on the way out, and the writer
 * thread (daemon) is torn down with the JVM.
 */
public class SubscribeExample {

    private static final String HOST = "127.0.0.1";
    /**
     * QuestDB Enterprise default. Both the REST {@code /exec} endpoint and
     * the QWP {@code /read/v1} WebSocket upgrade live on the HTTP port.
     */
    private static final int PORT = 9086;
    private static final String AUTH_HEADER = basicAuth("admin", "quest");

    public static void main(String[] args) throws Exception {
        // 1. Idempotent CREATE so the demo can be re-run without manual setup.
        execSql("CREATE TABLE IF NOT EXISTS quotes (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                "TIMESTAMP(ts) PARTITION BY DAY WAL");

        AtomicBoolean writerStop = new AtomicBoolean();
        try (QwpSubscribeClient client = QwpSubscribeClient.newPlainText(HOST, PORT)
                .withBasicAuth("admin", "quest")
                .withClientId("subscribe-example")) {
            client.connect();

            QwpSubscription sub = client.subscribe("quotes", new QwpSubscriptionHandler() {
                @Override
                public void onAck(long startTxn, int schemaId) {
                    System.out.println("ACK startTxn=" + startTxn + " schemaId=" + schemaId);
                }

                @Override
                public void onBatch(long txn, QwpColumnBatch batch) {
                    // The QwpColumnBatch view is valid only for the duration
                    // of this callback - copy any values you need to retain.
                    long batchSeq = batch.batchSeq();
                    int rowCount = batch.getRowCount();
                    int colCount = batch.getColumnCount();
                    for (int row = 0; row < rowCount; row++) {
                        StringBuilder line = new StringBuilder()
                                .append("<-- SUB txn=").append(txn)
                                .append(" seq=").append(batchSeq)
                                .append(" row=").append(row);
                        for (int col = 0; col < colCount; col++) {
                            line.append(' ').append(batch.getColumnName(col)).append('=');
                            line.append(formatValue(batch, col, row));
                        }
                        System.out.println(line);
                    }
                }

                @Override
                public void onEnd(byte reason, long lastTxn, String message) {
                    System.out.println("END reason=" + reasonName(reason)
                            + " lastTxn=" + lastTxn
                            + " msg=\"" + message + "\"");
                }
            });

            System.out.println("subscribed; subscriptionId=" + sub.getSubscriptionId()
                    + " startTxn=" + sub.getStartTxn());

            // 2. Background writer. Runs on a daemon thread so Ctrl-C exits
            //    the whole process cleanly. Each iteration: log the INSERT,
            //    issue it via the REST /exec endpoint, then sleep 1s.
            Thread writer = startWriterThread(writerStop);

            try {
                // 3. Drain inbound subscribe frames until the server
                //    terminates the subscription. poll() returns when one
                //    frame is processed or the timeout elapses.
                while (sub.isActive()) {
                    client.poll(2_000);
                }
            } finally {
                writerStop.set(true);
                writer.interrupt();
                writer.join(2_000);
            }
        }
    }

    private static String basicAuth(String user, String pwd) {
        return "Basic " + Base64.getEncoder().encodeToString(
                (user + ":" + pwd).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Posts {@code sql} to the REST {@code /exec} endpoint. The endpoint is
     * synchronous: a 2xx response means the statement has been accepted by
     * the server. Errors surface as a {@link RuntimeException} carrying the
     * server's response body.
     */
    private static void execSql(String sql) throws IOException {
        String urlStr = "http://" + HOST + ":" + PORT + "/exec?query="
                + URLEncoder.encode(sql, StandardCharsets.UTF_8.name());
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestProperty("Authorization", AUTH_HEADER);
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(5_000);
        int code = conn.getResponseCode();
        InputStream is = code / 100 == 2 ? conn.getInputStream() : conn.getErrorStream();
        StringBuilder body = new StringBuilder();
        if (is != null) {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    body.append(line);
                }
            }
        }
        if (code / 100 != 2) {
            throw new IOException("exec failed [code=" + code + ", body=" + body + "]");
        }
    }

    private static String formatValue(QwpColumnBatch batch, int col, int row) {
        byte wire = batch.getColumnWireType(col);
        switch (wire) {
            case QwpConstants.TYPE_BOOLEAN:
                return Boolean.toString(batch.getBoolValue(col, row));
            case QwpConstants.TYPE_BYTE:
                return Byte.toString(batch.getByteValue(col, row));
            case QwpConstants.TYPE_SHORT:
                return Short.toString(batch.getShortValue(col, row));
            case QwpConstants.TYPE_INT:
                return Integer.toString(batch.getIntValue(col, row));
            case QwpConstants.TYPE_LONG:
                return Long.toString(batch.getLongValue(col, row));
            case QwpConstants.TYPE_FLOAT:
                return Float.toString(batch.getFloatValue(col, row));
            case QwpConstants.TYPE_DOUBLE:
                return Double.toString(batch.getDoubleValue(col, row));
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_TIMESTAMP:
                return Long.toString(batch.getLongValue(col, row));
            case QwpConstants.TYPE_VARCHAR:
                return String.valueOf(batch.getString(col, row));
            case QwpConstants.TYPE_SYMBOL:
                return String.valueOf(batch.getSymbol(col, row));
            default:
                return "<wire=0x" + Integer.toHexString(wire & 0xFF) + ">";
        }
    }

    private static String reasonName(byte reason) {
        switch (reason) {
            case QwpSubscribeMsgKind.SUB_END_CLIENT_UNSUBSCRIBE:
                return "CLIENT_UNSUBSCRIBE";
            case QwpSubscribeMsgKind.SUB_END_TABLE_DROPPED:
                return "TABLE_DROPPED";
            case QwpSubscribeMsgKind.SUB_END_SCHEMA_CHANGED:
                return "SCHEMA_CHANGED";
            case QwpSubscribeMsgKind.SUB_END_STALE:
                return "STALE";
            case QwpSubscribeMsgKind.SUB_END_SECURITY_REVOKED:
                return "SECURITY_REVOKED";
            case QwpSubscribeMsgKind.SUB_END_SERVER_SHUTDOWN:
                return "SERVER_SHUTDOWN";
            case QwpSubscribeMsgKind.SUB_END_ERROR:
                return "ERROR";
            default:
                return "0x" + Integer.toHexString(reason & 0xFF);
        }
    }

    private static Thread startWriterThread(AtomicBoolean stop) {
        Thread t = new Thread(() -> {
            int n = 0;
            while (!stop.get()) {
                n++;
                double price = 100.0 + n;
                long ts = (long) n * 1_000_000L;
                String sql = String.format(
                        "INSERT INTO quotes VALUES ('AAPL', %.2f, %d::TIMESTAMP)",
                        price, ts);
                System.out.println(String.format("->> INSERT n=%d price=%.2f ts=%d", n, price, ts));
                try {
                    execSql(sql);
                } catch (IOException e) {
                    System.err.println("writer insert failed: " + e.getMessage());
                }
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "subscribe-example-writer");
        t.setDaemon(true);
        t.start();
        return t;
    }
}
