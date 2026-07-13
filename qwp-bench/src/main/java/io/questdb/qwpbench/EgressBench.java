package io.questdb.qwpbench;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.str.DirectUtf8Sequence;

/**
 * Columnar egress bench over QWP/WebSocket, mirroring the cadence of
 * examples/qwp_egress_c.c in c-questdb-client: populate the bench table (unless
 * {@code SKIP_POPULATE}), then measure reading it back through two paths --
 * {@code decode-only} (drain {@link QwpColumnBatch}es, count rows: the decode
 * floor) and {@code materialize} (additionally touch every cell through the
 * typed column accessors, folding values into a checksum so the JIT cannot
 * optimize the reads away: the e2e path). Every read pass opens a fresh
 * {@link QwpQueryClient} and tears it down at the end of the pass -- parity
 * with the C/Rust benches' "fresh reader per iteration".
 */
public final class EgressBench {
    /**
     * Populate batch size. Matches POPULATE_BATCH_ROWS in qwp_egress_c.c
     * (the Rust egress bench hardcodes {@code max_rows(10_000)}) -- fixed,
     * NOT read from MAX_BATCH_ROWS env (unlike IngressBench.run()'s own
     * ingress-mode batch size).
     */
    private static final long POPULATE_BATCH_ROWS = 10_000;

    public static int run() throws Exception {
        BenchSchema.Kind kind = BenchSchema.Kind.parse(Env.str("SCHEMA", "s1-narrow"));
        long rows = Env.zu("ROWS", 10_000_000);
        int symCard = (int) Env.zu("QUESTDB_COLUMN_BENCH_SYM_CARD", 8);
        int varcharLen = (int) Env.zu("QUESTDB_COLUMN_BENCH_VARCHAR_LEN", 16);
        int hiCard = (int) Env.zu("HI_SYM_CARD", 100_000);
        int iterations = (int) Env.zu("ITERATIONS", 5);
        int warmups = (int) Env.zu("WARMUPS", 2);
        String runMode = Env.str("RUN_MODE", "full");
        String host = Env.str("QDB_HOST", "127.0.0.1");
        long port = Env.zu("QDB_PORT", 9000);
        boolean skipPopulate = System.getenv("SKIP_POPULATE") != null;
        String table = kind.tableName();
        int columns = kind.columns();
        String select = kind.selectSql();

        String base = "http://" + host + ":" + port;
        // Fresh QwpQueryClient per pass; conf mirrors the C/Rust egress reader
        // conf exactly (raw compression, no pooling -- a new connection per call).
        String rconf = "ws::addr=" + host + ":" + port + ";compression=raw;";

        System.err.printf("[qwp_egress_java] schema=%s rows=%d it=%d wu=%d host=%s:%d%n",
                kind, rows, iterations, warmups, host, port);

        BenchHttp http = new BenchHttp(base);

        if (!skipPopulate) {
            http.execSql("DROP TABLE IF EXISTS " + table);
            http.execSql(kind.createSql());

            int tcount = BenchSchema.noteTemplateCount(rows);
            String[] notes = new String[tcount];
            for (int t = 0; t < tcount; t++) notes[t] = BenchSchema.noteTemplate(t, varcharLen);

            // Same ws:: conf as IngressBench.run() -- see IngressBench.ingestConf()
            // for why auto-flush is pinned to max thresholds rather than disabled.
            String ingestConf = IngressBench.ingestConf(host, port);
            try (Sender sender = Sender.fromConfig(ingestConf)) {
                IngressBench.pass(sender, kind, rows, symCard, hiCard, notes, POPULATE_BATCH_ROWS);
            }
            System.err.printf("[qwp_egress_java] waiting for WAL apply (count == %d)%n", rows);
            long count = http.waitForCount(table, rows);
            if (count != rows) {
                System.err.printf("[qwp_egress_java] populate count %d != %d%n", count, rows);
                return 2;
            }
        } else {
            System.err.printf("[qwp_egress_java] SKIP_POPULATE: reading existing %s%n", table);
        }

        long[] wallFloor = new long[iterations];
        long[] cpuFloor = new long[iterations];
        long[] gcFloor = new long[iterations];

        for (int w = 0; w < warmups; w++) {
            long seen = readPass(rconf, select, false, null);
            if (seen != rows) {
                System.err.printf("[qwp_egress_java] warmup row mismatch (decode-only) %d != %d%n", seen, rows);
                return 2;
            }
        }
        for (int it = 0; it < iterations; it++) {
            long g0 = BenchJson.gcMs(), c0 = BenchJson.processCpuNs(), t0 = BenchJson.nowNs();
            long seen = readPass(rconf, select, false, null);
            wallFloor[it] = BenchJson.nowNs() - t0;
            cpuFloor[it] = BenchJson.processCpuNs() - c0;
            gcFloor[it] = BenchJson.gcMs() - g0;
            if (seen != rows) {
                System.err.printf("[qwp_egress_java] rows mismatch (decode-only) %d != %d%n", seen, rows);
                return 2;
            }
        }
        double floorMedian = BenchJson.medianS(wallFloor, iterations);

        long[] wallE2e = new long[iterations];
        long[] cpuE2e = new long[iterations];
        long[] gcE2e = new long[iterations];
        double[] checksum = {0.0};

        for (int w = 0; w < warmups; w++) {
            long seen = readPass(rconf, select, true, checksum);
            if (seen != rows) {
                System.err.printf("[qwp_egress_java] warmup row mismatch (materialize) %d != %d%n", seen, rows);
                return 2;
            }
        }
        for (int it = 0; it < iterations; it++) {
            long g0 = BenchJson.gcMs(), c0 = BenchJson.processCpuNs(), t0 = BenchJson.nowNs();
            long seen = readPass(rconf, select, true, checksum);
            wallE2e[it] = BenchJson.nowNs() - t0;
            cpuE2e[it] = BenchJson.processCpuNs() - c0;
            gcE2e[it] = BenchJson.gcMs() - g0;
            if (seen != rows) {
                System.err.printf("[qwp_egress_java] rows mismatch (materialize) %d != %d%n", seen, rows);
                return 2;
            }
        }
        double e2eMedian = BenchJson.medianS(wallE2e, iterations);
        System.err.printf("[qwp_egress_java] checksum=%f%n", checksum[0]);

        BenchJson.Obj report = new BenchJson.Obj();
        report.put("schema", kind == BenchSchema.Kind.S1_NARROW ? "s1-narrow" : "s2-wide");
        report.put("rows", rows);
        report.put("columns", (long) columns);
        report.put("direction", "egress");
        report.put("client", "java-columnar");
        report.put("run_mode", runMode);
        report.put("warmups", (long) warmups);
        report.put("wire_bytes", 0L);
        BenchJson.Obj machine = new BenchJson.Obj();
        machine.put("platform", System.getProperty("os.name").toLowerCase().contains("linux") ? "linux" : "macos");
        machine.put("arch", System.getProperty("os.arch"));
        machine.put("jvm", System.getProperty("java.vm.version"));
        report.put("machine", machine);
        BenchJson.Obj commits = new BenchJson.Obj();
        String jc = System.getenv("JAVA_QUESTDB_CLIENT_COMMIT");
        if (jc != null) commits.put("java_questdb_client", jc); else commits.putNull("java_questdb_client");
        report.put("commits", commits);
        // Reaching this point means every warmup and iteration pass (decode-only
        // and materialize) already verified seen == rows above; a mismatch would
        // have returned 2 before any report was built. Mirrors qwp_egress_c.c's
        // main(), which likewise hardcodes {expected: rows, actual: rows, ok:
        // true} here rather than re-deriving it.
        BenchJson.Obj rcc = new BenchJson.Obj();
        rcc.put("expected", rows);
        rcc.put("actual", rows);
        rcc.put("ok", true);
        rcc.put("inflated", false);
        report.put("row_count_check", rcc);
        BenchJson.Obj headline = new BenchJson.Obj();
        headline.put("decode_floor_s", floorMedian);
        headline.put("materialize_s", e2eMedian);
        if (e2eMedian != 0.0) headline.put("materialize_rows_per_s", rows / e2eMedian);
        report.put("headline", headline);
        report.put("real_conf", rconf);
        report.put("http_base", base);
        BenchJson.Obj paths = new BenchJson.Obj();
        paths.put("decode-only", BenchJson.pathSummary(wallFloor, cpuFloor, gcFloor, iterations, rows, columns, 0, "floor", warmups > 0));
        paths.put("materialize", BenchJson.pathSummary(wallE2e, cpuE2e, gcE2e, iterations, rows, columns, 0, "e2e", warmups > 0));
        report.put("paths", paths);
        System.out.println(report.render());
        return 0;
    }

    /**
     * One read pass over a fresh {@link QwpQueryClient} connection. Mirrors
     * read_pass() in examples/qwp_egress_c.c: {@code materialize == false}
     * only counts rows (the decode floor); {@code materialize == true}
     * additionally touches every cell through the typed column accessors and
     * folds it into a checksum, by wire type -- long/timestamp kinds add the
     * raw value, double adds the raw value, symbol/varchar add the resolved
     * UTF-8 byte length. When non-null, {@code checksumOut[0]} is OVERWRITTEN
     * (not accumulated) with this pass's checksum, matching the C twin, whose
     * {@code *checksum_out = checksum} assignment means only the LAST pass's
     * checksum survives to the final stderr print.
     */
    private static long readPass(String rconf, String select, boolean materialize, double[] checksumOut) {
        QwpQueryClient client = QwpQueryClient.fromConfig(rconf);
        try {
            client.connect();
            long[] rowsSeen = {0};
            double[] checksum = {0.0};
            client.execute(select, new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                    int nrows = batch.getRowCount();
                    if (materialize) {
                        int ncols = batch.getColumnCount();
                        for (int c = 0; c < ncols; c++) {
                            ColumnView col = batch.column(c);
                            switch (col.getColumnWireType()) {
                                case QwpConstants.TYPE_LONG:
                                case QwpConstants.TYPE_TIMESTAMP:
                                case QwpConstants.TYPE_TIMESTAMP_NANOS:
                                    for (int r = 0; r < nrows; r++) {
                                        checksum[0] += (double) col.getLongValue(r);
                                    }
                                    break;
                                case QwpConstants.TYPE_DOUBLE:
                                    for (int r = 0; r < nrows; r++) {
                                        checksum[0] += col.getDoubleValue(r);
                                    }
                                    break;
                                case QwpConstants.TYPE_SYMBOL:
                                case QwpConstants.TYPE_VARCHAR:
                                    for (int r = 0; r < nrows; r++) {
                                        DirectUtf8Sequence s = col.getStrA(r);
                                        checksum[0] += s == null ? 0.0 : (double) s.size();
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                    rowsSeen[0] += nrows;
                }

                @Override
                public void onEnd(long totalRows) {
                }

                @Override
                public void onError(byte status, String message) {
                    throw new RuntimeException(String.format(
                            "[qwp_egress_java] query failed: status=0x%02X %s", status & 0xFF, message));
                }
            });
            if (checksumOut != null) checksumOut[0] = checksum[0];
            return rowsSeen[0];
        } finally {
            client.close();
        }
    }

    private EgressBench() {}
}
