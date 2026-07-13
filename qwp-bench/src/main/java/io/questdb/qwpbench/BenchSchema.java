package io.questdb.qwpbench;

/**
 * Deterministic schema + data-generation contract for the QWP bench, shared with the C
 * and Rust bench implementations. Transcribed from examples/bench_schema_c.c /
 * bench_schema_c.h in c-questdb-client — that file is normative; if this ever drifts
 * from it, the C source wins.
 */
public final class BenchSchema {
    public static final long TS_BASE_NANOS = 1704067200000000000L;
    public static final long TS_STEP_NANOS = 1000L;
    public static final int N_WIDE_DOUBLES = 5;
    public static final int N_WIDE_SYMS = 5;

    public enum Kind {
        S1_NARROW, S2_WIDE;

        public static Kind parse(String s) {
            if ("s1-narrow".equals(s)) return S1_NARROW;
            if ("s2-wide".equals(s)) return S2_WIDE;
            throw new IllegalArgumentException("unknown SCHEMA '" + s + "' (s1-narrow|s2-wide)");
        }

        public String tableName() {
            return this == S1_NARROW ? "bench_s1_narrow" : "bench_s2_wide";
        }

        public int columns() {
            return this == S1_NARROW ? 5 : 5 + N_WIDE_DOUBLES + N_WIDE_SYMS;
        }

        public String createSql() {
            // Byte-for-byte from schema_create_sql() in examples/bench_schema_c.c.
            if (this == S1_NARROW) {
                return "CREATE TABLE bench_s1_narrow (" +
                        "id LONG, price DOUBLE, sym SYMBOL, note VARCHAR, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts)";
            }
            return "CREATE TABLE bench_s2_wide (" +
                    "id LONG, price DOUBLE, sym SYMBOL, note VARCHAR, " +
                    "d1 DOUBLE, d2 DOUBLE, d3 DOUBLE, d4 DOUBLE, d5 DOUBLE, " +
                    "s1 SYMBOL CAPACITY 200000, s2 SYMBOL CAPACITY 200000, " +
                    "s3 SYMBOL CAPACITY 200000, s4 SYMBOL CAPACITY 200000, " +
                    "s5 SYMBOL CAPACITY 200000, ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts)";
        }

        public String selectSql() {
            // Byte-for-byte from schema_select_sql() in examples/bench_schema_c.c.
            return this == S1_NARROW
                    ? "SELECT ts, id, price, sym, note FROM bench_s1_narrow"
                    : "SELECT ts, id, price, sym, note, d1, d2, d3, d4, d5, s1, s2, s3, s4, s5 FROM bench_s2_wide";
        }
    }

    public static long id(long i) {
        return i;
    }

    public static double price(long i) {
        return i * 0.25;
    }

    public static String sym(long i, int card) {
        return String.format("sym_%04d", i % card);
    }

    public static int noteTemplateCount(long rows) {
        if (rows < 1) return 1;
        return rows > 1024 ? 1024 : (int) rows;
    }

    public static String noteTemplate(long idx, int varcharLen) {
        // out[j] = pat[j % strlen(pat)] — character cycling, per note_template() in C.
        String pat = String.format("note_%03d_", idx);
        char[] out = new char[varcharLen];
        for (int j = 0; j < varcharLen; j++) out[j] = pat.charAt(j % pat.length());
        return new String(out);
    }

    public static double wideDouble(long i, int k) {
        return i * (0.5 + k);
    }

    public static String hiSym(int col, long i, int card) {
        return String.format("s%d_%06d", col - 1, i % card);
    }

    public static long tsNanos(long i) {
        return TS_BASE_NANOS + i * TS_STEP_NANOS;
    }

    private BenchSchema() {}
}
