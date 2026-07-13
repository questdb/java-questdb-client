package io.questdb.qwpbench;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Sorted-key JSON reporter with C-contract formatting, transcribed from
 * examples/bench_json_c.c in c-questdb-client — that file is normative; if this
 * ever drifts from it, the C source wins. One deliberate addition beyond the C
 * contract: {@code pathSummary} also emits {@code gc_ms_median}, the median of
 * per-pass GC-time deltas (there is no C/native GC to measure on that side).
 */
public final class BenchJson {

    private BenchJson() {}

    /** Sorted-key JSON object builder mirroring bench_json's {@code json_obj}. */
    public static final class Obj {
        private final TreeMap<String, String> entries = new TreeMap<>();

        public void put(String k, String v) {
            entries.put(k, jsonEscape(v));
        }

        public void put(String k, long v) {
            entries.put(k, Long.toString(v));
        }

        public void put(String k, double v) {
            entries.put(k, f64(v));
        }

        public void put(String k, boolean v) {
            entries.put(k, v ? "true" : "false");
        }

        public void putNull(String k) {
            entries.put(k, "null");
        }

        public void put(String k, Obj child) {
            entries.put(k, child.render());
        }

        public String render() {
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            boolean first = true;
            for (Map.Entry<String, String> e : entries.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append('"').append(e.getKey()).append("\":").append(e.getValue());
            }
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     * {@code %.12f}, trailing zeros trimmed, then a trailing '.' trimmed.
     * Mirrors json_f64() in bench_json_c.c, including its "-0" -> "-0" quirk
     * (only an empty string or a lone "-" collapses to "0").
     *
     * <p>Rendering goes through BigDecimal on the exact binary value with
     * round-half-even rather than {@code String.format("%.12f")}: the latter is
     * not correctly rounded in rare cases (e.g. 4287.4130727087095 formats as
     * ...710 instead of C printf's correct ...709), while BigDecimal HALF_EVEN
     * matches printf's round-to-nearest semantics on the exact decimal
     * expansion. BigDecimal has no negative zero, so the sign of negative
     * values that round to zero is restored explicitly (C printf keeps it).
     */
    public static String f64(double v) {
        if (!Double.isFinite(v)) {
            return "null";
        }
        java.math.BigDecimal bd = new java.math.BigDecimal(v)
                .setScale(12, java.math.RoundingMode.HALF_EVEN);
        String s = bd.toPlainString();
        if (bd.signum() == 0 && Math.copySign(1.0, v) < 0) {
            s = "-" + s;
        }
        int dot = s.indexOf('.');
        if (dot >= 0) {
            int end = s.length() - 1;
            while (end > dot && s.charAt(end) == '0') {
                end--;
            }
            if (end == dot) {
                end = dot - 1;
            }
            s = s.substring(0, end + 1);
        }
        if (s.isEmpty() || s.equals("-")) {
            s = "0";
        }
        return s;
    }

    /**
     * Escapes a string as a JSON string literal (including the surrounding quotes),
     * mirroring json_escape() in bench_json_c.c: quote, backslash, newline, carriage
     * return, and tab get short escapes; any other codepoint below 0x20 is escaped
     * as a lowercase-hex unicode escape (four hex digits).
     */
    private static String jsonEscape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    /** Median (seconds) of a nanosecond sample array; mirrors median_s_of(). */
    public static double medianS(long[] ns, int n) {
        if (n == 0) {
            return 0.0;
        }
        double[] s = new double[n];
        for (int i = 0; i < n; i++) {
            s[i] = ns[i] / 1e9;
        }
        Arrays.sort(s);
        return medianOfSorted(s, n);
    }

    /** Median of a plain (already-scaled) sample array, no unit conversion. */
    private static double medianOfMs(long[] ms, int n) {
        if (n == 0) {
            return 0.0;
        }
        double[] s = new double[n];
        for (int i = 0; i < n; i++) {
            s[i] = ms[i];
        }
        Arrays.sort(s);
        return medianOfSorted(s, n);
    }

    private static double medianOfSorted(double[] sorted, int n) {
        if (n == 0) {
            return 0.0;
        }
        return (n % 2 == 1)
                ? sorted[n / 2]
                : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
    }

    private static double percentile(double[] sorted, int n, double p) {
        if (n == 0) {
            return 0.0;
        }
        int idx = (int) Math.round((n - 1) * p);
        if (idx > n - 1) {
            idx = n - 1;
        }
        if (idx < 0) {
            idx = 0;
        }
        return sorted[idx];
    }

    /**
     * Populates {@code obj} with the stats contract over {@code samplesNs}
     * (iterations, median_s, mean_s, min_s, max_s, p95_s, stdev_s, cov,
     * rows_per_s_median, cells_per_s_median, mib_per_s). Mirrors summarize()
     * in bench_json_c.c. {@code wireBytesForRate} is null unless the caller
     * wants a mib_per_s figure computed for this block (the C code passes a
     * NULL pointer for floor phases, a valid pointer — possibly to a 0 value
     * — for e2e phases).
     */
    private static void summarize(Obj obj, long[] samplesNs, int n, long rows, int columns, Long wireBytesForRate) {
        double[] s = new double[n];
        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            s[i] = samplesNs[i] / 1e9;
            sum += s[i];
        }
        double mean = n > 0 ? sum / n : 0.0;
        double[] sorted = s.clone();
        Arrays.sort(sorted);
        double median = medianOfSorted(sorted, n);
        double stdev = 0.0;
        if (n > 1) {
            double var = 0.0;
            for (int i = 0; i < n; i++) {
                double d = s[i] - mean;
                var += d * d;
            }
            stdev = Math.sqrt(var / (n - 1));
        }
        obj.put("iterations", (long) n);
        obj.put("median_s", median);
        obj.put("mean_s", mean);
        obj.put("min_s", n > 0 ? sorted[0] : 0.0);
        obj.put("max_s", n > 0 ? sorted[n - 1] : 0.0);
        obj.put("p95_s", percentile(sorted, n, 0.95));
        obj.put("stdev_s", stdev);
        obj.put("cov", mean != 0.0 ? stdev / mean : 0.0);
        if (median != 0.0) {
            obj.put("rows_per_s_median", (double) rows / median);
            obj.put("cells_per_s_median", (double) (rows * (long) columns) / median);
        } else {
            obj.putNull("rows_per_s_median");
            obj.putNull("cells_per_s_median");
        }
        if (wireBytesForRate != null && median != 0.0) {
            obj.put("mib_per_s", (wireBytesForRate / (1024.0 * 1024.0)) / median);
        } else {
            obj.putNull("mib_per_s");
        }
    }

    /**
     * Composes a full path summary: wall-clock stats, gc_ms_median (the one field
     * beyond the C contract), a nested process_cpu stats block over cpuNs, plus
     * phase/warm/wire_bytes. Mirrors path_summary() in bench_json_c.c — in
     * particular the mib_per_s branching: floor phases never get a rate (the C
     * code passes a NULL wire_bytes pointer to summarize()), e2e phases always do
     * (a valid pointer, so a 0 wire_bytes value still renders 0, not null,
     * whenever median != 0).
     */
    public static Obj pathSummary(long[] wallNs, long[] cpuNs, long[] gcMs, int n, long rows, int columns,
                                   long wireBytes, String phase, boolean warm) {
        boolean e2e = "e2e".equals(phase);
        Long rateWireBytes = e2e ? Long.valueOf(wireBytes) : null;

        Obj o = new Obj();
        summarize(o, wallNs, n, rows, columns, rateWireBytes);
        o.put("gc_ms_median", medianOfMs(gcMs, n));

        Obj cpu = new Obj();
        summarize(cpu, cpuNs, n, rows, columns, rateWireBytes);
        o.put("process_cpu", cpu);

        o.put("phase", phase);
        o.put("warm", warm);
        o.put("wire_bytes", wireBytes);
        return o;
    }

    /** {@code System.nanoTime()}; mirrors now_ns(). */
    public static long nowNs() {
        return System.nanoTime();
    }

    /** Process CPU time in ns via com.sun.management; mirrors process_cpu_ns(). */
    public static long processCpuNs() {
        java.lang.management.OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            long v = ((com.sun.management.OperatingSystemMXBean) osBean).getProcessCpuTime();
            return v < 0 ? 0 : v;
        }
        return 0;
    }

    /** Sum of getCollectionTime() over all garbage collector beans, in ms. */
    public static long gcMs() {
        long total = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long t = b.getCollectionTime();
            if (t > 0) {
                total += t;
            }
        }
        return total;
    }
}
