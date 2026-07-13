package io.questdb.qwpbench;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BenchJsonTest {
    @Test void f64TrimsLikeC() {
        assertEquals("0.333333333333", BenchJson.f64(1.0 / 3.0));
        assertEquals("2.5", BenchJson.f64(2.5));
        assertEquals("1", BenchJson.f64(1.0));
        assertEquals("0.516397779494", BenchJson.f64(0.5163977794943222));
    }
    @Test void sortedKeysAndEscapes() {
        BenchJson.Obj o = new BenchJson.Obj();
        o.put("b", 1L);
        o.put("a", "x");
        o.put("d", 1.0 / 3.0);
        o.put("c", 2.5);
        assertEquals("{\"a\":\"x\",\"b\":1,\"c\":2.5,\"d\":0.333333333333}", o.render());
        BenchJson.Obj e = new BenchJson.Obj();
        e.put("s", "a\"b\\c\nd\re\tfg");
        assertEquals("{\"s\":\"a\\\"b\\\\c\\nd\\re\\tf\\u0001g\"}", e.render());
    }
    @Test void statsMatchC() {
        // 5 samples in ns; medians/stdev per bench_json_c.c: midpoint median,
        // Bessel stdev, p95 index round((n-1)*0.95).
        long[] wall = {100_000_000L, 200_000_000L, 300_000_000L, 400_000_000L, 500_000_000L};
        long[] cpu  = {100_000_000L, 100_000_000L, 100_000_000L, 100_000_000L, 100_000_000L};
        long[] gc   = {0, 0, 0, 0, 0};
        BenchJson.Obj s = BenchJson.pathSummary(wall, cpu, gc, 5, 3_000_000L, 15, 0, "floor", true);
        String j = s.render();
        assertTrue(j.contains("\"median_s\":0.3"));
        assertTrue(j.contains("\"p95_s\":0.5"));
        assertTrue(j.contains("\"rows_per_s_median\":10000000"));
        assertTrue(j.contains("\"iterations\":5"));
        assertTrue(j.contains("\"warm\":true"));
        assertTrue(j.contains("\"gc_ms_median\":0"));
    }
}
