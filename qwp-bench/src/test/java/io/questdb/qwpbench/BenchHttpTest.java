package io.questdb.qwpbench;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BenchHttpTest {
    @Test void parsesLastDataset() {
        assertEquals(42L, BenchHttp.parseCount("{\"dataset\":[[42]]}"));
        assertEquals(7L, BenchHttp.parseCount("{\"x\":1,\"dataset\":[[3]],\"dataset\":[[7]]}"));
        assertEquals(-1L, BenchHttp.parseCount("{\"error\":\"nope\"}"));
        assertEquals(-1L, BenchHttp.parseCount(""));
    }
}
