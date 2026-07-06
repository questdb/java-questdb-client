package com.example.sender;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * The breadth of column setters and the designated-timestamp variants.
 * <p>
 * The row-building API is identical across transports; this shows the typed
 * setters over the pooled ws facade. To store SQL NULL for a column, simply
 * omit its setter before calling {@code at(...)} / {@code atNow()} -- the
 * column set for the batch is the union of all columns seen across rows.
 * <p>
 * Tables and columns are created automatically if they don't exist. Create
 * DECIMAL columns ahead of time as {@code DECIMAL(precision, scale)} so values
 * ingest with the expected precision.
 */
public class WsColumnTypesExample {

    public static void main(String[] args) {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try (Sender sender = db.borrowSender()) {

                // One row exercising the typed setters.
                sender.table("all_types")
                        .symbol("sym", "ETH-USD")                    // SYMBOL
                        .stringColumn("name", "ether")               // VARCHAR / STRING
                        .boolColumn("active", true)                  // BOOLEAN
                        .byteColumn("b", (byte) 7)                   // BYTE
                        .shortColumn("s", (short) 12000)             // SHORT
                        .intColumn("i", 42)                          // INT
                        .longColumn("l", 9_000_000_000L)             // LONG
                        .floatColumn("f", 1.5f)                      // FLOAT
                        .doubleColumn("d", 2615.54)                  // DOUBLE
                        .charColumn("c", 'Q')                        // CHAR
                        .timestampColumn("event_time", Instant.now())// TIMESTAMP (a value column)
                        .uuidColumn("id", 0x1234L, 0x5678L)          // UUID (lo, hi)
                        .long256Column("hash", 1L, 2L, 3L, 4L)       // LONG256 (least-significant word first)
                        .decimalColumn("amount", "1234.5678")        // DECIMAL(precision, scale)
                        .ipv4Column("ip", "192.168.0.1")             // IPv4 (dotted-quad)
                        .geoHashColumn("location", "u33d8")          // GEOHASH (base32; precision = len * 5 bits)
                        .at(Instant.now());                          // designated timestamp

                // Designated-timestamp variants:
                sender.table("all_types")
                        .doubleColumn("d", 1.0)
                        .at(System.currentTimeMillis() * 1000, ChronoUnit.MICROS); // explicit microseconds

                // ChronoUnit.NANOS creates a timestamp_ns column instead of a
                // microsecond TIMESTAMP.
                sender.table("ticks")
                        .doubleColumn("d", 1.0)
                        .at(System.currentTimeMillis() * 1_000_000L, ChronoUnit.NANOS);

                // Server-assigned wall-clock time.
                sender.table("all_types")
                        .doubleColumn("d", 1.0)
                        .atNow();

                sender.flush();
            }
        }
    }
}
