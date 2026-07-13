package io.questdb.qwpbench;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * DDL + count()-polling over QuestDB's HTTP /exec endpoint, transcribed from
 * bench_http_c.c in c-questdb-client — that file is normative; if this ever
 * drifts from it, the C source wins.
 */
public final class BenchHttp {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final long POLL_BUDGET_NANOS = Duration.ofSeconds(300).toNanos();
    private static final long POLL_INTERVAL_MS = 500;
    private static final String DATASET_KEY = "\"dataset\":[[";

    private final String base;
    private final HttpClient client;

    public BenchHttp(String base) {
        this.base = base;
        this.client = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
    }

    /**
     * GET {base}/exec?query=<urlencoded sql>. A non-2xx response or a transport
     * IOException is surfaced as a RuntimeException (body included for non-2xx,
     * mirroring http_exec_sql()'s stderr diagnostic).
     */
    public void execSql(String sql) {
        get(sql);
    }

    /**
     * Polls {@code select count() from <table>} every 500 ms until the observed
     * count reaches {@code expected} or the 300 s budget elapses. Mirrors
     * wait_for_count() in bench_http_c.c, including its guarded-assign fix: a
     * poll whose body doesn't parse (missing/malformed "dataset":[[, or any
     * transport/HTTP error) must never overwrite the last successfully parsed
     * count. Returns the last observed count (-1 if never parsed).
     */
    public long waitForCount(String table, long expected) {
        String sql = "select count() from " + table;
        long deadline = System.nanoTime() + POLL_BUDGET_NANOS;
        long n = -1;
        while (System.nanoTime() < deadline) {
            try {
                String body = get(sql);
                long c = parseCount(body);
                if (c >= 0) {
                    n = c;
                }
            } catch (RuntimeException e) {
                // transport/HTTP error for this poll: keep the last good count, try again
            }
            if (n >= expected) {
                return n;
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return n;
            }
        }
        return n;
    }

    /**
     * Parses the count from the LAST {@code "dataset":[[<digits>} occurrence in
     * a /exec JSON body (same find-based parse as bench_http_c.c's parse_count).
     * Returns -1 if the key is absent or isn't followed by at least one digit.
     */
    static long parseCount(String body) {
        if (body == null) {
            return -1;
        }
        int p = body.lastIndexOf(DATASET_KEY);
        if (p < 0) {
            return -1;
        }
        int start = p + DATASET_KEY.length();
        int i = start;
        while (i < body.length() && body.charAt(i) >= '0' && body.charAt(i) <= '9') {
            i++;
        }
        if (i == start) {
            return -1;
        }
        try {
            return Long.parseLong(body.substring(start, i));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private String get(String sql) {
        String encoded = URLEncoder.encode(sql, StandardCharsets.UTF_8);
        URI uri = URI.create(base + "/exec?query=" + encoded);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new RuntimeException("HTTP request failed for: " + sql, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("HTTP request interrupted for: " + sql, e);
        }
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException("HTTP " + status + " for: " + sql + "\n" + response.body());
        }
        return response.body();
    }
}
