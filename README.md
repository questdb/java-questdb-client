<div align="center">
  <a href="https://questdb.com/" target="blank"><img alt="QuestDB Logo" src="https://questdb.com/img/questdb-logo-themed.svg" width="305px"/></a>
</div>
<p>&nbsp;</p>

<div align="center">

[![Maven Central](https://img.shields.io/maven-central/v/org.questdb/questdb-client.svg)](https://central.sonatype.com/artifact/org.questdb/questdb-client)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

</div>

# QuestDB Client Library for Java

This is the official Java client library for [QuestDB](https://questdb.com/), a high-performance time-series database.

The recommended entry point is the **`QuestDB` facade**: a single, thread-safe handle that pools connections for both
**ingest** (writing rows) and **queries** (reading results) over QWP, the QuestDB WebSocket protocol. Construct one
`QuestDB` per deployment, share it across threads, and close it at shutdown. Borrow a `Sender` to write, borrow a
`Query` to read, and the facade manages the underlying connections, pooling, reconnection, and store-and-forward
buffering for you.

```java
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;

try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
    try (Sender sender = db.borrowSender()) {
        sender.table("trades")
                .symbol("symbol", "ETH-USD")
                .symbol("side", "sell")
                .doubleColumn("price", 2615.54)
                .doubleColumn("amount", 0.00044)
                .atNow();
        // close() flushes pending rows and returns the Sender to the pool.
    }
}
```

> **Use the `QuestDB` facade.** Application code should obtain senders and query handles from the facade
> (`db.borrowSender()` / `db.borrowQuery()`), not by constructing them directly. Directly instantiating a `Sender`
> (`Sender.fromConfig(...)`, `Sender.builder(...)`), a `LineUdpSender`, or a `QwpQueryClient`/`Query` bypasses the
> pooling, shared lifecycle, reconnection, and store-and-forward guarantees the facade provides — and every borrowed
> handle is single-owner and returns to the pool on `close()`. Reach for the low-level `Sender`/`QwpQueryClient` APIs
> only when you are integrating them into your own pooling layer.

## Quick Start

### Add Dependency

**Maven:**

```xml
<dependency>
    <groupId>org.questdb</groupId>
    <artifactId>questdb-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'org.questdb:questdb-client:1.0.0'
```

Replace `1.0.0` with the latest version from [Maven Central](https://central.sonatype.com/artifact/org.questdb/questdb-client).

### Start QuestDB

```bash
docker run -p 9000:9000 questdb/questdb
```

### Connect

A QuestDB cluster is one logical target reached over QWP for both ingest and queries, so the facade takes **one**
`ws`/`wss` configuration string. List every node in a single `addr` server list and both pools connect across it.

```java
import io.questdb.client.QuestDB;

// Single node
try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
    // ... use db ...
}

// Whole cluster in one config string
try (QuestDB db = QuestDB.connect("ws::addr=node1:9000,node2:9000,node3:9000;")) {
    // ... use db ...
}
```

## Examples

All examples assume an open `QuestDB db` handle (see [Connect](#connect)). Create the handle once and share it; the
snippets below borrow from its pools.

### Ingest Data

Borrow a `Sender`, write rows, and close it. `close()` flushes pending rows and returns the sender to the pool — the
underlying connection is only torn down when the `QuestDB` handle itself is closed.

```java
import io.questdb.client.Sender;

try (Sender sender = db.borrowSender()) {
    sender.table("trades")
            .symbol("symbol", "BTC-USD")
            .doubleColumn("price", 42_500.50)
            .longColumn("size", 100)
            .atNow();
    // No explicit flush() needed: close() flushes for you.
}
```

### Sending Batches of Rows

The sender buffers rows and sends them in batches. For high throughput, write many rows on one borrowed sender and
flush per batch rather than per row: each flush has a fixed cost, so batching raises throughput. `close()` flushes the
final partial batch.

```java
try (Sender sender = db.borrowSender()) {
    for (int i = 0; i < trades.size(); i++) {
        Trade t = trades.get(i);
        sender.table("trades")
                .symbol("symbol", t.symbol)
                .doubleColumn("price", t.price)
                .longColumn("size", t.size)
                .atNow();
        // Flush every 10k rows to bound buffer memory and latency.
        if ((i + 1) % 10_000 == 0) {
            sender.flush();
        }
    }
    // Remaining rows are flushed by close().
}
```

You can also let the client flush batches for you with the `auto_flush_rows` / `auto_flush_interval` config keys, e.g.
`ws::addr=localhost:9000;auto_flush_rows=10000;auto_flush_interval=1000;`.

**Confirm a batch is durably received.** Over QWP each flush returns a frame sequence number (FSN); `awaitAckedFsn`
blocks until the server has acknowledged it. Rows are safe in the store-and-forward log even if the ack has not landed
yet — they replay on reconnect.

```java
try (Sender sender = db.borrowSender()) {
    for (Trade t : batch) {
        sender.table("trades").symbol("symbol", t.symbol).doubleColumn("price", t.price).atNow();
    }
    long fsn = sender.flushAndGetSequence();          // publish the batch, get its sequence number
    if (sender.awaitAckedFsn(fsn, 30_000)) {          // block up to 30s for the server ack
        // batch acknowledged by the server
    } else {
        // not yet acked within the timeout; it stays buffered and replays on reconnect
    }
}
```

### Query Data

Borrow a `Query`, set the SQL and a result handler, submit, and await. The handler receives results a batch at a time;
`submit()` returns a `Completion` you can `await()` synchronously, time out on, or `cancel()`.

```java
import io.questdb.client.Query;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

QwpColumnBatchHandler handler = new QwpColumnBatchHandler() {
    @Override
    public void onBatch(QwpColumnBatch batch) {
        for (int r = 0; r < batch.getRowCount(); r++) {
            System.out.println(batch.getDoubleValue(0, r));
        }
    }

    @Override
    public void onEnd(long totalRows) {
        System.out.println("done: " + totalRows + " rows");
    }

    @Override
    public void onError(byte status, String message) {
        System.err.println("query error: status=" + status + ", message=" + message);
    }
};

try (Query q = db.borrowQuery()) {
    q.sql("SELECT price FROM trades WHERE symbol = 'BTC-USD' LIMIT 10")
            .handler(handler)
            .submit()
            .await();
}
```

### Query with Bind Parameters

Reuse the same SQL text with different values via bind parameters. The same SQL text reuses the server's
compiled-factory cache; interpolating values into the SQL string defeats that cache.

```java
import java.util.concurrent.TimeUnit;

try (Query q = db.borrowQuery()) {
    q.sql("SELECT price FROM trades WHERE symbol = $1 LIMIT $2")
            .binds(binds -> {
                binds.setVarchar(0, "BTC-USD");
                binds.setLong(1, 10L);
            })
            .handler(handler)
            .submit()
            .await();
}
```

### Cancel or Time Out a Query

`submit()` returns a `Completion`. `await(timeout, unit)` returns `false` if the query is still in flight; `cancel()`
stops it.

```java
import io.questdb.client.Completion;

try (Query q = db.borrowQuery()) {
    Completion c = q.sql("SELECT * FROM big_table ORDER BY ts")
            .handler(handler)
            .submit();
    if (!c.await(5, TimeUnit.SECONDS)) {
        c.cancel();
        c.await(); // wait for the terminal (cancelled) state
    }
}
```

> A borrowed `Query` handle is **single-flight**: submit one query at a time on it, then submit again or close it. To
> run queries concurrently, borrow one handle per concurrent query (up to `query_pool_max`).

### Custom Pool Sizing and Timeouts

Use the builder to override the defaults. `senderPoolSize`/`queryPoolSize` set a fixed pool size; `*Min`/`*Max` allow
an elastic pool that grows under load and is reaped back when idle.

```java
try (QuestDB db = QuestDB.builder()
        .fromConfig("ws::addr=node1:9000,node2:9000;")
        .senderPoolSize(8)
        .queryPoolSize(4)
        .acquireTimeoutMillis(10_000)
        .build()) {
    // ... use db ...
}
```

### Multiple Servers and Failover

List every cluster node in one `addr` server list; the single string configures both the ingest and query pools across
all of them. On the query side, `target` selects the node role to route to (`any`, `primary`, or `replica`) and
`failover=on` enables failover across the list. The ingest side reconnects across the same node list on its own — a
store-and-forward sender keeps buffering rows through a failover window and never drops them.

```java
try (QuestDB db = QuestDB.connect(
        "ws::addr=node1:9000,node2:9000,node3:9000;target=primary;failover=on;")) {
    try (Sender s = db.borrowSender()) {
        s.table("trades").symbol("symbol", "BTC-USD").doubleColumn("price", 42_500.50).atNow();
    }
    try (Query q = db.borrowQuery()) {
        q.sql("SELECT count() FROM trades").handler(handler).submit().await();
    }
}
```

### Zone-Aware Query Routing

In a multi-zone deployment, `zone` tells the query pool to prefer endpoints in the same zone, cutting cross-zone read
latency. It is a query-routing hint (opaque, case-insensitive), matched against each server's advertised zone; it
applies only to `target=any` / `target=replica` (a `primary` is followed across zones). Cross-zone hosts remain
fallbacks, so a same-zone outage still fails over. `client_id` is an opaque identifier surfaced server-side for
observability.

```java
try (QuestDB db = QuestDB.connect(
        "ws::addr=node1:9000,node2:9000,node3:9000;"
                + "target=replica;zone=eu-west-1a;failover=on;client_id=dashboard/2.0;")) {
    try (Query q = db.borrowQuery()) {
        q.sql("SELECT price FROM trades WHERE symbol = 'BTC-USD' LIMIT 10")
                .handler(handler)
                .submit()
                .await();
    }
}
```

### Observe Ingest Errors and Connection Events

Register callbacks on the builder to observe asynchronous ingest errors and connection transitions across the whole
sender pool. Callbacks run on the senders' I/O threads, so they must be thread-safe and must not block.

```java
try (QuestDB db = QuestDB.builder()
        .fromConfig("ws::addr=localhost:9000;")
        .errorHandler(error ->
                System.err.println("ingest error: " + error.getCategory() + " " + error.getServerMessage()))
        .connectionListener(event ->
                System.out.println("connection event: " + event.getKind()))
        .build()) {
    // ... use db ...
}
```

### Tolerant Startup When the Server May Be Down

Set `lazy_connect=true` to start the handle even when the server is down. The ingest side buffers writes
(store-and-forward) until the wire is up, and the read pool connects lazily on the first `borrowQuery()` once the
server is available — reads stay enabled, they are just deferred.

```java
try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;lazy_connect=true;")) {
    try (Sender s = db.borrowSender()) {
        s.table("trades").symbol("symbol", "ETH-USD").doubleColumn("price", 2615.54).atNow();
        // Buffers while the server is down; flushes once it comes up.
    }
    // Later, once the server is up, reads connect on first borrow:
    try (Query q = db.borrowQuery()) {
        q.sql("SELECT 1").handler(handler).submit().await();
    }
}
```

### Bound the Connect Timeout

`connect_timeout` (milliseconds) bounds the TCP connect and TLS handshake so a black-holed or firewalled host fails
fast instead of waiting out the OS-level connect timeout.

```java
try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;connect_timeout=5000;")) {
    // ... use db ...
}
```

### Authentication and TLS

Use the `wss` schema for TLS. Auth keys apply to both the ingest and query WebSocket upgrades.

**Bearer token (`wss`):**

```java
try (QuestDB db = QuestDB.connect("wss::addr=db.questdb.cloud:9000;token=YOUR_TOKEN_HERE;")) {
    // ... use db ...
}
```

**Username / password:**

```java
try (QuestDB db = QuestDB.connect("wss::addr=localhost:9000;username=admin;password=quest;")) {
    // ... use db ...
}
```

**Disable certificate validation (not for production):**

```java
try (QuestDB db = QuestDB.connect("wss::addr=localhost:9000;tls_verify=unsafe_off;")) {
    // ... use db ...
}
```

### Explicit Timestamps

```java
import java.time.Instant;
import java.time.temporal.ChronoUnit;

try (Sender sender = db.borrowSender()) {
    // Using an Instant
    sender.table("trades")
            .symbol("symbol", "ETH-USD")
            .doubleColumn("price", 2615.54)
            .at(Instant.now());

    // Using a long value with a time unit
    sender.table("trades")
            .symbol("symbol", "BTC-USD")
            .doubleColumn("price", 39_269.98)
            .at(1_000_000_000L, ChronoUnit.NANOS);
}
```

## Configuration Reference

The configuration string format is:

```
schema::key1=value1;key2=value2;
```

**Schemas (facade):** `ws`, `wss` (TLS). A single string configures the whole cluster for both ingest and queries.

### Common keys

| Key                  | Default      | Description                                                         |
| -------------------- | ------------ | ------------------------------------------------------------------- |
| `addr`               | _(required)_ | Server address(es) as `host:port`, comma-separated for a cluster    |
| `username` / `user`  |              | Basic-auth username                                                 |
| `password` / `pass`  |              | Basic-auth password                                                 |
| `token`              |              | Bearer token (sent as an `Authorization` header on the WS upgrade)  |
| `tls_verify`         | `on`         | TLS certificate validation (`on` or `unsafe_off`)                   |
| `tls_roots`          |              | Path to a custom truststore                                         |
| `tls_roots_password` |              | Truststore password                                                 |
| `connect_timeout`    | _(OS)_       | TCP connect + TLS handshake timeout, in milliseconds                |
| `auth_timeout_ms`    | `15000`      | Authentication/upgrade request timeout, in milliseconds             |

### Pool keys (facade only)

| Key                      | Default | Description                                                                    |
| ------------------------ | ------- | ------------------------------------------------------------------------------ |
| `lazy_connect`           | `off`   | Tolerant startup: async ingest + lazy reads so `build()` succeeds server-down  |
| `sender_pool_min`        | `1`     | Minimum ingest connections kept warm (`0` lets the pool drain when idle)       |
| `sender_pool_max`        | `4`     | Maximum ingest connections                                                     |
| `query_pool_min`         | `1`     | Minimum query connections kept warm (`0` under `lazy_connect`)                 |
| `query_pool_max`         | `4`     | Maximum query connections                                                      |
| `acquire_timeout_ms`     | `5000`  | How long `borrowSender()`/`borrowQuery()` waits for a free slot                |
| `idle_timeout_ms`        | `60000` | How long a pooled connection may stay idle before it is reaped                 |
| `max_lifetime_ms`        | `1800000` | Maximum lifetime of a pooled connection before it is recycled                |

### Query routing keys

Applied by the query pool to select and fail over between the nodes in the `addr` list.

| Key         | Default | Description                                                                              |
| ----------- | ------- | ---------------------------------------------------------------------------------------- |
| `target`    | `any`   | Node role to route reads to: `any`, `primary`, or `replica`                              |
| `failover`  | `off`   | Enable query-side failover across the `addr` list                                        |
| `zone`      |         | Prefer same-zone endpoints for `target=any`/`replica` (opaque, case-insensitive)         |
| `client_id` |         | Opaque client identifier surfaced server-side for observability                          |

The ingest side also accepts store-and-forward and reconnection tuning keys (`auto_flush_*`, `initial_connect_retry`,
`reconnect_*`, `request_durable_ack`, `sf_*`, `max_frame_rejections`, `poison_min_escalation_window_millis`, …). See the
[QuestDB documentation](https://questdb.com/docs/) for the full reference.

## Requirements

- Java 8 or later (the artifact ships as Java 8 bytecode)
- Maven 3+ (for building from source)

## Building from Source

```bash
git clone https://github.com/questdb/java-questdb-client.git
cd java-questdb-client
mvn clean package -DskipTests
```

The native libraries are **built from source** as part of the build — they are no longer committed to the repository.
`mvn package` compiles `libquestdb` for the host platform and bundles it into the client jar, so a fresh checkout needs
the native toolchain (see [Building Native Libraries](#building-native-libraries)) before `mvn -pl core test` can load
it.

## Releasing

Maven Central publishing is owned by the manually triggered `Release to Maven Central` GitHub Actions workflow, run
from the Actions tab. Do not publish from a local machine and do not run `mvn deploy` in the normal release path.

The workflow builds each shipped platform's native library **from source**, runs the full test suite with those
freshly built binaries bundled, and validates the signed bundle with the Central Portal **before** it pushes a git tag
or publishes anything. The Central publish is the single irreversible step and runs last; the next-development version
bump lands as a follow-up pull request, so `main` keeps its PR-only protection.

The `publish` step is gated by the `maven-release` GitHub environment; configure it with required reviewers so the
workflow pauses for human approval before any credentials are used or anything is published.

The release tag push uses a dedicated Maven release GitHub App that must be allowed to bypass the org
`restrict-tag-pushing` ruleset; the built-in `GITHUB_TOKEN`/`github-actions[bot]` cannot be added for that bypass.

Full release procedure, one-time setup, and failure handling: [artifacts/release/README.md](artifacts/release/README.md).

### Building Native Libraries

The client includes native libraries (C/C++ and assembly) for performance-critical operations. These are **not
committed** to the repository: CI and the release pipeline build them from source, and the local build produces one for
your host platform. Rebuild manually only if you are changing the native code.

Shipped release platforms: `darwin-aarch64`, `linux-x86-64`, `linux-aarch64`, `windows-x86-64`. (`darwin-x86-64` / Intel
macOS is not a shipped release target; build it from source locally if you need it.)

#### Prerequisites

| Tool           | Version              | Notes                               |
| -------------- | -------------------- | ----------------------------------- |
| CMake          | 3.5+                 | Build system generator              |
| NASM           | 2.14+                | Netwide Assembler for assembly code |
| C/C++ Compiler | GCC, Clang, or MinGW | C++17 support required              |
| Make           | Any                  | Build tool                          |
| JDK            | 8+                   | For JNI headers                     |

#### macOS (ARM64 or x86-64)

```bash
# Install build tools
brew install cmake nasm

# Set deployment target
export MACOSX_DEPLOYMENT_TARGET=13.0

# Build native library
cd core
cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release --config Release
```

#### Linux x86-64

```bash
# Install build tools (Debian/Ubuntu)
sudo apt-get install cmake nasm build-essential

# Build native library
cd core
cmake -DCMAKE_BUILD_TYPE=Release -B cmake-build-release -S.
cmake --build cmake-build-release --config Release
```

#### Linux ARM64

```bash
# Install build tools (Debian/Ubuntu)
sudo apt-get install cmake nasm build-essential

# Build using ARM64 toolchain
cd core
cmake -DCMAKE_TOOLCHAIN_FILE=./src/main/c/toolchains/linux-arm64.cmake \
      -DCMAKE_BUILD_TYPE=Release -B cmake-build-release-arm64 -S.
cmake --build cmake-build-release-arm64 --config Release
```

#### Windows x86-64 (Cross-compilation from Linux)

```bash
# Install cross-compilation tools (Debian/Ubuntu)
sudo apt-get install cmake nasm gcc-mingw-w64 g++-mingw-w64

# Build using Windows toolchain
cd core
cmake -DCMAKE_TOOLCHAIN_FILE=./src/main/c/toolchains/windows-x86_64.cmake \
      -DCMAKE_CROSSCOMPILING=True -DCMAKE_BUILD_TYPE=Release \
      -B cmake-build-release-win64
cmake --build cmake-build-release-win64 --config Release
```

#### Native Library Output Location

The build writes the library where the client loads it first (the "dev CXX lib" path):

```
core/target/classes/io/questdb/client/bin-local/
├── libquestdb.dylib  # macOS
├── libquestdb.so     # Linux
└── libquestdb.dll    # Windows
```

## Community

- [QuestDB Documentation](https://questdb.com/docs/)
- [QuestDB Community Forum](https://community.questdb.io/)
- [QuestDB Slack](https://slack.questdb.io/)
- [GitHub Issues](https://github.com/questdb/java-questdb-client/issues)

## License

This project is licensed under the [Apache License 2.0](LICENSE.txt).
