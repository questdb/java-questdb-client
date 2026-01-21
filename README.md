<div align="center">
  <a href="https://questdb.com/" target="blank"><img alt="QuestDB Logo" src="https://questdb.com/img/questdb-logo-themed.svg" width="305px"/></a>
</div>
<p>&nbsp;</p>

<div align="center">

[![Maven Central](https://img.shields.io/maven-central/v/org.questdb/client.svg)](https://central.sonatype.com/artifact/org.questdb/client)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

</div>

# QuestDB Client Library for Java

This is the official Java client library for [QuestDB](https://questdb.com/), a high-performance time-series database.

The client uses the [InfluxDB Line Protocol](https://questdb.com/docs/reference/api/ilp/overview/) (ILP) to insert data into QuestDB over HTTP, TCP, or UDP.

| Transport | Description                                                                    |
| --------- | ------------------------------------------------------------------------------ |
| HTTP      | Recommended. Provides feedback on errors and supports authentication and TLS.  |
| TCP       | Legacy. No error feedback from server. Useful for compatibility.               |
| UDP       | Fire-and-forget. No error feedback or delivery guarantees. Supports multicast. |

## Quick Start

### Add Dependency

**Maven:**

```xml
<dependency>
    <groupId>org.questdb</groupId>
    <artifactId>client</artifactId>
    <version>1.0.0</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'org.questdb:client:1.0.0'
```

Replace `1.0.0` with the latest version from [Maven Central](https://central.sonatype.com/artifact/org.questdb/client).

### Start QuestDB

```bash
docker run -p 9000:9000 questdb/questdb
```

### Insert Data

```java
import io.questdb.client.Sender;

public class Main {
    public static void main(String[] args) {
        try (Sender sender = Sender.fromConfig("http::addr=localhost:9000;")) {
            sender.table("trades")
                    .symbol("symbol", "ETH-USD")
                    .symbol("side", "sell")
                    .doubleColumn("price", 2615.54)
                    .doubleColumn("amount", 0.00044)
                    .atNow();
        }
    }
}
```

## More Examples

### Using the Builder API

```java
import io.questdb.client.Sender;

try (Sender sender = Sender.builder(Sender.Transport.HTTP)
        .address("localhost:9000")
        .autoFlushRows(1000)
        .autoFlushIntervalMillis(5000)
        .build()) {
    sender.table("trades")
            .symbol("symbol", "ETH-USD")
            .doubleColumn("price", 2615.54)
            .atNow();
}
```

### TCP Transport

```java
try (Sender sender = Sender.fromConfig("tcp::addr=localhost:9009;")) {
    sender.table("trades")
            .symbol("symbol", "ETH-USD")
            .symbol("side", "sell")
            .doubleColumn("price", 2615.54)
            .doubleColumn("amount", 0.00044)
            .atNow();
}
```

### UDP Transport

UDP uses `LineUdpSender` directly (not available via `Sender.fromConfig()`). It is fire-and-forget with no delivery guarantees.

```java
import io.questdb.client.cutlass.line.LineUdpSender;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;

// Parameters: interface IPv4 address, target IPv4 address, target port, buffer capacity, TTL
int lo = Numbers.parseIPv4("127.0.0.1");
int target = Numbers.parseIPv4("127.0.0.1");
try (LineUdpSender sender = new LineUdpSender(lo, target, 9009, 1024, 2)) {
    sender.table("trades")
            .symbol("symbol", "ETH-USD")
            .doubleColumn("price", 2615.54)
            .atNow();
    sender.flush();
}
```

### Authentication and TLS

**HTTP with username/password:**

```java
try (Sender sender = Sender.fromConfig("https::addr=localhost:9000;username=admin;password=quest;")) {
    // ...
}
```

**HTTP with bearer token:**

```java
try (Sender sender = Sender.fromConfig("http::addr=localhost:9000;token=my_bearer_token;")) {
    // ...
}
```

**TCP with authentication:**

```java
try (Sender sender = Sender.fromConfig("tcp::addr=localhost:9009;user=admin;token=my_token;")) {
    // ...
}
```

**TLS with certificate validation disabled (not for production):**

```java
try (Sender sender = Sender.fromConfig("https::addr=localhost:9000;tls_verify=unsafe_off;")) {
    // ...
}
```

### Explicit Timestamps

```java
import java.time.Instant;
import java.time.temporal.ChronoUnit;

try (Sender sender = Sender.fromConfig("http::addr=localhost:9000;")) {
    // Using an Instant
    sender.table("trades")
            .symbol("symbol", "ETH-USD")
            .doubleColumn("price", 2615.54)
            .at(Instant.now());

    // Using a long value with time unit
    sender.table("trades")
            .symbol("symbol", "BTC-USD")
            .doubleColumn("price", 39269.98)
            .at(1_000_000_000L, ChronoUnit.NANOS);
}
```

### Configuration via Environment Variable

Instead of hardcoding the configuration string, set the `QDB_CLIENT_CONF` environment variable:

```bash
export QDB_CLIENT_CONF="http::addr=localhost:9000;"
```

Then create the sender:

```java
try (Sender sender = Sender.fromEnv()) {
    // ...
}
```

## Configuration Reference

The configuration string format is:

```
schema::key1=value1;key2=value2;
```

**Schemas:** `http`, `https`, `tcp`, `tcps`

| Key                      | Default      | Description                                             |
| ------------------------ | ------------ | ------------------------------------------------------- |
| `addr`                   | _(required)_ | Server address as `host:port`                           |
| `username`               |              | HTTP basic auth username                                |
| `password`               |              | HTTP basic auth password                                |
| `token`                  |              | Bearer token (HTTP) or private key token (TCP)          |
| `user`                   |              | Username for TCP auth                                   |
| `tls_verify`             | `on`         | TLS certificate validation (`on` or `unsafe_off`)       |
| `tls_roots`              |              | Path to custom truststore                               |
| `tls_roots_password`     |              | Truststore password                                     |
| `auto_flush`             | `on`         | Enable auto-flush (`on` or `off`)                       |
| `auto_flush_rows`        | `75000`      | Flush after N rows (HTTP only)                          |
| `auto_flush_interval`    | `1000`       | Flush interval in milliseconds (HTTP; `off` to disable) |
| `request_timeout`        | `30000`      | HTTP request timeout in milliseconds                    |
| `request_min_throughput` | `102400`     | Min expected throughput in bytes/sec (HTTP)             |
| `retry_timeout`          | `10000`      | Total retry duration in milliseconds (HTTP)             |
| `max_buf_size`           | `104857600`  | Maximum buffer capacity in bytes                        |
| `max_name_len`           | `127`        | Maximum table/column name length                        |
| `protocol_version`       | `auto`       | ILP protocol version (`1`, `2`, `3`, or `auto`)         |

For the full configuration reference, see the [QuestDB ILP documentation](https://questdb.com/docs/reference/api/ilp/overview/).

## Requirements

- Java 11 or later
- Maven 3+ (for building from source)

## Building from Source

```bash
git clone https://github.com/questdb/java-questdb-client.git
cd java-questdb-client
mvn clean package -DskipTests
```

## Community

- [QuestDB Documentation](https://questdb.com/docs/)
- [QuestDB Community Forum](https://community.questdb.io/)
- [QuestDB Slack](https://slack.questdb.io/)
- [GitHub Issues](https://github.com/questdb/java-questdb-client/issues)

## License

This project is licensed under the [Apache License 2.0](LICENSE.txt).
