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

### Insert Data

```java
import io.questdb.client.Sender;

public class Main {
    static void main(String[] args) {
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

### OIDC Sign-In (Device Flow)

For QuestDB Enterprise instances secured with OIDC, `OidcDeviceAuth` signs a user in interactively using the [OAuth 2.0 Device Authorization Grant](https://www.rfc-editor.org/rfc/rfc8628). It works from environments that have no local browser — a remote notebook kernel, a container, a headless job — because the user authorizes on any device (laptop or phone) while the process only makes outbound calls to the identity provider.

On first use it prints a verification URL and a short code, and opens the URL in your default browser when one is available; authorize there (or open the URL on any device, such as your phone), enter the code, and the token is cached in memory and refreshed silently on later calls.

```java
import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.OidcDeviceAuth;

// Discover the client id, scope and endpoints from the QuestDB server's /settings:
try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB("https://questdb.example.com:9000")) {
    auth.getToken(); // sign in once: prompts on first use, then caches and refreshes

    // Pass a token provider, not a fixed string: the sender pulls a freshly refreshed token on each
    // request, so a long-lived sender keeps working as the token rotates. getTokenSilently() refreshes
    // silently and never prompts on the flush path.
    try (Sender sender = Sender.builder(Sender.Transport.HTTP)
            .address("questdb.example.com:9000")
            .enableTls()
            .httpTokenProvider(auth::getTokenSilently)
            .build()) {
        sender.table("trades")
                .symbol("symbol", "ETH-USD")
                .doubleColumn("price", 2615.54)
                .atNow();
    }
}
```

Prefer `httpTokenProvider(auth::getTokenSilently)` for a long-lived sender: it pulls a freshly refreshed token on every request, so the sender keeps working as the token rotates. A fixed `httpToken(token)` captures the token once, so a sender that outlives the token's lifetime starts failing with 401s. Either way, hand the token to the client through the builder (or the header/password fields below), not by embedding it in a `Sender.fromConfig(...)` string or the `QDB_CLIENT_CONF` environment variable, which are easily logged, persisted, or left in shell history.

By default the prompt prints the verification URL and code to `System.out` **and** tries to open the URL in your default browser. The browser open is best-effort: it only opens an `http(s)` URL, is skipped on a headless host or a JVM without the `java.desktop` module, and never blocks sign-in — the URL and code are always printed too, so a remote or browserless process still works. To disable the browser launch for a whole process (a server, automation, CI), set the system property `-Dquestdb.client.oidc.open.browser=false`. To print only (no browser) for a single client, pass `DeviceCodePrompt.SYSTEM_OUT`; to render the challenge yourself (a clickable link or QR code in a notebook), pass any `DeviceCodePrompt`:

```java
// print only, do not open a browser:
try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(
        "https://questdb.example.com:9000",
        new OidcDeviceAuth.DiscoveryOptions().prompt(DeviceCodePrompt.SYSTEM_OUT))) {
    auth.getToken();
}
```

The same token can be presented to QuestDB over any auth path the server already validates:

- **REST API:** send it as an `Authorization: Bearer <token>` header (`auth.getAuthorizationHeaderValue()` returns the full value).
- **PG-wire:** connect as user `_sso` with the token as the password (requires `acl.oidc.pg.token.as.password.enabled=true` on the server).

To configure the identity provider explicitly instead of discovering it from the server:

```java
OidcDeviceAuth auth = OidcDeviceAuth.builder()
        .clientId("questdb")
        .deviceAuthorizationEndpoint("https://idp.example.com/as/device_authz.oauth2")
        .tokenEndpoint("https://idp.example.com/as/token.oauth2")
        .scope("openid groups")
        .groupsInToken(true) // matches acl.oidc.groups.encoded.in.token on the server
        .build();
```

Discovery via `fromQuestDB(...)` reads the OIDC client id, scope and endpoints from the server's `/settings`, and the identity provider's client must have the device authorization grant enabled. When the server does not advertise its device authorization endpoint (today's servers), pin the identity provider by its issuer so the client can discover the endpoint from the issuer's `.well-known/openid-configuration` document:

```java
try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(
        "https://questdb.example.com:9000",
        new OidcDeviceAuth.DiscoveryOptions().issuer("https://idp.example.com"))) {
    auth.getToken();
}
```

By default the device authorization and token endpoints must use `https`, so tokens are never sent in cleartext; an `http` endpoint is rejected. For local development against an `http` endpoint, opt in explicitly with `.allowInsecureTransport(true)` on the builder, or `OidcDeviceAuth.fromQuestDB(url, new OidcDeviceAuth.DiscoveryOptions().allowInsecureTransport(true))`.

`fromQuestDB(...)` takes the identity provider endpoints from the server's unauthenticated `/settings`, so it trusts that server to designate where you sign in: a spoofed, compromised, or man-in-the-middled server could redirect the sign-in to an attacker-controlled identity provider. Only use it against a server you trust, reached over `https`. Passing an issuer hardens this: the token and device authorization endpoints are then pinned to the issuer's origin, and an endpoint outside it is rejected; the issuer itself comes from you out of band, so a tampered `/settings` cannot move it. When the server is not trusted, configure the identity provider explicitly with `OidcDeviceAuth.builder()` (optionally with `.issuer(...)`) instead of discovering it.

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

## Releasing

Maven Central publishing is owned by the manually triggered `Release to Maven Central` GitHub Actions workflow, run
from the Actions tab. Do not publish from a local machine and do not run `mvn deploy` in the normal release path.

The workflow builds every platform's native library, runs the full test suite with those freshly built binaries
bundled, and validates the signed bundle with the Central Portal **before** it pushes a git tag or publishes anything.
The Central publish is the single irreversible step and runs last; the next-development version bump lands as a
follow-up pull request, so `main` keeps its PR-only protection.

The `publish` step is gated by the `maven-release` GitHub environment; configure it with required reviewers so the
workflow pauses for human approval before any credentials are used or anything is published.

The release tag push uses a dedicated Maven release GitHub App that must be allowed to bypass the org
`restrict-tag-pushing` ruleset; the built-in `GITHUB_TOKEN`/`github-actions[bot]` cannot be added for that bypass.

Full release procedure, one-time setup, and failure handling: [artifacts/release/README.md](artifacts/release/README.md).

### Building Native Libraries

The client includes native libraries (C/C++ and assembly) for performance-critical operations. Pre-built binaries are included in the repository, but you can rebuild them locally if needed.

#### Prerequisites

| Tool           | Version              | Notes                               |
| -------------- | -------------------- | ----------------------------------- |
| CMake          | 3.5+                 | Build system generator              |
| NASM           | 2.14+                | Netwide Assembler for assembly code |
| C/C++ Compiler | GCC, Clang, or MinGW | C++17 support required              |
| Make           | Any                  | Build tool                          |
| JDK            | 11+                  | For JNI headers                     |

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

#### Native Library Output Locations

Built libraries are placed in the resources directory for each platform:

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
