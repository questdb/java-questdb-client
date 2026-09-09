# Pooled store-and-forward schema-rejection demo

This demo shows the default `REJECT_AND_CONTINUE` behavior for a one-slot
pooled WebSocket sender. A borrow that publishes a schema-mismatched row fails,
but returning that borrow releases the slot. A later borrow from the same pool
then publishes a valid row successfully.

Before retiring the rejected range, a disk-backed sender preserves its raw QWP
frames and dictionary state in a completed archive. The owning borrow receives
a synchronous `LineSenderServerException` identifying the rejected FSN range.
The configured asynchronous error handler receives a `SenderError` containing
the completed archive path. The demo checks both signals and verifies that the
valid row reaches QuestDB.

It uses a real QuestDB server with QWP available at `localhost:9000`. Start an
ephemeral test server:

```bash
docker run --rm -d --name qdb-java-sfa-poison-demo \
  -p 9000:9000 questdb/questdb:nightly
```

The demo drops and recreates only the table `java_sfa_poison_demo`; do not point
it at a production database.

Build the current client and choose a new temporary directory. A source build
needs CMake, NASM, a C/C++ compiler, and the checked-out zstd submodule:

```bash
git submodule update --init --recursive
cmake -DCMAKE_BUILD_TYPE=Release -B core/cmake-build-release -S core
cmake --build core/cmake-build-release --config Release
mvn -pl core -Dmaven.test.skip=true install
mvn -f examples/pom.xml -DskipTests compile
export QDB_POISON_DEMO_SF_DIR="$(mktemp -d)"
```

Run the demo once with that empty directory:

```bash
mvn -f examples/pom.xml \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=com.example.sender.WsPooledSchemaPoisonDemo \
  -Dexec.args="${QDB_POISON_DEMO_SF_DIR}"
```

The output names the failed borrow's FSN range, the preserved-copy directory,
and the successful valid row. Pass `host:port` as the final argument to use a
server other than `localhost:9000`.
