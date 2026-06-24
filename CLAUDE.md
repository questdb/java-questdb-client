# CLAUDE.md

Guidance for Claude Code when working with the QuestDB Java client
(`java-questdb-client`).

## Project Overview

The standalone client repo for QuestDB ingestion (legacy ILP and QWP). It is
a Git submodule of the `questdb` repository and ships as the
`io.questdb:questdb-client` Maven artifact.

**Dual JDK target — always aim at both.** The client ships as a Java 8
artifact (JDK 8 is the source of truth: CI builds, tests, and releases on
JDK 8) but must also compile cleanly on JDK 11+ (CI runs a JDK 25
compile/javadoc smoke job). The right Maven profile (`java8` /
`java11+`) auto-activates from the running JDK; a `src/main/java8` vs
`src/main/java11` source root supplies the per-JDK shims. Practically this
means **Java 8 is the language/API floor** — every change must build on
both, so write to Java 8 and never introduce an API or syntax that only a
newer JDK has.

Disallowed here (newer than Java 8), among others:
- `var` local-variable type inference (10), enhanced `switch` (14),
  multiline text blocks (15), pattern variables in `instanceof` (16)
- `List.of` / `List.copyOf` / `Map.of` and friends (9/10),
  `String.repeat` / `String.strip` / `String.isBlank` (11)
- `ProcessHandle` (9), `Thread.onSpinWait` (9),
  the `Provider(String, String, String)` ctor (9 — use the Java 8
  `Provider(String, double, String)` overload)

When in doubt, check the API's `@since` and keep it `<= 1.8`. JDK
9+-only behaviour belongs behind the `src/main/java11` shim with a Java 8
counterpart in `src/main/java8`.

## Git & PR Conventions

- **PRs are squash-merged. Commit history on a PR branch is throwaway** —
  only the squashed commit message that lands on the default branch is
  preserved. Do not spend effort tidying the branch's history: no soft
  resets to "commit all at once", no rewording prior commits, no force
  pushes to clean up. Adding a fix-up commit on top is always fine. The
  squash flow folds the lot at merge time anyway.
- This repo is a submodule of `questdb`. Commit here first, then bump the
  submodule pointer in the parent repo in a separate commit — never push
  a parent-repo submodule pointer that refers to an unpushed client SHA.

## Investigating failures

- **Never dismiss a failure as "pre-existing", "flaky", "unrelated", or "a
  known issue" without actually proving it.** That label is a hypothesis,
  not a conclusion. Treat any red test, red CI job, or surprising log line
  as a live bug to investigate until the evidence — git log, reproduction
  on master, a real timing constraint, an upstream report — forces a
  different conclusion. Only after that proof can the issue be set aside,
  and the proof itself should be reported back so it can be verified.

## Build

Standard Maven build. Dual-target (Java 8 floor, Java 11+ also supported);
the `java8` / `java11+` profile auto-activates from the JDK running Maven:

```bash
mvn clean install -DskipTests   # build + install to local Maven cache
mvn -pl core test                # run client unit tests
```

Build on a JDK 8 to validate the shipping artifact (the source-of-truth
target); also confirm it compiles on a modern JDK (11+) before merging, the
same two fronts CI guards.

The parent `questdb` repo's `local-client` profile pulls this module as a
sub-module so server changes can build against unpublished client code; if
you change client code, install it (or pass `-P local-client` in the parent)
before running parent-repo tests.
