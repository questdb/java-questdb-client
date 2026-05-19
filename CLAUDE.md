# CLAUDE.md

Guidance for Claude Code when working with the QuestDB Java client
(`java-questdb-client`).

## Project Overview

The standalone client repo for QuestDB ingestion (legacy ILP and QWP). It is
a Git submodule of the `questdb` repository and ships as the
`io.questdb:questdb-client` Maven artifact. The module targets Java 11, so
only legacy Java features are available here — no enhanced switch, no
multiline strings, no pattern variables in `instanceof`.

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

Standard Maven build. Targets Java 11:

```bash
mvn clean install -DskipTests   # build + install to local Maven cache
mvn -pl core test                # run client unit tests
```

The parent `questdb` repo's `local-client` profile pulls this module as a
sub-module so server changes can build against unpublished client code; if
you change client code, install it (or pass `-P local-client` in the parent)
before running parent-repo tests.
