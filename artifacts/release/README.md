# Release steps

Steps to release `org.questdb:questdb-client` to Maven Central.

The release is owned by the manually triggered
[`Release to Maven Central`](../../.github/workflows/maven_central_release.yml) workflow. Do not create release tags
by hand and do not run `mvn deploy` locally during the normal release path.

The workflow is built so that nothing irreversible happens until the release has been proven good: it builds every
native library, runs the full test suite against those freshly built binaries, and validates the signed bundle with
the Central Portal **before** it ever pushes a git tag or publishes to Maven Central. The release tag is created last
and points at the exact tree that was verified and published.

## One-time setup

The `publish` job pushes the release tag using the built-in `GITHUB_TOKEN` (`github-actions[bot]`). The org-wide
`restrict-tag-pushing` ruleset blocks tag creation by default, so `github-actions[bot]` must be added as a **bypass
actor** on that ruleset (Organization settings -> Rules -> `restrict-tag-pushing` -> Bypass list).

The branch ruleset on `main` is intentionally **not** bypassed. The next-development snapshot bump lands as an ordinary
pull request, so `main` keeps its "PR-only, squash, one approval" protection.

The AWS secret referenced by `MAVEN_RELEASE_AWS_SECRET_ARN` must expose these JSON keys (they become environment
variables of the same name): `MAVEN_GPG_PRIVATE_KEY`, `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, and
optionally `MAVEN_GPG_PASSPHRASE` (omit or leave empty for a passphrase-less signing key).

Configure the `maven-release` GitHub environment with required reviewers. The `publish` job is attached to that
environment, so the workflow pauses for human approval before any credentials are used and before anything is
published.

## Prepare release notes

Create a draft GitHub release with the intended version and notes. Do not create the git tag up front -- the workflow
creates it. Finalize the GitHub release after Maven Central propagation (see [Post-release](#post-release)).

## Publish

Start the `Release to Maven Central` workflow from the Actions tab with these inputs:

- `source_ref`: branch/ref to release from, usually `main`
- `release_version_override`: blank unless doing a non-standard version
- `next_development_version_override`: blank unless doing a non-standard next snapshot

The workflow runs as a pipeline:

1. **resolve** -- derives the release and next-development versions from the current `-SNAPSHOT` POM, and fails fast if
   the tag already exists or the version is already on Maven Central.
2. **build (5 jobs)** -- builds the native library for each platform (darwin-aarch64, darwin-x86-64, linux-x86-64,
   linux-aarch64, windows-x86-64) from the resolved source commit, and smoke-loads each one.
3. **verify** -- bundles all five native libraries and runs the full test suite against them with the release version
   applied. This is the quality gate; it requires no credentials.
4. **publish** (gated by the `maven-release` environment) -- after approval: signs and uploads the bundle to the
   Central Portal as a droppable `VALIDATED` deployment, then publishes it, then pushes the release tag.
5. **open-bump-pr** -- opens the next-development-version bump PR (post-release, see below).

Approve the `publish` job when prompted. The run is green once the Central Portal has accepted the deployment for
publishing and the tag has been pushed.

## Versioning

In the normal path, leave both override inputs blank. The workflow derives:

- release version from the current POM, for example `1.3.2-SNAPSHOT` -> `1.3.2`
- release tag equal to the release version
- next development version by bumping the patch, for example `1.3.3-SNAPSHOT`

Use `release_version_override` and `next_development_version_override` only for non-standard releases (for example a
new minor or major line).

## The snapshot-bump PR

`main` stays at its current `-SNAPSHOT` during the release; the `open-bump-pr` job opens a PR that bumps it to the next
development version. That PR is **post-release housekeeping** -- it does not affect the release you just shipped, and a
delay in merging it does not invalidate anything.

It must, however, be merged **before the next release**. If it is not, `main` is still at the just-released
`-SNAPSHOT`, and the next run's `resolve` step will refuse to re-release a version whose tag already exists. Merge the
bump PR (squash, like any PR to `main`) once the release is confirmed.

## Failure handling

The pipeline is ordered so failures are clean:

- A failure in `resolve`, any `build`, or `verify` happens **before** anything is tagged or published. Fix the cause
  and rerun; nothing was mutated.
- In `publish`, the bundle is uploaded as a droppable `VALIDATED` deployment first. If validation fails, nothing is
  published and the deployment can be dropped from the Central Portal. The release tag is pushed only **after** the
  Central Portal has accepted the deployment for publishing.

If a run fails inside `publish` after the Central Portal accepted the deployment, the artifact is on its way to Maven
Central and the coordinate is immutable -- do not reuse the version. If the tag push is what failed, create the tag
manually on the published commit; do not start a fresh release for the same version.

## Post-release

Publishing to Maven Central is asynchronous. After the Central Portal accepts the deployment, propagation to
`central.sonatype.com` and the public index typically takes a few minutes but can occasionally take longer, so a green
run does not mean the artifact is immediately downloadable.

1. Merge the `open-bump-pr` pull request.
2. Watch [Maven Central](https://central.sonatype.com/artifact/org.questdb/questdb-client) until the new version is
   listed.
3. Finalize the draft GitHub release against the pushed tag and add the release notes.
