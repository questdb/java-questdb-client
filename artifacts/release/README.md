# Release steps

Steps to release `org.questdb:questdb-client` to Maven Central. Examples below use `1.2.2` (release) and
`1.2.3-SNAPSHOT` (next snapshot); substitute the actual versions when running.

**Prerequisite:** tag creation is restricted by an org-wide ruleset, so you must be a member of the `questdb/release`
team to push the release tag. Confirm membership before starting.

## Edit release notes

Create a draft release with the intended version and notes. Do not create the git tag up front -- pick the tag name
in the draft and let GitHub create it when the release is published. Match the style of previous release notes.

## Create a release branch

Direct pushes to `main` are blocked by the org ruleset (one-approval squash-merged PR is the only path), so release
commits live on a dedicated branch.

```bash
git fetch
git checkout main
git pull
git checkout -b release/1.2.2
```

Make sure your working tree is clean.

## Clear previous release "memory"

```bash
mvn release:clean
```

Removes any `release.properties` and `*.releaseBackup` files left over from a previous attempt.

## Roll versions and create the tag

`release:prepare` will:

- roll parent and module versions from snapshot to release (`1.2.2-SNAPSHOT` -> `1.2.2`)
- commit the release POMs
- create the release tag locally
- roll the versions to the next snapshot (`1.2.3-SNAPSHOT`)
- commit the next-snapshot POMs

```bash
mvn -B release:prepare \
  -DautoVersionSubmodules=true \
  -DpushChanges=false \
  -DreleaseVersion=1.2.2 \
  -DdevelopmentVersion=1.2.3-SNAPSHOT \
  -Dtag=1.2.2
```

`-B` runs non-interactively; drop it for special versions (e.g. a new major) to get the prompts. `-DpushChanges=false`
keeps the commits and tag local until you have verified them.

If `release:prepare` fails partway through:

```bash
mvn release:rollback
git tag -d 1.2.2
```

`release:rollback` reverts the prepare commits and removes the backup files but does **not** delete the tag -- drop
it manually or the next attempt at the same version fails. If `release.properties` is already gone, use
`git reset --hard <previous-HEAD>` instead (and still drop the tag).

## Push the release branch and tag

```bash
git push origin release/1.2.2
git push origin 1.2.2
```

The tag push triggers the Maven Central workflow (see below). The branch is merged to `main` afterwards -- see
[Merge the release branch to `main`](#merge-the-release-branch-to-main).

## Publish to Maven Central

The [`Release to Maven Central`](../../.github/workflows/maven_central_release.yml) workflow fires automatically when
a tag matching `X.Y.Z` is pushed. No manual dispatch. It:

- checks out the pushed tag
- assumes an AWS IAM role via OIDC and reads the GPG key and Sonatype credentials from AWS Secrets Manager
- verifies the tag matches the parent POM version and is not a snapshot
- signs the artifacts and publishes them through the Sonatype Central Portal

## Merge the release branch to `main`

Once the workflow finishes and the artifacts appear on Maven Central, open a PR from `release/1.2.2` to `main` and
squash-merge it after approval. Delete the release branch afterwards.

Squash-merge is the only merge method allowed by the org ruleset on `main`, so the original `[maven-release-plugin]`
commits will not appear in `main`'s history. The tag remains the canonical pointer to the released code; `main`
carries a single squashed commit that bumps the snapshot version.

## Post-release

Verify the version appears on
[Maven Central](https://central.sonatype.com/artifact/org.questdb/questdb-client) (propagation can take a few
minutes). Finalize the GitHub release draft against the new tag and add the release notes.
