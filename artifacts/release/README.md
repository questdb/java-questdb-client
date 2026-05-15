# Release steps

This is a short guide to outline the steps involved in releasing `org.questdb:questdb-client` to Maven Central.

## Edit release notes

First step is to create a draft release with the intended release version number and release notes. The git tag should
not be created up front — choose the intended tag name in the draft and let GitHub create it when the release is
published. When crafting new release notes, please use previous release notes as style guidelines. Releases should not
look too dissimilar.

## Switch to `main`

```bash
git fetch
git checkout main
git pull
```

Releases must be cut from the latest `main`. Make sure your working tree is clean.

## Clear previous release "memory"

```bash
mvn release:clean
```

This removes any `release.properties` and `*.releaseBackup` files left over from a previous attempt.

## Roll versions and create the tag

This step will do the following:

- roll the parent and module versions from snapshot to release, e.g. from `1.2.2-SNAPSHOT` to `1.2.2`
- commit the release POMs
- create the release tag in the local git repo
- roll the versions to the next snapshot, e.g. `1.2.3-SNAPSHOT`
- commit the next-snapshot POMs

```bash
mvn -B release:prepare \
  -DautoVersionSubmodules=true \
  -DpushChanges=false \
  -DreleaseVersion=<release-version> \
  -DdevelopmentVersion=<next-snapshot-version> \
  -Dtag=<release-version>
```

For example, when releasing `1.2.2`:

```bash
mvn -B release:prepare \
  -DautoVersionSubmodules=true \
  -DpushChanges=false \
  -DreleaseVersion=1.2.2 \
  -DdevelopmentVersion=1.2.3-SNAPSHOT \
  -Dtag=1.2.2
```

The `-B` flag will make assumptions about the release version. Use it for routine releases. For a special version (for
example a new major like `2.0`), drop `-B` and answer the interactive prompts.

`-DpushChanges=false` keeps both commits and the tag local until you have verified they look right.

If `release:prepare` fails partway through, recover with:

```bash
mvn release:rollback
```

This reverts the prepare commits and removes the backup files. If `release.properties` has already been cleaned up, you
can reset the unwanted commits manually with `git reset --hard <previous-HEAD>` instead.

## Push the branch and tag

Once the local commits and tag look correct, publish them:

```bash
git push origin main
git push origin <release-version>
```

For example:

```bash
git push origin main
git push origin 1.2.2
```

## Publish to Maven Central

The jar publication to Maven Central is performed by the
[`Release to Maven Central`](../../.github/workflows/maven_central_release.yml) GitHub Actions workflow added in this PR.

The workflow is triggered automatically when a release tag matching `X.Y.Z` (three numeric segments) is pushed in the previous step. No manual
dispatch is required. The workflow:

- checks out the pushed tag
- assumes an AWS IAM role via OIDC and reads the GPG key and Sonatype credentials from AWS Secrets Manager
- verifies that the tag name matches the parent POM version and that the version is not a snapshot
- signs the artifacts and publishes them through the Sonatype Central Portal

You need push access to the repository to push the release tag, which is what triggers the workflow.

## Post-release

Verify that the new version appears on
[Maven Central](https://central.sonatype.com/artifact/org.questdb/questdb-client). Propagation may take some time after
publishing.

Finalize the GitHub release draft from the tag created by the release process and add the release notes for the
version.
