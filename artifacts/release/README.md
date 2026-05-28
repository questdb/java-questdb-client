# Release steps

Steps to release `org.questdb:questdb-client` to Maven Central.

The release is owned by the manually triggered
[`Release to Maven Central`](../../.github/workflows/maven_central_release.yml) workflow. Do not create release tags
by hand, do not push version tags to trigger publishing, and do not run `mvn deploy` locally during the normal release
path.

## Prepare release notes

Create a draft GitHub release with the intended version and notes. Do not create the git tag up front. The workflow
creates the tag through `mvn release:prepare`; finalize the GitHub release after Maven Central propagation.

## Publish

Start the `Release to Maven Central` workflow from the Actions tab.

Use these inputs:

- `source_ref`: branch/ref to release from, usually `main`
- `release_version_override`: blank unless doing a non-standard version
- `next_development_version_override`: blank unless doing a non-standard next snapshot

The release path uses the generated Maven tag as the source of truth. It pushes the `mvn release:prepare` commits
back to `source_ref`, pushes the generated release tag, builds native libraries from that tag, downloads the native
artifacts into the Maven build, signs the release artifacts, and uploads through the Sonatype Central Portal.

The final Central upload runs in the `publish-central` job, which is attached to the `maven-release` GitHub
environment. As a second layer of protection, configure that environment with required reviewers so the workflow also
pauses for human approval before the immutable Maven Central publish step.

The workflow returns once Sonatype has validated the upload and taken ownership of the artifacts. Physical propagation
to Maven Central happens asynchronously after the workflow finishes, so a green run does not guarantee the artifacts
are immediately visible on `central.sonatype.com`.

## Versioning

In the normal path, leave both version override inputs blank. `mvn release:prepare` derives:

- release version from the current POM, for example `1.3.2-SNAPSHOT` -> `1.3.2`
- release tag from the release version, via `tagNameFormat=@{project.version}`
- next development version from Maven Release Plugin defaults, for example `1.3.3-SNAPSHOT`

Use `release_version_override` and `next_development_version_override` only for non-standard releases.

## Failure handling

If a release fails before the final Maven Central upload, inspect whether the prepare commits or tag were pushed.
If they were pushed, either rerun from the same source state after fixing the workflow or clean up the failed release
state deliberately. Do not reuse a version that may already have reached Maven Central; Maven Central coordinates are
immutable.

## Post-release

Check [Maven Central](https://central.sonatype.com/artifact/org.questdb/questdb-client) until the new version is
listed, then finalize the GitHub release draft against the generated tag and add the release notes.
