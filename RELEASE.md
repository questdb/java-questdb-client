# Release Guide

This document describes how to release `org.questdb:client` to Maven Central.

## Overview

Releases use the [Maven Release Plugin](https://maven.apache.org/maven-release/maven-release-plugin/) together with the [Sonatype Central Publishing Plugin](https://central.sonatype.org/publish/publish-portal-maven/). The `maven-central-release` profile handles signing, Javadoc generation, source attachment, and publishing.

The repo now includes a GitHub Actions workflow at `.github/workflows/maven_central_release.yml` that:

- assumes an AWS IAM role via GitHub OIDC
- reads the release credentials from AWS Secrets Manager
- imports the GPG key at runtime
- runs `mvn release:prepare release:perform`
- pushes the release commits and tag only after Central publication succeeds

## Automated GitHub Release

### AWS secret payload

Store a single JSON secret in AWS Secrets Manager with these keys:

```json
{
  "MAVEN_GPG_PRIVATE_KEY": "-----BEGIN PGP PRIVATE KEY BLOCK-----\n...\n-----END PGP PRIVATE KEY BLOCK-----",
  "MAVEN_GPG_PASSPHRASE": "your-passphrase",
  "MAVEN_CENTRAL_USERNAME": "your-central-username",
  "MAVEN_CENTRAL_PASSWORD": "your-central-password-or-token"
}
```

Export the private key in ASCII armor form:

```bash
gpg --armor --export-secret-keys <YOUR_KEY_ID>
```

### Repository configuration

Create these GitHub repository variables:

- `MAVEN_RELEASE_AWS_REGION`
- `MAVEN_RELEASE_AWS_ROLE_ARN`
- `MAVEN_RELEASE_AWS_SECRET_ID`

Optional GitHub repository secret:

- `MAVEN_RELEASE_GIT_TOKEN`

Use `MAVEN_RELEASE_GIT_TOKEN` only if the default `GITHUB_TOKEN` cannot push the release commits and tag to the default branch because of branch protection.

### AWS permissions

The IAM role assumed by the workflow needs:

- `secretsmanager:GetSecretValue` on the release secret
- `secretsmanager:ListSecrets`
- `kms:Decrypt` if the secret uses a customer-managed KMS key

Its trust policy must allow GitHub Actions to assume the role through the GitHub OIDC provider.

### Trigger the release

Run the `Release to Maven Central` workflow from the default branch and provide:

- `release_version`, for example `1.2.1`
- `next_development_version`, for example `1.2.2-SNAPSHOT`

The workflow creates the standard Maven release commits:

1. `[maven-release-plugin] prepare release <release-version>`
2. `[maven-release-plugin] prepare for next development iteration`

It also creates a Git tag named after the release version.

## Manual Fallback

If the workflow is unavailable, the release can still be performed locally.

### Prerequisites

#### 1. GPG key

A GPG key is required to sign the release artifacts. If you don't have one:

```bash
gpg --gen-key
gpg --keyserver keyserver.ubuntu.com --send-keys <YOUR_KEY_ID>
```

More details on GPG key generation can be found in the [Sonatype guide](https://central.sonatype.org/publish/requirements/gpg/).

#### 2. Sonatype credentials

You need credentials for the Sonatype Central Portal (https://central.sonatype.com/).

#### 3. Maven `settings.xml`

Configure your `~/.m2/settings.xml` with the Sonatype server credentials:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>YOUR_SONATYPE_USERNAME</username>
      <password>YOUR_SONATYPE_PASSWORD</password>
    </server>
  </servers>
</settings>
```

#### 4. Repository access

You need push access to the `questdb/java-questdb-client` repository on GitHub.

### Release commands

Prepare the release:

```bash
mvn release:prepare
```

Perform the release:

```bash
mvn release:perform
```

If required, push the resulting commits and tags:

```bash
git push origin main --tags
```

## Post-Release

Verify that the new version appears on [Maven Central](https://central.sonatype.com/artifact/org.questdb/client). Propagation may take some time after publishing.

If you also want a GitHub release entry, draft one from the tag created by the Maven release plugin and add release notes for the version.
