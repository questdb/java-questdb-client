# Release Guide

This document describes how to release `org.questdb:client` to Maven Central.

## Overview

Releases are performed using the [Maven Release Plugin](https://maven.apache.org/maven-release/maven-release-plugin/) combined with the [Sonatype Central Publishing Plugin](https://central.sonatype.org/publish/publish-portal-maven/). The `maven-central-release` profile handles signing, Javadoc generation, source attachment, and publishing.

## Prerequisites

### 1. GPG Key

A GPG key is required to sign the release artifacts. If you don't have one:

```bash
gpg --gen-key
gpg --keyserver keyserver.ubuntu.com --send-keys <YOUR_KEY_ID>
```

More details on GPG key generation can be found in the [Sonatype guide](https://central.sonatype.org/publish/requirements/gpg/).

### 2. Sonatype Credentials

You need credentials for the Sonatype Central Portal (https://central.sonatype.com/).

### 3. Maven `settings.xml`

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

More details can be found in the [Sonatype guide](https://central.sonatype.org/publish/publish-portal-maven/).

### 4. Repository Access

You need push access to the `questdb/java-questdb-client` repository on GitHub.

## Release Process

### Step 1: Prepare the Release

This bumps the version, creates a tag, and commits the changes:

```bash
mvn release:prepare
```

The plugin will prompt for:

- **Release version** (e.g., `9.3.1`) — the version to release
- **SCM tag** (e.g., `9.3.1`) — the Git tag name (uses the `tagNameFormat` of `@{project.version}`)
- **Next development version** (e.g., `9.3.2-SNAPSHOT`) — the next snapshot version

This creates two commits:

1. `[maven-release-plugin] prepare release 9.3.1` — sets the release version
2. `[maven-release-plugin] prepare for next development iteration` — sets the next snapshot version

And a Git tag (e.g., `9.3.1`).

### Step 2: Perform the Release

This builds, signs, and publishes the artifacts to Maven Central:

```bash
mvn release:perform
```

The `maven-central-release` profile is activated automatically. It:

- Compiles the source
- Generates Javadoc
- Attaches sources JAR
- Signs all artifacts with GPG
- Publishes to Maven Central via the Sonatype Central Publishing Plugin
- Waits until the artifacts are published (`waitUntil=published`)

### Step 3: Push Tags

If not pushed automatically:

```bash
git push origin main --tags
```

## Post-Release

### Verify on Maven Central

Check that the new version appears on [Maven Central](https://central.sonatype.com/artifact/org.questdb/client). Propagation may take some time after publishing.

### Create a GitHub Release

1. Go to [GitHub Releases](https://github.com/questdb/java-questdb-client/releases).
2. Click **Draft a new release**.
3. Select the tag created by the release plugin.
4. Add release notes describing the changes.
5. Publish the release.
