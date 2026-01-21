# Contributing to QuestDB Java Client

Thank you for your interest in contributing to the QuestDB Java client library.

## Table of Contents

- [Contributing to QuestDB Java Client](#contributing-to-questdb-java-client)
  - [Table of Contents](#table-of-contents)
  - [License and Code of Conduct](#license-and-code-of-conduct)
  - [Reporting Bugs and Requesting Features](#reporting-bugs-and-requesting-features)
  - [Environment Setup](#environment-setup)
    - [Requirements](#requirements)
    - [Repository Structure](#repository-structure)
  - [Building](#building)
  - [Running Tests](#running-tests)
  - [Before You Submit](#before-you-submit)
    - [Code Style](#code-style)
    - [Testing](#testing)
    - [Commit Messages](#commit-messages)
    - [PR Checklist](#pr-checklist)
  - [Pull Request Review Process](#pull-request-review-process)

## License and Code of Conduct

This project is licensed under the [Apache License 2.0](LICENSE.txt). By contributing, you agree that your contributions will be licensed under the same license.

All participants are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting Bugs and Requesting Features

- **Bugs and feature requests:** Open an issue on [GitHub Issues](https://github.com/questdb/java-questdb-client/issues).
- **Questions and discussions:** Join the [QuestDB Slack](https://slack.questdb.io/) or post on the [Community Forum](https://community.questdb.io/).
- **Security vulnerabilities:** Please follow the process described in [SECURITY.md](SECURITY.md). Do not open a public issue.

## Environment Setup

### Requirements

- **Java:** JDK 11 or later
- **Maven:** 3.0 or later
- **QuestDB:** A running instance is required for integration tests. Start one with Docker:
  ```bash
  docker run -p 9000:9000 -p 9009:9009 questdb/questdb
  ```

### Repository Structure

```
client-java/
  core/         # Main client library (org.questdb:client)
  examples/     # Example applications
```

## Building

Build the project without running tests:

```bash
mvn clean package -DskipTests
```

Build with Javadoc generation:

```bash
mvn clean package -DskipTests -Pjavadoc
```

## Running Tests

Tests require a running QuestDB instance. Start one with Docker (see above), then:

```bash
mvn test
```

To generate a code coverage report:

```bash
mvn test -Pjacoco
```

The coverage report will be available at `core/target/site/jacoco/index.html`.

## Before You Submit

### Code Style

- Follow existing code conventions in the project.
- Use Java 11 language features as the minimum baseline.
- Ensure new public API methods include Javadoc.

### Testing

- Add or update tests for any new functionality or bug fixes.
- Verify all existing tests pass before submitting.

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

```
type: short description

Optional longer description.
```

Common types: `feat`, `fix`, `docs`, `test`, `refactor`, `build`, `ci`, `chore`.

Examples:

```
feat: add support for decimal columns
fix: handle connection timeout in HTTP sender
docs: update README with TLS configuration
```

### PR Checklist

Before opening a pull request, ensure:

- [ ] Code compiles without warnings
- [ ] All existing tests pass
- [ ] New tests are added for new functionality
- [ ] Commit messages follow the Conventional Commits format
- [ ] Public API changes include Javadoc

## Pull Request Review Process

1. Open a pull request against the `main` branch.
2. Provide a clear description of the change and its motivation.
3. A maintainer will review your PR and may request changes.
4. Once approved, a maintainer will merge the PR.

If you are unsure about an approach, open an issue or start a discussion on [Slack](https://slack.questdb.com/) before writing code.
