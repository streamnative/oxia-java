# Oxia Java

[![Build](https://github.com/streamnative/oxia-client-java/actions/workflows/pr-build-and-test.yml/badge.svg)](https://github.com/streamnative/oxia-client-java/actions/workflows/pr-build-and-test.yml)

## Overview

This project comprises JDK language compatible modules for the [Oxia][oxia] service. It provides
the following capabilities:

- [Client](client/) for the Oxia service
- [Testcontainer](testcontainers/) for local integration testing with an Oxia service.

## Build

Requirements:

* JDK 17
* Maven 3.8.6+
* [protoc](https://grpc.io/docs/protoc-installation/)

Common build actions:

|             Action              |                 Command                  |
|---------------------------------|------------------------------------------|
| Full build and test             | `mvn clean verify`                       |
| Skip tests                      | `mvn clean verify -DskipTests`           |
| Skip Jacoco test coverage check | `mvn clean verify -Djacoco.skip`         |
| Skip Checkstyle standards check | `mvn clean verify -Dcheckstyle.skip`     |
| Skip Spotless formatting check  | `mvn clean verify -Dspotless.check.skip` |
| Format code                     | `mvn spotless:apply`                     |
| Generate license headers        | `mvn license:format`                     |

## Release

1. Go to the [release workflow][release-workflow]
2. Click the gray <kbd>Run workflow</kbd> button on the righthand side
3. Choose:
    * **use workflow from**: branch `master`
    * **Git repository**: `streamnative/oxia-java`
    * **JDK version**: `17`
4. Click the green <kbd>Run workflow</kbd> button

### Questions

* **Q: _Why do we publish it in `streamnative-ci`?_**<br/>
  **A:** `streamnative-ci` repo has some secrets and scripts to use streamnative bot account
* **Q: _What can I do if want to put the release `nar` package to `pulsar-cloud` image?_**<br/>
  **A:** Ask the EE team to add the release version in the [`images/dependencies.json`][dependencies-file] file of th [`streamnative-ci`][ci-repo] repo

[oxia]: https://github.com/streamnative/oxia
[release-workflow]: https://github.com/streamnative/streamnative-ci/actions/workflows/maven-tag-deploy.yml
[dependencies-file]: https://github.com/streamnative/streamnative-ci/blob/master/images/dependencies.json
[ci-repo]: https://github.com/streamnative/streamnative-ci
