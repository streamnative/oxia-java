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

[oxia]: https://github.com/streamnative/oxia

## Release

1. Go to `https://github.com/streamnative/streamnative-ci/actions/workflows/maven-tag-deploy.yml`
2. Click `Run workflow` gray button in right side.
3. Choose `use workflow from` `master`, input `streamnative/oxia-java` in `Git repository`, input `17` `JDK version`
4. Click `Run worklow` green button

> **Q:** Why do we publish it in `streamnative-ci` ?
>
> **A:** `streamnative-ci` repo has some secrets and scripts to use streamnative bot account.
>
> **Q:** What can I do if want to put the release `nar` package to `pulsar-cloud` image ?
>
> **A:** Ask EE team to add the release version in `images/dependencies.json` of `streamnative-ci` repo.

