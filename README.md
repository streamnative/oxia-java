# Oxia Java

[![Build](https://github.com/streamnative/oxia-client-java/actions/workflows/pr-build-and-test.yml/badge.svg)](https://github.com/streamnative/oxia-client-java/actions/workflows/pr-build-and-test.yml)

## Overview

This project comprises JDK language compatible modules for the [Oxia][oxia] service. Currently, it provides:

- [Client](client/) for the Oxia service
- [Testcontainer](testcontainers/) for local integration testing with an Oxia service.

## Build

Requirements:

* JDK 17
* Maven 3.8.6+

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

