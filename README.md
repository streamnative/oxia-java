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
  **A:** Ask the EE team to add the release version in the [`images/dependencies.json`][dependencies-file] file in the [`streamnative-ci`][ci-repo] repo

[oxia]: https://github.com/streamnative/oxia
[release-workflow]: https://github.com/streamnative/streamnative-ci/actions/workflows/maven-tag-deploy.yml
[dependencies-file]: https://github.com/streamnative/streamnative-ci/blob/master/images/dependencies.json
[ci-repo]: https://github.com/streamnative/streamnative-ci

## Metrics

The Oxia Java client collects various metrics about the operations being performed. There is an
[API](client-metrics-api/src/main/java/io/streamnative/oxia/client/metrics/api/Metrics.java) and an OpenTelemetry
[implementation](client-metrics-opentelemetry/src/main/java/io/streamnative/oxia/client/metrics/opentelemetry/OpenTelemetryMetrics.java)
is provided.

There are two levels at which metrics are captured - at the individual operation level and at the batch level. All
metrics have two attributes - type & result.

### Operation Level

|             Name              |                                  Description                                   |
|-------------------------------|--------------------------------------------------------------------------------|
| `oxia_client_operation_timer` | The time an operation takes to be executed from request submission to response |
| `oxia_client_operation_size`  | The record value size. Only applicable for `put` and `get`                     |

#### Attributes

|   Name   |     Description      |                     Values                     |
|----------|----------------------|------------------------------------------------|
| `type`   | The operation type   | `put`, `delete`, `delete_range`, `get`, `list` |
| `result` | The operation result | `success`, `failure`                           |

### Batch Level

|              Name               |                            Description                             |
|---------------------------------|--------------------------------------------------------------------|
| `oxia_client_batch_total_timer` | The time a batch takes to be completed from creation to response   |
| `oxia_client_batch_exec_timer`  | The time a batch takes to be completed from submission to response |
| `oxia_client_batch_size`        | The size of all the record values in a batch                       |
| `oxia_client_batch_requests`    | The count of requests in a batch                                   |

#### Attributes

|   Name   |     Description      |        Values        |
|----------|----------------------|----------------------|
| `type`   | The operation type   | `write`, `read`      |
| `result` | The operation result | `success`, `failure` |

### Cache Level

| Name                            | Description                                       |
|---------------------------------|---------------------------------------------------|
| `oxia_client_cache_load_timer`  | The time it took to load a record in to the cache |
| `oxia_client_cache_hits`        | Counts of cache hits and misses                   |
| `oxia_client_cache_evictions`   | Counts of record evictions from the cache         |

#### Attributes

| Name            | Description                     | Values                                                 | Notes                            |
|-----------------|---------------------------------|--------------------------------------------------------|----------------------------------|
| `type`          | The operation type              | `load`, `hit`, `eviction`                              |                                  |
| `result`        | The operation result            | `success`, `failure`                                   |                                  |
| `removal_cause` | See: [`RemovalCause`][caffeine] | `explicit`, `replaced`, `collected`, `expired`, `size` | Applies to `type: eviction` only |

   [caffeine]: https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/RemovalCause.java