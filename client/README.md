# Oxia Java Client

A JDK based client for [Oxia][oxia].

## Features

- Request batching
- Asynchronous operation
- Notifications callback

## Usage

See the included [integration test][it] for executable examples.

### Get and Put

```java
var client = new OxiaClientBuilder("localhost:6648").asyncClient();
var value = client.get("key").join().getValue();
// This Put is unconditional
var version = client.put("key", "hello".getBytes()).join().getVersion();   
```

### Put — conditional on key absence

```java
try {
  var result = client.put(
        "key", value, OxiaClient.KeyNotExistsVersionId
  ).join();
} catch (KeyAlreadyExistsException e) {
  ...
}
```

### Put — conditional on version ID

```java
try {
  var result = client.put(
    "key", value, expectedVersionId
  ).join();
} catch (UnexpectedVersionIdException e) {
  ...
}
```

### Delete — unconditional

```java
var deleted = client.delete("key").join();
```

### Delete — conditional on version ID

```java
try {
  var deleted = client.delete(
    "key", expectedVersionId
  ).join();
} catch (UnexpectedVersionIdException e) {
  ...
}
```

### Delete Range

```java
client.deleteRange("aMinInc","bMaxExc").join();
```

### List Key Range

```java
var keys = client.list("aMinInc","bMaxExc").join();
```

### Subscribe to notifications

A stream of record change notifications that can be consumed.

```java
var client = new OxiaClientBuilder("localhost:6648")
  .notificationCallback(
    notification -> System.out.println("Received notification: " + notification)
  ).asyncClient();
```

## Configuration

| Option                   | Default value | Description                                                  |
|:-------------------------|--------------:|:-------------------------------------------------------------|
| `serviceAddress`         |             - | Address of Oxia cluster — `<host>:<port>`                    |
| `batchLinger`            |           5ms | Max time to wait for new operations before closing batch.    |
| `maxRequestsPerBatch`    |          1000 | Max number of operations in a batch.                         |
| `operationQueueCapacity` |          1000 | Number of operations that can be buffered prior to batching. |

[oxia]: https://github.com/streamnative/oxia
[it]: client/src/test/java/io/streamnative/oxia/client/OxiaClientIT.java

