# Oxia Java Client

A JDK based client for [Oxia][oxia].

## Features

- Request batching
- Asynchronous operation
- Notifications callback
- Record caching

## Usage

See the included [integration test][it] for executable examples.

### Get and Put

```java
var client = new OxiaClientBuilder("localhost:6648").asyncClient().join();
var value = client.get("key").join().getValue();
// This Put is unconditional
var version = client.put("key", "hello".getBytes(), PutOptions.none()).join().getVersion();   
```

### Put — conditional on key absence

```java
try {
  var result = client.put(
        "key", value, PutOption.IfRecordDoesNotExist
  ).join();
} catch (KeyAlreadyExistsException e) {
  ...
}
```

### Put — conditional on version ID

```java
try {
  var result = client.put(
    "key", value, PutOption.ifVersionIdEquals(1L)
  ).join();
} catch (UnexpectedVersionIdException e) {
  ...
}
```

### Put — ephemeral record

```java

var result = client.put(
"key", value, PutOption.AsEphemeralRecord
).join();

```

### Delete — unconditional

```java
var deleted = client.delete("key").join();
```

### Delete — conditional on version ID

```java
try {
  var deleted = client.delete(
    "key", DeleteOption.ifVersionIdEquals(1L)
  ).join();
} catch (UnexpectedVersionIdException e) {
  ...
}
```

### Delete Range

Consult the [Oxia documentation][sort] for detail concerning the ordering of keys.

```java
client.deleteRange("aMinInc", "bMaxExc").join();
```

### List Key Range

Consult the [Oxia documentation][sort] for detail concerning the ordering of keys.

```java
var keys = client.list("aMinInc", "bMaxExc").join();
```

### Subscribe to notifications

A stream of record change notifications that can be consumed.

```java
client.notifications(
  notification -> switch (notification) {
    case KeyCreated c -> System.out.println("Created " + c.key() + ":" + c.version());
    case KeyDeleted d -> System.out.println("Deleted " + d.key());
    case KeyModified m -> System.out.println("Modified " + m.key() + ":" + m.version());
  }
);
```

## Configuration

| Option                   | Default value | Description                                                                |
|:-------------------------|--------------:|:---------------------------------------------------------------------------|
| `serviceAddress`         |             - | Address of Oxia cluster — `<host>:<port>`                                  |
| `requestTimeout`         |           30s | GRPC Request timeout                                                       |
| `batchLinger`            |           5ms | Max time to wait for new operations before closing batch.                  |
| `maxRequestsPerBatch`    |         1,000 | Max number of operations in a batch.                                       |
| `maxBatchSize`           |     1,048,576 | Max byte size of a batch (approximate).                                    |
| `operationQueueCapacity` |         1,000 | Number of operations that can be buffered prior to batching.               |
| `recordCacheCapacity`    |        10,000 | Number or records that can be cached by the client (`0` disables caching). |
| `sessionTimeout`         |           15s | Period of inactivity after which session will be closed.                   |
| `clientIdentifier`       | <random UUID> | String that uniquely identifies this client instance.                      |

[oxia]: https://github.com/streamnative/oxia
[it]: src/test/java/io/streamnative/oxia/client/OxiaClientIT.java
[sort]: https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md

