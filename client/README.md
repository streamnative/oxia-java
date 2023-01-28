# Oxia Java Client

A JDK based client for [Oxia][oxia].

## Features

- Request batching
- Asynchronous operation
- Notifications callback

## Usage

```java
 var client = new OxiaClientBuilder("localhost:6648").asyncClient();
 var value = client.get("key").join().getValue();
 var version = client.put("key", "hello".getBytes()).join().getVersion();   

```

[oxia]: https://github.com/streamnative/oxia

