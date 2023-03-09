# Performance Test Tool

## Usage

```commandline
Usage: oxia-java perf [options]
  Options:
    --batch-linger-ms
      Batch linger time
      Default: 5
    -h, --help
      Help message
    -k, --keys-cardinality
      Number of unique keys
      Default: 1000
    --max-requests-per-batch
      Maximum requests per batch
      Default: 1000
    -r, --rate
      Request rate, ops/s
      Default: 100.0
    -p, --read-write-percent
      Percentage of read requests, compared to total requests
      Default: 80.0
    --request-timeout-ms
      Requests timeout
      Default: 30000
    -a, --service-addr
      Oxia Service Address
      Default: localhost:6648
    -s, --value-size
      Size of the values to write
      Default: 128
```

