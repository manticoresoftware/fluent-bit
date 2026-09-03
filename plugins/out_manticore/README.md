# Manticore Search output

The `manticore` output sends log records to the native Manticore Search
[`/bulk`](https://manual.manticoresearch.com/Data_creation_and_modification/Adding_documents_to_a_table/Adding_documents_to_a_real-time_table#Chunked-transfer-in-/bulk)
endpoint as newline-delimited JSON over HTTP/1.1 chunked transfer encoding.

The plugin formats each Fluent Bit record as one Manticore operation:

```json
{"replace":{"table":"logs","id":42,"doc":{"message":"hello","status":200}}}
```

It converts records incrementally and buffers at most `stream_chunk_size` bytes
before writing an HTTP chunk. A record larger than that limit is sent as its own
chunk. The plugin never builds the complete NDJSON request body in memory.

## Requirements

The target table must exist before Fluent Bit sends data. Manticore's native
`/bulk` endpoint does not create tables automatically.

For retry-safe delivery, every record should contain a stable ID in `id_key` and
the default `replace` action should be used. If a record has no such key,
Manticore generates an ID; replaying that record after a network failure can
then create a duplicate.

## Configuration

```ini
[OUTPUT]
    Name               manticore
    Match              *
    Host               manticore
    Port               9308
    Table              logs
    Action             replace
    Id_Key             id
    Stream_Chunk_Size  64K
```

TLS uses the standard Fluent Bit output options:

```ini
    TLS        On
    TLS.Verify On
```

HTTP Basic authentication is available through `HTTP_User` and `HTTP_Passwd`.

| Option | Description | Default |
|---|---|---|
| `table` | Existing target Manticore table. Required. | none |
| `action` | Native `/bulk` action: `insert` or `replace`. | `replace` |
| `id_key` | Top-level record key moved to the operation's `id`; it is removed from `doc`. | `id` |
| `stream_chunk_size` | Maximum NDJSON bytes buffered before an HTTP chunk is written. A single larger record is sent separately. | `64K` |
| `buffer_size` | Maximum buffer used to read the Manticore response. | `64K` |
| `http_user` | HTTP Basic authentication user. | none |
| `http_passwd` | HTTP Basic authentication password. | empty |

## Response and retry behavior

- HTTP `2xx` with `"errors": false`: the Fluent Bit chunk is acknowledged.
- HTTP `2xx` with `"errors": true` and any item status `408`, `429`, or
  `5xx`: Fluent Bit retries the whole chunk.
- HTTP `408`, `429`, or `5xx`, and transport failures: Fluent Bit retries the
  whole chunk.
- Other HTTP `4xx`, or HTTP `2xx` with only permanent item errors: the chunk
  is rejected as a permanent error.

A mixed `/bulk` response can contain successful and transiently failed items.
Fluent Bit can only retry its original chunk, so successful items are replayed.
Stable IDs plus the default `replace` action make that replay idempotent. Using
`insert` trades that safety for strict insert semantics.
