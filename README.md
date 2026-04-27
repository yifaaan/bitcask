# bitcask

TODO

## HTTP API

Build target `bitcask_http` to run a small HTTP wrapper around the DB:

```sh
bitcask_http --data-dir ./bitcask_data --host 127.0.0.1 --port 8080
```

Available endpoints:

- `GET /v1/health`
- `PUT /v1/kv/<key>` with the value in the raw request body
- `GET /v1/kv/<key>`
- `DELETE /v1/kv/<key>`
- `GET /v1/keys`
- `GET /v1/entries?prefix=<prefix>&reverse=true`
- `GET /v1/stats`
- `POST /v1/sync`
- `POST /v1/merge`
- `POST /v1/backup?dest=<path>`

## License

This project is distributed under the terms of MIT.

See [LICENSE](LICENSE.md) for details.

Copyright 2025 Yifan Liu
