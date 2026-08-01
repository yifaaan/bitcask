# Bitcask

Bitcask is an embedded key-value store written in Go. It follows the Bitcask
design of append-only data files and an in-memory index that points each key to
its latest record on disk.

## Current Status

This project is under active development. The implementation is currently
being built from the file IO layer up to the database layer.

Implemented:

- Go module and package layout.
- `fio.FileIO` with append writes, offset-based reads, sync, close, and file
  size operations.
- `data.DataFile` with data-file naming, write-offset tracking, and the record
  read path.
- A B-tree index backed by `github.com/google/btree`, with synchronized update
  operations.
- Database startup flow: create the data directory, discover existing data
  files, select the active file, and rebuild the in-memory index.
- `DB.Put`, `DB.Get`, and `DB.Delete` API scaffolding, including key
  validation and data-file rotation logic.

Still in progress:

- `data.EncodeLogRecord`, `decodeLogRecordHeader`, and `getLogRecordCRC` are
  not implemented yet. They currently return zero or nil values.
- The incomplete record codec means that end-to-end writes, reads, and crash
  recovery are not production-ready.
- ART index support is declared but not implemented.
- Database-level integration tests, close lifecycle handling, and complete
  persistence tests are still needed.
- There is no command-line tool or server yet.

## Architecture

The current write path is:

```text
DB -> DataFile -> IOManager -> FileIO -> os.File
```

The index stores the latest position for each key:

```text
key -> { file id, file offset }
```

The intended record layout is an encoded header followed by the key and value:

```text
CRC | record type | key size | value size | key | value
```

The size fields use varint encoding. Delete operations are represented by a
delete record and remove the key from the in-memory index during recovery.

## Repository Layout

```text
.
|-- db.go                 Database open, recovery, and key operations
|-- options.go            Database options
|-- errors.go             Package errors
|-- data/
|   |-- data_file.go      Append-only data files
|   `-- log_record.go     Record types and codec
|-- fio/
|   |-- file_io.go        os.File implementation
|   `-- io_manager.go     IO abstraction
`-- index/
    |-- index.go          Index interface and types
    `-- btree.go          B-tree index implementation
```

## Requirements

- Go 1.24.5 or newer

## Run Tests

```bash
go test ./...
```

## License

This project is distributed under the terms of the MIT License.

See [LICENSE](LICENSE.md) for details.

Copyright 2025 Yifan Liu
