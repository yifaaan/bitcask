# bitcask

A C++20 implementation of the [Bitcask](https://riak.com/assets/bitcask-intro.pdf) log-structured key-value storage engine.

All writes are appended sequentially to data files, with an in-memory index mapping every key to its on-disk position. The read path checks the index first, then seeks directly into the data file.

## License

This project is distributed under the terms of the MIT license.

Copyright 2025 Yifan Liu
