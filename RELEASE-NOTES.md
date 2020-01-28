# singu release notes

## 2020-01-27 - v0.1.1

- (Breaking change) Signature of function `IQueue.OrphanMessage` changed to `OrphanMessages(numSeconds, numMessages int) ([]*QueueMessage, error)`


## 2020-01-25 - v0.1.0

First release:

- Queue message struct and queue API.
- In-memory queue implementation.
- Queue implementation using [LevelDB](https://github.com/google/leveldb) as backend storage.
