# singu

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/singu)](https://goreportcard.com/report/github.com/btnguyen2k/singu)
[![GoDoc](https://godoc.org/github.com/btnguyen2k/singu?status.svg)](https://godoc.org/github.com/btnguyen2k/singu)

`singu` designs a unified and simple API to interact with various message queue implementations:
- Enqueue/Dequeue messages.
- Retrieve list of orphan messages.

Latest release [v0.1.1](RELEASE-NOTES.md).

## Queue Usage Flow

- Create an `IQueue` instance. Pre-made implementations can be used out-of-the-box, see below.
- Call `IQueue.Queue(...)` enqueue a message.
- Call `IQueue.Take(...)` to dequeue a message.
- Do something with the message.
  - When done, call `IQueue.Finish(msgId)`
  - If not done and the message need to be re-queued, call `IQueue.Requeue(msgId, ...)` msg) to put back the message to queue.

## Queue Storage Implementation

Queue has 2 message storages:
- _Queue storage_: (required) main storage where messages are enqueued and dequeued. Queue storage is implemented as FIFO list.
- _Ephemeral storage_: (optional) messages taken from queue storage are temporarily stored in ephemeral storage until finished or re-queued.
- `IQueue.Take()` moves the dequeued message from queue to ephemeral storage.
- `IQueue.Finish(...)` removes the message from ephemeral storage.
- On the other hand, `IQueue.Requeue(...)` moves back the message from ephemeral to queue storage.

> Queue implementation is required to implement Queue storage. Ephemeral storage is optional.

The idea of the ephemeral storage is to make sure messages are not lost in the case the
application crashes in between `IQueue.Take()` and `IQueue.Finish(...)` (or `IQueue.Requeue(...)`).

## Orphan Messages

If the application crashes in between `IQueue.Take()` and `IQueue.Finish(...)` (or `IQueue.Requeue(...)`),
there could be orphan messages left in the _ephemeral storage_. To deal with orphan messages:

- Call `IQueue.OrphanMessages(numSeconds, numMessages int) ([]*QueueMessage, error)` to retrieve all messages that have been staying in the _ephemeral storage_ for more than `numSeconds` seconds.
- Call `IQueue.Finish(...)` on each message to completely remove the orphan message, or
- Call `IQueue.Requeue(...)` to re-queue the message.

## Built-in Queue Implementations

| Implementation | Bounded Size | Persistent | Ephemeral Storage | Multi-Clients |
|----------------|:------------:|:----------:|:-----------------:|:-------------:|
| In-memory      | Optional     | No         | Yes               | No            |
| LevelDB        | Optional     | Yes (*)    | Yes               | No            |

- *Bounded Size*: size of queue/ephemeral storage is bounded.
  - Queue implementation can set a hard limit on maximum number of messages can be stored in queue/ephemeral storage.
  - If no hard limit is set:
    - In-memory queue: number of messages is limited by memory capacity.
    - LevelDB queue: number of messages is limited by disk capacity.
- *Persistent*: queue messages are persistent between application restarts.
- *Ephemeral Storage*: supports retrieval of orphan messages.
- *Multi-Clients*: multi-clients can share a same queue backend storage.

### In-memory Queue

The built-in [in-memory queue implementation](https://godoc.org/github.com/btnguyen2k/singu#InmemQueue)
implements queue storage using a FIFO linked-list and ephemeral storage using a map.

> Messages in in-memory queues are _not_ persistent between application restarts.

### LevelDB Queue

[![GoDoc](https://godoc.org/github.com/btnguyen2k/singu/leveldb?status.svg)](https://godoc.org/github.com/btnguyen2k/singu/leveldb)

The built-in [LevelDB queue implementation](https://godoc.org/github.com/btnguyen2k/singu/leveldb#LeveldbQueue)
uses [LevelDB](https://github.com/google/leveldb) as storage backend.

> Messages in LevelDB queues are _not_ persistent between application restarts.

## License

MIT - see [LICENSE.md](LICENSE.md).
