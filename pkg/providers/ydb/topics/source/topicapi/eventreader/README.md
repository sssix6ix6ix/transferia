# EventReader

A wrapper over YDB TopicListener for ordered reading of topic events. The package hides the complexity of working with callbacks and provides a simple sequential interface `NextEvent()` for receiving events.

## Event Types
- **StartEvent** - partition assigned for reading (requires `Confirm()`)
- **ReadEvent** - batch of messages from a single partition
- **StopEvent** - partition is being released (requires `Confirm()`)

## Reading
### One Partition
```mermaid
    NextEvent() <-- StartEvent <-- ReadEvent <-- ... <-- ReadEvent <-- StopEvent
```

### Few Partitions Example
```mermaid
    NextEvent() <-- StartEvent: p3 <-- StartEvent: p1  <-- ReadEvent: p1 <-- StartEvent: p2 <-- ReadEvent: p1 <-- StopEvent: p1 <-- ReadEvent: p2 <-- StopEvent: p2 <-- StopEvent: p3
```

## Guarantees
* Events for a single partition are delivered strictly sequentially
* No other events for a partition can arrive before **StartEvent**
* No other events for a partition can arrive after **StopEvent**
* If **StartEvent** is not confirmed, partition reading will not start
