package parsequeue

// WaitableQueue unifies standard (AsyncSink) and queue-to-S3 (QueueToS3Sink) replication modes,
// allowing sources to work with both WaitableQueue implementations through a common interface.
type WaitableQueue[TData any] interface {
	Add(data TData) error
	Close()
	Wait()
}
