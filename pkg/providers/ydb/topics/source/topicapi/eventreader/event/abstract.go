package event

type Event interface {
	isEvent()
}

type PartitionInfo struct {
	TopicPath   string
	PartitionID int64
	LastOffset  int64
}
