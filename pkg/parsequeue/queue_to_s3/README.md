# ParseQueue vs QueueToS3ParseQueue

**ParseQueue** works with `abstract.AsyncSink` for general sources, while **QueueToS3ParseQueue** works with `abstract.QueueToS3Sink` for queue sources (Kafka, YDS, LB) writing to S3. The key difference is that QueueToS3 returns structured results with offsets for commit operations and supports context-aware S3 file rotation.

