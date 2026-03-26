// Package raw_to_table registers parsers that turn raw queue messages into table rows
// (topic, partition, offset, optional timestamp/headers/key, and a typed value column).
//
// Messages that fail validation for the configured types are routed to a dead-letter (DLQ) table
// whose name is the main table name plus DLQSuffix (see ParserConfigRawToTableCommon and
// ParserConfigRawToTableLb). The DLQ sink does not mirror the main table toggles: it always
// writes timestamp, headers, key and value as raw bytes, plus failure_reason, so the original
// payload stays inspectable regardless of IsTimestampEnabled, IsHeadersEnabled, IsKeyEnabled,
// KeyType, or ValueType on the primary sink.
package raw_to_table
