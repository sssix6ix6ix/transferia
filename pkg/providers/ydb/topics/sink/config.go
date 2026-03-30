package topicsink

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type Config struct {
	Connection topiccommon.ConnectionConfig

	Topic            string
	TopicPrefix      string
	CompressionCodec CompressionCodec
	FormatSettings   model.SerializationFormat

	Shard string

	AddSystemTables bool
	SaveTxOrder     bool
}

type CompressionCodec string

const (
	CompressionCodecUnspecified CompressionCodec = ""
	CompressionCodecRaw         CompressionCodec = "raw"
	CompressionCodecGzip        CompressionCodec = "gzip"
	CompressionCodecZstd        CompressionCodec = "zstd"
)

func (e CompressionCodec) ToTopicTypesCodec() topictypes.Codec {
	switch e {
	case CompressionCodecGzip:
		return topictypes.CodecGzip
	case CompressionCodecZstd:
		return topictypes.CodecZstd
	default:
		return topictypes.CodecRaw
	}
}
