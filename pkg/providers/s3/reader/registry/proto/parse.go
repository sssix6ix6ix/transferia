package proto

import (
	"context"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
)

const perPushBatchSize = 15 * humanize.MiByte

func readFileAndParse(ctx context.Context, r *ProtoReader, filePath string, pusher s3_pusher.Pusher) error {
	s3RawReader, err := r.newS3RawReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open reader: %w", err)
	}

	if s3RawReader.Size() > perPushBatchSize {
		chunkReader := s3_reader.NewChunkReader(s3RawReader, perPushBatchSize, r.logger)
		defer chunkReader.Close()
		return streamParseFile(ctx, r, filePath, chunkReader, pusher, s3RawReader.LastModified())
	}

	fullFile, err := s3raw.ReadWholeFile(ctx, s3RawReader, r.blockSize)
	if err != nil {
		return xerrors.Errorf("unable to read whole file: %w", err)
	}
	msg := constructMessage(s3RawReader.LastModified(), fullFile, []byte(filePath))
	parser, err := r.parserBuilder.BuildLazyParser(msg, abstract.NewEmptyPartition())
	if err != nil {
		return xerrors.Errorf("unable to prepare parser: %w", err)
	}

	var buff []abstract.ChangeItem
	buffSize := int64(0)

	for item := parser.Next(); item != nil; item = parser.Next() {
		if r.unparsedPolicy == s3_model.UnparsedPolicyFail {
			if err := parsers.VerifyUnparsed(*item); err != nil {
				return abstract.NewFatalError(xerrors.Errorf("unable to parse: %w", err))
			}
		}
		buff = append(buff, *item)
		buffSize += int64(item.Size.Read)
		if item.Size.Read == 0 {
			r.logger.Warn("Got item with 0 raw read size")
			buffSize += 64 * humanize.KiByte
		}
		if buffSize > perPushBatchSize {
			if err := s3_reader.FlushChunk(ctx, filePath, 0, buffSize, buff, pusher); err != nil {
				return xerrors.Errorf("unable to push batch: %w", err)
			}
			buff = nil
			buffSize = 0
		}
	}

	if len(buff) > 0 {
		if err := s3_reader.FlushChunk(ctx, filePath, 0, buffSize, buff, pusher); err != nil {
			return xerrors.Errorf("unable to push last batch: %w", err)
		}
	}
	return nil
}
