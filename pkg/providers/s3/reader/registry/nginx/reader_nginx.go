package reader

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const timeLocalLayout = "02/Jan/2006:15:04:05 -0700"

var (
	_ s3_reader.Reader             = (*NginxReader)(nil)
	_ s3_reader.RowsCountEstimator = (*NginxReader)(nil)
)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatNginx, NewNginxReader)
}

type NginxReader struct {
	table                   abstract.TableID
	bucket                  string
	client                  s3iface.S3API
	logger                  log.Logger
	metrics                 *stats.SourceStats
	tableSchema             *abstract.TableSchema
	fastCols                abstract.FastTableSchema
	colNames                []string
	hideSystemCols          bool
	batchSize               int
	blockSize               int64
	pathPrefix              string
	pathPattern             string
	unparsedPolicy          s3_model.UnparsedPolicy
	unexpectedFieldBehavior s3_model.NginxUnexpectedFieldBehavior

	compiledFormat *compiledNginxFormat
}

func (r *NginxReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	if r.tableSchema != nil && len(r.tableSchema.Columns()) != 0 {
		return r.tableSchema, nil
	}
	return abstract.NewTableSchema(r.compiledFormat.schema), nil
}

func (r *NginxReader) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	sr, err := s3raw.NewS3RawReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create S3 reader for %s: %w", filePath, err)
	}
	return sr, nil
}

func (r *NginxReader) Read(ctx context.Context, filePath string, pusher s3_pusher.Pusher) error {
	s3RawReader, err := r.newS3RawReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open reader: %w", err)
	}

	lineCounter := uint64(1)
	chunkReader := s3_reader.NewChunkReader(s3RawReader, int(r.blockSize), r.logger)
	defer chunkReader.Close()

	for lastRound := false; !lastRound; {
		if ctx.Err() != nil {
			r.logger.Info("Read canceled")
			return nil
		}

		if err := chunkReader.ReadNextChunk(); err != nil {
			return xerrors.Errorf("failed to read from file: %w", err)
		}

		if chunkReader.IsEOF() {
			if len(chunkReader.Data()) == 0 {
				break
			}
			lastRound = true
		}

		data := string(chunkReader.Data())
		var buff []abstract.ChangeItem
		var currentSize int64

		// Find the last newline to determine the boundary of complete lines.
		lastNewlinePos := strings.LastIndexByte(data, '\n')
		var processable string
		if lastNewlinePos >= 0 {
			processable = data[:lastNewlinePos+1]
		} else if lastRound {
			processable = data
		} else {
			// No complete line in this chunk, save everything for the next chunk.
			chunkReader.FillBuffer([]byte(data))
			if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
				return xerrors.Errorf("unable to push nginx last batch: %w", err)
			}
			continue
		}

		for line := range strings.SplitSeq(processable, "\n") {
			line = strings.TrimRight(line, "\r")
			if strings.TrimSpace(line) == "" {
				continue
			}

			fieldValues, consumed, err := r.compiledFormat.parseEntry(line)
			if err != nil {
				unparsedCI, handleErr := s3_reader.HandleParseError(
					r.table, r.unparsedPolicy, filePath, int(lineCounter), err,
				)
				if handleErr != nil {
					return xerrors.Errorf("failed to parse nginx log entry %d: %w", lineCounter, handleErr)
				}
				buff = append(buff, *unparsedCI)
				lineCounter++
				continue
			}

			if err := checkUnexpectedFields(line, consumed, r.unexpectedFieldBehavior); err != nil {
				ci, handleErr := s3_reader.HandleParseError(r.table, r.unparsedPolicy, filePath, int(lineCounter), err)
				if handleErr != nil {
					return xerrors.Errorf("failed to parse nginx log entry %d: %w", lineCounter, handleErr)
				}
				buff = append(buff, *ci)
				lineCounter++
				continue
			}

			currentSize += int64(len(line))

			ci, err := r.buildCI(fieldValues, filePath, s3RawReader.LastModified(), lineCounter)
			if err != nil {
				unparsedCI, handleErr := s3_reader.HandleParseError(
					r.table, r.unparsedPolicy, filePath, int(lineCounter), err,
				)
				if handleErr != nil {
					return xerrors.Errorf("failed to build change item for entry %d: %w", lineCounter, handleErr)
				}
				buff = append(buff, *unparsedCI)
				lineCounter++
				continue
			}

			lineCounter++
			buff = append(buff, *ci)

			if len(buff) > r.batchSize {
				if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
					return xerrors.Errorf("unable to push nginx batch: %w", err)
				}
				currentSize = 0
				buff = nil
			}
		}

		// Save incomplete trailing line (after last \n) for the next chunk.
		if lastNewlinePos >= 0 && lastNewlinePos+1 < len(data) {
			chunkReader.FillBuffer([]byte(data[lastNewlinePos+1:]))
		} else {
			chunkReader.FillBuffer(nil)
		}

		if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
			return xerrors.Errorf("unable to push nginx last batch: %w", err)
		}
	}

	return nil
}

func (r *NginxReader) buildCI(fieldValues []string, filePath string, lastModified time.Time, lineCounter uint64) (*abstract.ChangeItem, error) {
	ci, err := r.constructCI(fieldValues, filePath, lastModified, lineCounter)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct change item: %w", err)
	}

	if err := strictify.Strictify(ci, r.fastCols); err != nil {
		return nil, xerrors.Errorf("failed to convert value to expected data type: %w", err)
	}

	return ci, nil
}

func (r *NginxReader) constructCI(fieldValues []string, fname string, lModified time.Time, rowNumber uint64) (*abstract.ChangeItem, error) {
	vals := make([]any, len(r.tableSchema.Columns()))
	for i, col := range r.tableSchema.Columns() {
		if s3_reader.SystemColumnNames[col.ColumnName] {
			if r.hideSystemCols {
				continue
			}
			switch col.ColumnName {
			case s3_reader.FileNameSystemCol:
				vals[i] = fname
			case s3_reader.RowIndexSystemCol:
				vals[i] = rowNumber
			}
			continue
		}

		index, err := strconv.Atoi(col.Path)
		if err != nil {
			return nil, xerrors.Errorf("failed to get index of column %s: %w", col.ColumnName, err)
		}
		if index < 0 || index >= len(fieldValues) {
			vals[i] = abstract.DefaultValue(&col)
			continue
		}

		converted, err := convertNginxValue(fieldValues[index], col)
		if err != nil {
			return nil, xerrors.Errorf("column %s: %w", col.ColumnName, err)
		}
		vals[i] = converted
	}

	return &abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   uint64(lModified.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       r.table.Namespace,
		Table:        r.table.Name,
		PartID:       fname,
		ColumnNames:  r.colNames,
		ColumnValues: vals,
		TableSchema:  r.tableSchema,
		OldKeys:      abstract.EmptyOldKeys(),
		Size:         abstract.RawEventSize(util.DeepSizeof(vals)),
		TxID:         "",
		Query:        "",
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    "",
			PartitionNum: 0,
			Offset:       0,
			Index:        0,
		},
	}, nil
}

// checkUnexpectedFields returns an error if the line has unconsumed non-whitespace content
// and the behavior is set to Error. Returns nil for Ignore/Unspecified or when there are no extra fields.
func checkUnexpectedFields(line string, consumed int, behavior s3_model.NginxUnexpectedFieldBehavior) error {
	switch behavior {
	case s3_model.NginxUnexpectedFieldBehaviorError:
		if consumed < len(line) && strings.TrimSpace(line[consumed:]) != "" {
			return xerrors.Errorf("unexpected extra fields after parsed entry: %q", strings.TrimSpace(line[consumed:]))
		}
	}
	return nil
}

// convertNginxValue converts a string value to the appropriate Go type based on the column schema.
// Datetime fields (time_local) require explicit parsing; other types are handled by strictify.
// A bare "-" is treated as a missing value and returns nil (ClickHouse will apply its column
// default via insert_null_as_default, same as JSON reader does for null fields).
func convertNginxValue(value string, col abstract.ColSchema) (any, error) {
	if value == "-" {
		return nil, nil
	}
	switch col.DataType {
	case ytschema.TypeDatetime.String(), ytschema.TypeDate.String():
		// Common strictify.Strictify uses spf13/cast.ToTimeE, which does not know timeLocalLayout.
		t, err := time.Parse(timeLocalLayout, value)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse datetime %q for column %s: %w", value, col.ColumnName, err)
		}
		return t, nil
	default:
		return value, nil
	}
}

func (r *NginxReader) ParsePassthrough(chunk s3_pusher.Chunk) []abstract.ChangeItem {
	return chunk.Items
}

func (r *NginxReader) ObjectsFilter() s3_reader.ObjectsFilter {
	return s3_reader.IsNotEmpty
}

// Row count estimation

func (r *NginxReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, r.ObjectsFilter())
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list: %w", err)
	}
	return r.estimateRows(ctx, files)
}

func (r *NginxReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	return r.estimateRows(ctx, []*aws_s3.Object{obj})
}

func (r *NginxReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	totalSize, sampleReader, err := s3_reader.EstimateTotalSize(ctx, r.logger, files, r.newS3RawReader)
	if err != nil {
		return 0, xerrors.Errorf("unable to estimate rows: %w", err)
	}
	if totalSize > 0 && sampleReader != nil {
		chunkReader := s3_reader.NewChunkReader(sampleReader, int(r.blockSize), r.logger)
		defer chunkReader.Close()
		if err := chunkReader.ReadNextChunk(); err != nil && !xerrors.Is(err, io.EOF) {
			return 0, xerrors.Errorf("failed to estimate row count: %w", err)
		}
		if len(chunkReader.Data()) > 0 {
			data := string(chunkReader.Data())
			entries := 0
			for line := range strings.SplitSeq(data, "\n") {
				line = strings.TrimRight(line, "\r")
				if strings.TrimSpace(line) == "" {
					continue
				}
				if _, _, err := r.compiledFormat.parseEntry(line); err != nil {
					break
				}
				entries++
			}
			if entries > 0 {
				bytesPerEntry := float64(len(data)) / float64(entries)
				return uint64(math.Ceil(float64(totalSize) / bytesPerEntry)), nil
			}
		}
	}
	return 0, nil
}

func ValidateFormat(format string) error {
	_, err := compileFormat(format)
	return err
}

func NewNginxReader(src *s3_model.S3Source, lgr log.Logger, sess *aws_session.Session, metrics *stats.SourceStats) (s3_reader.Reader, error) {
	if src == nil || src.Format.NginxSetting == nil {
		return nil, xerrors.New("uninitialized settings for nginx reader")
	}

	compiled, err := compileFormat(src.Format.NginxSetting.Format)
	if err != nil {
		return nil, xerrors.Errorf("failed to compile nginx format: %w", err)
	}

	reader := &NginxReader{
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:                  src.Bucket,
		client:                  aws_s3.New(sess),
		logger:                  lgr,
		metrics:                 metrics,
		tableSchema:             abstract.NewTableSchema(src.OutputSchema),
		fastCols:                abstract.NewTableSchema(src.OutputSchema).FastColumns(),
		colNames:                nil,
		hideSystemCols:          src.HideSystemCols,
		batchSize:               src.ReadBatchSize,
		blockSize:               src.Format.NginxSetting.BlockSize,
		pathPrefix:              src.PathPrefix,
		pathPattern:             src.PathPattern,
		unparsedPolicy:          src.UnparsedPolicy,
		unexpectedFieldBehavior: src.Format.NginxSetting.UnexpectedFieldBehavior,
		compiledFormat:          compiled,
	}

	if len(reader.tableSchema.Columns()) == 0 {
		reader.tableSchema, err = reader.ResolveSchema(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve schema: %w", err)
		}
	} else {
		fieldIndex := make(map[string]int, len(compiled.fields))
		for i, name := range compiled.fields {
			fieldIndex[name] = i
		}
		var cols []abstract.ColSchema
		for _, col := range reader.tableSchema.Columns() {
			if col.Path == "" {
				idx, ok := fieldIndex[col.ColumnName]
				if !ok {
					continue
				}
				col.Path = fmt.Sprintf("%d", idx)
			}
			if col.OriginalType == "" {
				col.OriginalType = fmt.Sprintf("nginx:%s", col.DataType)
			}
			cols = append(cols, col)
		}
		reader.tableSchema = abstract.NewTableSchema(cols)
	}

	if !reader.hideSystemCols {
		cols := reader.tableSchema.Columns()
		hasPkey := reader.tableSchema.Columns().HasPrimaryKey()
		reader.tableSchema = s3_reader.AppendSystemColsTableSchema(cols, !hasPkey)
	}

	reader.colNames = yslices.Map(reader.tableSchema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })
	reader.fastCols = reader.tableSchema.FastColumns()

	return reader, nil
}
