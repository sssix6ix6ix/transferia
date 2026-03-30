package parquet

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	parquet_format "github.com/parquet-go/parquet-go/format"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
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

var (
	_ s3_reader.Reader             = (*ReaderParquet)(nil)
	_ s3_reader.RowsCountEstimator = (*ReaderParquet)(nil)
)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatPARQUET, NewParquet)
}

type ReaderParquet struct {
	table          abstract.TableID
	bucket         string
	client         s3iface.S3API
	logger         log.Logger
	tableSchema    *abstract.TableSchema
	colNames       []string
	hideSystemCols bool
	batchSize      int
	pathPrefix     string
	pathPattern    string
	metrics        *stats.SourceStats
	s3RawReader    s3raw.S3RawReader
}

func (r *ReaderParquet) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	meta, err := r.openReader(ctx, *obj.Key)
	if err != nil {
		return 0, xerrors.Errorf("unable to read file meta: %s: %w", *obj.Key, err)
	}
	defer meta.Close()

	return uint64(meta.NumRows()), nil
}

func (r *ReaderParquet) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	res := uint64(0)
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, r.ObjectsFilter())
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list: %w", err)
	}
	for i, file := range files {
		meta, err := r.openReader(ctx, *file.Key)
		if err != nil {
			return 0, xerrors.Errorf("unable to read file meta: %s: %w", *file.Key, err)
		}
		res += uint64(meta.NumRows())
		_ = meta.Close()
		// once we reach limit of files to estimate - stop and approximate
		if i > s3_reader.EstimateFilesLimit {
			break
		}
	}
	if len(files) > s3_reader.EstimateFilesLimit {
		multiplier := float64(len(files)) / float64(s3_reader.EstimateFilesLimit)
		return uint64(float64(res) * multiplier), nil
	}
	return res, nil
}

func (r *ReaderParquet) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	if r.tableSchema != nil && len(r.tableSchema.Columns()) != 0 {
		return r.tableSchema, nil
	}

	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, aws.Int(1), r.ObjectsFilter())
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	if len(files) < 1 {
		return nil, xerrors.Errorf("unable to resolve schema, no parquet files found for preifx '%s'", r.pathPrefix)
	}

	return r.resolveSchema(ctx, *files[0].Key)
}

func (r *ReaderParquet) ObjectsFilter() s3_reader.ObjectsFilter {
	return s3_reader.IsNotEmpty
}

func (r *ReaderParquet) resolveSchema(ctx context.Context, filePath string) (*abstract.TableSchema, error) {
	meta, err := r.openReader(ctx, filePath)
	if err != nil {
		return nil, xerrors.Errorf("unable to read meta: %s: %w", filePath, err)
	}
	defer meta.Close()
	var cols []abstract.ColSchema
	for _, el := range meta.Schema().Fields() {
		if el.Type() == nil {
			continue
		}
		typ := ytschema.TypeAny
		if el.Type().PhysicalType() != nil {
			switch *el.Type().PhysicalType() {
			case parquet_format.Boolean:
				typ = ytschema.TypeBoolean
			case parquet_format.Int32:
				typ = ytschema.TypeInt32
			case parquet_format.Int64:
				typ = ytschema.TypeInt64
			case parquet_format.Float:
				typ = ytschema.TypeFloat32
			case parquet_format.Double:
				typ = ytschema.TypeFloat64
			case parquet_format.Int96:
				typ = ytschema.TypeString
			case parquet_format.ByteArray, parquet_format.FixedLenByteArray:
				typ = ytschema.TypeBytes
			default:
			}
		}
		if el.Type().LogicalType() != nil {
			lt := el.Type().LogicalType()
			switch {
			case lt.Date != nil:
				typ = ytschema.TypeDate
			case lt.UTF8 != nil:
				typ = ytschema.TypeString
			case lt.Integer != nil:
				if lt.Integer.IsSigned {
					typ = ytschema.TypeInt64
				} else {
					typ = ytschema.TypeUint64
				}
			case lt.Decimal != nil:
				if lt.Decimal.Precision > 8 {
					typ = ytschema.TypeString
				} else {
					typ = ytschema.TypeFloat64
				}
			case lt.Timestamp != nil:
				typ = ytschema.TypeTimestamp
			case lt.UUID != nil:
				typ = ytschema.TypeString
			case lt.Enum != nil:
				typ = ytschema.TypeString
			}
		}
		if el.Type().ConvertedType() != nil {
			switch *el.Type().ConvertedType() {
			case deprecated.UTF8:
				typ = ytschema.TypeString
			case deprecated.Date:
				typ = ytschema.TypeDate
			case deprecated.Decimal:
				typ = ytschema.TypeFloat64
			}
		}
		col := abstract.NewColSchema(el.Name(), typ, false)
		col.OriginalType = fmt.Sprintf("parquet:%s", el.Type().String())
		cols = append(cols, col)
	}

	return abstract.NewTableSchema(cols), nil
}

func (r *ReaderParquet) openReader(ctx context.Context, filePath string) (*parquet.Reader, error) {
	sr, err := s3raw.NewS3RawReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader at: %w", err)
	}
	r.s3RawReader = sr

	// For compressed files, parquet-go requires correct Size() which should return uncompressed size.
	// wrappedReader.Size() returns compressed size from S3 metadata, which breaks footer reading.
	// Solution: if reader implements ReaderAll (i.e., it's a compressed file wrapper),
	// load the full uncompressed content and create bytes.Reader.
	if readerAll, ok := sr.(s3raw.ReaderAll); ok {
		data, err := readerAll.ReadAll()
		if err != nil {
			return nil, xerrors.Errorf("unable to read full object for parquet: %w", err)
		}
		return parquet.NewReader(bytes.NewReader(data)), nil
	}

	return parquet.NewReader(sr), nil
}

func (r *ReaderParquet) Read(ctx context.Context, filePath string, pusher s3_pusher.Pusher) error {
	pr, err := r.openReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open file: %w", err)
	}
	defer pr.Close()
	rowCount := uint64(pr.NumRows())
	r.logger.Infof("part: %s extracted row count: %v", filePath, rowCount)
	var buff []abstract.ChangeItem

	rowFields := map[string]parquet.Field{}
	for _, field := range pr.Schema().Fields() {
		rowFields[field.Name()] = field
	}
	r.logger.Infof("schema: \n%s", pr.Schema())

	var currentSize int64
	for i := uint64(0); i < rowCount; {
		if ctx.Err() != nil {
			r.logger.Info("Read canceled")
			return nil
		}
		row := map[string]any{}
		if err := pr.Read(&row); err != nil {
			return xerrors.Errorf("unable to read row: %w", err)
		}
		i += 1
		ci, err := r.constructCI(rowFields, row, filePath, r.s3RawReader.LastModified(), i)
		if err != nil {
			return xerrors.Errorf("unable to construct change item: %w", err)
		}
		currentSize += int64(ci.Size.Values)
		buff = append(buff, ci)
		if len(buff) > r.batchSize {
			if err := s3_reader.FlushChunk(ctx, filePath, i, currentSize, buff, pusher); err != nil {
				return xerrors.Errorf("unable to push parquet batch: %w", err)
			}
			currentSize = 0
			buff = []abstract.ChangeItem{}
		}
	}
	if err := s3_reader.FlushChunk(ctx, filePath, rowCount, currentSize, buff, pusher); err != nil {
		return xerrors.Errorf("unable to push parquet last batch: %w", err)
	}

	return nil
}

func (r *ReaderParquet) constructCI(parquetSchema map[string]parquet.Field, row map[string]any, fname string,
	lModified time.Time, idx uint64,
) (abstract.ChangeItem, error) {
	vals := make([]interface{}, len(r.tableSchema.Columns()))
	for i, col := range r.tableSchema.Columns() {
		if s3_reader.SystemColumnNames[col.ColumnName] {
			if r.hideSystemCols {
				continue
			}
			switch col.ColumnName {
			case s3_reader.FileNameSystemCol:
				vals[i] = fname
			case s3_reader.RowIndexSystemCol:
				vals[i] = idx
			default:
				continue
			}
			continue
		}
		val, ok := row[col.ColumnName]
		if !ok {
			vals[i] = nil
		} else {
			vals[i] = r.parseParquetField(parquetSchema[col.ColumnName], val, col)
		}
	}

	return abstract.ChangeItem{
		ID:               0,
		LSN:              0,
		CommitTime:       uint64(lModified.UnixNano()),
		Counter:          int(idx),
		Kind:             abstract.InsertKind,
		Schema:           r.table.Namespace,
		Table:            r.table.Name,
		PartID:           fname,
		ColumnNames:      r.colNames,
		ColumnValues:     vals,
		TableSchema:      r.tableSchema,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.RawEventSize(util.DeepSizeof(vals)),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}, nil
}

func (r *ReaderParquet) parseLogicalDate(field parquet.Field, val any) any {
	switch {
	case field.Type().LogicalType().Date != nil:
		switch v := val.(type) {
		case int32:
			// handle logical int32 variations:
			if field.Type().LogicalType().Date != nil {
				return time.Unix(0, 0).Add(24 * time.Duration(v) * time.Hour)
			}
		}
	}
	return val
}

func (r *ReaderParquet) parseParquetField(field parquet.Field, val interface{}, col abstract.ColSchema) interface{} {
	if field == nil || field.Type() == nil {
		return val
	}
	if legacyInt96, ok := val.(deprecated.Int96); ok {
		return legacyInt96.String()
	}
	if field.Type().LogicalType() != nil {
		switch {
		case field.Type().LogicalType().Date != nil:
			return r.parseLogicalDate(field, val)
		}
	}
	if field.Type().ConvertedType() != nil {
		switch *field.Type().ConvertedType() {
		case deprecated.Date:
			return r.parseLogicalDate(field, val)
		}
	}
	return abstract.Restore(col, val)
}

func (r *ReaderParquet) ParsePassthrough(chunk s3_pusher.Chunk) []abstract.ChangeItem {
	// the most complex and useful method in the world
	return chunk.Items
}

func NewParquet(src *s3_model.S3Source, lgr log.Logger, sess *aws_session.Session, metrics *stats.SourceStats) (s3_reader.Reader, error) {
	if src == nil {
		return nil, xerrors.New("uninitialized settings for parquet reader")
	}
	reader := &ReaderParquet{
		bucket:         src.Bucket,
		hideSystemCols: src.HideSystemCols,
		batchSize:      src.ReadBatchSize,
		pathPrefix:     src.PathPrefix,
		pathPattern:    src.PathPattern,
		client:         aws_s3.New(sess),
		logger:         lgr,
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		tableSchema: abstract.NewTableSchema(src.OutputSchema),
		colNames:    nil,
		metrics:     metrics,
		s3RawReader: nil,
	}

	if len(reader.tableSchema.Columns()) == 0 {
		var err error
		reader.tableSchema, err = reader.ResolveSchema(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve schema: %w", err)
		}
	}

	// append system columns at the end if necessary
	if !reader.hideSystemCols {
		cols := reader.tableSchema.Columns()
		userDefinedSchemaHasPkey := reader.tableSchema.Columns().HasPrimaryKey()
		reader.tableSchema = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
	}

	reader.colNames = yslices.Map(reader.tableSchema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })
	return reader, nil
}
