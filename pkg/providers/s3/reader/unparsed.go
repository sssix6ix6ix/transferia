package reader

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	generic_parser "github.com/transferia/transferia/pkg/parsers/generic"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

func HandleParseError(
	tableID abstract.TableID,
	unparsedPolicy s3_model.UnparsedPolicy,
	filePath string,
	lineCounter int,
	parseErr error,
) (*abstract.ChangeItem, error) {
	switch unparsedPolicy {
	case s3_model.UnparsedPolicyFail:
		return nil, abstract.NewFatalError(xerrors.Errorf("unable to parse: %s:%v: %w", filePath, lineCounter, parseErr))
	case s3_model.UnparsedPolicyRetry:
		return nil, xerrors.Errorf("unable to parse: %s:%v: %w", filePath, lineCounter, parseErr)
	default:
		ci := generic_parser.NewUnparsed(
			abstract.NewEmptyPartition(),
			tableID.Name,
			[]byte(fmt.Sprintf("%s:%v", filePath, lineCounter)),
			parseErr.Error(),
			lineCounter,
			0,
			time.Now(),
		)
		return &ci, nil
	}
}
