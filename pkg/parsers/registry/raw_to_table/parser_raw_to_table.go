package raw_to_table

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/raw_to_table/engine"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserRawToTable(inWrapped any, sniff bool, logger log.Logger, registry *stats.SourceStats) (parsers.Parser, error) {
	var parser *engine.RawToTableImpl
	var err error

	switch in := inWrapped.(type) {
	case *ParserConfigRawToTableCommon:
		config := in.toCommonConfig()
		parser, err = engine.NewRawToTable(
			config,
		)
	case *ParserConfigRawToTableLb:
		config := in.toCommonConfig()
		parser, err = engine.NewRawToTable(
			config,
		)
	default:
		return nil, xerrors.Errorf("unknown parserConfig type: %T", inWrapped)
	}

	return parser, err
}

func init() {
	parsers.Register(
		NewParserRawToTable,
		[]parsers.AbstractParserConfig{new(ParserConfigRawToTableLb), new(ParserConfigRawToTableCommon)},
	)
}
