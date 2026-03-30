package cloudlogging

import (
	"github.com/transferia/transferia/pkg/parsers"
	cloudlogging_engine "github.com/transferia/transferia/pkg/parsers/registry/cloudlogging/engine"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserCloudLogging(inWrapped interface{}, sniff bool, logger log.Logger, registry *stats.SourceStats) (parsers.Parser, error) {
	return cloudlogging_engine.NewCloudLoggingImpl(sniff, logger, registry), nil
}

func init() {
	parsers.Register(
		NewParserCloudLogging,
		[]parsers.AbstractParserConfig{new(ParserConfigCloudLoggingCommon)},
	)
}
