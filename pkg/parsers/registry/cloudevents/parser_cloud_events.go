package cloudevents

import (
	"github.com/transferia/transferia/pkg/parsers"
	cloudevents_engine "github.com/transferia/transferia/pkg/parsers/registry/cloudevents/engine"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserCloudEvents(inWrapped interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	switch in := inWrapped.(type) {
	case *ParserConfigCloudEventsCommon:
		return cloudevents_engine.NewCloudEventsImpl(in.TLSFile, in.Username, in.Password, in.PasswordFallback, true, logger, nil), nil
	case *ParserConfigCloudEventsLb:
		return cloudevents_engine.NewCloudEventsImpl(in.TLSFile, in.Username, in.Password, in.PasswordFallback, true, logger, nil), nil
	}
	return nil, nil
}

func init() {
	parsers.Register(
		NewParserCloudEvents,
		[]parsers.AbstractParserConfig{new(ParserConfigCloudEventsCommon), new(ParserConfigCloudEventsLb)},
	)
}
