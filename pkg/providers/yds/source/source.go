package source

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	gp "github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/providers/ydb"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	pqv1source "github.com/transferia/transferia/pkg/providers/ydb/topics/source/pqv1"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSourceWithParser(transferID string, cfg *YDSSource, logger log.Logger, sourceMetrics *stats.SourceStats, parser parsers.Parser) (abstract.Source, error) {
	consumer := cfg.Consumer
	if consumer == "" {
		consumer = transferID
	}

	topicSourceCfg := &topicsource.Config{
		Connection: topiccommon.ConnectionConfig{
			Endpoint:    topiccommon.FormatEndpoint(cfg.Endpoint, cfg.Port),
			Database:    cfg.Database,
			Credentials: cfg.Credentials,
			TLSEnabled:  cfg.TLSEnalbed,
			RootCAFiles: cfg.RootCAFiles,
		},

		Topics:   []string{cfg.Stream},
		Consumer: consumer,
		ReaderOpts: topicsource.ReaderOptions{
			ReadOnlyLocal:       false,
			MaxMemory:           300 * 1024 * 1024,
			MaxReadSize:         1 * 1024 * 1024,
			MaxReadMessageCount: 0,
			MaxTimeLag:          0,
			MinReadInterval:     0,
		},
		Transformer: cfg.Transformer,

		IsYDBTopicSink:             cfg.IsLbSink,
		AllowTTLRewind:             cfg.AllowTTLRewind,
		ParseQueueParallelism:      cfg.ParseQueueParallelism,
		UseFullTopicNameForParsing: false,
	}

	if cfg.Transformer != nil {
		topicSourceCfg.ReaderOpts.MaxMemory = int(cfg.Transformer.BufferSize * 10)
	}

	return pqv1source.NewSource(topicSourceCfg, parser, logger, sourceMetrics)
}

func NewSource(transferID string, cfg *YDSSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	if cfg.Credentials == nil {
		var err error
		cfg.Credentials, err = ydb.ResolveCredentials(
			cfg.UserdataAuth,
			string(cfg.Token),
			ydb.JWTAuthParams{
				KeyContent:      cfg.SAKeyContent,
				TokenServiceURL: cfg.TokenServiceURL,
			},
			cfg.ServiceAccountID,
			nil,
			logger,
		)
		if err != nil {
			return nil, xerrors.Errorf("Cannot create YDB credentials: %w", err)
		}
	}

	var parser parsers.Parser
	sourceMetrics := stats.NewSourceStats(registry)
	if cfg.ParserConfig != nil {
		var err error
		parser, err = parsers.NewParserFromMap(cfg.ParserConfig, false, logger, sourceMetrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to make parser, err: %w", err)
		}

		// Dirty hack for back compatibility. yds transfer users (including us)
		// use generic parser name field set from cfg.Stream, but topic parametr
		// was removed from parsers conustructors. therefor, we cast parser to
		// generic parser and set it manually
		// subj: TM-6012
		switch wp := parser.(type) {
		case *parsers.ResourceableParser:
			switch p := wp.Unwrap().(type) {
			case *gp.GenericParser:
				p.SetTopic(cfg.Stream)
			}
		}
	}

	return NewSourceWithParser(transferID, cfg, logger, sourceMetrics, parser)
}
