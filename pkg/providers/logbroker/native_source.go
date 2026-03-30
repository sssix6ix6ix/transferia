package logbroker

import (
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	parser_native "github.com/transferia/transferia/pkg/parsers/registry/native"
	yds_source "github.com/transferia/transferia/pkg/providers/yds/source"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/xtls"
	"go.ytsaurus.tech/library/go/core/log"
)

func newNativeSource(cfg *LbSource, logger log.Logger, registry core_metrics.Registry) (abstract.Source, error) {
	var opts persqueue.ReaderOptions
	opts.Logger = corelogadapter.New(logger)
	opts.Endpoint = cfg.Instance
	opts.Database = cfg.Database
	opts.ManualPartitionAssignment = true
	opts.Consumer = cfg.Consumer
	opts.Topics = []persqueue.TopicInfo{{Topic: cfg.Topic}}
	opts.MaxReadSize = 1 * 1024 * 1024
	opts.MaxMemory = 100 * 1024 * 1024 // 100 mb max memory usage
	opts.RetryOnFailure = true
	opts.Port = cfg.Port
	opts.Credentials = cfg.Credentials

	if cfg.TLS == EnabledTLS {
		tls, err := xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("failed to get TLS config for cloud: %w", err)
		}
		opts.TLSConfig = tls
	}

	ydsCfg := ydsSourceConfig(cfg.AllowTTLRewind, cfg.IsLbSink, 10)

	parser, err := parsers.NewParserFromParserConfig(&parser_native.ParserConfigNativeLb{}, false, logger, stats.NewSourceStats(registry))
	if err != nil {
		return nil, xerrors.Errorf("unable to make native parser, err: %w", err)
	}

	// transferID is empty because it is used to specify the consumer, and it is already specified in the readerOpts
	transferID := ""
	return yds_source.NewSourceWithOpts(transferID, ydsCfg, logger, registry,
		yds_source.WithCreds(cfg.Credentials),
		yds_source.WithReaderOpts(&opts),
		yds_source.WithParser(parser),
	)
}
