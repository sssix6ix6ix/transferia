package opensearch

import (
	"github.com/elastic/go-elasticsearch/v7"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_elastic "github.com/transferia/transferia/pkg/providers/elastic"
	"go.ytsaurus.tech/library/go/core/log"
)

type Sink struct {
	elasticSink abstract.Sinker
}

func (s *Sink) Push(input []abstract.ChangeItem) error {
	return s.elasticSink.Push(input)
}

func (s *Sink) Close() error {
	return s.elasticSink.Close()
}

func NewSinkImpl(cfg *OpenSearchDestination, logger log.Logger, registry core_metrics.Registry, client *elasticsearch.Client) (abstract.Sinker, error) {
	elasticDst, _ := cfg.ToElasticSearchDestination()
	elasticSink, err := provider_elastic.NewSinkImpl(elasticDst, logger, registry, client)
	if err != nil {
		return nil, xerrors.Errorf("unable to create elastic sink, err: %w", err)
	}
	return &Sink{
		elasticSink: elasticSink,
	}, nil
}

func NewSink(cfg *OpenSearchDestination, logger log.Logger, registry core_metrics.Registry) (abstract.Sinker, error) {
	elasticDst, serverType := cfg.ToElasticSearchDestination()
	config, err := provider_elastic.ConfigFromDestination(logger, elasticDst, serverType)
	if err != nil {
		return nil, xerrors.Errorf("failed to create elastic configuration: %w", err)
	}
	client, err := provider_elastic.WithLogger(*config, log.With(logger, log.Any("component", "esclient")), serverType)
	if err != nil {
		return nil, xerrors.Errorf("failed to create elastic client: %w", err)
	}
	return NewSinkImpl(cfg, logger, registry, client)
}
