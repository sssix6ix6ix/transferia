package clickhouse

import (
	"context"
	"errors"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	"go.ytsaurus.tech/library/go/core/log"
)

type SourcesChain struct {
	sources []abstract2.EventSource
	logger  log.Logger
}

func (p *SourcesChain) Progress() (abstract2.EventSourceProgress, error) {
	for _, source := range p.sources {
		if progressable, ok := source.(abstract2.ProgressableEventSource); ok {
			return progressable.Progress()
		}
	}
	return nil, xerrors.New("progressable event source not found in chain")
}

func (p *SourcesChain) Running() bool {
	for _, source := range p.sources {
		if source.Running() {
			return true
		}
	}
	return false
}

func (p *SourcesChain) Start(ctx context.Context, target abstract2.EventTarget) error {
	for _, source := range p.sources {
		if err := source.Start(ctx, target); err != nil {
			return xerrors.Errorf("unable to start %T event source: %w", source, err)
		}
		p.logger.Infof("source completed: %T", source)
	}
	return nil
}

func (p *SourcesChain) Stop() error {
	var errs []error
	for _, provider := range p.sources {
		if err := provider.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return xerrors.Errorf("unable to stop sources chain: %w", err)
	}
	return nil
}

func NewSourcesChain(logger log.Logger, sources ...abstract2.EventSource) abstract2.ProgressableEventSource {
	return &SourcesChain{
		sources: sources,
		logger:  logger,
	}
}
