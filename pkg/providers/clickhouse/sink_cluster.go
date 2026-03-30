// Package ch
// cluster - it's like stand-alone cluster with multimaster
// []*SinkServer - masters (AltHosts). We don't care in which SinkServer we are writing - it's like multimaster.
// We choose alive masters (by bestSinkServer()) and then round-robin them
package clickhouse

import (
	"context"
	stderrors "errors"
	"sync"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/ptr"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	clickhouse_errors "github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type sinkCluster struct {
	sinkServers []*SinkServer
	metrics     *stats.ChStats
	logger      log.Logger
	config      clickhouse_model.ChSinkClusterParams
	counter     int
	topology    *topology.Topology

	distributedDDLMu      sync.Mutex
	distributedDDLEnabled *bool
}

func (c *sinkCluster) bestSinkServer() *SinkServer {
	r := make([]*SinkServer, 0)
	for _, sinkServer := range c.sinkServers {
		if sinkServer.Alive() {
			r = append(r, sinkServer)
		}
	}
	if len(r) == 0 {
		return c.sinkServers[0]
	}
	i := c.counter % len(r)
	c.counter++
	c.logger.Infof("choose sinkServer %v (%v from %v counter %v)", r[i].host, i, len(r), c.counter)
	return r[i]
}

type TableSpec struct {
	Name   string
	Schema *abstract.TableSchema
}

func (c *sinkCluster) Insert(spec *TableSpec, rows []abstract.ChangeItem) error {
	return c.bestSinkServer().Insert(spec, rows)
}

func (c *sinkCluster) TruncateTable(tableName string) error {
	ctx := context.TODO()
	if c.perHostDDL() {
		var errs []error
		for _, ss := range c.sinkServers {
			if err := ss.TruncateTable(ctx, tableName, false); err != nil {
				errs = append(errs, err)
			}
		}
		if err := stderrors.Join(errs...); err != nil {
			return xerrors.Errorf("cannot truncate table in per host style: %w", err)
		}
		return nil
	}

	return c.execDDL(func(distributed bool) error {
		if err := c.bestSinkServer().TruncateTable(ctx, tableName, distributed); err != nil {
			return xerrors.Errorf("cannot truncate table (distributed=%v): %w", distributed, err)
		}
		return nil
	})
}

func (c *sinkCluster) DropTable(tableName string) error {
	ctx := context.TODO()
	if c.perHostDDL() {
		var errs []error
		for _, ss := range c.sinkServers {
			if err := ss.DropTable(ctx, tableName, false); err != nil {
				errs = append(errs, err)
			}
		}
		if err := stderrors.Join(errs...); err != nil {
			return xerrors.Errorf("cannot drop table in per host style: %w", err)
		}
		return nil
	}

	return c.execDDL(func(distributed bool) error {
		if err := c.bestSinkServer().DropTable(ctx, tableName, distributed); err != nil {
			return xerrors.Errorf("cannot drop table (distributed=%v): %w", distributed, err)
		}
		return nil
	})
}

func (c *sinkCluster) execDDL(executor func(distributed bool) error) error {
	c.distributedDDLMu.Lock()

	if c.distributedDDLEnabled == nil && (c.topology.ClusterName() == "" || c.topology.SingleNode()) {
		if !c.topology.SingleNode() {
			return xerrors.Errorf("resolved empty cluster name for non-single-node cluster")
		}
		c.logger.Warn("cluster name is empty, disabling distributed DDL")
		c.distributedDDLEnabled = ptr.Bool(false)
	}

	if c.distributedDDLEnabled != nil {
		c.distributedDDLMu.Unlock()
		if err := executor(*c.distributedDDLEnabled); err != nil {
			return xerrors.Errorf("error executing DDL (distributed=%v): %w", *c.distributedDDLEnabled, err)
		}
		return nil
	}

	defer c.distributedDDLMu.Unlock()
	err := executor(true)
	if err == nil {
		c.distributedDDLEnabled = ptr.Bool(true)
		c.logger.Info("distributed DDL is enabled")
		return nil
	}

	if !clickhouse_errors.IsDistributedDDLError(err) {
		return xerrors.Errorf("error executing DDL: %w", err)
	}
	c.logger.Error("Got distributed DDL error", log.Error(err))

	if !c.topology.SingleNode() {
		c.logger.Error("cluster is not single node and distributed DDL is not available")
		return clickhouse_errors.ForbiddenDistributedDDLError
	}

	if err := executor(false); err != nil {
		return xerrors.Errorf("error executing DDL: %w", err)
	}
	c.logger.Warn("disabling distributed DDL for cluster")
	c.distributedDDLEnabled = ptr.Bool(false)
	return nil
}

func (c *sinkCluster) perHostDDL() bool {
	return c.config.ShardByTransferID()
}

func (c *sinkCluster) Close() error {
	var closeErrs []error
	for _, s := range c.sinkServers {
		if err := s.Close(); err != nil {
			closeErrs = append(closeErrs, xerrors.Errorf("failed to close SinkServer: %w", err))
		}
	}
	return stderrors.Join(closeErrs...)
}

func (c *sinkCluster) Reset() error {
	var closeErrs []error
	for _, s := range c.sinkServers {
		if err := s.Close(); err != nil {
			closeErrs = append(closeErrs, err)
		}
	}

	if err := stderrors.Join(closeErrs...); err != nil {
		logger.Log.Warn("ClickHouse cluster reset encountered Close error(s)", log.Error(err))
	}

	return c.Init()
}

func (c *sinkCluster) Init() error {
	st := make([]*SinkServer, 0)
	var errs []error
	for _, host := range yslices.Shuffle(c.config.AltHosts(), nil) {
		c.logger.Debugf("init sinkServer %v", host.String())
		sinkServer, err := NewSinkServer(
			c.config.MakeChildServerParams(host),
			c.logger,
			c.metrics,
			c,
		)
		if err != nil {
			c.logger.Warn("unable to init sink server, skip", log.Error(err))
			errs = append(errs, err)
			continue
		}

		st = append(st, sinkServer)
	}

	if len(st) > 0 {
		c.sinkServers = st
	} else {
		return xerrors.Errorf("no sink servers in cluster with: %v hosts: %w", len(c.config.AltHosts()), stderrors.Join(errs...))
	}

	return nil
}

func (c *sinkCluster) RemoveOldParts(keepPartCount int, table string) error {
	for _, s := range c.sinkServers {
		if err := s.CleanupPartitions(keepPartCount, table); err != nil {
			return err
		}
	}
	return nil
}

func newSinkCluster(config clickhouse_model.ChSinkClusterParams, lgr log.Logger, metrics *stats.ChStats, topology *topology.Topology) (*sinkCluster, error) {
	cl := new(sinkCluster)
	cl.metrics = metrics
	cl.logger = lgr
	cl.config = config
	cl.topology = topology

	if err := cl.Init(); err != nil {
		return nil, err
	}

	return cl, nil
}
