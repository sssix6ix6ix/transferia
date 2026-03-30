package topology

import conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"

type ShardHostMap map[int][]*conn_clickhouse.Host

type Cluster struct {
	Topology Topology
	Shards   ShardHostMap
}

func (c *Cluster) Name() string {
	return c.Topology.ClusterName()
}

func (c *Cluster) SingleNode() bool {
	return c.Topology.SingleNode()
}
