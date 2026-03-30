package abstract

import (
	"fmt"
	"strings"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
)

func Rows(metrics core_metrics.Registry, table string, rows int) {
	if !strings.Contains(table, TableConsumerKeeper) {
		metrics.Counter(fmt.Sprintf("sink.table.%v.rows", table)).Add(int64(rows))
	}
}
