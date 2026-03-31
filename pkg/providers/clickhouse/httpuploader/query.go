package httpuploader

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/multibuf"
	"github.com/transferia/transferia/pkg/util/worker_pool"
)

type query = *multibuf.PooledMultiBuffer

func newInsertQuery(insertParams clickhouse_model.InsertParams, db string, table string, rowCount int, pool *sync.Pool) query {
	q := multibuf.NewPooledMultiBuffer(rowCount+1, pool)
	buf := q.AcquireBuffer(0)

	fmt.Fprintf(buf, "INSERT INTO `%s`.`%s` %s FORMAT JSONEachRow\n", db, table, insertParams.AsQueryPart())
	return q
}

func marshalQuery(batch []abstract.ChangeItem, rules *MarshallingRules, q query, avgRowSize int, parallelism uint64) error {
	var errs []error
	errCh := make(chan error)
	type marshalTask struct {
		buf *bytes.Buffer
		row abstract.ChangeItem
	}

	// TODO: use errgroup instead of homegrown pool
	taskPool := worker_pool.NewDefaultWorkerPool(func(row interface{}) {
		task := row.(*marshalTask)
		if err := MarshalCItoJSON(task.row, rules, task.buf); err != nil {
			errCh <- err
		}
	}, parallelism)
	// Cannot just do defer taskPool.Close() because in case all tasks are succesfuly added to the pool,
	// pool.Close must be called before checking errs list. In this case defer must be canceled
	rb := util.Rollbacks{}
	defer rb.Do()
	rb.Add(func() {
		_ = taskPool.Close()
		close(errCh)
	})

	if err := taskPool.Run(); err != nil {
		return xerrors.Errorf("error running marshalling pool: %w", err)
	}

	go func() {
		for err := range errCh {
			errs = append(errs, err)
		}
	}()

	for _, row := range batch {
		buf := q.AcquireBuffer(int(float64(avgRowSize) * MemReserveFactor))
		if err := taskPool.Add(&marshalTask{buf, row}); err != nil {
			return xerrors.Errorf("error adding marshalling task to pool: %w", err)
		}
	}

	_ = taskPool.Close()
	close(errCh)
	rb.Cancel()
	return errors.Join(errs...)
}
