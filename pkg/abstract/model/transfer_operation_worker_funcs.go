package model

import (
	"errors"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

func AggregateWorkerErrors(workers []*OperationWorker, operationID string) error {
	var result []error
	for _, worker := range workers {
		if worker.Err != "" {
			result = append(result, xerrors.Errorf("secondary worker [%v] of operation '%v' failed: %v", worker.WorkerIndex, operationID, worker.Err))
		}
	}
	return errors.Join(result...)
}
