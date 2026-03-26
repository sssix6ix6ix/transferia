package middlewares

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/metering"
	"github.com/transferia/transferia/pkg/middlewares/asynchronizer"
)

func InputDataMetering() func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newInputDataMetering(s)
	}
}

func OutputDataMetering() func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newOutputDataMetering(s)
	}
}

func OutputDataBatchMetering() func(asynchronizer.Asynchronizer) asynchronizer.Asynchronizer {
	return func(t asynchronizer.Asynchronizer) asynchronizer.Asynchronizer {
		return newOutputDataBatchMetering(t)
	}
}

type inputDataMetering struct {
	sink abstract.Sinker
}

func newInputDataMetering(s abstract.Sinker) *inputDataMetering {
	return &inputDataMetering{
		sink: s,
	}
}

func (m *inputDataMetering) Close() error {
	return m.sink.Close()
}

func (m *inputDataMetering) Push(input []abstract.ChangeItem) error {
	pushErr := m.sink.Push(input)
	if pushErr == nil {
		metering.Agent().CountInputRows(input)
	}
	return pushErr
}

type outputDataMetering struct {
	sink abstract.Sinker
}

func newOutputDataMetering(s abstract.Sinker) *outputDataMetering {
	return &outputDataMetering{
		sink: s,
	}
}

func (m *outputDataMetering) Close() error {
	return m.sink.Close()
}

func (m *outputDataMetering) Push(input []abstract.ChangeItem) error {
	pushErr := m.sink.Push(input)
	if pushErr == nil {
		metering.Agent().CountOutputRows(input)
	}
	return pushErr
}

type outputDataBatchMetering struct {
	asynk asynchronizer.Asynchronizer
}

func newOutputDataBatchMetering(t asynchronizer.Asynchronizer) *outputDataBatchMetering {
	return &outputDataBatchMetering{
		asynk: t,
	}
}

func (m *outputDataBatchMetering) Close() error {
	return m.asynk.Close()
}

func (m *outputDataBatchMetering) Push(input abstract2.EventBatch) error {
	pushErr := m.asynk.Push(input)
	if pushErr == nil {
		metering.Agent().CountOutputBatch(input)
	}
	return pushErr
}
