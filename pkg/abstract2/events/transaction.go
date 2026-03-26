package events

import (
	"github.com/transferia/transferia/pkg/abstract2"
)

type TransactionState int

// It is important for serialization not to use iota
const (
	TransactionBegin  TransactionState = TransactionState(1)
	TransactionCommit TransactionState = TransactionState(2)
)

type TransactionEvent interface {
	abstract2.TransactionalEvent
	State() TransactionState
}

type DefaultTransactionEvent struct {
	position    abstract2.LogPosition
	transaction abstract2.Transaction
	state       TransactionState
}

func NewDefaultTransactionEvent(position abstract2.LogPosition, transaction abstract2.Transaction, state TransactionState) *DefaultTransactionEvent {
	return &DefaultTransactionEvent{
		position:    position,
		transaction: transaction,
		state:       state,
	}
}

func (event *DefaultTransactionEvent) Position() abstract2.LogPosition {
	return event.position
}

func (event *DefaultTransactionEvent) Transaction() abstract2.Transaction {
	return event.transaction
}

func (event *DefaultTransactionEvent) State() TransactionState {
	return event.state
}
