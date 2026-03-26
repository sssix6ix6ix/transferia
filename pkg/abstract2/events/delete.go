package events

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract2"
)

type DeleteEvent interface {
	abstract2.SupportsOldChangeItem
	Table() abstract2.Table
	OldValuesCount() int
	OldValue(i int) abstract2.Value
}

type LoggedDeleteEvent interface {
	abstract2.LoggedEvent
	DeleteEvent
}

type TransactionalDeleteEvent interface {
	abstract2.TransactionalEvent
	LoggedDeleteEvent
}

type DefaultDeleteEvent struct {
	table     abstract2.Table
	oldValues []abstract2.Value
}

func NewDefaultDeleteEvent(table abstract2.Table) *DefaultDeleteEvent {
	return &DefaultDeleteEvent{
		table:     table,
		oldValues: []abstract2.Value{},
	}
}

func NewDefaultDeleteEventWithValues(table abstract2.Table, oldValues []abstract2.Value) (*DefaultDeleteEvent, error) {
	for _, oldValue := range oldValues {
		if err := validateValue(oldValue); err != nil {
			return nil, err
		}
	}
	event := &DefaultDeleteEvent{
		table:     table,
		oldValues: oldValues,
	}
	return event, nil
}

func (event *DefaultDeleteEvent) Table() abstract2.Table {
	return event.table
}

func (event *DefaultDeleteEvent) OldValuesCount() int {
	return len(event.oldValues)
}

func (event *DefaultDeleteEvent) OldValue(i int) abstract2.Value {
	return event.oldValues[i]
}

func (event *DefaultDeleteEvent) AddOldValue(value abstract2.Value) error {
	if err := validateValue(value); err != nil {
		return err
	}
	event.oldValues = append(event.oldValues, value)
	return nil
}

func (event *DefaultDeleteEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	oldTable, err := event.table.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("Table cannot be converted to old format: %w", err)
	}

	changeItem := &abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   0,
		Counter:      0,
		Kind:         abstract.DeleteKind,
		Schema:       event.table.Schema(),
		Table:        event.table.Name(),
		PartID:       "",
		ColumnNames:  nil,
		ColumnValues: nil,
		TableSchema:  oldTable,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{},
			KeyTypes:  []string{},
			KeyValues: []interface{}{},
		},
		Size:             abstract.EmptyEventSize(),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}

	for _, value := range event.oldValues {
		if !value.Column().Key() {
			continue
		}
		changeItem.OldKeys.KeyNames = append(changeItem.OldKeys.KeyNames, value.Column().Name())
		oldValue, err := value.ToOldValue()
		if err != nil {
			return nil, xerrors.Errorf("Value cannot be converted to old format: %w", err)
		}
		changeItem.OldKeys.KeyValues = append(changeItem.OldKeys.KeyValues, oldValue)
	}

	return changeItem, nil
}

type DefaultLoggedDeleteEvent struct {
	DefaultDeleteEvent
	position abstract2.LogPosition
}

func NewDefaultLoggedDeleteEvent(
	table abstract2.Table,
	position abstract2.LogPosition,
) *DefaultLoggedDeleteEvent {
	return &DefaultLoggedDeleteEvent{
		DefaultDeleteEvent: *NewDefaultDeleteEvent(table),
		position:           position,
	}
}

func NewDefaultLoggedDeleteEventWithValues(
	table abstract2.Table,
	position abstract2.LogPosition,
	oldValues []abstract2.Value,
) (*DefaultLoggedDeleteEvent, error) {
	deleteEvent, err := NewDefaultDeleteEventWithValues(table, oldValues)
	if err != nil {
		return nil, err
	}
	loggedDeleteEvent := &DefaultLoggedDeleteEvent{
		DefaultDeleteEvent: *deleteEvent,
		position:           position,
	}
	return loggedDeleteEvent, nil
}

func (event *DefaultLoggedDeleteEvent) Position() abstract2.LogPosition {
	return event.position
}

func (event *DefaultLoggedDeleteEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	changeItem, err := event.DefaultDeleteEvent.ToOldChangeItem()
	if err != nil {
		return nil, xerrors.Errorf("Can't convert event to legacy change item: %w", err)
	}
	if supportsLegacyLSN, ok := event.Position().(abstract2.SupportsOldLSN); ok {
		lsn, err := supportsLegacyLSN.ToOldLSN()
		if err != nil {
			return nil, xerrors.Errorf("Can't get legacy LSN from log position: %w", err)
		}
		changeItem.LSN = lsn
	}
	if supportsLegacyCommitTime, ok := event.Position().(abstract2.SupportsOldCommitTime); ok {
		commitTime, err := supportsLegacyCommitTime.ToOldCommitTime()
		if err != nil {
			return nil, xerrors.Errorf("Can't get legacy LSN from log position: %w", err)
		}
		changeItem.CommitTime = commitTime
	}
	return changeItem, nil
}

type DefaultTransactionalDeleteEvent struct {
	DefaultLoggedDeleteEvent
	transaction abstract2.Transaction
}

func NewDefaultTransactionalDeleteEvent(
	table abstract2.Table,
	position abstract2.LogPosition,
	transaction abstract2.Transaction,
) *DefaultTransactionalDeleteEvent {
	return &DefaultTransactionalDeleteEvent{
		DefaultLoggedDeleteEvent: *NewDefaultLoggedDeleteEvent(table, position),
		transaction:              transaction,
	}
}

func NewDefaultTransactionalDeleteEventWithValues(
	table abstract2.Table,
	position abstract2.LogPosition,
	transaction abstract2.Transaction,
	oldValues []abstract2.Value,
) (*DefaultTransactionalDeleteEvent, error) {
	loggedDeleteEvent, err := NewDefaultLoggedDeleteEventWithValues(table, position, oldValues)
	if err != nil {
		return nil, err
	}
	transactionalDeleteEvent := &DefaultTransactionalDeleteEvent{
		DefaultLoggedDeleteEvent: *loggedDeleteEvent,
		transaction:              transaction,
	}
	return transactionalDeleteEvent, nil
}

func (event *DefaultTransactionalDeleteEvent) Transaction() abstract2.Transaction {
	return event.transaction
}

func (event *DefaultTransactionalDeleteEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	if changeItem, err := event.DefaultLoggedDeleteEvent.ToOldChangeItem(); err != nil {
		return nil, err
	} else {
		changeItem.TxID = event.Transaction().String()
		return changeItem, nil
	}
}
