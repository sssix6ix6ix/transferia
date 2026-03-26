package events

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract2"
)

type InsertEvent interface {
	abstract2.SupportsOldChangeItem
	Table() abstract2.Table
	NewValuesCount() int
	NewValue(i int) (abstract2.Value, error)
}

type LoggedInsertEvent interface {
	abstract2.LoggedEvent
	InsertEvent
}

type TransactionalInsertEvent interface {
	abstract2.TransactionalEvent
	LoggedInsertEvent
}

type DefaultInsertEvent struct {
	table     abstract2.Table
	newValues []abstract2.Value
}

func NewDefaultInsertEvent(table abstract2.Table) *DefaultInsertEvent {
	return &DefaultInsertEvent{
		table:     table,
		newValues: []abstract2.Value{},
	}
}

func NewDefaultInsertEventWithValues(table abstract2.Table, newValues []abstract2.Value) (*DefaultInsertEvent, error) {
	for _, newValue := range newValues {
		if err := validateValue(newValue); err != nil {
			return nil, err
		}
	}
	event := &DefaultInsertEvent{
		table:     table,
		newValues: newValues,
	}
	return event, nil
}

func (event *DefaultInsertEvent) Table() abstract2.Table {
	return event.table
}

func (event *DefaultInsertEvent) NewValuesCount() int {
	return len(event.newValues)
}

func (event *DefaultInsertEvent) NewValue(i int) (abstract2.Value, error) {
	return event.newValues[i], nil
}

func (event *DefaultInsertEvent) AddNewValue(value abstract2.Value) error {
	if err := validateValue(value); err != nil {
		return err
	}
	event.newValues = append(event.newValues, value)
	return nil
}

func (event *DefaultInsertEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	oldTable, err := event.table.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("Table cannot be converted to old format: %w", err)
	}

	valCnt := len(event.newValues)
	changeItem := &abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   0,
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       event.table.Schema(),
		Table:        event.table.Name(),
		PartID:       "",
		TableSchema:  oldTable,
		ColumnNames:  make([]string, valCnt),
		ColumnValues: make([]interface{}, valCnt),
		OldKeys: abstract.OldKeysType{
			KeyNames:  nil,
			KeyTypes:  nil,
			KeyValues: nil,
		},
		Size:             abstract.EmptyEventSize(),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}

	for idx, value := range event.newValues {
		changeItem.ColumnNames[idx] = value.Column().Name()
		oldValue, err := value.ToOldValue()
		if err != nil {
			return nil, xerrors.Errorf("Value cannot be converted to old format: %w", err)
		}
		changeItem.ColumnValues[idx] = oldValue
	}

	return changeItem, nil
}

type DefaultLoggedInsertEvent struct {
	DefaultInsertEvent
	position abstract2.LogPosition
}

func NewDefaultLoggedInsertEvent(
	table abstract2.Table,
	position abstract2.LogPosition,
) *DefaultLoggedInsertEvent {
	return &DefaultLoggedInsertEvent{
		DefaultInsertEvent: *NewDefaultInsertEvent(table),
		position:           position,
	}
}

func NewDefaultLoggedInsertEventWithValues(
	table abstract2.Table,
	position abstract2.LogPosition,
	newValues []abstract2.Value,
) (*DefaultLoggedInsertEvent, error) {
	insertEvent, err := NewDefaultInsertEventWithValues(table, newValues)
	if err != nil {
		return nil, err
	}
	loggedInsertEvent := &DefaultLoggedInsertEvent{
		DefaultInsertEvent: *insertEvent,
		position:           position,
	}
	return loggedInsertEvent, nil
}

func (event *DefaultLoggedInsertEvent) Position() abstract2.LogPosition {
	return event.position
}

func (event *DefaultLoggedInsertEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	changeItem, err := event.DefaultInsertEvent.ToOldChangeItem()
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

type DefaultTransactionalInsertEvent struct {
	DefaultLoggedInsertEvent
	transaction abstract2.Transaction
}

func NewDefaultTransactionalInsertEvent(
	table abstract2.Table,
	position abstract2.LogPosition,
	transaction abstract2.Transaction,
) *DefaultTransactionalInsertEvent {
	return &DefaultTransactionalInsertEvent{
		DefaultLoggedInsertEvent: *NewDefaultLoggedInsertEvent(table, position),
		transaction:              transaction,
	}
}

func NewDefaultTransactionalInsertEventWithValues(
	table abstract2.Table,
	position abstract2.LogPosition,
	transaction abstract2.Transaction,
	newValues []abstract2.Value,
) (*DefaultTransactionalInsertEvent, error) {
	loggedInsertEvent, err := NewDefaultLoggedInsertEventWithValues(table, position, newValues)
	if err != nil {
		return nil, err
	}
	transactionalInsertEvent := &DefaultTransactionalInsertEvent{
		DefaultLoggedInsertEvent: *loggedInsertEvent,
		transaction:              transaction,
	}
	return transactionalInsertEvent, nil
}

func (event *DefaultTransactionalInsertEvent) Transaction() abstract2.Transaction {
	return event.transaction
}

func (event *DefaultTransactionalInsertEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	if changeItem, err := event.DefaultLoggedInsertEvent.ToOldChangeItem(); err != nil {
		return nil, err
	} else {
		changeItem.TxID = event.Transaction().String()
		return changeItem, nil
	}
}
