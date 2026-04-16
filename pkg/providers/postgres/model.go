package postgres

import (
	"fmt"
)

type PgObjectType string

const (
	PgObjectTypeTable            PgObjectType = "TABLE"
	PgObjectTypeTableAttach      PgObjectType = "TABLE_ATTACH"
	PgObjectTypePrimaryKey       PgObjectType = "PRIMARY_KEY"
	PgObjectTypeView             PgObjectType = "VIEW"
	PgObjectTypeSequence         PgObjectType = "SEQUENCE"
	PgObjectTypeSequenceSet      PgObjectType = "SEQUENCE_SET"
	PgObjectTypeSequenceOwnedBy  PgObjectType = "SEQUENCE_OWNED_BY"
	PgObjectTypeRule             PgObjectType = "RULE"
	PgObjectTypeType             PgObjectType = "TYPE"
	PgObjectTypeConstraint       PgObjectType = "CONSTRAINT"
	PgObjectTypeFkConstraint     PgObjectType = "FK_CONSTRAINT"
	PgObjectTypeIndex            PgObjectType = "INDEX"
	PgObjectTypeIndexAttach      PgObjectType = "INDEX_ATTACH"
	PgObjectTypeFunction         PgObjectType = "FUNCTION"
	PgObjectTypeCollation        PgObjectType = "COLLATION"
	PgObjectTypeTrigger          PgObjectType = "TRIGGER"
	PgObjectTypePolicy           PgObjectType = "POLICY"
	PgObjectTypeCast             PgObjectType = "CAST"
	PgObjectTypeMaterializedView PgObjectType = "MATERIALIZED_VIEW"
	PgObjectTypeDefault          PgObjectType = "DEFAULT"
)

func (pot PgObjectType) IsValid() error {
	switch pot {
	case PgObjectTypeTable, PgObjectTypeTableAttach, PgObjectTypePrimaryKey, PgObjectTypeView, PgObjectTypeSequence, PgObjectTypeSequenceSet, PgObjectTypeSequenceOwnedBy,
		PgObjectTypeRule, PgObjectTypeType, PgObjectTypeConstraint, PgObjectTypeFkConstraint, PgObjectTypeIndex, PgObjectTypeIndexAttach,
		PgObjectTypeFunction, PgObjectTypeCollation, PgObjectTypeTrigger, PgObjectTypePolicy, PgObjectTypeCast, PgObjectTypeMaterializedView:
		return nil
	}
	return fmt.Errorf("invalid PgObjectType: %v", pot)
}
