package model

import (
	"fmt"
)

type CleanupType string

const (
	Drop            CleanupType = "Drop"
	Truncate        CleanupType = "Truncate"
	DisabledCleanup CleanupType = "Disabled"
	UseTmpPolicy    CleanupType = "Replace"
)

func (ct CleanupType) IsValid() error {
	switch ct {
	case Drop, Truncate, DisabledCleanup, UseTmpPolicy:
		return nil
	}
	return fmt.Errorf("invalid cleanup type: %v", ct)
}

const TmpTableSuffix = "_tmp"

func MakeTmpTableName(tableName, transferId string) string {
	return fmt.Sprintf("%s_%s_%s", tableName, transferId, TmpTableSuffix)
}
