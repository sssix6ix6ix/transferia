package errors

import (
	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// full list of error codes here - https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
var NonRetryableCode = map[int32]bool{
	62:  true,
	349: true,
}

func IsClickhouseError(err error) bool {
	var exception *clickhouse_go.Exception
	return xerrors.As(err, &exception)
}

func IsFatalClickhouseError(err error) bool {
	exception := new(clickhouse_go.Exception)
	if !xerrors.As(err, &exception) {
		return false
	}
	return NonRetryableCode[exception.Code]
}
