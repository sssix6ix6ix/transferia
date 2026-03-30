package abstract

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func GetTypeFromString(dataType string) (ytschema.Type, error) {
	switch dataType {
	// int types
	case string(ytschema.TypeInt8):
		return ytschema.TypeInt8, nil
	case string(ytschema.TypeInt16):
		return ytschema.TypeInt16, nil
	case string(ytschema.TypeInt32):
		return ytschema.TypeInt32, nil
	case string(ytschema.TypeInt64):
		return ytschema.TypeInt64, nil

	// uint Types
	case string(ytschema.TypeUint8):
		return ytschema.TypeUint8, nil
	case string(ytschema.TypeUint16):
		return ytschema.TypeUint16, nil
	case string(ytschema.TypeUint32):
		return ytschema.TypeUint32, nil
	case string(ytschema.TypeUint64):
		return ytschema.TypeUint64, nil

	// float types
	case string(ytschema.TypeFloat32):
		return ytschema.TypeFloat32, nil
	case string(ytschema.TypeFloat64):
		return ytschema.TypeFloat64, nil

	// textual data types
	case string(ytschema.TypeString):
		return ytschema.TypeString, nil
	case string(ytschema.TypeBytes):
		return ytschema.TypeBytes, nil

	// date data types
	case string(ytschema.TypeDate):
		return ytschema.TypeDate, nil
	case string(ytschema.TypeDatetime):
		return ytschema.TypeDatetime, nil
	case string(ytschema.TypeTimestamp):
		return ytschema.TypeTimestamp, nil
	case string(ytschema.TypeInterval):
		return ytschema.TypeInterval, nil

	// bool data type
	case string(ytschema.TypeBoolean):
		return ytschema.TypeBoolean, nil

	// any data type
	case string(ytschema.TypeAny):
		return ytschema.TypeAny, nil

	default:
		return "", NewFatalError(xerrors.Errorf("unknown data type provided %s", dataType))
	}
}
