//go:build !arcadia
// +build !arcadia

package confluent

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func ResolveYSRNamespaceIDToConnectionParams(namespaceID string) (params YSRConnectionParams, err error) {
	return params, xerrors.New("not implemented for open-source")
}
