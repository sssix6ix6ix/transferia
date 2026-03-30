package topiccommon

import (
	"fmt"

	"github.com/transferia/transferia/pkg/providers/ydb"
)

const defaultPort = 2135

type ConnectionConfig struct {
	Endpoint    string
	Database    string
	Credentials ydb.TokenCredentials

	TLSEnabled  bool
	RootCAFiles []string
}

// FormatEndpoint returns an instance if it has a port, or joins the instance with a port.
// If the port is 0, default port will be used.
func FormatEndpoint(instance string, port int) string {
	if instanceContainsPort(instance) {
		return instance
	}

	endpointInstance, endpointPort := instance, defaultPort
	if port != 0 {
		endpointPort = port
	}

	return fmt.Sprintf("%s:%d", endpointInstance, endpointPort)
}

func instanceContainsPort(instance string) bool {
	for i := range instance {
		if instance[i] == ':' {
			return true
		}
	}
	return false
}
