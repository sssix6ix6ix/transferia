package topiccommon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatEndpoint(t *testing.T) {
	// With port
	require.Equal(t, "logbroker.yandex.net:1111", FormatEndpoint("logbroker.yandex.net:1111", 0))

	// Without port
	require.Equal(t, "logbroker.yandex.net:1111", FormatEndpoint("logbroker.yandex.net", 1111))

	// With default port
	require.Equal(t, "logbroker.yandex.net:2135", FormatEndpoint("logbroker.yandex.net", 0))
}
