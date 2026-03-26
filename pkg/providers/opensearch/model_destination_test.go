package opensearch

import (
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func skip(t *testing.T) {
	t.SkipNow()
}

func TestCheckOpenSearchEqualElasticSearch(t *testing.T) {
	skip(t)

	openSearch, err := os.ReadFile("./model_opensearch_destination.go")
	require.NoError(t, err)

	elasticSearch, err := os.ReadFile("./model_elasticsearch_destination.go")
	require.NoError(t, err)

	openSearchExpected := replaceAllRE(`ElasticSearch`, string(elasticSearch), "OpenSearch")
	openSearchExpected = replaceAllRE(`ELASTICSEARCH`, openSearchExpected, "OPENSEARCH")
	openSearchExpected = replaceAllRE(`elasticsearch`, openSearchExpected, "opensearch")
	require.Equal(t, string(openSearch), openSearchExpected)
}

func replaceAllRE(pattern, src, repl string) string {
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(src, repl)
}
