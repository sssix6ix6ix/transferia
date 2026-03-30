package example

import (
	"testing"

	mongoutil "github.com/transferia/transferia/recipe/mongo/pkg/util"
)

func TestSample(t *testing.T) {
	mongoutil.TestMongoShardedClusterRecipe(t)
}
