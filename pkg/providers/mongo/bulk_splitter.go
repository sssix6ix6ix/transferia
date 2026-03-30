package mongo

import (
	"github.com/transferia/transferia/pkg/util/set"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
)

type bulkSplitter struct {
	bulks              [][]mongo_driver.WriteModel
	currentOperations  []mongo_driver.WriteModel
	currentDocumentIDs set.Set[string]
}

func (b *bulkSplitter) Add(operation mongo_driver.WriteModel, id documentID, isolated bool) {
	if b.currentContains(id) || isolated {
		b.flush()
	}
	b.currentOperations = append(b.currentOperations, operation)
	b.currentDocumentIDs.Add(id.String)
	if isolated {
		b.flush()
	}
}

func (b *bulkSplitter) Get() [][]mongo_driver.WriteModel {
	b.flush()
	return b.bulks
}

func (b *bulkSplitter) currentBulkSize() int {
	return len(b.currentOperations)
}

func (b *bulkSplitter) currentContains(id documentID) bool {
	return b.currentDocumentIDs.Contains(id.String)
}

func (b *bulkSplitter) flush() {
	if b.currentBulkSize() == 0 {
		return
	}
	b.bulks = append(b.bulks, b.currentOperations)
	b.currentOperations = []mongo_driver.WriteModel{}
	b.currentDocumentIDs = *set.New[string]()
}

func newBulkSplitter() bulkSplitter {
	return bulkSplitter{
		bulks:              [][]mongo_driver.WriteModel{},
		currentOperations:  []mongo_driver.WriteModel{},
		currentDocumentIDs: *set.New[string](),
	}
}
