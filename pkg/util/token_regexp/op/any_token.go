package op

import (
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type AnyTokenOp struct {
	token_regexp_abstract.Relatives
}

func (t *AnyTokenOp) IsOp() {}

func (t *AnyTokenOp) ConsumePrimitive(tokens []*token_regexp_abstract.Token) []int {
	if len(tokens) == 0 {
		return nil
	}
	return []int{1}
}

func AnyToken() *AnyTokenOp {
	return &AnyTokenOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
	}
}
