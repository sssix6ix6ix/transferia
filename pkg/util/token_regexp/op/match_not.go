package op

import (
	"strings"

	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type MatchNotOp struct {
	token_regexp_abstract.Relatives
	v map[string]bool
}

func (t *MatchNotOp) IsOp() {}

func (t *MatchNotOp) ConsumePrimitive(tokens []*token_regexp_abstract.Token) []int {
	if len(tokens) == 0 {
		return nil
	}
	if !t.v[tokens[0].LowerText] {
		return []int{1}
	}
	return nil
}

func MatchNot(in ...string) *MatchNotOp {
	v := make(map[string]bool)
	for _, vv := range in {
		v[strings.ToLower(vv)] = true
	}
	return &MatchNotOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
		v:         v,
	}
}
