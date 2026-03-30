package op

import (
	"strings"

	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type MatchOp struct {
	token_regexp_abstract.Relatives
	v string
}

func (t *MatchOp) IsOp() {}

func (t *MatchOp) ConsumePrimitive(tokens []*token_regexp_abstract.Token) []int {
	if len(tokens) == 0 {
		return nil
	}
	if t.v == tokens[0].LowerText {
		return []int{1}
	}
	return nil
}

func Match(in string) *MatchOp {
	return &MatchOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
		v:         strings.ToLower(in),
	}
}
