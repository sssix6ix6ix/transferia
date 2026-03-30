package op

import (
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type MatchParenthesesOp struct {
	token_regexp_abstract.Relatives
}

func (t *MatchParenthesesOp) IsOp() {}

func (t *MatchParenthesesOp) ConsumePrimitive(tokens []*token_regexp_abstract.Token) []int {
	if len(tokens) < 2 {
		return nil
	}
	if tokens[0].LowerText != "(" {
		return nil
	}
	nestingCount := 1
	index := 1
	for ; index < len(tokens); index++ {
		if nestingCount == 0 {
			break
		}
		switch tokens[index].LowerText {
		case "(":
			nestingCount++
		case ")":
			nestingCount--
		}
	}
	if nestingCount == 0 {
		return []int{index}
	}
	return nil
}

func MatchParentheses() *MatchParenthesesOp {
	return &MatchParenthesesOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
	}
}
