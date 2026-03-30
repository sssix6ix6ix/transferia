package op

import (
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type MatchCurlyBracesOp struct {
	token_regexp_abstract.Relatives
}

func (t *MatchCurlyBracesOp) IsOp() {}
func (t *MatchCurlyBracesOp) ConsumePrimitive(tokens []*token_regexp_abstract.Token) []int {
	if len(tokens) < 2 {
		return nil
	}
	if tokens[0].LowerText != "{" {
		return nil
	}
	nestingCount := 1
	index := 1
	for ; index < len(tokens); index++ {
		if nestingCount == 0 {
			break
		}
		switch tokens[index].LowerText {
		case "{":
			nestingCount++
		case "}":
			nestingCount--
		}
	}
	if nestingCount == 0 {
		return []int{index}
	}
	return nil
}

func MatchCurlyBraces() *MatchCurlyBracesOp {
	return &MatchCurlyBracesOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
	}
}
