package op

import (
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type OrOp struct {
	token_regexp_abstract.Relatives
	ops []token_regexp_abstract.Op
}

func (t *OrOp) IsOp() {}

func (t *OrOp) ConsumeComplex(tokens []*token_regexp_abstract.Token) *token_regexp_abstract.MatchedResults {
	result := token_regexp_abstract.NewMatchedResults()
	for index := range t.ops {
		switch currOp := t.ops[index].(type) {
		case token_regexp_abstract.OpPrimitive:
			lengths := currOp.ConsumePrimitive(tokens)
			result.AddMatchedPathsAfterConsumePrimitive(lengths, t.ops[index], tokens)
		case token_regexp_abstract.OpComplex:
			localResults := currOp.ConsumeComplex(tokens)
			result.AddLocalResults(localResults, t.ops[index], nil)
		}
	}
	return result
}

func Or(args ...any) *OrOp {
	resultArgs := make([]token_regexp_abstract.Op, 0, len(args))
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			resultArgs = append(resultArgs, Match(v))
		case token_regexp_abstract.Op:
			resultArgs = append(resultArgs, v)
		default:
			return nil
		}
	}
	result := &OrOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
		ops:       resultArgs,
	}
	for _, childOp := range result.ops {
		childOp.SetParent(result)
	}
	return result
}
