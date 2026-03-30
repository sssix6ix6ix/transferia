package op

import (
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type OptOp struct {
	token_regexp_abstract.Relatives
	op token_regexp_abstract.Op
}

func (t *OptOp) IsOp() {}

func (t *OptOp) ConsumeComplex(tokens []*token_regexp_abstract.Token) *token_regexp_abstract.MatchedResults {
	result := token_regexp_abstract.NewMatchedResults()
	if len(tokens) == 0 {
		result.AddMatchedPath(token_regexp_abstract.NewMatchedPathEmpty())
		return result
	}
	switch v := t.op.(type) {
	case token_regexp_abstract.OpPrimitive:
		lengths := v.ConsumePrimitive(tokens)
		result.AddMatchedPathsAfterConsumePrimitive(lengths, t.op, tokens)
	case token_regexp_abstract.OpComplex:
		localResults := v.ConsumeComplex(tokens)
		result.AddLocalResults(localResults, t.op, nil)
	}
	result.AddMatchedPath(token_regexp_abstract.NewMatchedPathEmpty())
	return result
}

func Opt(in any) *OptOp {
	var result *OptOp = nil
	switch v := in.(type) {
	case string:
		result = &OptOp{
			Relatives: token_regexp_abstract.NewRelativesImpl(),
			op:        Match(v),
		}
	case token_regexp_abstract.Op:
		result = &OptOp{
			Relatives: token_regexp_abstract.NewRelativesImpl(),
			op:        v,
		}
	default:
		return nil
	}
	result.op.SetParent(result)
	return result
}
