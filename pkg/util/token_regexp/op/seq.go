package op

import (
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type SeqOp struct {
	token_regexp_abstract.Relatives
	ops []token_regexp_abstract.Op
}

func (t *SeqOp) IsOp() {}

func exec(tokens []*token_regexp_abstract.Token, ops []token_regexp_abstract.Op) *token_regexp_abstract.MatchedResults {
	currOp := ops[0]
	opResult := token_regexp_abstract.NewMatchedResults()
	switch v := currOp.(type) {
	case token_regexp_abstract.OpPrimitive:
		lengths := v.ConsumePrimitive(tokens)
		opResult.AddMatchedPathsAfterConsumePrimitive(lengths, currOp, tokens)
	case token_regexp_abstract.OpComplex:
		localResults := v.ConsumeComplex(tokens)
		opResult.AddLocalResults(localResults, currOp, nil)
	}
	if !opResult.IsMatched() {
		return token_regexp_abstract.NewMatchedResults() // NOT FOUND
	}

	leastTempls := ops[1:]
	if len(leastTempls) == 0 { // we successfully executed all op commands
		return opResult
	}

	result := token_regexp_abstract.NewMatchedResults()
	for index := range opResult.Size() {
		currLocalPath := opResult.Index(index)
		localResults := exec(tokens[currLocalPath.Length():], leastTempls)
		result.AddLocalResults(localResults, currOp, tokens[0:currLocalPath.Length()])
	}
	return result
}

func (t *SeqOp) ConsumeComplex(tokens []*token_regexp_abstract.Token) *token_regexp_abstract.MatchedResults {
	return exec(tokens, t.ops)
}

func Seq(in ...any) *SeqOp {
	ops := make([]token_regexp_abstract.Op, 0)
	for _, el := range in {
		switch v := el.(type) {
		case string:
			ops = append(ops, Match(v))
		case token_regexp_abstract.Op:
			ops = append(ops, v)
		default:
			return nil
		}
	}
	result := &SeqOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
		ops:       ops,
	}
	for _, childOp := range result.ops {
		childOp.SetParent(result)
	}
	return result
}
