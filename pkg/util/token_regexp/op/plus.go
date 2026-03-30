package op

import (
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type PlusOp struct {
	token_regexp_abstract.Relatives
	op token_regexp_abstract.Op
}

func (t *PlusOp) IsOp() {}

// recursive function - every call is every match repeat of '+' operator
func consumeComplex(op token_regexp_abstract.Op, tokens []*token_regexp_abstract.Token) *token_regexp_abstract.MatchedResults {
	if len(tokens) == 0 {
		result := token_regexp_abstract.NewMatchedResults()
		return result
	}

	opResult := token_regexp_abstract.NewMatchedResults()

	switch currOp := op.(type) {
	case token_regexp_abstract.OpPrimitive:
		lengths := currOp.ConsumePrimitive(tokens)
		opResult.AddMatchedPathsAfterConsumePrimitive(lengths, op, tokens)
	case token_regexp_abstract.OpComplex:
		localResults := currOp.ConsumeComplex(tokens)
		opResult.AddLocalResults(localResults, op, nil)
	}

	result := token_regexp_abstract.NewMatchedResults()
	for index := range opResult.Size() {
		currOpPath := opResult.Index(index)
		currPathLength := currOpPath.Length()
		localResult := consumeComplex(op, tokens[currPathLength:])

		for localIndex := range localResult.Size() {
			currPath := token_regexp_abstract.NewMatchedPathParentPathChildPath(currOpPath, localResult.Index(localIndex))
			result.AddMatchedPath(currPath)
		}
		result.AddMatchedPath(currOpPath)
	}
	return result
}

func (t *PlusOp) ConsumeComplex(tokens []*token_regexp_abstract.Token) *token_regexp_abstract.MatchedResults {
	return consumeComplex(t.op, tokens)
}

func Plus(arg any) *PlusOp {
	var op token_regexp_abstract.Op = nil

	switch v := arg.(type) {
	case string:
		op = Match(v)
	case token_regexp_abstract.Op:
		op = v
	default:
		return nil
	}

	result := &PlusOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
		op:        op,
	}
	result.op.SetParent(result)
	return result
}
