package op

import token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"

type StarOp struct {
	token_regexp_abstract.Relatives
	op token_regexp_abstract.Op
}

func (t *StarOp) IsOp() {}
func (t *StarOp) ConsumeComplex(tokens []*token_regexp_abstract.Token) *token_regexp_abstract.MatchedResults {
	result := consumeComplex(t.op, tokens)
	result.AddMatchedPath(token_regexp_abstract.NewMatchedPathEmpty())
	return result
}

func Star(arg any) *StarOp {
	var op token_regexp_abstract.Op = nil
	switch v := arg.(type) {
	case string:
		op = Match(v)
	case token_regexp_abstract.Op:
		op = v
	default:
		return nil
	}
	result := &StarOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
		op:        op,
	}
	result.op.SetParent(result)
	return result
}
