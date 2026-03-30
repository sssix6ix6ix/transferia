package op

import (
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type CapturingGroupOp struct {
	token_regexp_abstract.Relatives
	seq *SeqOp
}

func (t *CapturingGroupOp) IsOp() {}

func (t *CapturingGroupOp) IsCapturingGroup() {}

func (t *CapturingGroupOp) ConsumeComplex(tokens []*token_regexp_abstract.Token) *token_regexp_abstract.MatchedResults {
	result := t.seq.ConsumeComplex(tokens)
	return result
}

func CapturingGroup(in ...any) *CapturingGroupOp {
	result := &CapturingGroupOp{
		Relatives: token_regexp_abstract.NewRelativesImpl(),
		seq:       Seq(in...),
	}
	result.seq.SetParent(result)
	return result
}
