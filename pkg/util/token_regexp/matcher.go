package token_regexp

import (
	"github.com/antlr4-go/antlr/v4"
	token_regexp_abstract "github.com/transferia/transferia/pkg/util/token_regexp/abstract"
	"github.com/transferia/transferia/pkg/util/token_regexp/op"
)

type TokenRegexp struct {
	ops []token_regexp_abstract.Op
}

func (m *TokenRegexp) prepareSeqAndTokens(tokens []antlr.Token) (*op.SeqOp, []*token_regexp_abstract.Token) {
	matcherTokens := make([]*token_regexp_abstract.Token, 0, len(tokens))
	for _, currToken := range tokens {
		matcherTokens = append(matcherTokens, token_regexp_abstract.NewToken(currToken))
	}
	args := make([]any, 0, len(m.ops))
	for _, el := range m.ops {
		args = append(args, el)
	}
	seq := op.Seq(args...)
	return seq, matcherTokens
}

// FindAll - just like in package 'regexp', method Regexp.FindAll
func (m *TokenRegexp) FindAll(tokens []antlr.Token) *token_regexp_abstract.MatchedResults {
	seq, matcherTokens := m.prepareSeqAndTokens(tokens)
	result := token_regexp_abstract.NewMatchedResults()
	for index := range matcherTokens {
		currResult := seq.ConsumeComplex(matcherTokens[index:])
		result.AddAllPaths(currResult)
	}
	return result
}

// MatchAll - just like in package 'regexp', method Regexp.MatchAll
func (m *TokenRegexp) MatchAll(tokens []antlr.Token) *token_regexp_abstract.MatchedResults {
	seq, matcherTokens := m.prepareSeqAndTokens(tokens)
	result := token_regexp_abstract.NewMatchedResults()
	currResult := seq.ConsumeComplex(matcherTokens)
	result.AddAllPaths(currResult)
	return result
}

// NewTokenRegexp - ctor for 'TokenRegexp' object
//   - expr - can be either Op (see 'op' package) or `string` (means op.Match - just to improve readability)
func NewTokenRegexp(expr []any) *TokenRegexp {
	ops := make([]token_regexp_abstract.Op, 0, len(expr))
	for _, el := range expr {
		switch v := el.(type) {
		case string:
			ops = append(ops, op.Match(v))
		case token_regexp_abstract.OpPrimitive:
			ops = append(ops, v)
		case token_regexp_abstract.OpComplex:
			ops = append(ops, v)
		default:
			return nil
		}
	}
	return &TokenRegexp{
		ops: ops,
	}
}
