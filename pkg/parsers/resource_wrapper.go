package parsers

import (
	"github.com/transferia/transferia/pkg/abstract"
	parsers_resources "github.com/transferia/transferia/pkg/parsers/resources"
)

type ResourceableParser struct {
	res    parsers_resources.AbstractResources
	parser Parser
}

func (p *ResourceableParser) Unwrap() Parser {
	return p.parser
}

func (p *ResourceableParser) ResourcesObj() parsers_resources.AbstractResources {
	return p.res
}

func (p *ResourceableParser) Do(msg Message, partition abstract.Partition) []abstract.ChangeItem {
	return p.parser.Do(msg, partition)
}

func (p *ResourceableParser) DoBatch(batch MessageBatch) []abstract.ChangeItem {
	return p.parser.DoBatch(batch)
}

func WithResource(parser Parser, res parsers_resources.AbstractResources) Parser {
	return &ResourceableParser{
		res:    res,
		parser: parser,
	}
}
