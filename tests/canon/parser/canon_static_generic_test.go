package parser

import (
	"embed"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	dt_metrics "github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/tests/canon/parser/testcase"
	"github.com/transferia/transferia/tests/canon/validator"
)

//go:embed samples/static/generic/*
var TestGenericSamples embed.FS

func TestGenericParsers(t *testing.T) {
	cases := testcase.LoadStaticTestCases(t, TestGenericSamples)

	for tc := range cases {
		t.Run(tc, func(t *testing.T) {
			currCase := cases[tc]
			parser, err := parsers.NewParserFromParserConfig(currCase.ParserConfig, false, logger.Log, stats.NewSourceStats(dt_metrics.NewRegistry().WithTags(map[string]string{
				"id": "TestParser_Do",
			})))
			require.NoError(t, err)
			require.NotNil(t, parser)
			res := parser.Do(currCase.Data, abstract.Partition{Topic: currCase.TopicName})
			require.NotNil(t, res)
			sink := validator.New(
				false,
				validator.ValuesTypeChecker,
				validator.Canonizator(t),
			)()
			require.NoError(t, sink.Push(res))
			require.NoError(t, sink.Close())
		})
	}
}
