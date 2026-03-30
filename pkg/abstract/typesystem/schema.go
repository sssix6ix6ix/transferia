package typesystem

import (
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	RestPlaceholder         = "REST..."
	NotSupportedPlaceholder = "N/A"
)

type Rule struct {
	// `Target` stores the mapping of `schema.Type`-s to provider-specific types for each `Target` provider.
	// example:
	//	{schema.TypeString: "TEXT"}
	Target map[ytschema.Type]string
	// `Source` stores the conversion rules from the provider-specific type to the `schema.Type` for each `Source` provider.
	// example:
	//	{"TINYTEXT": schema.TypeString}
	Source map[string]ytschema.Type
}

var (
	// `sourceTypeRegistry` stores the conversion rules from the provider-specific type to the transfer type for each `Source` provider.
	// example:
	//	{MYSQL: {"TINYTEXT": schema.TypeString}}
	sourceTypeRegistry = map[abstract.ProviderType]map[string]ytschema.Type{}
	// `targetTypeRegistry` stores the mapping of transfer types to provider-specific types for each `Target` provider.
	// example:
	//	{MYSQL: {schema.TypeString: "TEXT"}}
	targetTypeRegistry = map[abstract.ProviderType]map[ytschema.Type]string{}
)

func SourceRules(provider abstract.ProviderType, rules map[ytschema.Type][]string) {
	sourceTypeRegistry[provider] = map[string]ytschema.Type{}
	for target, sources := range rules {
		for _, sourceTyp := range sources {
			sourceTypeRegistry[provider][sourceTyp] = target
		}
	}
}

func TargetRule(provider abstract.ProviderType, rules map[ytschema.Type]string) {
	targetTypeRegistry[provider] = rules
}

func SupportedTypes() []ytschema.Type {
	return []ytschema.Type{
		ytschema.TypeInt64,
		ytschema.TypeInt32,
		ytschema.TypeInt16,
		ytschema.TypeInt8,
		ytschema.TypeUint64,
		ytschema.TypeUint32,
		ytschema.TypeUint16,
		ytschema.TypeUint8,
		ytschema.TypeFloat32,
		ytschema.TypeFloat64,
		ytschema.TypeBytes,
		ytschema.TypeString,
		ytschema.TypeBoolean,
		ytschema.TypeDate,
		ytschema.TypeDatetime,
		ytschema.TypeTimestamp,
		ytschema.TypeAny,
	}
}

func RuleFor(provider abstract.ProviderType) Rule {
	return Rule{
		Target: targetTypeRegistry[provider],
		Source: sourceTypeRegistry[provider],
	}
}
