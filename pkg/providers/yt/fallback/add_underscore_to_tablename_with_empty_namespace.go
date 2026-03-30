package fallback

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
)

func init() {
	typesystem.AddFallbackTargetFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To: 9,
			Picker: func(endpoint model.EndpointParams) bool {
				if endpoint.GetProviderType() != provider_yt.ProviderType {
					return false
				}

				dstParams, ok := endpoint.(*provider_yt.YtDestinationWrapper)
				if !ok {
					return false
				}
				return dstParams.Static()
			},
			Function: func(item *abstract.ChangeItem) (*abstract.ChangeItem, error) {
				if item.Schema == "" {
					item.Table = "_" + item.Table
				}
				return item, nil
			},
		}
	})
}
