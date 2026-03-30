package packer

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewKeyPackerFromDebeziumParameters(connectorParameters map[string]string, logger log.Logger) (Packer, error) {
	if url := debezium_parameters.GetKeyConverterSchemaRegistryURL(connectorParameters); url != "" {
		caCert := debezium_parameters.GetKeyConverterSslCa(connectorParameters)
		srClient, err := confluent.NewSchemaRegistryClientWithTransport(url, caCert, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client, err: %w", err)
		}
		authData := debezium_parameters.GetKeyConverterSchemaRegistryUserPassword(connectorParameters)
		if authData != "" {
			userAndPassword := strings.SplitN(authData, ":", 2)
			if len(userAndPassword) != 2 {
				return nil, xerrors.Errorf("invalid auth data format. Param %v must be in `user:password` format or empty",
					debezium_parameters.KeyConverterBasicAuthUserInfo)
			}
			srClient.SetCredentials(userAndPassword[0], userAndPassword[1])
		}
		return NewPackerCacheFinalSchema(NewPackerSchemaRegistry(
			srClient,
			debezium_parameters.GetKeySubjectNameStrategy(connectorParameters),
			true,
			debezium_parameters.UseWriteIntoOneFullTopicName(connectorParameters),
			debezium_parameters.GetTopicPrefix(connectorParameters),
			debezium_parameters.GetKeyConverterDTJSONGenerateClosedContentSchema(connectorParameters),
		)), nil
	}
	if debezium_parameters.IsKeySchemaDisabled(connectorParameters) {
		return NewPackerSkipSchema(), nil
	}
	return NewPackerCacheFinalSchema(NewPackerIncludeSchema()), nil
}

func NewValuePackerFromDebeziumParameters(connectorParameters map[string]string, logger log.Logger) (Packer, error) {
	if url := debezium_parameters.GetValueConverterSchemaRegistryURL(connectorParameters); url != "" {
		caCert := debezium_parameters.GetValueConverterSslCa(connectorParameters)
		srClient, err := confluent.NewSchemaRegistryClientWithTransport(url, caCert, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client, err: %w", err)
		}
		authData := debezium_parameters.GetValueConverterSchemaRegistryUserPassword(connectorParameters)
		if authData != "" {
			userAndPassword := strings.SplitN(authData, ":", 2)
			if len(userAndPassword) != 2 {
				return nil, xerrors.Errorf("invalid auth data format. Param %v must be in `user:password` format or empty",
					debezium_parameters.ValueConverterBasicAuthUserInfo)
			}
			srClient.SetCredentials(userAndPassword[0], userAndPassword[1])
		}
		return NewPackerCacheFinalSchema(NewPackerSchemaRegistry(
			srClient,
			debezium_parameters.GetValueSubjectNameStrategy(connectorParameters),
			false,
			debezium_parameters.UseWriteIntoOneFullTopicName(connectorParameters),
			debezium_parameters.GetTopicPrefix(connectorParameters),
			debezium_parameters.GetValueConverterDTJSONGenerateClosedContentSchema(connectorParameters),
		)), nil
	}
	if namespaceID := debezium_parameters.GetYSRNamespaceID(connectorParameters); namespaceID != "" {
		return NewPackerRenewableOnExpiration(func() (Packer, abstract.Expirer, error) {
			ysrConnectionParameters, err := confluent.ResolveYSRNamespaceIDToConnectionParams(namespaceID)
			if err != nil {
				return nil, nil, xerrors.Errorf("failed to resolve namespace id, err: %w", err)
			}
			caCert := debezium_parameters.GetValueConverterSslCa(connectorParameters)
			srClient, err := confluent.NewSchemaRegistryClientWithTransport(ysrConnectionParameters.URL, caCert, logger)
			if err != nil {
				return nil, nil, xerrors.Errorf("Unable to create schema registry client, err: %w", err)
			}
			srClient.SetCredentials(ysrConnectionParameters.Username, ysrConnectionParameters.Password)

			return NewPackerCacheFinalSchema(NewPackerSchemaRegistry(
				srClient,
				debezium_parameters.GetValueSubjectNameStrategy(connectorParameters),
				false,
				debezium_parameters.UseWriteIntoOneFullTopicName(connectorParameters),
				debezium_parameters.GetTopicPrefix(connectorParameters),
				debezium_parameters.GetValueConverterDTJSONGenerateClosedContentSchema(connectorParameters),
			)), &ysrConnectionParameters, nil
		}, logger)
	}

	if debezium_parameters.IsValueSchemaDisabled(connectorParameters) {
		return NewPackerSkipSchema(), nil
	}
	return NewPackerCacheFinalSchema(NewPackerIncludeSchema()), nil
}
