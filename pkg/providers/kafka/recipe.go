package kafka

import (
	"context"
	"os"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_kafka "github.com/transferia/transferia/pkg/connection/kafka"
	tc_kafka "github.com/transferia/transferia/tests/tcrecipes/kafka"
)

func ContainerNeeded() bool {
	return os.Getenv("USE_TESTCONTAINERS") == "1" && os.Getenv("KAFKA_RECIPE_BROKER_LIST") == ""
}

func SourceRecipe() (*KafkaSource, error) {
	if ContainerNeeded() {
		if err := StartKafkaContainer(); err != nil {
			return nil, xerrors.Errorf("unable to setup container: %w", err)
		}
	}
	brokers := os.Getenv("KAFKA_RECIPE_BROKER_LIST")
	src := new(KafkaSource)
	src.Connection = &KafkaConnectionOptions{
		ClusterID:      "",
		TLS:            model.DisabledTLS,
		TLSFile:        "",
		UserEnabledTls: nil,
		Brokers:        []string{brokers},
		SubNetworkID:   "",
		ConnectionID:   "",
	}
	src.Auth = &KafkaAuth{
		Enabled:   false,
		Mechanism: conn_kafka.KafkaSaslSecurityMechanism_UNSPECIFIED,
		User:      "",
		Password:  "",
	}
	src.BufferSize = model.BytesSize(1024)
	return src, nil
}

func MustSourceRecipe() *KafkaSource {
	result, err := SourceRecipe()
	if err != nil {
		panic(err)
	}
	return result
}

func StartKafkaContainer() error {
	cntr, err := tc_kafka.RunContainer(context.Background())
	if err != nil {
		return xerrors.Errorf("unable to start kafka container: %w", err)
	}
	brokers, err := cntr.Brokers(context.Background())
	if err != nil {
		return xerrors.Errorf("unable fetch brokers: %w", err)
	}
	if err := os.Setenv("KAFKA_RECIPE_BROKER_LIST", strings.Join(brokers, ",")); err != nil {
		return xerrors.Errorf("unable to set broker list env: %w", err)
	}
	return nil
}

func DestinationRecipe() (*KafkaDestination, error) {
	if ContainerNeeded() {
		if err := StartKafkaContainer(); err != nil {
			return nil, xerrors.Errorf("unable to setup container: %w", err)
		}
	}
	brokers := os.Getenv("KAFKA_RECIPE_BROKER_LIST")

	dst := new(KafkaDestination)
	dst.Connection = &KafkaConnectionOptions{
		ClusterID:      "",
		TLS:            model.DisabledTLS,
		TLSFile:        "",
		UserEnabledTls: nil,
		Brokers:        []string{brokers},
		SubNetworkID:   "",
		ConnectionID:   "",
	}
	dst.Auth = &KafkaAuth{
		Enabled:   false,
		Mechanism: conn_kafka.KafkaSaslSecurityMechanism_UNSPECIFIED,
		User:      "",
		Password:  "",
	}
	dst.FormatSettings = model.SerializationFormat{
		Name:             model.SerializationFormatAuto,
		Settings:         nil,
		SettingsKV:       nil,
		BatchingSettings: nil,
	}
	return dst, nil
}
