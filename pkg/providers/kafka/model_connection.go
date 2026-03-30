package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"net"

	segmentio_sasl "github.com/segmentio/kafka-go/sasl"
	segmentio_scram "github.com/segmentio/kafka-go/sasl/scram"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_kafka "github.com/transferia/transferia/pkg/connection/kafka"
	"github.com/transferia/transferia/pkg/util/validators"
	franz_sasl "github.com/twmb/franz-go/pkg/sasl"
	franz_scram "github.com/twmb/franz-go/pkg/sasl/scram"
)

type KafkaAuth struct {
	Enabled   bool                                  `log:"true"`
	Mechanism conn_kafka.KafkaSaslSecurityMechanism `log:"true"`
	User      string                                `log:"true"`
	Password  string
}

func (a *KafkaAuth) GetAuthMechanism() (segmentio_sasl.Mechanism, error) {
	if !a.Enabled {
		return nil, nil
	}
	var algo segmentio_scram.Algorithm
	if a.Mechanism == conn_kafka.KafkaSaslSecurityMechanism_SCRAM_SHA512 {
		algo = segmentio_scram.SHA512
	} else {
		algo = segmentio_scram.SHA256
	}
	m, err := segmentio_scram.Mechanism(algo, a.User, a.Password)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (a *KafkaAuth) GetFranzAuthMechanism() franz_sasl.Mechanism {
	if !a.Enabled {
		return nil
	}
	auth := franz_scram.Auth{
		User: a.User,
		Pass: a.Password,
	}
	if a.Mechanism == conn_kafka.KafkaSaslSecurityMechanism_SCRAM_SHA512 {
		return auth.AsSha512Mechanism()
	}

	return auth.AsSha256Mechanism()
}

type KafkaConnectionOptions struct {
	ClusterID      string        `log:"true"`
	TLS            model.TLSMode `log:"true"`
	TLSFile        string        `model:"PemFileContent"`
	UserEnabledTls *bool
	Brokers        []string `log:"true"`
	SubNetworkID   string   `log:"true"`
	ConnectionID   string   `log:"true"`
}

func (o *KafkaConnectionOptions) TLSConfig() (*tls.Config, error) {
	if o.TLS == model.DisabledTLS {
		return nil, nil
	}
	if o.TLSFile != "" {
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM([]byte(o.TLSFile)) {
			return nil, xerrors.Errorf("credentials: failed to append certificates")
		}
		return &tls.Config{
			RootCAs: cp,
		}, nil
	}
	return &tls.Config{
		InsecureSkipVerify: true,
	}, nil
}

// BrokersHostnames returns a list of brokers' hostnames
func (o *KafkaConnectionOptions) BrokersHostnames() ([]string, error) {
	result := make([]string, len(o.Brokers))
	for i, b := range o.Brokers {
		if host, _, err := net.SplitHostPort(b); err != nil {
			return nil, xerrors.Errorf("failed to split Kafka broker URL %q to host and port: %w", b, err)
		} else {
			if err := validators.Host(host); err != nil {
				return nil, xerrors.Errorf("failed to validate Kafka broker host: %w", err)
			}
			result[i] = host
		}
	}
	return result, nil
}
