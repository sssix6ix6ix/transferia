package kafka

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kgo"
)

func kafkaClientCommonOptions(cfg *KafkaSource) ([]kgo.Opt, error) {
	tlsConfig, err := cfg.Connection.TLSConfig()
	if err != nil {
		return nil, xerrors.Errorf("unable to get TLS config: %w", err)
	}
	cfg.Auth.Password, err = ResolvePassword(cfg.Connection, cfg.Auth)
	if err != nil {
		return nil, xerrors.Errorf("unable to get password: %w", err)
	}
	mechanism := cfg.Auth.GetFranzAuthMechanism()
	brokers, err := ResolveBrokers(cfg.Connection)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve brokers: %w", err)
	}

	// common kafka client options
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.FetchMaxBytes(10 * 1024 * 1024), // 10MB
		kgo.ConnIdleTimeout(30 * time.Second),
		kgo.RequestTimeoutOverhead(20 * time.Second),
	}

	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

func checkTopicExistence(clientOpts []kgo.Opt, topics []string) error {
	kfClient, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return xerrors.Errorf("unable to create kafka client to ensure topics: %w", err)
	}
	defer kfClient.Close()

	if err := ensureTopicsExistWithRetries(kfClient, topics...); err != nil {
		return xerrors.Errorf("unable to ensure topic existence: %w", err)
	}

	return nil
}
