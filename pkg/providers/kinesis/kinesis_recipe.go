package kinesis

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_kinesis "github.com/aws/aws-sdk-go/service/kinesis"
	testcontainers_go "github.com/testcontainers/testcontainers-go"
	tc_network "github.com/testcontainers/testcontainers-go/network"
	"github.com/transferia/transferia/library/go/core/xerrors"
	tc_localstack "github.com/transferia/transferia/tests/tcrecipes/localstack"
)

func Prepare(img string) (string, error) {
	ctx := context.Background()
	net, err := tc_network.New(ctx)
	if err != nil {
		return "", xerrors.Errorf("Failed to create network: %w", err)
	}

	res, err := tc_localstack.Run(
		ctx,
		img,
		tc_network.WithNetwork([]string{"localstack"}, net),
		testcontainers_go.WithEnv(map[string]string{"SERVICES": "kinesis"}),
	)
	if err != nil {
		return "", xerrors.Errorf("Failed to run localstack container: %w", err)
	}

	endpoint, err := tc_localstack.GetEndpoint(res, ctx)
	if err != nil {
		return "", xerrors.Errorf("Failed to retrieve endpoint: %w", err)
	}

	return endpoint, nil
}

func NewClient(src *KinesisSource) (*aws_kinesis.Kinesis, error) {
	session := aws_session.Must(aws_session.NewSession(
		&aws.Config{
			Region: &src.Region,
			Credentials: aws_credentials.NewStaticCredentials(src.AccessKey,
				string(src.SecretKey), ""),
			Endpoint: &src.Endpoint,
		}),
	)

	client := *aws_kinesis.New(session)
	return &client, nil
}

func CreateStream(streamName string, client *aws_kinesis.Kinesis) error {
	if _, err := client.CreateStream(&aws_kinesis.CreateStreamInput{
		StreamName: &streamName,
	}); err != nil {
		return xerrors.Errorf("Failed to create stream: %w", err)
	}

	if err := client.WaitUntilStreamExists(&aws_kinesis.DescribeStreamInput{
		StreamName: &streamName,
	}); err != nil {
		return xerrors.Errorf("Failed to create stream: %w", err)
	}
	return nil
}

func SourceRecipe() (*KinesisSource, error) {
	endpoint, err := Prepare("localstack/localstack:2.0.0")
	if err != nil {
		return nil, xerrors.Errorf("Failed to start localstack: %w", err)
	}

	src := new(KinesisSource)
	src.Region = "us-west-2"
	src.Stream = "test_stream"
	src.AccessKey = "AKID"
	src.SecretKey = "secretkey"
	src.Endpoint = endpoint

	client, err := NewClient(src)
	if err != nil {
		return nil, xerrors.Errorf("Failed to create a Kinesis stream: %w", err)
	}

	if err = CreateStream(src.Stream, client); err != nil {
		return nil, xerrors.Errorf("Failed to create stream: %w", err)
	}

	return src, nil
}

func MustSource() *KinesisSource {
	res, err := SourceRecipe()
	if err != nil {
		panic(err)
	}
	return res
}
