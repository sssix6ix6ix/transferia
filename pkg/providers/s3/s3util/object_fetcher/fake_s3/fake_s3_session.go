package fake_s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
)

type myResolverT struct{}

func (t *myResolverT) EndpointFor(service, region string, opts ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
	return endpoints.ResolvedEndpoint{}, nil
}

func NewSess() *aws_session.Session {
	return &aws_session.Session{
		Config: &aws.Config{
			EndpointResolver: &myResolverT{},
		},
	}
}
