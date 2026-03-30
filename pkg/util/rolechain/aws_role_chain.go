package rolechain

import (
	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
)

func newSession(
	creds *aws_credentials.Credentials,
) *aws_session.Session {
	return aws_session.Must(aws_session.NewSession(
		aws.NewConfig().WithCredentials(creds),
	))
}

func singleStep(
	ses *aws_session.Session,
	roleArn string,
) *aws_session.Session {
	creds := stscreds.NewCredentials(ses, roleArn)
	return newSession(creds)
}

// NewSession allows to create Session using multiple role assumptions.
// For example: RoleA assumes RoleB, RoleB assumes RoleC.
func NewSession(
	ses *aws_session.Session,
	roles ...string,
) *aws_session.Session {
	for _, role := range roles {
		ses = singleStep(ses, role)
	}
	return ses
}
