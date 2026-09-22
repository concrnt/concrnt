package objectstore

import (
	"context"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// NewS3 builds an S3 client from the AWS SDK's default configuration chain
// (~/.aws/config, ~/.aws/credentials, AWS_* environment variables, IRSA /
// instance roles, ...). concrnt's own config carries no object-store
// settings: region, credentials, a custom endpoint_url for S3-compatible
// stores and checksum behaviour (request_checksum_calculation) are all the
// operator's AWS configuration. Only path-style addressing has no shared
// config key, so it is a parameter (MinIO needs it).
func NewS3(ctx context.Context, pathStyle bool) (*s3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = pathStyle
	}), nil
}
