package datastore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
)

// RegisterBuiltinProviders registers all the built-in datastore providers
func RegisterBuiltinProviders() {
	// Register GCS provider
	DefaultRegistry.Register("GCS", func(ctx context.Context, params map[string]string, schema DataStoreSchema) (DataStore, error) {
		destinationBucketPath, ok := params["destination_bucket_path"]
		if !ok {
			return nil, fmt.Errorf("invalid GCS config, no destination_bucket_path")
		}
		return NewGCSDataStore(ctx, destinationBucketPath, schema)
	})

	// Register GCS OAuth provider
	DefaultRegistry.Register("GCS_OAUTH", func(ctx context.Context, params map[string]string, schema DataStoreSchema) (DataStore, error) {
		destinationBucketPath, ok := params["destination_bucket_path"]
		if !ok {
			return nil, fmt.Errorf("invalid GCS config, no destination_bucket_path")
		}
		accessToken, ok := params["access_token"]
		if !ok {
			return nil, fmt.Errorf("invalid GCS config, no access_token")
		}
		return NewGCSDataStoreWithOAuth(ctx, destinationBucketPath, schema, accessToken)
	})

	// Register FS provider
	DefaultRegistry.Register("FS", func(ctx context.Context, params map[string]string, schema DataStoreSchema) (DataStore, error) {
		basePath, ok := params["base_path"]
		if !ok {
			return nil, fmt.Errorf("invalid FS config, no base_path")
		}
		return NewFSDataStore(basePath, schema)
	})

	// Register S3 provider
	DefaultRegistry.Register("S3", func(ctx context.Context, params map[string]string, schema DataStoreSchema) (DataStore, error) {
		bucketName, ok := params["bucket_name"]
		if !ok {
			return nil, fmt.Errorf("invalid S3 config, no bucket_name")
		}

		region := params["region"]
		if region == "" {
			region = "us-east-1" // default region
		}

		endpoint := params["endpoint"]
		forcePathStyle := params["force_path_style"] == "true"

		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(region),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}

		s3Cfg := S3Config{
			BucketName:     bucketName,
			Region:         region,
			Endpoint:       endpoint,
			ForcePathStyle: forcePathStyle,
		}

		return NewS3DataStore(cfg, s3Cfg, schema)
	})
}

func init() {
	RegisterBuiltinProviders()
}
