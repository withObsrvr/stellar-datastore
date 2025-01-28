package datastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3DataStore implements DataStore for S3 and S3-compatible storage
type S3DataStore struct {
	client     *s3.Client
	bucketName string
	schema     DataStoreSchema
}

// S3Config holds the configuration for S3DataStore
type S3Config struct {
	BucketName     string
	Region         string
	Endpoint       string // Optional: for S3-compatible APIs
	ForcePathStyle bool   // Optional: set true for S3-compatible APIs
}

func NewS3DataStore(cfg aws.Config, s3Cfg S3Config, schema DataStoreSchema) (DataStore, error) {
	// Configure S3 client options
	options := []func(*s3.Options){
		func(o *s3.Options) {
			if s3Cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(s3Cfg.Endpoint)
			}
			o.UsePathStyle = s3Cfg.ForcePathStyle
		},
	}

	client := s3.NewFromConfig(cfg, options...)

	// Verify bucket exists
	_, err := client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(s3Cfg.BucketName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to access bucket %s: %w", s3Cfg.BucketName, err)
	}

	return &S3DataStore{
		client:     client,
		bucketName: s3Cfg.BucketName,
		schema:     schema,
	}, nil
}

func (s *S3DataStore) GetFileMetadata(ctx context.Context, path string) (map[string]string, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if strings.Contains(err.Error(), "NotFound") || errors.As(err, &nsk) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}

	metadata := map[string]string{
		"size":         fmt.Sprintf("%d", output.ContentLength),
		"modified":     output.LastModified.String(),
		"etag":         strings.Trim(*output.ETag, "\""),
		"content-type": aws.ToString(output.ContentType),
	}

	// Add custom metadata
	for k, v := range output.Metadata {
		metadata[k] = v
	}

	return metadata, nil
}

func (s *S3DataStore) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if strings.Contains(err.Error(), "NotFound") || errors.As(err, &nsk) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return output.Body, nil
}

func (s *S3DataStore) PutFile(ctx context.Context, path string, in io.WriterTo, metadata map[string]string) error {
	// Create a pipe to stream the data
	pr, pw := io.Pipe()

	// Start a goroutine to write the data to the pipe
	go func() {
		_, err := in.WriteTo(pw)
		pw.CloseWithError(err)
	}()

	// Upload to S3
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(s.bucketName),
		Key:      aws.String(path),
		Body:     pr,
		Metadata: metadata,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

func (s *S3DataStore) PutFileIfNotExists(ctx context.Context, path string, in io.WriterTo, metadata map[string]string) (bool, error) {
	exists, err := s.Exists(ctx, path)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	err = s.PutFile(ctx, path, in, metadata)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *S3DataStore) Exists(ctx context.Context, path string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if strings.Contains(err.Error(), "NotFound") || errors.As(err, &nsk) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *S3DataStore) Size(ctx context.Context, path string) (int64, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if strings.Contains(err.Error(), "NotFound") || errors.As(err, &nsk) {
			return 0, os.ErrNotExist
		}
		return 0, err
	}
	return *output.ContentLength, nil
}

func (s *S3DataStore) GetSchema() DataStoreSchema {
	return s.schema
}

func (s *S3DataStore) Close() error {
	return nil
}
