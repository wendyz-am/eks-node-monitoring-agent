package autocapture

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
)

// ObjectKey builds the S3 key for a capture bundle. Node comes first so every
// node's captures group under one prefix, and the UTC timestamp (so keys sort
// consistently) keeps repeated captures for the same condition from overwriting
// each other.
func ObjectKey(node string, condition corev1.NodeConditionType, now time.Time) string {
	return "node-captures/" + node + "/" + now.UTC().Format("20060102T150405Z") +
		"-" + string(condition) + ".tar.gz"
}

// PutObjectAPI narrows the dependency to the one S3 call this package makes, so
// the worker can be driven by a fake in tests without a real AWS client.
type PutObjectAPI interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// NewS3Client builds the S3 client for the capture worker. The region is taken
// from IMDS, and retries are capped so a persistently failing upload gives up and
// frees the worker for the next capture rather than blocking indefinitely.
func NewS3Client(ctx context.Context) (PutObjectAPI, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithEC2IMDSRegion(),
		awsconfig.WithRetryMaxAttempts(3),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

// Upload sends the capture bundle (a gzipped tarball of the collected logs) to
// S3. Retries are left to the SDK, so there's a single place that decides how
// hard to retry.
func Upload(ctx context.Context, client PutObjectAPI, bucket, key string, bundle io.Reader) error {
	// Buffer into a seekable reader so the SDK can re-read from the start on a
	// retry; a plain reader would be spent after the first try and resend nothing.
	data, err := io.ReadAll(bundle)
	if err != nil {
		return fmt.Errorf("uploading to s3://%s/%s: %w", bucket, key, err)
	}
	body := bytes.NewReader(data)

	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          body,
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/gzip"),
	}); err != nil {
		return fmt.Errorf("uploading to s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}
