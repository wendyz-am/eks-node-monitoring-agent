package autocapture

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
)

// uploadPartSize is the multipart part size. With Concurrency 1 this is also
// the upper bound on how much of the stream is held in memory at once.
const uploadPartSize = 8 * 1024 * 1024

// ObjectKey builds the S3 key for a capture bundle. Node comes first so every
// node's captures group under one prefix, and the UTC timestamp (so keys sort
// consistently) keeps repeated captures for the same condition from overwriting
// each other.
func ObjectKey(node string, condition corev1.NodeConditionType, now time.Time) string {
	return "node-captures/" + node + "/" + now.UTC().Format("20060102T150405Z") +
		"-" + string(condition) + ".tar.gz"
}

// ObjectUploader is the one transfermanager call this package makes, narrowed
// so the worker can be driven by a fake in tests.
type ObjectUploader interface {
	UploadObject(ctx context.Context, in *transfermanager.UploadObjectInput,
		opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
}

// Compile-time checks: the real S3 client drives the transfer manager, and the
// transfer manager satisfies the narrowed uploader the worker depends on.
var (
	_ transfermanager.S3APIClient = (*s3.Client)(nil)
	_ ObjectUploader              = (*transfermanager.Client)(nil)
)

// newTransferClient wraps an S3 client in a transfer manager tuned to stream
// one part at a time, so memory stays flat regardless of bundle size.
func newTransferClient(s3Client transfermanager.S3APIClient, partSize int64) *transfermanager.Client {
	return transfermanager.New(s3Client, func(o *transfermanager.Options) {
		o.Concurrency = 1 // one part in flight: bounds memory
		o.PartSizeBytes = partSize
		o.MultipartUploadThreshold = partSize // default is 16 MiB: it would buffer that much first
		o.FailTimeout = 30 * time.Second      // abort must still run after our context expired
	})
}

// NewUploader builds the transfer-manager uploader for the capture worker. The
// region is taken from IMDS, and retries are capped so a persistently failing
// upload gives up and frees the worker rather than blocking indefinitely.
func NewUploader(ctx context.Context) (ObjectUploader, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithEC2IMDSRegion(), awsconfig.WithRetryMaxAttempts(3))
	if err != nil {
		return nil, err
	}
	return newTransferClient(s3.NewFromConfig(cfg), uploadPartSize), nil
}

// UploadStream streams body to bucket/key as application/gzip. The transfer
// manager reads body incrementally; if body fails (archive error or size cap),
// the manager aborts the multipart upload so no object or orphan parts remain.
func UploadStream(ctx context.Context, up ObjectUploader, bucket, key string, body io.Reader) error {
	_, err := up.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String("application/gzip"),
	})
	if err != nil {
		return fmt.Errorf("uploading to s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}
