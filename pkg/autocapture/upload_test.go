package autocapture

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/eks-node-monitoring-agent/pkg/conditions"
)

func TestObjectKey(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 34, 56, 0, time.UTC)
	tests := []struct {
		name string
		node string
		want string
	}{
		{
			name: "instance id node name",
			node: "i-0091abcd",
			want: "node-captures/i-0091abcd/20261001T123456Z-KernelReady.tar.gz",
		},
		{
			name: "private dns node name",
			node: "ip-10-0-1-42.ec2.internal",
			want: "node-captures/ip-10-0-1-42.ec2.internal/20261001T123456Z-KernelReady.tar.gz",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ObjectKey(tc.node, conditions.KernelReady, now)
			if got != tc.want {
				t.Errorf("ObjectKey = %q, want %q", got, tc.want)
			}
		})
	}
}

// fakePutObject records the last PutObject call and returns a configurable error.
type fakePutObject struct {
	gotInput *s3.PutObjectInput
	calls    int
	err      error
}

func (f *fakePutObject) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.calls++
	f.gotInput = in
	if f.err != nil {
		return nil, f.err
	}
	return &s3.PutObjectOutput{}, nil
}

func TestUploadSuccess(t *testing.T) {
	want := []byte("archive-bytes-123")
	fake := &fakePutObject{}

	if err := Upload(context.Background(), fake, "my-bucket", "my-key", bytes.NewReader(want)); err != nil {
		t.Fatal(err)
	}

	in := fake.gotInput
	if in == nil {
		t.Fatal("PutObject was not called")
	}
	if in.Bucket == nil || *in.Bucket != "my-bucket" {
		t.Errorf("Bucket = %v, want my-bucket", in.Bucket)
	}
	if in.Key == nil || *in.Key != "my-key" {
		t.Errorf("Key = %v, want my-key", in.Key)
	}
	if in.ContentType == nil || *in.ContentType != "application/gzip" {
		t.Errorf("ContentType = %v, want application/gzip", in.ContentType)
	}
	got, err := io.ReadAll(in.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Body = %q, want %q", got, want)
	}
}

func TestUploadBodyIsRetrySafe(t *testing.T) {
	want := []byte("archive-bytes-retry")
	fake := &fakePutObject{}

	if err := Upload(context.Background(), fake, "bucket", "key", bytes.NewReader(want)); err != nil {
		t.Fatal(err)
	}

	// The SDK rewinds the body between retry attempts, so Body must be seekable
	// and must yield the full bundle each time it is read.
	seeker, ok := fake.gotInput.Body.(io.Seeker)
	if !ok {
		t.Fatal("Body does not implement io.Seeker; a retry would resend an empty/partial body")
	}

	first, err := io.ReadAll(fake.gotInput.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, want) {
		t.Errorf("first read = %q, want %q", first, want)
	}

	if _, err := seeker.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	second, err := io.ReadAll(fake.gotInput.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second, want) {
		t.Errorf("second read after Seek = %q, want %q", second, want)
	}
}

func TestUploadError(t *testing.T) {
	sentinel := errors.New("boom")
	fake := &fakePutObject{err: sentinel}

	err := Upload(context.Background(), fake, "bucket", "key", bytes.NewReader([]byte("x")))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected error wrapping %v, got %v", sentinel, err)
	}
	if fake.calls != 1 {
		t.Errorf("expected PutObject called exactly once (no extra retry loop), got %d", fake.calls)
	}
}
