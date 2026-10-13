package autocapture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
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

// fakeUploader records the UploadObjectInput and returns a configurable error.
type fakeUploader struct {
	gotInput *transfermanager.UploadObjectInput
	err      error
}

func (f *fakeUploader) UploadObject(ctx context.Context, in *transfermanager.UploadObjectInput,
	opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
	f.gotInput = in
	if f.err != nil {
		return nil, f.err
	}
	return &transfermanager.UploadObjectOutput{}, nil
}

func TestUploadStreamBuildsInput(t *testing.T) {
	want := []byte("archive-bytes-123")
	fake := &fakeUploader{}

	if err := UploadStream(context.Background(), fake, "my-bucket", "my-key", bytes.NewReader(want)); err != nil {
		t.Fatal(err)
	}

	in := fake.gotInput
	if in == nil {
		t.Fatal("UploadObject was not called")
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

func TestUploadStreamError(t *testing.T) {
	sentinel := errors.New("boom")
	fake := &fakeUploader{err: sentinel}

	err := UploadStream(context.Background(), fake, "my-bucket", "my-key", bytes.NewReader([]byte("x")))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected error wrapping %v, got %v", sentinel, err)
	}
	if want := "s3://my-bucket/my-key"; !bytes.Contains([]byte(err.Error()), []byte(want)) {
		t.Errorf("expected error message to contain %q, got %q", want, err.Error())
	}
}

// fakeS3 implements the parts of transfermanager.S3APIClient the upload path
// uses. The embedded interface satisfies the methods we do not override
// (GetObject, HeadObject, ListObjectsV2); calling one would panic on the nil
// interface, which is the signal that the upload path used an unexpected call.
type fakeS3 struct {
	transfermanager.S3APIClient

	mu sync.Mutex

	uploadID string

	putCalls      int
	putBody       []byte
	createCalls   int
	completeCalls int
	abortCalls    int
	abortUploadID string
	abortCtxErr   error

	parts map[int32][]byte // part number -> bytes

	// uploadPartErr, when set, is returned by UploadPart after enough bytes
	// have been received (the reader itself fails in the streaming tests, so
	// this is unused there and kept nil).
	uploadPartErr error
}

func newFakeS3() *fakeS3 {
	return &fakeS3{uploadID: "test-upload-id", parts: map[int32][]byte{}}
}

func (f *fakeS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, err := io.ReadAll(in.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls++
	f.putBody = b
	if err != nil {
		return nil, err
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(f.uploadID)}, nil
}

func (f *fakeS3) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.uploadPartErr != nil {
		return nil, f.uploadPartErr
	}
	part := int32(1)
	if in.PartNumber != nil {
		part = *in.PartNumber
	}
	buf := make([]byte, len(b))
	copy(buf, b)
	f.parts[part] = buf
	return &s3.UploadPartOutput{ETag: aws.String(fmt.Sprintf("etag-%d", part))}, nil
}

func (f *fakeS3) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeCalls++
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeS3) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abortCalls++
	if in.UploadId != nil {
		f.abortUploadID = *in.UploadId
	}
	f.abortCtxErr = ctx.Err()
	return &s3.AbortMultipartUploadOutput{}, nil
}

// reassemble returns the parts concatenated in part-number order.
func (f *fakeS3) reassemble() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	nums := make([]int, 0, len(f.parts))
	for n := range f.parts {
		nums = append(nums, int(n))
	}
	sort.Ints(nums)
	var out []byte
	for _, n := range nums {
		out = append(out, f.parts[int32(n)]...)
	}
	return out
}

// errAfterReader yields the body, then fails with err once limit bytes have
// been read, so a mid-stream failure can be exercised deterministically.
type errAfterReader struct {
	data  []byte
	off   int
	limit int
	err   error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if r.off >= r.limit {
		return 0, r.err
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	if r.off >= r.limit {
		// deliver the error on the next read, after handing back these bytes
		return n, nil
	}
	return n, nil
}

const oneMiB = 1 << 20

func TestUploadStreamSmallBodySinglePut(t *testing.T) {
	fake := newFakeS3()
	up := newTransferClient(fake, oneMiB)

	body := bytes.Repeat([]byte("a"), 512*1024) // < 1 MiB
	if err := UploadStream(context.Background(), up, "b", "k", bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}

	if fake.putCalls != 1 {
		t.Errorf("expected exactly one PutObject, got %d", fake.putCalls)
	}
	if fake.createCalls != 0 || fake.completeCalls != 0 || fake.abortCalls != 0 {
		t.Errorf("expected no multipart calls, got create=%d complete=%d abort=%d",
			fake.createCalls, fake.completeCalls, fake.abortCalls)
	}
	if !bytes.Equal(fake.putBody, body) {
		t.Errorf("PutObject body mismatch: got %d bytes, want %d", len(fake.putBody), len(body))
	}
}

func TestUploadStreamMultipart(t *testing.T) {
	fake := newFakeS3()
	up := newTransferClient(fake, oneMiB)

	// 2.5 MiB with 1 MiB parts -> 3 parts (1 + 1 + 0.5).
	body := make([]byte, (5*oneMiB)/2)
	for i := range body {
		body[i] = byte(i)
	}
	if err := UploadStream(context.Background(), up, "b", "k", bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}

	if fake.createCalls != 1 {
		t.Errorf("expected 1 CreateMultipartUpload, got %d", fake.createCalls)
	}
	if got := len(fake.parts); got != 3 {
		t.Errorf("expected 3 UploadPart calls, got %d", got)
	}
	if fake.completeCalls != 1 {
		t.Errorf("expected 1 CompleteMultipartUpload, got %d", fake.completeCalls)
	}
	if fake.abortCalls != 0 {
		t.Errorf("expected no AbortMultipartUpload, got %d", fake.abortCalls)
	}
	if fake.putCalls != 0 {
		t.Errorf("expected no PutObject for a multipart upload, got %d", fake.putCalls)
	}
	if got := fake.reassemble(); !bytes.Equal(got, body) {
		t.Errorf("reassembled parts mismatch: got %d bytes, want %d", len(got), len(body))
	}
}

func TestUploadStreamAbortsOnReaderError(t *testing.T) {
	fake := newFakeS3()
	up := newTransferClient(fake, oneMiB)

	customErr := errors.New("reader exploded")
	data := make([]byte, 3*oneMiB)
	r := &errAfterReader{data: data, limit: 3 * oneMiB / 2, err: customErr} // fail after 1.5 MiB

	err := UploadStream(context.Background(), up, "b", "k", r)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, customErr) {
		t.Errorf("expected error wrapping %v, got %v", customErr, err)
	}
	if fake.abortCalls != 1 {
		t.Errorf("expected 1 AbortMultipartUpload, got %d", fake.abortCalls)
	}
	if fake.abortUploadID != fake.uploadID {
		t.Errorf("Abort UploadId = %q, want %q", fake.abortUploadID, fake.uploadID)
	}
	if fake.completeCalls != 0 {
		t.Errorf("expected Complete NOT called, got %d", fake.completeCalls)
	}
}

func TestUploadStreamAbortRunsAfterContextCancelled(t *testing.T) {
	fake := newFakeS3()
	up := newTransferClient(fake, oneMiB)

	ctx, cancel := context.WithCancel(context.Background())

	customErr := errors.New("reader exploded after cancel")
	data := make([]byte, 3*oneMiB)
	// Cancel the context, then fail the reader, so the abort path runs with an
	// already-cancelled parent context.
	r := &cancelThenFailReader{
		data:   data,
		limit:  3 * oneMiB / 2,
		err:    customErr,
		cancel: cancel,
	}

	err := UploadStream(ctx, up, "b", "k", r)
	if err == nil {
		t.Fatal("expected an error")
	}
	if fake.abortCalls != 1 {
		t.Errorf("expected 1 AbortMultipartUpload, got %d", fake.abortCalls)
	}
	// FailTimeout gives Abort a fresh context, so inside Abort ctx.Err() is nil
	// even though the parent context was cancelled.
	if fake.abortCtxErr != nil {
		t.Errorf("expected Abort context not expired (ctx.Err()==nil), got %v", fake.abortCtxErr)
	}
}

// cancelThenFailReader cancels the upload context once limit bytes have been
// read, then fails on the next read, so the abort runs after cancellation.
type cancelThenFailReader struct {
	data     []byte
	off      int
	limit    int
	err      error
	cancel   context.CancelFunc
	canceled bool
}

func (r *cancelThenFailReader) Read(p []byte) (int, error) {
	if r.off >= r.limit {
		if !r.canceled {
			r.cancel()
			r.canceled = true
		}
		return 0, r.err
	}
	n := copy(p, r.data[r.off:r.limit])
	r.off += n
	return n, nil
}
