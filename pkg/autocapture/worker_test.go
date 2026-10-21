package autocapture

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/aws/eks-node-monitoring-agent/api/v1alpha1"
	"github.com/aws/eks-node-monitoring-agent/pkg/manager"
	"github.com/aws/eks-node-monitoring-agent/pkg/util/captureguard"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.SchemeBuilder.AddToScheme(scheme))
	return scheme
}

func autoLogDiagnostic(name, bucket string, created time.Time) *v1alpha1.NodeDiagnostic {
	return &v1alpha1.NodeDiagnostic{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: v1alpha1.NodeDiagnosticSpec{
			AutoLogCapture: &v1alpha1.AutoLogCapture{Bucket: bucket},
		},
	}
}

func TestFindBucket(t *testing.T) {
	tests := []struct {
		name        string
		objects     []client.Object
		wantBucket  string
		wantName    string
		wantIgnored int
	}{
		{
			name: "no NodeDiagnostics",
		},
		{
			name:       "single bucket returned",
			objects:    []client.Object{autoLogDiagnostic("alpha", "my-bucket", time.Unix(100, 0))},
			wantBucket: "my-bucket",
			wantName:   "alpha",
		},
		{
			// the newest object's bucket must win
			name: "newest wins",
			objects: []client.Object{
				autoLogDiagnostic("older-name", "old-bucket", time.Unix(100, 0)),
				autoLogDiagnostic("newer-name", "new-bucket", time.Unix(200, 0)),
			},
			wantBucket:  "new-bucket",
			wantName:    "newer-name",
			wantIgnored: 1,
		},
		{
			// equal timestamps: the lexically larger name wins
			name: "tie broken by larger name",
			objects: []client.Object{
				autoLogDiagnostic("aaa", "a-bucket", time.Unix(100, 0)),
				autoLogDiagnostic("zzz", "z-bucket", time.Unix(100, 0)),
			},
			wantBucket:  "z-bucket",
			wantName:    "zzz",
			wantIgnored: 1,
		},
		{
			// object with an empty bucket is not a match
			name: "empty bucket skipped",
			objects: []client.Object{
				autoLogDiagnostic("alpha", "", time.Unix(100, 0)),
				autoLogDiagnostic("beta", "real-bucket", time.Unix(200, 0)),
			},
			wantBucket: "real-bucket",
			wantName:   "beta",
		},
		{
			// object with only logCapture is not a match
			name: "logCapture only skipped",
			objects: []client.Object{
				&v1alpha1.NodeDiagnostic{
					ObjectMeta: metav1.ObjectMeta{Name: "log-only", CreationTimestamp: metav1.NewTime(time.Unix(50, 0))},
					Spec: v1alpha1.NodeDiagnosticSpec{
						LogCapture: &v1alpha1.LogCapture{UploadDestination: "node"},
					},
				},
				autoLogDiagnostic("auto", "real-bucket", time.Unix(200, 0)),
			},
			wantBucket: "real-bucket",
			wantName:   "auto",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tc.objects...).Build()

			bucket, name, ignored, err := findBucket(context.Background(), c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantBucket, bucket)
			assert.Equal(t, tc.wantName, name)
			assert.Equal(t, tc.wantIgnored, ignored)
		})
	}
}

func TestFindBucket_ListErrorReturned(t *testing.T) {
	wantErr := errors.New("list failed")
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return wantErr
			},
		}).
		Build()

	bucket, name, ignored, err := findBucket(context.Background(), c)
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Equal(t, "", bucket)
	assert.Equal(t, "", name)
	assert.Equal(t, 0, ignored)
}

// countingLogger records each emitted message so tests can assert how many
// times a given line was logged.
func countingLogger(counts map[string]int) logr.Logger {
	return logr.New(&countingSink{counts: counts})
}

type countingSink struct {
	counts map[string]int
}

func (s *countingSink) Init(logr.RuntimeInfo)                 {}
func (s *countingSink) Enabled(int) bool                      { return true }
func (s *countingSink) WithValues(...any) logr.LogSink        { return s }
func (s *countingSink) WithName(string) logr.LogSink          { return s }
func (s *countingSink) Error(error, string, ...any)           {}
func (s *countingSink) Info(level int, msg string, kv ...any) { s.counts[msg]++ }

const (
	msgOff       = "auto log capture is off: no NodeDiagnostic sets spec.autoLogCapture.bucket"
	msgRequest   = "capture requested"
	msgSkip      = "capture skipped: cooldown active"
	msgSkipGuard = "capture skipped: another log collection is running"
	msgCollected = "capture collected"
	msgTimedOut  = "capture timed out, collector may be blocked on host I/O"
	msgFailed    = "failed to collect logs for capture"
)

// recordingLogger is like countingLogger but also records the error passed
// to Error calls, so tests can assert a specific failure was logged.
func recordingLogger(counts map[string]int, errs map[string]error) logr.Logger {
	return logr.New(&recordingSink{counts: counts, errs: errs})
}

type recordingSink struct {
	counts map[string]int
	errs   map[string]error
}

func (s *recordingSink) Init(logr.RuntimeInfo)          {}
func (s *recordingSink) Enabled(int) bool               { return true }
func (s *recordingSink) WithValues(...any) logr.LogSink { return s }
func (s *recordingSink) WithName(string) logr.LogSink   { return s }
func (s *recordingSink) Info(level int, msg string, kv ...any) {
	s.counts[msg]++
}
func (s *recordingSink) Error(err error, msg string, kv ...any) {
	s.counts[msg]++
	s.errs[msg] = err
}

// stubCollect replaces w.collect with one that records calls and creates a
// temp dir with nFiles small files, so tests never read the real host.
func stubCollect(t *testing.T, nFiles int) (fn func(context.Context, []v1alpha1.LogCategory, Trigger) (string, int, error), calls *int) {
	t.Helper()
	n := 0
	fn = func(context.Context, []v1alpha1.LogCategory, Trigger) (string, int, error) {
		n++
		dir, err := os.MkdirTemp("", "autocapture-test-*")
		require.NoError(t, err)
		for i := 0; i < nFiles; i++ {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "f"+strconv.Itoa(i)), []byte("xx"), 0644))
		}
		return dir, 0, nil
	}
	return fn, &n
}

func TestWorker_NoBucketLogsOffOnce(t *testing.T) {
	counts := map[string]int{}
	ch := make(chan manager.CaptureRequest, 2)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	w := NewWorker(ch, c, nil, &captureguard.Guard{}, "node-1", countingLogger(counts))

	// Two requests with no bucket configured: the "off" message logs once.
	req := manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down", At: time.Now()}
	w.handle(context.Background(), req)
	w.handle(context.Background(), req)

	assert.Equal(t, 1, counts[msgOff], "off message must be logged once for repeated requests")
	assert.Equal(t, 0, counts[msgRequest])
}

func TestWorker_BucketHandlesRequest(t *testing.T) {
	counts := map[string]int{}
	ch := make(chan manager.CaptureRequest, 1)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(autoLogDiagnostic("alpha", "my-bucket", time.Unix(100, 0))).Build()
	w := NewWorker(ch, c, nil, &captureguard.Guard{}, "node-1", countingLogger(counts))
	collectFn, _ := stubCollect(t, 0)
	w.collect = collectFn

	w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down", At: time.Now()})

	assert.Equal(t, 1, counts[msgRequest], "a configured bucket must produce a capture-requested log")
	assert.Equal(t, 0, counts[msgOff])
}

func TestWorker_StartReturnsNilOnCancel(t *testing.T) {
	ch := make(chan manager.CaptureRequest)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	w := NewWorker(ch, c, nil, &captureguard.Guard{}, "node-1", logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "Start must return nil when the context is canceled")
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

func TestWorker_Cooldown(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	const condA = "NetworkingReady"
	const condB = "StorageReady"

	// step is one request: at offset from base, for a condition.
	type step struct {
		offset    time.Duration
		condition corev1.NodeConditionType
	}
	tests := []struct {
		name        string
		steps       []step
		wantRequest int
		wantSkip    int
	}{
		{
			name: "second request for same condition inside window is dropped",
			steps: []step{
				{0, condA},
				{29 * time.Minute, condA},
			},
			wantRequest: 1,
			wantSkip:    1,
		},
		{
			name: "request exactly at the window is handled",
			steps: []step{
				{0, condA},
				{30 * time.Minute, condA},
			},
			wantRequest: 2,
		},
		{
			name: "request after the window is handled",
			steps: []step{
				{0, condA},
				{31 * time.Minute, condA},
			},
			wantRequest: 2,
		},
		{
			name: "different conditions do not block each other",
			steps: []step{
				{0, condA},
				{1 * time.Minute, condB},
			},
			wantRequest: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			counts := map[string]int{}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).
				WithObjects(autoLogDiagnostic("alpha", "my-bucket", time.Unix(100, 0))).Build()
			w := NewWorker(make(chan manager.CaptureRequest), c, nil, &captureguard.Guard{}, "node-1", countingLogger(counts))
			collectFn, _ := stubCollect(t, 0)
			w.collect = collectFn

			var cur time.Time
			w.now = func() time.Time { return cur }
			for _, s := range tc.steps {
				cur = base.Add(s.offset)
				w.handle(context.Background(), manager.CaptureRequest{Condition: s.condition, Reason: "Down"})
			}

			assert.Equal(t, tc.wantRequest, counts[msgRequest], "capture-requested count")
			assert.Equal(t, tc.wantSkip, counts[msgSkip], "cooldown-skip count")
		})
	}
}

func TestWorker_NoBucketDoesNotStartCooldown(t *testing.T) {
	counts := map[string]int{}
	// A NodeDiagnostic whose bucket we flip on later, backing the same client.
	nd := autoLogDiagnostic("alpha", "", time.Unix(100, 0))
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(nd).Build()
	w := NewWorker(make(chan manager.CaptureRequest), c, nil, &captureguard.Guard{}, "node-1", countingLogger(counts))
	collectFn, _ := stubCollect(t, 0)
	w.collect = collectFn

	cur := time.Unix(1_000_000, 0)
	w.now = func() time.Time { return cur }

	// First request: no bucket, so nothing is captured and no cooldown starts.
	w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})
	assert.Equal(t, 0, counts[msgRequest])

	// Configure the bucket, then a request one minute later must be handled,
	// proving the earlier no-bucket request did not start a cooldown.
	nd.Spec.AutoLogCapture.Bucket = "my-bucket"
	require.NoError(t, c.Update(context.Background(), nd))

	cur = cur.Add(1 * time.Minute)
	w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})
	assert.Equal(t, 1, counts[msgRequest], "a request after a bucket is set must be handled")
	assert.Equal(t, 0, counts[msgSkip])
}

func bucketClient(t *testing.T) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(autoLogDiagnostic("alpha", "my-bucket", time.Unix(100, 0))).Build()
}

// TestWorker_BuildsTriggerFromRequest confirms the worker threads the
// CaptureRequest's Condition/Reason/Message/At into the Trigger passed to
// collect, and fills Node/NMAVersion from the worker's own construction
// rather than leaving them empty.
func TestWorker_BuildsTriggerFromRequest(t *testing.T) {
	counts := map[string]int{}
	w := NewWorker(make(chan manager.CaptureRequest), bucketClient(t), nil, &captureguard.Guard{}, "node-1", countingLogger(counts))

	var gotTrigger Trigger
	collectFn, _ := stubCollect(t, 0)
	w.collect = func(ctx context.Context, cats []v1alpha1.LogCategory, trig Trigger) (string, int, error) {
		gotTrigger = trig
		return collectFn(ctx, cats, trig)
	}

	at := time.Now()
	w.handle(context.Background(), manager.CaptureRequest{
		Condition: "NetworkingReady",
		Reason:    "InterfaceDown",
		Message:   "eth0 is down",
		At:        at,
	})

	assert.Equal(t, corev1.NodeConditionType("NetworkingReady"), gotTrigger.Condition)
	assert.Equal(t, "InterfaceDown", gotTrigger.Reason)
	assert.Equal(t, "eth0 is down", gotTrigger.Message)
	assert.Equal(t, at, gotTrigger.At)
	assert.Equal(t, "node-1", gotTrigger.Node, "Node must come from the worker's configured node name")
	assert.NotEmpty(t, gotTrigger.NMAVersion, "NMAVersion must be populated")
}

func TestWorker_CollectsAndDeletesOnBucket(t *testing.T) {
	counts := map[string]int{}
	w := NewWorker(make(chan manager.CaptureRequest), bucketClient(t), nil, &captureguard.Guard{}, "node-1", countingLogger(counts))

	var capturedDir string
	collectFn, calls := stubCollect(t, 2)
	w.collect = func(ctx context.Context, cats []v1alpha1.LogCategory, trig Trigger) (string, int, error) {
		dir, failed, err := collectFn(ctx, cats, trig)
		capturedDir = dir
		return dir, failed, err
	}

	w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})

	assert.Equal(t, 1, *calls, "collect must be called once")
	assert.Equal(t, 1, counts[msgCollected], "a successful collect must log 'capture collected'")
	_, statErr := os.Stat(capturedDir)
	assert.True(t, os.IsNotExist(statErr), "the collected folder must be deleted afterwards")
}

func TestWorker_GuardHeldSkipsCollectionAndFreesCooldown(t *testing.T) {
	counts := map[string]int{}
	guard := &captureguard.Guard{}
	ch := make(chan manager.CaptureRequest)
	c := bucketClient(t)
	w := NewWorker(ch, c, nil, guard, "node-1", countingLogger(counts))
	collectFn, calls := stubCollect(t, 0)
	w.collect = collectFn

	// Simulate the NodeDiagnostic controller already holding the guard.
	require.True(t, guard.TryAcquire())

	req := manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"}
	w.handle(context.Background(), req)

	assert.Equal(t, 0, *calls, "collect must not run while the guard is held elsewhere")
	assert.Equal(t, 1, counts[msgSkipGuard], "a guard conflict must log the skip message")

	// Release as the controller would, then confirm the skipped request did
	// not consume the cooldown: a later request for the same condition must
	// still be collected.
	guard.Release()
	w.handle(context.Background(), req)
	assert.Equal(t, 1, *calls, "a later request must be collected once the guard is free")
	assert.Equal(t, 0, counts[msgSkip], "the earlier guard skip must not have consumed the cooldown")
}

func TestWorker_GuardFreedAfterHandleReturns(t *testing.T) {
	for _, tc := range []struct {
		name    string
		collect func(context.Context, []v1alpha1.LogCategory, Trigger) (string, int, error)
	}{
		{
			name: "success",
			collect: func(context.Context, []v1alpha1.LogCategory, Trigger) (string, int, error) {
				dir, err := os.MkdirTemp("", "autocapture-guard-test-*")
				require.NoError(t, err)
				return dir, 0, nil
			},
		},
		{
			name: "collect error",
			collect: func(context.Context, []v1alpha1.LogCategory, Trigger) (string, int, error) {
				return "", 0, errors.New("collect boom")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counts := map[string]int{}
			guard := &captureguard.Guard{}
			w := NewWorker(make(chan manager.CaptureRequest), bucketClient(t), nil, guard, "node-1", countingLogger(counts))
			calls := 0
			w.collect = func(ctx context.Context, cats []v1alpha1.LogCategory, trig Trigger) (string, int, error) {
				calls++
				return tc.collect(ctx, cats, trig)
			}

			w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})

			assert.Equal(t, 1, calls, "collect must be called exactly once")
			assert.True(t, guard.TryAcquire(), "the guard must be free once handle returns")
			guard.Release()
		})
	}
}

// TestWorker_GuardHeldUntilCollectReturns proves the guard stays held for as
// long as the collection goroutine is actually running, not merely until
// handle times out and returns. The stub blocks on an external signal
// independent of ctx cancellation, so the guard being held can only be
// explained by the goroutine still running, not by ctx semantics.
func TestWorker_GuardHeldUntilCollectReturns(t *testing.T) {
	counts := map[string]int{}
	guard := &captureguard.Guard{}
	w := NewWorker(make(chan manager.CaptureRequest), bucketClient(t), nil, guard, "node-1", countingLogger(counts))
	w.captureTimeout = 10 * time.Millisecond

	release := make(chan struct{})
	returned := make(chan struct{})
	w.collect = func(ctx context.Context, _ []v1alpha1.LogCategory, _ Trigger) (string, int, error) {
		<-release
		close(returned)
		return "", 0, errors.New("collect boom")
	}

	done := make(chan struct{})
	go func() {
		w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handle did not return after the capture timeout")
	}

	// handle has returned, but the stub is still blocked on release: the
	// guard must still be held.
	assert.False(t, guard.TryAcquire(), "the guard must still be held while collection is outstanding")

	close(release)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("stubbed collect never returned")
	}

	assert.Eventually(t, func() bool {
		return guard.TryAcquire()
	}, 2*time.Second, 10*time.Millisecond, "the guard must be released once collection actually finishes")
}

func TestWorker_NoBucketDoesNotCollect(t *testing.T) {
	counts := map[string]int{}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	w := NewWorker(make(chan manager.CaptureRequest), c, nil, &captureguard.Guard{}, "node-1", countingLogger(counts))
	collectFn, calls := stubCollect(t, 1)
	w.collect = collectFn

	w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})

	assert.Equal(t, 0, *calls, "no bucket must not trigger collection")
	assert.Equal(t, 0, counts[msgCollected])
}

func TestWorker_CooldownDoesNotCollect(t *testing.T) {
	counts := map[string]int{}
	w := NewWorker(make(chan manager.CaptureRequest), bucketClient(t), nil, &captureguard.Guard{}, "node-1", countingLogger(counts))
	collectFn, calls := stubCollect(t, 1)
	w.collect = collectFn

	cur := time.Unix(1_000_000, 0)
	w.now = func() time.Time { return cur }
	req := manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"}

	w.handle(context.Background(), req)
	cur = cur.Add(1 * time.Minute) // inside the cooldown window
	w.handle(context.Background(), req)

	assert.Equal(t, 1, *calls, "a request inside the cooldown must not collect again")
}

func TestWorker_CollectErrorIsLoggedNotFatal(t *testing.T) {
	counts := map[string]int{}
	errs := map[string]error{}
	w := NewWorker(make(chan manager.CaptureRequest), bucketClient(t), nil, &captureguard.Guard{}, "node-1", recordingLogger(counts, errs))
	wantErr := errors.New("collect boom")
	w.collect = func(context.Context, []v1alpha1.LogCategory, Trigger) (string, int, error) {
		return "", 0, wantErr
	}

	// Must not panic; the error path returns before "capture collected".
	w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})
	assert.Equal(t, 0, counts[msgCollected], "a collect error must not log a successful collection")
	assert.Equal(t, 1, counts[msgFailed], "the collect error must be logged")
	assert.ErrorIs(t, errs[msgFailed], wantErr, "the logged error must be the one collect returned")
}

// TestWorker_CollectTimeoutReturnsAndCleansUp verifies that handle does not
// block past captureTimeout when collect is stuck, and that the directory
// the stuck collect eventually creates is still removed once it finishes.
func TestWorker_CollectTimeoutReturnsAndCleansUp(t *testing.T) {
	counts := map[string]int{}
	w := NewWorker(make(chan manager.CaptureRequest), bucketClient(t), nil, &captureguard.Guard{}, "node-1", countingLogger(counts))
	w.captureTimeout = 10 * time.Millisecond

	var capturedDir string
	finished := make(chan struct{})
	w.collect = func(ctx context.Context, _ []v1alpha1.LogCategory, _ Trigger) (string, int, error) {
		<-ctx.Done() // block until handle's timeout fires
		dir, err := os.MkdirTemp("", "autocapture-timeout-test-*")
		require.NoError(t, err)
		capturedDir = dir
		close(finished)
		return dir, 0, nil
	}

	done := make(chan struct{})
	go func() {
		w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handle did not return after the capture timeout")
	}
	assert.Equal(t, 1, counts[msgTimedOut], "a stuck collect must log the timeout message")

	// The late result arrives after handle already returned; the goroutine
	// must remove the directory itself since nothing else will.
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("stubbed collect never finished")
	}
	assert.Eventually(t, func() bool {
		_, err := os.Stat(capturedDir)
		return os.IsNotExist(err)
	}, 2*time.Second, 10*time.Millisecond, "the late result's temp dir must be cleaned up")
}

// TestWorker_CollectRaceWithTimeoutStillCleansUp exercises the boundary case
// where collect finishes at essentially the same instant the deadline fires,
// so the result may already be sitting in done when the timeout branch is
// chosen. handle must still clean it up rather than leaking it.
func TestWorker_CollectRaceWithTimeoutStillCleansUp(t *testing.T) {
	for i := 0; i < 50; i++ {
		counts := map[string]int{}
		w := NewWorker(make(chan manager.CaptureRequest), bucketClient(t), nil, &captureguard.Guard{}, "node-1", countingLogger(counts))
		w.captureTimeout = time.Millisecond

		dirCh := make(chan string, 1)
		w.collect = func(context.Context, []v1alpha1.LogCategory, Trigger) (string, int, error) {
			dir, err := os.MkdirTemp("", "autocapture-race-test-*")
			require.NoError(t, err)
			dirCh <- dir
			return dir, 0, nil
		}

		done := make(chan struct{})
		go func() {
			w.handle(context.Background(), manager.CaptureRequest{Condition: "NetworkingReady", Reason: "Down"})
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("handle did not return")
		}

		// Wait for the stub's send before reading the dir it created: it may
		// race the deadline, so the directory can still be in flight when
		// handle returns.
		var capturedDir string
		select {
		case capturedDir = <-dirCh:
		case <-time.After(5 * time.Second):
			t.Fatal("stubbed collect never finished")
		}
		assert.Eventually(t, func() bool {
			_, err := os.Stat(capturedDir)
			return os.IsNotExist(err)
		}, 2*time.Second, 10*time.Millisecond, "a result racing the deadline must still be cleaned up")
	}
}

func TestDirSize(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("12345"), 0644)) // 5 bytes
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b"), []byte("678"), 0644))   // 3 bytes
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.Mkdir(sub, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "c"), []byte("90"), 0644)) // 2 bytes

	files, size, err := dirSize(dir)
	require.NoError(t, err)
	assert.Equal(t, 3, files)
	assert.Equal(t, int64(10), size)
}
