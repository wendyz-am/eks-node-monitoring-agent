package autocapture

import (
	"context"
	"os"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/eks-node-monitoring-agent/api/v1alpha1"
	"github.com/aws/eks-node-monitoring-agent/internal/version"
	"github.com/aws/eks-node-monitoring-agent/pkg/config"
	"github.com/aws/eks-node-monitoring-agent/pkg/manager"
	"github.com/aws/eks-node-monitoring-agent/pkg/util/captureguard"
)

// defaultCooldown keeps a flapping condition from uploading a bundle per flip.
const defaultCooldown = 30 * time.Minute

// defaultCaptureTimeout only bounds a stuck or very large capture, not a
// target.
const defaultCaptureTimeout = 10 * time.Minute

// Worker collects logs for a capture request from the node exporter.
// Uploading the collected bundle arrives in a later change.
type Worker struct {
	requests <-chan manager.CaptureRequest
	client   client.Reader
	nodeName string
	log      logr.Logger

	// guard is shared with the NodeDiagnostic controller, so the two never
	// collect at once and load up an already-unhealthy node's disk.
	guard *captureguard.Guard

	// Guard once-only informational logs so repeated requests don't spam.
	loggedOff     bool
	loggedIgnored bool

	cooldown time.Duration
	now      func() time.Time

	// A field, not a const, so tests can use a short timeout instead of
	// waiting out the real one.
	captureTimeout time.Duration
	// Kept in memory, not persisted: after a restart the worst case is one
	// extra capture per still-failing condition, not worth persisting for.
	// No lock needed: only handle touches the map, on Start's one goroutine.
	lastCapture map[corev1.NodeConditionType]time.Time

	// A field so tests can swap in a fake and never read the real host.
	collect func(ctx context.Context, categories []v1alpha1.LogCategory, trigger Trigger) (dir string, failed int, err error)
}

func NewWorker(requests <-chan manager.CaptureRequest, reader client.Reader, runtimeContext *config.RuntimeContext, guard *captureguard.Guard, nodeName string, log logr.Logger) *Worker {
	return &Worker{
		requests:       requests,
		client:         reader,
		nodeName:       nodeName,
		log:            log,
		guard:          guard,
		cooldown:       defaultCooldown,
		now:            time.Now,
		captureTimeout: defaultCaptureTimeout,
		lastCapture:    make(map[corev1.NodeConditionType]time.Time),
		collect: func(ctx context.Context, categories []v1alpha1.LogCategory, trigger Trigger) (string, int, error) {
			return CollectToDir(ctx, runtimeContext, categories, trigger)
		},
	}
}

// Start runs until the context is canceled. It always returns nil: as a
// manager Runnable, a returned error would stop the whole agent, and one
// failed capture must never take monitoring down.
func (w *Worker) Start(ctx context.Context) error {
	w.log.Info("starting auto log capture worker")
	for {
		select {
		case <-ctx.Done():
			return nil
		case req := <-w.requests:
			w.handle(ctx, req)
		}
	}
}

// handle processes one request. Errors are logged and the request dropped:
// there's nothing to retry against, since the next failure sends a fresh one.
func (w *Worker) handle(ctx context.Context, req manager.CaptureRequest) {
	bucket, name, ignored, err := findBucket(ctx, w.client)
	if err != nil {
		w.log.Error(err, "failed to look up the automatic log capture bucket",
			"condition", req.Condition, "reason", req.Reason)
		return
	}
	if bucket == "" {
		if !w.loggedOff {
			w.log.Info("auto log capture is off: no NodeDiagnostic sets spec.autoLogCapture.bucket")
			w.loggedOff = true
		}
		return
	}

	// Checked only after a bucket is found, so an off worker never burns the
	// cooldown window on a request it couldn't have acted on anyway.
	now := w.now()
	if last, ok := w.lastCapture[req.Condition]; ok {
		if remaining := w.cooldown - now.Sub(last); remaining > 0 {
			w.log.V(1).Info("capture skipped: cooldown active",
				"condition", req.Condition, "remaining", remaining)
			return
		}
	}
	w.lastCapture[req.Condition] = now

	w.log.Info("capture requested",
		"condition", req.Condition, "reason", req.Reason,
		"bucket", bucket, "nodeDiagnostic", name)

	// Surface the ambiguity once so an operator knows other buckets are inert.
	if ignored > 0 && !w.loggedIgnored {
		w.log.Info("multiple NodeDiagnostics set spec.autoLogCapture.bucket; using the newest and ignoring the rest",
			"using", name, "ignored", ignored)
		w.loggedIgnored = true
	}

	// The NodeDiagnostic controller collects using this same guard, so the
	// two never run at once and their disk/I/O load never stacks on a node
	// that may already be unhealthy.
	if !w.guard.TryAcquire() {
		w.log.Info("capture skipped: another log collection is running",
			"condition", req.Condition, "reason", req.Reason)
		delete(w.lastCapture, req.Condition)
		return
	}

	ctxCap, cancel := context.WithTimeout(ctx, w.captureTimeout)
	defer cancel()
	started := w.now()

	trigger := Trigger{
		Condition:  req.Condition,
		Reason:     req.Reason,
		Message:    req.Message,
		At:         req.At,
		Node:       w.nodeName,
		NMAVersion: version.String(),
	}

	type collectResult struct {
		dir    string
		failed int
		err    error
	}
	done := make(chan collectResult, 1)
	go func() {
		dir, failed, err := w.collect(ctxCap, CategoriesFor(req.Condition), trigger)
		// Released before publishing, so a caller which receives the result
		// always observes the collection as free again. TryAcquire stays
		// where it is above: nothing between it and this goroutine starting
		// can return, so the guard can never leak.
		w.guard.Release()
		done <- collectResult{dir: dir, failed: failed, err: err}
	}()

	var dir string
	var failed int
	select {
	case <-ctxCap.Done():
		if ctx.Err() != nil {
			w.log.Info("capture stopped",
				"condition", req.Condition, "reason", req.Reason)
		} else {
			w.log.Info("capture timed out, collector may be blocked on host I/O",
				"condition", req.Condition, "reason", req.Reason)
		}
		// done may already hold a result, or still be waiting on collect:
		// either way, handle (not the goroutine) owns draining it, so a
		// result racing the deadline is never left unreachable.
		go func() {
			if res := <-done; res.err == nil {
				os.RemoveAll(res.dir)
			}
		}()
		return
	case res := <-done:
		if res.err != nil {
			w.log.Error(res.err, "failed to collect logs for capture",
				"condition", req.Condition, "reason", req.Reason)
			return
		}
		dir, failed = res.dir, res.failed
	}
	// Delete the bundle here: nothing uploads it yet, so leaving it would fill
	// the node's disk one capture at a time.
	defer os.RemoveAll(dir)

	files, size, err := dirSize(dir)
	if err != nil {
		w.log.Error(err, "failed to measure collected logs", "condition", req.Condition)
	}
	w.log.Info("capture collected",
		"condition", req.Condition, "reason", req.Reason, "nodeDiagnostic", name,
		"files", files, "bytes", size, "failedCollectors", failed,
		"seconds", w.now().Sub(started).Seconds())
}

// findBucket returns the bucket for automatic log capture and the name of the
// NodeDiagnostic it came from, or ("", "", 0, nil) when none sets a bucket.
//
// When several set a bucket, the newest wins (ties broken by name) so the
// choice is deterministic rather than list-order dependent. ignored counts the
// other candidates, so a caller can warn that some objects were passed over.
func findBucket(ctx context.Context, c client.Reader) (bucket, name string, ignored int, err error) {
	var list v1alpha1.NodeDiagnosticList
	if err := c.List(ctx, &list); err != nil {
		return "", "", 0, err
	}

	var chosen *v1alpha1.NodeDiagnostic
	matches := 0
	for i := range list.Items {
		nd := &list.Items[i]
		if nd.Spec.AutoLogCapture == nil || nd.Spec.AutoLogCapture.Bucket == "" {
			continue
		}
		matches++
		if chosen == nil || isNewer(nd, chosen) {
			chosen = nd
		}
	}

	if chosen == nil {
		return "", "", 0, nil
	}
	return chosen.Spec.AutoLogCapture.Bucket, chosen.Name, matches - 1, nil
}

func isNewer(a, b *v1alpha1.NodeDiagnostic) bool {
	at := a.CreationTimestamp.Time
	bt := b.CreationTimestamp.Time
	if at.Equal(bt) {
		return a.Name > b.Name // tie-break with name for a stable choice
	}
	return at.After(bt)
}
