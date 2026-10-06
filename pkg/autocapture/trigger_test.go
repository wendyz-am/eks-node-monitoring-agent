package autocapture

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/eks-node-monitoring-agent/pkg/conditions"
)

func TestTriggerRender(t *testing.T) {
	at := time.Date(2026, 10, 1, 23, 21, 5, 0, time.UTC)

	t.Run("full trigger", func(t *testing.T) {
		tr := Trigger{
			Condition:  conditions.KernelReady,
			Reason:     "KernelPanic",
			Message:    "a panic occurred",
			At:         at,
			Node:       "i-0091abcd",
			NMAVersion: "v1.2.3",
		}
		want := "condition: KernelReady\n" +
			"reason:    KernelPanic\n" +
			"message:   a panic occurred\n" +
			"time:      2026-10-01T23:21:05Z\n" +
			"node:      i-0091abcd\n" +
			"nma:       v1.2.3\n"
		if got := tr.Render(); got != want {
			t.Errorf("Render() mismatch:\n got %q\nwant %q", got, want)
		}
	})

	t.Run("zero time prints unknown", func(t *testing.T) {
		tr := Trigger{
			Condition:  conditions.KernelReady,
			Reason:     "KernelPanic",
			Message:    "a panic occurred",
			Node:       "i-0091abcd",
			NMAVersion: "v1.2.3",
		}
		want := "condition: KernelReady\n" +
			"reason:    KernelPanic\n" +
			"message:   a panic occurred\n" +
			"time:      unknown\n" +
			"node:      i-0091abcd\n" +
			"nma:       v1.2.3\n"
		if got := tr.Render(); got != want {
			t.Errorf("Render() mismatch:\n got %q\nwant %q", got, want)
		}
	})

	t.Run("multiline message rendered on one line", func(t *testing.T) {
		tr := Trigger{
			Condition:  conditions.NetworkingReady,
			Reason:     "IPAMDNotRunning",
			Message:    "line one\nline two\r\nline three",
			At:         at,
			Node:       "n1",
			NMAVersion: "v0",
		}
		want := "condition: NetworkingReady\n" +
			"reason:    IPAMDNotRunning\n" +
			"message:   line one line two  line three\n" +
			"time:      2026-10-01T23:21:05Z\n" +
			"node:      n1\n" +
			"nma:       v0\n"
		if got := tr.Render(); got != want {
			t.Errorf("Render() mismatch:\n got %q\nwant %q", got, want)
		}
	})
}

func TestWriteTrigger(t *testing.T) {
	dir := t.TempDir()
	tr := Trigger{
		Condition:  conditions.KernelReady,
		Reason:     "KernelPanic",
		Message:    "a panic occurred",
		At:         time.Date(2026, 10, 1, 23, 21, 5, 0, time.UTC),
		Node:       "i-0091abcd",
		NMAVersion: "v1.2.3",
	}

	if err := writeTrigger(dir, tr); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, triggerFileName)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected trigger file at %q: %v", path, err)
	}
	if string(got) != tr.Render() {
		t.Errorf("file content mismatch:\n got %q\nwant %q", got, tr.Render())
	}
}
