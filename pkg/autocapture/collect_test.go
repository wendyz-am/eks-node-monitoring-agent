package autocapture

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/aws/eks-node-monitoring-agent/api/v1alpha1"
	"github.com/aws/eks-node-monitoring-agent/pkg/conditions"
)

func TestCategoriesFor(t *testing.T) {
	tests := []struct {
		name      string
		condition corev1.NodeConditionType
		want      []v1alpha1.LogCategory
	}{
		{
			name:      "accelerated hardware -> Device",
			condition: conditions.AcceleratedHardwareReady,
			want:      []v1alpha1.LogCategory{v1alpha1.LogCategoryBase, v1alpha1.LogCategoryDevice},
		},
		{
			name:      "networking -> Networking",
			condition: conditions.NetworkingReady,
			want:      []v1alpha1.LogCategory{v1alpha1.LogCategoryBase, v1alpha1.LogCategoryNetworking},
		},
		{
			name:      "kernel -> System",
			condition: conditions.KernelReady,
			want:      []v1alpha1.LogCategory{v1alpha1.LogCategoryBase, v1alpha1.LogCategorySystem},
		},
		{
			name:      "container runtime -> Runtime",
			condition: conditions.ContainerRuntimeReady,
			want:      []v1alpha1.LogCategory{v1alpha1.LogCategoryBase, v1alpha1.LogCategoryRuntime},
		},
		{
			name:      "unknown -> Base only",
			condition: corev1.NodeConditionType("SomethingUnknown"),
			want:      []v1alpha1.LogCategory{v1alpha1.LogCategoryBase},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CategoriesFor(tc.condition)
			if len(got) == 0 || got[0] != v1alpha1.LogCategoryBase {
				t.Fatalf("Base must always be first, got %v", got)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("expected %v, got %v", tc.want, got)
				}
			}
		})
	}
}

func TestSaveToHost(t *testing.T) {
	dir := t.TempDir()

	// Redirect SaveToHost's target directory to the temp dir for this test.
	original := hostDir
	hostDir = func() string { return dir }
	t.Cleanup(func() { hostDir = original })

	want := []byte("fake-archive-bytes")
	now := time.Date(2026, 10, 1, 12, 34, 56, 0, time.UTC)

	path, err := SaveToHost(bytes.NewReader(want), conditions.KernelReady, now)
	if err != nil {
		t.Fatal(err)
	}

	if gotDir := filepath.Dir(path); gotDir != dir {
		t.Fatalf("expected file written under %q, got %q", dir, gotDir)
	}
	base := filepath.Base(path)
	if !strings.HasSuffix(base, "-KernelReady.tar.gz") {
		t.Errorf("expected filename to end in -KernelReady.tar.gz, got %q", base)
	}
	if !strings.HasPrefix(base, "20261001T123456Z-") {
		t.Errorf("expected UTC timestamp prefix, got %q", base)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("file contents mismatch: got %q, want %q", got, want)
	}
}
