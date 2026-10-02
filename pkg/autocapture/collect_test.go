package autocapture

import (
	"testing"

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
