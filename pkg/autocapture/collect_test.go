package autocapture

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"github.com/aws/eks-node-monitoring-agent/api/v1alpha1"
	"github.com/aws/eks-node-monitoring-agent/pkg/conditions"
)

// CollectToDir has no unit test here: it reads the real host filesystem
// through collect.NewAccessor, which a unit test cannot stand in for. Its
// behavior is exercised through the worker tests, which fake w.collect.

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
			assert.Equal(t, v1alpha1.LogCategory(v1alpha1.LogCategoryBase), got[0], "Base must always be first")
			assert.Equal(t, tc.want, got)
		})
	}
}
