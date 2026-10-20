package autocapture

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/aws/eks-node-monitoring-agent/api/v1alpha1"
	"github.com/aws/eks-node-monitoring-agent/pkg/conditions"
	"github.com/aws/eks-node-monitoring-agent/pkg/config"
	"github.com/aws/eks-node-monitoring-agent/pkg/log_collector/collect"
	"github.com/aws/eks-node-monitoring-agent/pkg/logcollection"
)

// CategoriesFor returns the categories for a failed condition: Base plus the
// one relevant to the condition, to avoid collecting unrelated subsystems.
func CategoriesFor(condition corev1.NodeConditionType) []v1alpha1.LogCategory {
	categories := []v1alpha1.LogCategory{v1alpha1.LogCategoryBase}
	switch condition {
	case conditions.AcceleratedHardwareReady:
		categories = append(categories, v1alpha1.LogCategoryDevice)
	case conditions.NetworkingReady:
		categories = append(categories, v1alpha1.LogCategoryNetworking)
	case conditions.KernelReady:
		categories = append(categories, v1alpha1.LogCategorySystem)
	case conditions.ContainerRuntimeReady:
		categories = append(categories, v1alpha1.LogCategoryRuntime)
	}
	return categories
}

// CollectToDir gathers the requested log categories into a fresh temp
// directory and returns it, the number of collectors that failed, and an
// error. The caller owns the directory and must remove it.
func CollectToDir(ctx context.Context, runtimeContext *config.RuntimeContext, categories []v1alpha1.LogCategory) (dir string, failed int, err error) {
	logger := log.FromContext(ctx)

	logDir, err := os.MkdirTemp("", "eks-log-collector-*")
	if err != nil {
		return "", 0, fmt.Errorf("failed creating temp directory due to: %s", err)
	}

	// Fresh slice: appending onto runtimeContext.Tags() could grow into the
	// shared backing array and corrupt the runtime context for other readers.
	tags := append([]string{}, runtimeContext.Tags()...)
	tags = append(tags, runtimeContext.OSDistro(), runtimeContext.AcceleratedHardware())
	cfg := collect.Config{
		Root:        config.HostRoot(),
		Destination: logDir,
		Tags:        tags,
	}

	acc, err := collect.NewAccessor(ctx, cfg)
	if err != nil {
		// Remove the dir on a hard error: no one else knows it exists, so it
		// would leak otherwise.
		os.RemoveAll(logDir)
		return "", 0, fmt.Errorf("failed to create accessor: %s", err)
	}

	const logCaptureReportLog = "log-capture-errors.log"
	subTasksFailed := 0
	var errBuf bytes.Buffer
	for _, collector := range logcollection.GetCollectors(categories...) {
		// The caller gave up (e.g. the capture timed out): stop collecting
		// and remove the bundle instead of finishing a run no one will use.
		if ctx.Err() != nil {
			os.RemoveAll(logDir)
			return "", 0, ctx.Err()
		}
		collectorName := reflect.TypeOf(collector).Elem().Name()
		// One failed collector must not sink the capture: a partial bundle
		// still helps, so record the failure and keep going.
		if err := collector.Collect(acc); err != nil {
			logger.Error(err, "failed collection task", "collector", collectorName)
			fmt.Fprintf(&errBuf, "--- Errors in collector %q ---\n%s\n", collectorName, err)
			subTasksFailed++
		}
	}
	if errBuf.Len() > 0 {
		if err := os.WriteFile(filepath.Join(logDir, logCaptureReportLog), errBuf.Bytes(), 0644); err != nil {
			os.RemoveAll(logDir)
			return "", 0, fmt.Errorf("failed to write capture summary: %s", err)
		}
	}
	return logDir, subTasksFailed, nil
}

// dirSize sums the regular file sizes under dir, for the worker's log line.
func dirSize(dir string) (files int, size int64, err error) {
	err = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			files++
			size += info.Size()
		}
		return nil
	})
	return files, size, err
}
