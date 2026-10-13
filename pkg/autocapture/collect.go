// Package autocapture implements the automatic log capture POC: when a managed
// node condition flips into a failing state, a background worker collects the
// relevant log categories into a temp directory, then streams a gzipped tar of
// that directory to S3 so memory stays flat regardless of bundle size.
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

// CategoriesFor returns the log categories to collect for a failed condition.
// Base is always collected first, plus exactly one category specific to the
// condition. Unrecognized conditions collect only Base.
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

// CollectToDir gathers the requested log categories from the host into a fresh
// temp directory and returns that directory, the number of individual
// collectors that failed, and an error. It mirrors the controller's
// collectLogs recipe: partial collection failures are recorded in
// log-capture-errors.log inside the directory rather than failing the whole
// capture.
//
// The caller owns the returned directory and must remove it. On an error
// return, CollectToDir removes the directory itself and returns "".
func CollectToDir(ctx context.Context, runtimeContext *config.RuntimeContext, categories []v1alpha1.LogCategory, trigger Trigger) (string, int, error) {
	logger := log.FromContext(ctx)

	logDir, err := os.MkdirTemp("", "eks-log-collector-*")
	if err != nil {
		return "", 0, fmt.Errorf("failed creating temp directory due to: %s", err)
	}

	// The trigger file explains why the bundle was captured. It is best-effort:
	// a bundle without it is still useful, so never fail the capture over it.
	if err := writeTrigger(logDir, trigger); err != nil {
		logger.Error(err, "failed to write capture trigger file")
	}

	cfg := collect.Config{
		Root:        config.HostRoot(),
		Destination: logDir,
		Tags: append(
			runtimeContext.Tags(),
			runtimeContext.OSDistro(),
			runtimeContext.AcceleratedHardware(),
		),
	}

	acc, err := collect.NewAccessor(ctx, cfg)
	if err != nil {
		os.RemoveAll(logDir)
		return "", 0, fmt.Errorf("failed to create accessor: %s", err)
	}

	// if there are any errors in the collection process they will be written to
	// this errors logfile so that users can investigate partial successes.
	const logCaptureReportLog = "log-capture-errors.log"

	subTasksFailed := 0
	var errBuf bytes.Buffer
	for _, collector := range logcollection.GetCollectors(categories...) {
		collectorName := reflect.TypeOf(collector).Elem().Name()
		if err := collector.Collect(acc); err != nil {
			// if there are errors during one step of the collection don't fail,
			// but indicate the failures to show the logs may be incomplete
			logger.Error(err, "failed collection task", "collector", collectorName)
			fmt.Fprintf(&errBuf, "--- Errors in collector %q ---\n%s\n", collectorName, err)
			subTasksFailed += 1
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
