// Package autocapture implements the automatic log capture POC: when a managed
// node condition flips into a failing state, a background worker collects the
// relevant log categories, archives them, and (for now) writes the archive to
// the node's disk. S3 upload and Events are added in later steps.
package autocapture

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/aws/eks-node-monitoring-agent/api/v1alpha1"
	"github.com/aws/eks-node-monitoring-agent/pkg/conditions"
	"github.com/aws/eks-node-monitoring-agent/pkg/config"
	"github.com/aws/eks-node-monitoring-agent/pkg/log_collector/collect"
	"github.com/aws/eks-node-monitoring-agent/pkg/logcollection"
	fileutil "github.com/aws/eks-node-monitoring-agent/pkg/util/file"
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

// Collect gathers the requested log categories from the host and returns an
// in-memory tar.gz archive, the number of individual collectors that failed,
// and an error. It mirrors the controller's collectLogs recipe: partial
// collection failures are recorded in log-capture-errors.log inside the
// archive rather than failing the whole capture.
func Collect(ctx context.Context, runtimeContext *config.RuntimeContext, categories []v1alpha1.LogCategory) (io.Reader, int, error) {
	logger := log.FromContext(ctx)

	logDir, err := os.MkdirTemp("", "eks-log-collector-*")
	if err != nil {
		return nil, 0, fmt.Errorf("failed creating temp directory due to: %s", err)
	}
	defer os.RemoveAll(logDir)

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
		return nil, 0, fmt.Errorf("failed to create accessor: %s", err)
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
			return nil, 0, fmt.Errorf("failed to write capture summary: %s", err)
		}
	}

	archiveReader, err := fileutil.TarGzipDir(logDir)
	if err != nil {
		return nil, 0, fmt.Errorf("failed archiving logs due to: %s", err)
	}

	return archiveReader, subTasksFailed, nil
}

// hostDir returns the directory on the node where auto-capture archives are
// written. It is a var so tests can redirect writes to a temp directory.
var hostDir = func() string {
	return filepath.Join(config.HostRoot(), "tmp", "nma-auto-capture")
}

// SaveToHost writes the archive to the node's disk under hostDir() using a
// timestamped, condition-tagged filename, and returns the full path.
//
// POC only: this will be replaced by the S3 upload in Step 3.
func SaveToHost(archive io.Reader, condition corev1.NodeConditionType, now time.Time) (string, error) {
	return saveToDir(hostDir(), archive, condition, now)
}

// saveToDir performs the write into an explicit directory. SaveToHost is a thin
// wrapper that supplies the host directory; tests exercise this directly.
func saveToDir(dir string, archive io.Reader, condition corev1.NodeConditionType, now time.Time) (string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("failed creating capture directory: %s", err)
	}

	name := now.UTC().Format("20060102T150405Z") + "-" + string(condition) + ".tar.gz"
	path := filepath.Join(dir, name)

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("failed creating capture file: %s", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, archive); err != nil {
		return "", fmt.Errorf("failed writing capture file: %s", err)
	}
	return path, nil
}
