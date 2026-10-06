package autocapture

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// triggerFileName is the archive-root file describing what triggered a capture.
const triggerFileName = "capture-trigger.txt"

// Trigger describes the condition flip that caused a capture. It is written to
// the archive root so anyone reading a bundle can see why it was collected.
type Trigger struct {
	Condition  corev1.NodeConditionType
	Reason     string
	Message    string
	At         time.Time
	Node       string
	NMAVersion string
}

// Render returns the six-line trigger file body, one field per line. The flip
// time is rendered in UTC RFC3339, or "unknown" when unset. Newlines and
// carriage returns in the message are flattened to spaces so every field stays
// on a single line.
func (t Trigger) Render() string {
	at := "unknown"
	if !t.At.IsZero() {
		at = t.At.UTC().Format(time.RFC3339)
	}

	message := strings.NewReplacer("\n", " ", "\r", " ").Replace(t.Message)

	var b strings.Builder
	b.WriteString("condition: " + string(t.Condition) + "\n")
	b.WriteString("reason:    " + t.Reason + "\n")
	b.WriteString("message:   " + message + "\n")
	b.WriteString("time:      " + at + "\n")
	b.WriteString("node:      " + t.Node + "\n")
	b.WriteString("nma:       " + t.NMAVersion + "\n")
	return b.String()
}

// writeTrigger writes the rendered trigger into dir as capture-trigger.txt.
func writeTrigger(dir string, t Trigger) error {
	return os.WriteFile(filepath.Join(dir, triggerFileName), []byte(t.Render()), 0644)
}
