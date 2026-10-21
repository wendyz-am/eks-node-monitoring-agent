package autocapture

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// triggerFileName is written at the archive root of every auto-captured
// bundle, since the bundle's contents and name otherwise say nothing about
// why it exists. The only other place the reason survives is the Node
// condition (gone once the node is deleted) or the API server audit log
// (off by default, billed to the customer).
const triggerFileName = "capture-trigger.txt"

// Trigger records why a bundle was captured: the condition that went False,
// its reason and message, when, on which node, and the NMA version that
// produced it. A struct rather than loose arguments because the upload PR
// reuses the same fields for object metadata/key.
type Trigger struct {
	Condition  corev1.NodeConditionType
	Reason     string
	Message    string
	At         time.Time
	Node       string
	NMAVersion string
}

// Render formats the trigger as plain text so it can be read with cat/grep
// and parsed by splitting each line on the first ": ". One field per line,
// always in the same order, so a support script can rely on the shape.
func (t Trigger) Render() string {
	at := "unknown"
	if !t.At.IsZero() {
		at = t.At.UTC().Format(time.RFC3339)
	}
	// Newlines/carriage returns in the message would otherwise masquerade as
	// extra fields, breaking the one-field-per-line contract.
	message := strings.NewReplacer("\n", " ", "\r", " ").Replace(t.Message)

	var b strings.Builder
	b.WriteString("condition: " + string(t.Condition) + "\n")
	b.WriteString("reason: " + t.Reason + "\n")
	b.WriteString("message: " + message + "\n")
	b.WriteString("time: " + at + "\n")
	b.WriteString("node: " + t.Node + "\n")
	b.WriteString("nma: " + t.NMAVersion + "\n")
	return b.String()
}

// writeTrigger writes the trigger file into dir. Called from CollectToDir,
// which treats a write failure as fatal to the capture: a bundle without its
// reason is the exact failure mode this file exists to prevent.
func writeTrigger(dir string, t Trigger) error {
	return os.WriteFile(filepath.Join(dir, triggerFileName), []byte(t.Render()), 0644)
}
