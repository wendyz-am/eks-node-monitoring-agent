package autocapture

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrigger_Render(t *testing.T) {
	at := time.Date(2024, 3, 15, 10, 30, 0, 0, time.FixedZone("EST", -5*60*60))

	tests := []struct {
		name string
		t    Trigger
		want string
	}{
		{
			name: "all fields set",
			t: Trigger{
				Condition:  "NetworkingReady",
				Reason:     "InterfaceDown",
				Message:    "eth0 is down",
				At:         at,
				Node:       "node-1",
				NMAVersion: "v1.2.3 (abc123)",
			},
			want: "condition: NetworkingReady\n" +
				"reason: InterfaceDown\n" +
				"message: eth0 is down\n" +
				"time: 2024-03-15T15:30:00Z\n" +
				"node: node-1\n" +
				"nma: v1.2.3 (abc123)\n",
		},
		{
			name: "zero At renders unknown",
			t: Trigger{
				Condition:  "KernelReady",
				Reason:     "Oops",
				Message:    "kernel oops detected",
				Node:       "node-2",
				NMAVersion: "v1.2.3 (abc123)",
			},
			want: "condition: KernelReady\n" +
				"reason: Oops\n" +
				"message: kernel oops detected\n" +
				"time: unknown\n" +
				"node: node-2\n" +
				"nma: v1.2.3 (abc123)\n",
		},
		{
			name: "message with newlines and carriage returns is flattened",
			t: Trigger{
				Condition:  "StorageReady",
				Reason:     "DiskPressure",
				Message:    "line one\nline two\r\nline three",
				At:         at,
				Node:       "node-3",
				NMAVersion: "v1.2.3 (abc123)",
			},
			want: "condition: StorageReady\n" +
				"reason: DiskPressure\n" +
				"message: line one line two  line three\n" +
				"time: 2024-03-15T15:30:00Z\n" +
				"node: node-3\n" +
				"nma: v1.2.3 (abc123)\n",
		},
		{
			name: "empty reason and message still produce the line",
			t: Trigger{
				Condition:  "ContainerRuntimeReady",
				Reason:     "",
				Message:    "",
				At:         at,
				Node:       "node-4",
				NMAVersion: "v1.2.3 (abc123)",
			},
			want: "condition: ContainerRuntimeReady\n" +
				"reason: \n" +
				"message: \n" +
				"time: 2024-03-15T15:30:00Z\n" +
				"node: node-4\n" +
				"nma: v1.2.3 (abc123)\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.t.Render()
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestWriteTrigger(t *testing.T) {
	dir := t.TempDir()
	trig := Trigger{
		Condition:  "NetworkingReady",
		Reason:     "InterfaceDown",
		Message:    "eth0 is down",
		At:         time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC),
		Node:       "node-1",
		NMAVersion: "v1.2.3 (abc123)",
	}

	require.NoError(t, writeTrigger(dir, trig))

	got, err := os.ReadFile(filepath.Join(dir, triggerFileName))
	require.NoError(t, err)
	assert.Equal(t, trig.Render(), string(got))
}

func TestWriteTrigger_UnwritableDirReturnsError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits do not apply the same way on windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory write permission bits")
	}

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0555)) // read+execute only, no write

	// Confirm this environment actually enforces the permission before
	// relying on it below, rather than assuming os.WriteFile's behavior.
	err := writeTrigger(dir, Trigger{})
	require.Error(t, err, "writing into a read-only directory must fail in this environment")
}
