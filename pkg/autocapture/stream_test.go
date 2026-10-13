package autocapture

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// readTarGz reads a gzipped tar stream into a name->contents map.
func readTarGz(t *testing.T, r io.Reader) map[string]string {
	t.Helper()
	gr, err := gzip.NewReader(r)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	out := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, tr); err != nil {
			t.Fatalf("tar copy: %v", err)
		}
		out[hdr.Name] = buf.String()
	}
	return out
}

func TestStreamTarGzipRoundTrip(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"a.txt":     "alpha",
		"b.txt":     "bravo",
		"sub/c.txt": "charlie",
	}
	for name, content := range files {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	rc := StreamTarGzip(dir, 0)
	defer rc.Close()

	got := readTarGz(t, rc)
	if len(got) != len(files) {
		t.Fatalf("expected %d entries, got %d: %v", len(files), len(got), got)
	}
	for name, content := range files {
		// header names use the OS separator trimmed of the dir prefix
		want := filepath.ToSlash(name)
		found := ""
		for g, c := range got {
			if filepath.ToSlash(g) == want {
				found = c
			}
		}
		if found != content {
			t.Errorf("file %q: got %q, want %q", name, found, content)
		}
	}
}

func TestStreamTarGzipCap(t *testing.T) {
	dir := t.TempDir()
	// Write enough incompressible-ish data that the compressed stream exceeds a
	// tiny cap. Random-looking bytes keep gzip from shrinking it to nothing.
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = byte(i*7 + 3)
	}
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), big, 0644); err != nil {
		t.Fatal(err)
	}

	rc := StreamTarGzip(dir, 1024) // 1 KiB cap, far below the compressed size
	defer rc.Close()

	_, err := io.ReadAll(rc)
	if err == nil {
		t.Fatal("expected an error reading a capped stream, got nil")
	}
	if !errors.Is(err, ErrBundleTooLarge) {
		t.Errorf("expected ErrBundleTooLarge, got %v", err)
	}
	if errors.Is(err, io.EOF) {
		t.Error("cap error must not be io.EOF")
	}
}

func TestStreamTarGzipMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")

	rc := StreamTarGzip(missing, 0)
	defer rc.Close()

	_, err := io.ReadAll(rc)
	if err == nil {
		t.Fatal("expected an error for a missing directory, got nil")
	}
	if errors.Is(err, io.EOF) {
		t.Error("missing-dir error must not be io.EOF")
	}
}
