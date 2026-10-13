package autocapture

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ErrBundleTooLarge is returned through the stream when the compressed bundle
// exceeds the configured size cap. It aborts the upload mid-stream rather than
// letting an unbounded bundle run the node (and S3 costs) away.
var ErrBundleTooLarge = errors.New("capture bundle exceeds size cap")

// countingWriter forwards writes to w while tracking the running total. Once
// more than max bytes have passed through, every subsequent Write fails with
// ErrBundleTooLarge. A max of zero or less disables the cap.
type countingWriter struct {
	w     io.Writer
	max   int64
	count int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.count += int64(n)
	if c.max > 0 && c.count > c.max {
		return n, ErrBundleTooLarge
	}
	return n, err
}

// StreamTarGzip returns the read end of a pipe that yields a gzipped tar of
// dir, built on the fly by a background goroutine so nothing is buffered whole
// in memory. maxBytes caps the compressed size (<= 0 means no cap); exceeding
// it fails the stream with ErrBundleTooLarge and leaves no complete object.
//
// The caller must Close the returned reader. Closing it early unblocks and
// stops the goroutine, so an uploader that gives up partway never leaks it.
func StreamTarGzip(dir string, maxBytes int64) io.ReadCloser {
	pr, pw := io.Pipe()

	go func() {
		// Compressed bytes are counted between gzip and the pipe, so the cap
		// bounds what actually gets uploaded. Order: tar -> gzip -> count -> pipe.
		cw := &countingWriter{w: pw, max: maxBytes}
		gw := gzip.NewWriter(cw)
		tw := tar.NewWriter(gw)

		err := filepath.Walk(dir, func(path string, info fs.FileInfo, walkErr error) error {
			// Check the walk error first: unlike TarGzipDir, which calls
			// info.IsDir() before testing walkErr, a failed walk hands us a nil
			// info and that would panic.
			if walkErr != nil {
				return walkErr
			}
			if info.IsDir() {
				return nil
			}
			header, err := tar.FileInfoHeader(info, path)
			if err != nil {
				return err
			}
			// remove the collection directory prefix
			header.Name = strings.TrimPrefix(path, dir+string(filepath.Separator))
			if err := tw.WriteHeader(header); err != nil {
				return err
			}
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, file); err != nil {
				file.Close()
				return err
			}
			return file.Close()
		})
		if err != nil {
			// Fail the read side with the real cause and never fall through to a
			// clean Close, which would make the reader see a valid short archive.
			pw.CloseWithError(err)
			return
		}

		if err := tw.Close(); err != nil {
			pw.CloseWithError(err)
			return
		}
		if err := gw.Close(); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()

	return &pipeReadCloser{pr: pr}
}

// pipeReadCloser adapts the pipe's read end so Close unblocks the writer
// goroutine (via CloseWithError) instead of only closing the reader.
type pipeReadCloser struct {
	pr *io.PipeReader
}

func (p *pipeReadCloser) Read(b []byte) (int, error) {
	return p.pr.Read(b)
}

func (p *pipeReadCloser) Close() error {
	// Propagate a closed-pipe error to the writer so a goroutine blocked on
	// pw.Write returns and exits rather than leaking.
	return p.pr.CloseWithError(io.ErrClosedPipe)
}
