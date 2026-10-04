package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ResultStore is the shared output resource. Instead of a lock, one collector
// goroutine owns it: workers send results on a channel, and only the
// collector touches the slice and the completion log. Go's guideline is to
// share memory by communicating rather than communicate by sharing memory
type ResultStore struct {
	resultsPath string
	logFile     *os.File
	log         *bufio.Writer
	results     []Result
}

// OpenResultStore creates the output folder and the completion log. The
// error is returned to the caller rather than handled here
func OpenResultStore(resultsPath string) (*ResultStore, error) {
	dir := filepath.Dir(resultsPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create output folder: %w", err)
	}
	base := strings.TrimSuffix(filepath.Base(resultsPath), filepath.Ext(resultsPath))
	f, err := os.Create(filepath.Join(dir, base+"-completion.log"))
	if err != nil {
		return nil, fmt.Errorf("open completion log: %w", err)
	}
	return &ResultStore{resultsPath: resultsPath, logFile: f, log: bufio.NewWriter(f)}, nil
}

// Collect receives results until the channel is closed. It runs in exactly
// one goroutine, so no other goroutine ever touches the store while it runs
func (s *ResultStore) Collect(in <-chan Result, onResult func(Result)) error {
	var firstErr error
	for r := range in {
		s.results = append(s.results, r)
		if _, err := s.log.WriteString(r.CompletionLine() + "\n"); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("write completion log: %w", err)
		}
		if err := s.log.Flush(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("flush completion log: %w", err)
		}
		if onResult != nil {
			onResult(r)
		}
	}
	return firstErr
}

// WriteResults writes the results file sorted by task id. It is called only
// after Collect has returned, so it reads the slice without a lock
func (s *ResultStore) WriteResults(total int, cancelled bool) (err error) {
	sorted := append([]Result(nil), s.results...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Task.ID < sorted[j].Task.ID })
	ok := 0
	for _, r := range sorted {
		if r.OK {
			ok++
		}
	}

	f, err := os.Create(s.resultsPath)
	if err != nil {
		return fmt.Errorf("create results file: %w", err)
	}
	// the deferred close reports its own error if nothing failed before it
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close results file: %w", cerr)
		}
	}()

	w := bufio.NewWriter(f)
	note := ""
	if cancelled {
		note = ", run cancelled"
	}
	fmt.Fprintf(w, "# results: %d tasks, %d ok, %d failed%s\n", total, ok, len(sorted)-ok, note)
	for _, r := range sorted {
		fmt.Fprintln(w, r.ResultLine())
	}
	return w.Flush()
}

func (s *ResultStore) Results() []Result { return s.results }

func (s *ResultStore) Close() error {
	if err := s.log.Flush(); err != nil {
		s.logFile.Close()
		return err
	}
	return s.logFile.Close()
}
