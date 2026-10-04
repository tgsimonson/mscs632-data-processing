package main

// run with: go test -race -v
// the -race flag makes go's race detector watch every memory access, so a
// missing lock or an unsafe share fails the test. the cases match
// java/test/dataproc/EngineTest.java one for one

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func quietLogger(t *testing.T) *Logger {
	return NewLogger(filepath.Join(t.TempDir(), "test.log"), false)
}

func writeFile(t *testing.T, dir, name, text string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFactorAndDigitSum(t *testing.T) {
	cases := map[int64]string{360: "2^3*3^2*5", 97: "97", 1: "1", 600851475143: "71*839*1471*6857"}
	for v, want := range cases {
		if got := factor(v); got != want {
			t.Errorf("factor(%d) = %s, want %s", v, got, want)
		}
	}
	if digitSum(123456) != 21 {
		t.Error("digit sum")
	}
}

func TestInvalidValuesReturnProcessingError(t *testing.T) {
	for _, bad := range []string{"abc", "-42", "0", "1000000000001"} {
		_, err := Process(context.Background(), Task{1, bad}, "t", 0)
		var perr *ProcessingError
		if !errors.As(err, &perr) {
			t.Errorf("value %q: got %v, want a ProcessingError", bad, err)
		}
	}
}

func TestGetTaskOnClosedEmptyQueueReturnsErrQueueClosed(t *testing.T) {
	q, _ := NewTaskQueue(2)
	q.Close()
	if _, err := q.GetTask(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestClosedQueueStillHandsOutQueuedTasks(t *testing.T) {
	q, _ := NewTaskQueue(4)
	ctx := context.Background()
	q.AddTask(ctx, Task{ID: 1})
	q.AddTask(ctx, Task{ID: 2})
	q.Close()
	a, _ := q.GetTask(ctx)
	b, _ := q.GetTask(ctx)
	if a.ID != 1 || b.ID != 2 {
		t.Fatalf("drain order %d %d", a.ID, b.ID)
	}
}

func TestAddTaskOnClosedQueueReturnsError(t *testing.T) {
	q, _ := NewTaskQueue(4)
	q.Close()
	q.Close() // a second close is harmless
	if err := q.AddTask(context.Background(), Task{ID: 1}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestCloseWakesBlockedWorker(t *testing.T) {
	q, _ := NewTaskQueue(1)
	woke := make(chan error, 1)
	go func() {
		_, err := q.GetTask(context.Background())
		woke <- err
	}()
	time.Sleep(50 * time.Millisecond)
	q.Close()
	select {
	case err := <-woke:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker was not woken by Close")
	}
}

func TestManyProducersAndConsumersLoseAndRepeatNothing(t *testing.T) {
	q, _ := NewTaskQueue(3)
	ctx := context.Background()
	var producers, consumers sync.WaitGroup
	var mu sync.Mutex
	taken := map[int]int{}
	for p := 0; p < 8; p++ {
		producers.Add(1)
		go func(base int) {
			defer producers.Done()
			for i := 0; i < 500; i++ {
				if err := q.AddTask(ctx, Task{ID: base + i}); err != nil {
					t.Error(err)
					return
				}
			}
		}(p * 1000)
	}
	for c := 0; c < 8; c++ {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				task, err := q.GetTask(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				taken[task.ID]++
				mu.Unlock()
			}
		}()
	}
	producers.Wait()
	q.Close()
	consumers.Wait()
	if len(taken) != 4000 {
		t.Fatalf("took %d distinct ids, want 4000", len(taken))
	}
	for id, n := range taken {
		if n != 1 {
			t.Fatalf("id %d taken %d times", id, n)
		}
	}
}

func TestFullRunProcessesEveryTaskOnceAndRecordsFailures(t *testing.T) {
	dir := t.TempDir()
	tasks := writeFile(t, dir, "tasks.txt", "# test\n1,12\n2,abc\n3,97\n\nbad line\n4,0\n5,100\n")
	out := filepath.Join(dir, "out", "results.txt")
	s, err := Run(context.Background(), Config{tasks, out, 3, 2, 0}, quietLogger(t), Events{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Loaded != 5 || s.Skipped != 1 || s.OK != 3 || s.Failed != 2 || !s.Complete() {
		t.Fatalf("summary:\n%s", s.Report())
	}
	data, _ := os.ReadFile(out)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if lines[0] != "# results: 5 tasks, 3 ok, 2 failed" ||
		lines[2] != "id=0002 value=abc status=ERROR message=value is not a whole number" {
		t.Fatalf("results file:\n%s", data)
	}
	log, _ := os.ReadFile(filepath.Join(dir, "out", "results-completion.log"))
	if n := len(strings.Split(strings.TrimSpace(string(log)), "\n")); n != 5 {
		t.Fatalf("completion log has %d lines", n)
	}
}

func TestMissingTaskFileReturnsNotExist(t *testing.T) {
	dir := t.TempDir()
	_, err := Run(context.Background(), Config{filepath.Join(dir, "missing.txt"),
		filepath.Join(dir, "r.txt"), 2, 2, 0}, quietLogger(t), Events{})
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("got %v", err)
	}
}

func TestUnwritableOutputReturnsError(t *testing.T) {
	dir := t.TempDir()
	blocker := writeFile(t, dir, "blocker", "a file, not a folder")
	tasks := writeFile(t, dir, "t.txt", "1,2\n")
	_, err := Run(context.Background(), Config{tasks, filepath.Join(blocker, "r.txt"), 2, 2, 0},
		quietLogger(t), Events{})
	if err == nil {
		t.Fatal("no error for an output path inside a file")
	}
}

func TestCancelStopsWorkersAndReportsCancelled(t *testing.T) {
	dir := t.TempDir()
	var b strings.Builder
	for i := 1; i <= 200; i++ {
		b.WriteString(strconv.Itoa(i) + ",1000\n")
	}
	tasks := writeFile(t, dir, "big.txt", b.String())
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)
	s, err := Run(ctx, Config{tasks, filepath.Join(dir, "c.txt"), 2, 4, 1.0}, quietLogger(t), Events{})
	if err != nil {
		t.Fatal(err)
	}
	if !s.Cancelled || s.Processed >= 200 || len(s.Duplicates) != 0 {
		t.Fatalf("summary:\n%s", s.Report())
	}
}
