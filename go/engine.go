package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config holds the settings for one run
type Config struct {
	TasksFile     string
	ResultsFile   string
	Workers       int
	QueueCapacity int
	DelayScale    float64
}

func (c Config) validate() error {
	if c.Workers < 1 || c.Workers > 64 {
		return errors.New("workers must be from 1 to 64")
	}
	if c.DelayScale < 0 {
		return errors.New("delay scale cannot be negative")
	}
	return nil
}

// Events receives progress for the web view. Any field may be nil. Calls come
// from worker goroutines, so a receiver must be safe for concurrent use
type Events struct {
	WorkerStarted func(worker string)
	TaskStarted   func(worker string, t Task)
	TaskFinished  func(worker string, r Result)
	WorkerStopped func(worker string, processed int)
	QueueDepth    func(depth, capacity int)
}

// Summary is what a run produced, checked for missed and repeated tasks
type Summary struct {
	Loaded, Skipped, Processed, OK, Failed int
	Missing, Duplicates                    []int
	PerWorker                              map[string]int
	ElapsedMillis                          int64
	Cancelled                              bool
}

func (s Summary) Complete() bool {
	return len(s.Missing) == 0 && len(s.Duplicates) == 0 && !s.Cancelled
}

func listOrNone(ids []int) string {
	if len(ids) == 0 {
		return "none"
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Itoa(id)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// Report prints the same block as the java version
func (s Summary) Report() string {
	names := make([]string, 0, len(s.PerWorker))
	for name := range s.PerWorker {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = fmt.Sprintf("%s=%d", name, s.PerWorker[name])
	}

	var b strings.Builder
	fmt.Fprintf(&b, "tasks loaded:      %d\n", s.Loaded)
	if s.Skipped > 0 {
		fmt.Fprintf(&b, "lines skipped:     %d\n", s.Skipped)
	}
	fmt.Fprintf(&b, "tasks processed:   %d (%d ok, %d failed)\n", s.Processed, s.OK, s.Failed)
	fmt.Fprintf(&b, "missed tasks:      %s\n", listOrNone(s.Missing))
	fmt.Fprintf(&b, "repeated tasks:    %s\n", listOrNone(s.Duplicates))
	fmt.Fprintf(&b, "per worker:        {%s}\n", strings.Join(parts, ", "))
	fmt.Fprintf(&b, "elapsed:           %d ms\n", s.ElapsedMillis)
	result := "INCOMPLETE"
	switch {
	case s.Cancelled:
		result = "CANCELLED"
	case s.Complete():
		result = "COMPLETE, every task processed exactly once"
	}
	fmt.Fprintf(&b, "result:            %s", result)
	return b.String()
}

// LoadTasks reads id,value lines. Blank lines and # comments are ignored; a
// line that is not id,value is logged and skipped. The file is closed by defer
// on every return path
func LoadTasks(path string, log *Logger) ([]Task, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	var tasks []Task
	skipped, lineNo := 0, 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lineNo++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		idText, value, found := strings.Cut(text, ",")
		id, convErr := strconv.Atoi(strings.TrimSpace(idText))
		if !found || idText == "" || convErr != nil {
			skipped++
			log.Warn("main", fmt.Sprintf("skipping line %d of %s: expected id,value", lineNo, baseName(path)))
			continue
		}
		tasks = append(tasks, Task{ID: id, Value: strings.TrimSpace(value)})
	}
	if err := sc.Err(); err != nil {
		return nil, 0, fmt.Errorf("read %s: %w", path, err)
	}
	return tasks, skipped, nil
}

func baseName(path string) string {
	if i := strings.LastIndexAny(path, `/\`); i >= 0 {
		return path[i+1:]
	}
	return path
}

// processSafely turns a panic inside Process into an error, so one bad task
// fails that task instead of crashing the program
func processSafely(ctx context.Context, t Task, worker string, scale float64) (r Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("unexpected error: %v", p)
		}
	}()
	return Process(ctx, t, worker, scale)
}

// Run processes one batch. A producer (this goroutine) fills the queue while
// cfg.Workers goroutines drain it and send results to the collector. Run
// returns once every goroutine it started has exited
func Run(ctx context.Context, cfg Config, log *Logger, ev Events) (Summary, error) {
	if err := cfg.validate(); err != nil {
		return Summary{}, err
	}
	started := time.Now()
	tasks, skipped, err := LoadTasks(cfg.TasksFile, log)
	if err != nil {
		return Summary{}, err
	}
	log.Info("main", fmt.Sprintf("loaded %d tasks from %s; starting %d workers, queue capacity %d",
		len(tasks), baseName(cfg.TasksFile), cfg.Workers, cfg.QueueCapacity))

	queue, err := NewTaskQueue(cfg.QueueCapacity)
	if err != nil {
		return Summary{}, err
	}
	store, err := OpenResultStore(cfg.ResultsFile)
	if err != nil {
		return Summary{}, err
	}
	defer store.Close()

	results := make(chan Result)
	collectDone := make(chan error, 1)
	go func() {
		collectDone <- store.Collect(results, nil)
	}()

	var (
		wg        sync.WaitGroup
		countMu   sync.Mutex
		perWorker = map[string]int{}
	)
	for i := 1; i <= cfg.Workers; i++ {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			processed := 0
			defer func() {
				countMu.Lock()
				perWorker[name] = processed
				countMu.Unlock()
				log.Info(name, fmt.Sprintf("finished after %d tasks", processed))
				if ev.WorkerStopped != nil {
					ev.WorkerStopped(name, processed)
				}
			}()
			log.Info(name, "started")
			if ev.WorkerStarted != nil {
				ev.WorkerStarted(name)
			}

			for {
				t, err := queue.GetTask(ctx)
				if errors.Is(err, ErrQueueClosed) {
					return // no more work: the normal way out
				}
				if err != nil {
					log.Warn(name, "cancelled, stopping")
					return
				}
				if ev.QueueDepth != nil {
					ev.QueueDepth(queue.Len(), queue.Capacity())
				}
				if ev.TaskStarted != nil {
					ev.TaskStarted(name, t)
				}

				t0 := time.Now()
				r, err := processSafely(ctx, t, name, cfg.DelayScale)
				var perr *ProcessingError
				switch {
				case err == nil:
					log.Info(name, fmt.Sprintf("completed task %d in %d ms", t.ID, r.Millis))
				case errors.As(err, &perr):
					r = Result{Task: t, Worker: name, Message: perr.Reason, Millis: time.Since(t0).Milliseconds()}
					log.Error(name, fmt.Sprintf("task %d failed: %s (value %q)", t.ID, perr.Reason, t.Value))
				case ctx.Err() != nil:
					log.Warn(name, "interrupted, stopping")
					return
				default:
					r = Result{Task: t, Worker: name, Message: err.Error(), Millis: time.Since(t0).Milliseconds()}
					log.Error(name, fmt.Sprintf("task %d hit an unexpected error: %v", t.ID, err))
				}

				results <- r
				processed++
				if ev.TaskFinished != nil {
					ev.TaskFinished(name, r)
				}
			}
		}(fmt.Sprintf("worker-%d", i))
	}

	// the producer: AddTask blocks while the queue is full
	func() {
		defer queue.Close() // closing is what lets the workers finish
		for _, t := range tasks {
			if err := queue.AddTask(ctx, t); err != nil {
				log.Warn("main", "producer stopped early: "+err.Error())
				return
			}
			if ev.QueueDepth != nil {
				ev.QueueDepth(queue.Len(), queue.Capacity())
			}
		}
		log.Info("main", fmt.Sprintf("producer queued all %d tasks", len(tasks)))
	}()

	wg.Wait()      // every worker has exited
	close(results) // so the collector's range loop ends
	if err := <-collectDone; err != nil {
		log.Error("main", "could not write a result: "+err.Error())
	}

	cancelled := ctx.Err() != nil
	if err := store.WriteResults(len(tasks), cancelled); err != nil {
		return Summary{}, err
	}
	log.Info("main", "results written to "+cfg.ResultsFile)
	return summarize(tasks, skipped, store.Results(), perWorker, time.Since(started).Milliseconds(), cancelled), nil
}

func summarize(tasks []Task, skipped int, results []Result, perWorker map[string]int,
	millis int64, cancelled bool) Summary {
	seen := map[int]int{}
	ok := 0
	for _, r := range results {
		seen[r.Task.ID]++
		if r.OK {
			ok++
		}
	}
	var missing, duplicates []int
	for _, t := range tasks {
		if seen[t.ID] == 0 {
			missing = append(missing, t.ID)
		}
	}
	for id, n := range seen {
		if n > 1 {
			duplicates = append(duplicates, id)
		}
	}
	sort.Ints(duplicates)
	return Summary{
		Loaded: len(tasks), Skipped: skipped, Processed: len(results), OK: ok,
		Failed: len(results) - ok, Missing: missing, Duplicates: duplicates,
		PerWorker: perWorker, ElapsedMillis: millis, Cancelled: cancelled,
	}
}
