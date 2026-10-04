package main

import (
	"context"
	"strconv"
	"strings"
	"time"
)

const maxValue = 1_000_000_000_000

// Task is one line of the input file: an id and the raw value text. The value
// stays text so that validating it is part of processing
type Task struct {
	ID    int
	Value string
}

// Result is the outcome of one task, success or failure
type Result struct {
	Task     Task
	Worker   string
	OK       bool
	Factors  string
	DigitSum int
	Message  string
	Millis   int64
}

// ProcessingError marks a task whose data cannot be processed. errors.As lets
// a caller tell it apart from a cancellation
type ProcessingError struct {
	TaskID int
	Reason string
}

func (e *ProcessingError) Error() string { return e.Reason }

// ResultLine is the line written to the sorted results file; it leaves out
// the worker and timing so every run, in either language, writes the same file
func (r Result) ResultLine() string {
	id := strconv.Itoa(r.Task.ID)
	for len(id) < 4 {
		id = "0" + id
	}
	if r.OK {
		return "id=" + id + " value=" + r.Task.Value + " status=OK factors=" + r.Factors +
			" digitsum=" + strconv.Itoa(r.DigitSum)
	}
	return "id=" + id + " value=" + r.Task.Value + " status=ERROR message=" + r.Message
}

// CompletionLine is appended to the completion log as each task finishes
func (r Result) CompletionLine() string {
	return r.Worker + " " + r.ResultLine() + " ms=" + strconv.FormatInt(r.Millis, 10)
}

// baseDelay is the simulated work time in milliseconds, before scaling
func baseDelay(t Task) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(t.Value), 10, 64)
	if err != nil {
		return 20
	}
	m := v % 50
	if m < 0 {
		m += 50
	}
	return 20 + m
}

// Process sleeps to simulate a slow computation, then factors the value. It
// returns a *ProcessingError for bad data, or ctx.Err() if the run is cancelled
// during the delay
func Process(ctx context.Context, t Task, worker string, scale float64) (Result, error) {
	started := time.Now()
	delay := time.Duration(float64(baseDelay(t))*scale+0.5) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}

	v, err := strconv.ParseInt(strings.TrimSpace(t.Value), 10, 64)
	if err != nil {
		// out-of-range numbers count as not whole numbers too, as in java
		return Result{}, &ProcessingError{t.ID, "value is not a whole number"}
	}
	if v < 1 || v > maxValue {
		return Result{}, &ProcessingError{t.ID, "value must be from 1 to 1000000000000"}
	}
	return Result{
		Task: t, Worker: worker, OK: true,
		Factors: factor(v), DigitSum: digitSum(v),
		Millis: time.Since(started).Milliseconds(),
	}, nil
}

// factor writes the prime factors as 2^3*3^2*5, or 1 for the value 1
func factor(v int64) string {
	if v == 1 {
		return "1"
	}
	var b strings.Builder
	add := func(p int64, power int) {
		if b.Len() > 0 {
			b.WriteByte('*')
		}
		b.WriteString(strconv.FormatInt(p, 10))
		if power > 1 {
			b.WriteByte('^')
			b.WriteString(strconv.Itoa(power))
		}
	}
	n := v
	for p := int64(2); p*p <= n; p++ {
		power := 0
		for n%p == 0 {
			n /= p
			power++
		}
		if power > 0 {
			add(p, power)
		}
	}
	if n > 1 {
		add(n, 1)
	}
	return b.String()
}

func digitSum(v int64) int {
	sum := 0
	for n := v; n > 0; n /= 10 {
		sum += int(n % 10)
	}
	return sum
}
