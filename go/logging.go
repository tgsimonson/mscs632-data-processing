package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Logger writes lines in the same shape as the java version:
//
//	14:02:31.118 [worker-2] INFO  completed task 7
//
// go has no thread names, so each goroutine passes its own name. A mutex keeps
// lines from different goroutines from interleaving mid-line
type Logger struct {
	mu   sync.Mutex
	out  io.Writer
	file *os.File
	hook func(line string)
}

// NewLogger writes to the console (if asked) and to logFile. If the log file
// cannot be opened the logger falls back to the console and reports why
func NewLogger(logFile string, console bool) *Logger {
	var writers []io.Writer
	if console {
		writers = append(writers, os.Stderr)
	}
	l := &Logger{}
	var openErr error
	if err := os.MkdirAll(filepath.Dir(logFile), 0o755); err != nil {
		openErr = err
	} else if f, err := os.Create(logFile); err != nil {
		openErr = err
	} else {
		l.file = f
		writers = append(writers, f)
	}
	if openErr != nil && !console {
		writers = append(writers, os.Stderr)
	}
	l.out = io.MultiWriter(writers...)
	if openErr != nil {
		l.Warn("main", "cannot open log file "+logFile+": "+openErr.Error())
	}
	return l
}

// SetHook sends every line to fn as well, which is how the web view gets them
func (l *Logger) SetHook(fn func(string)) {
	l.mu.Lock()
	l.hook = fn
	l.mu.Unlock()
}

func (l *Logger) write(who, level, msg string) {
	line := fmt.Sprintf("%s [%s] %-5s %s\n", time.Now().Format("15:04:05.000"), who, level, msg)
	l.mu.Lock()
	defer l.mu.Unlock()
	io.WriteString(l.out, line)
	if l.hook != nil {
		l.hook(line)
	}
}

func (l *Logger) Info(who, msg string)  { l.write(who, "INFO", msg) }
func (l *Logger) Warn(who, msg string)  { l.write(who, "WARN", msg) }
func (l *Logger) Error(who, msg string) { l.write(who, "ERROR", msg) }

func (l *Logger) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}
