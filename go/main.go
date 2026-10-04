// Command dataproc is the go version of the data processing system. With no
// arguments it opens the web dashboard; "run" processes the task file on the
// console:
//
//	dataproc run [-workers N] [-capacity N] [-scale X] [-tasks FILE] [-output FILE]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] == "gui" {
		port := flag.NewFlagSet("gui", flag.ExitOnError)
		addr := port.String("addr", "127.0.0.1:8080", "address for the dashboard")
		noOpen := port.Bool("no-open", false, "do not open the browser")
		if len(os.Args) > 2 {
			port.Parse(os.Args[2:])
		}
		os.Exit(serve(*addr, !*noOpen))
	}
	if os.Args[1] != "run" {
		fmt.Fprintln(os.Stderr, "usage: dataproc [gui] | run [-workers N] [-capacity N] [-scale X] [-tasks FILE] [-output FILE]")
		os.Exit(1)
	}
	os.Exit(runConsole(os.Args[2:]))
}

func runConsole(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	workers := fs.Int("workers", 4, "number of worker goroutines")
	capacity := fs.Int("capacity", 8, "capacity of the shared queue")
	scale := fs.Float64("scale", 1.0, "multiplier for the simulated work time")
	tasks := fs.String("tasks", "../data/tasks.txt", "task file")
	output := fs.String("output", "results/go-results.txt", "results file")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	log := NewLogger("logs/go-run.log", true)
	defer log.Close()

	// ctrl+c cancels the context; workers see it and stop cleanly
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	summary, err := Run(ctx, Config{
		TasksFile: *tasks, ResultsFile: *output,
		Workers: *workers, QueueCapacity: *capacity, DelayScale: *scale,
	}, log, Events{})
	if err != nil {
		return reportError(log, err)
	}
	fmt.Println()
	fmt.Println(summary.Report())
	if !summary.Complete() {
		return 2
	}
	return 0
}

// reportError logs a run that could not finish and picks the exit code
func reportError(log *Logger, err error) int {
	var pathErr *fs.PathError
	switch {
	case errors.Is(err, fs.ErrNotExist) && errors.As(err, &pathErr) && pathErr.Op == "open":
		log.Error("main", "task file not found: "+pathErr.Path)
		return 3
	case errors.As(err, &pathErr):
		log.Error("main", "file error: "+err.Error())
		return 3
	default:
		log.Error("main", err.Error())
		return 1
	}
}
