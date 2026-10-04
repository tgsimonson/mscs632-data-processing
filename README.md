# Data Processing System: Java and Go

MSCS 632 Advanced Programming Languages. A multi-worker data processing system
built twice, in Java and in Go, to compare the two languages' concurrency
models and error handling. Workers take tasks from a shared queue, process
them with a simulated delay, and record the results in a shared result store.
Both versions have a desktop-style interface and a console mode, and both
write byte-identical results files for the same input.

## What a run does

1. A producer reads `data/tasks.txt` (one `id,value` per line) and adds each
   task to a bounded shared queue, waiting whenever the queue is full.
2. A fixed number of workers take tasks from the queue. Each one sleeps to
   simulate work, then factors the value into primes and adds up its digits.
3. Workers record each result in the shared result store, which appends it to
   a completion log in the order tasks finish.
4. When the producer has queued every task it closes the queue. Workers drain
   what is left, see that the queue is closed and empty, and exit.
5. The results file is written sorted by task id, and the run checks that
   every task was processed exactly once, with none missed and none repeated.

Three values in the task file are invalid on purpose (`-42`, `abc`, `0`).
Processing them raises an error that is logged and recorded as a failed
result while the other workers carry on.

## How each requirement is met

| Requirement | Java | Go |
|---|---|---|
| Shared queue with `addTask()` / `getTask()` | `TaskQueue`: `ArrayDeque` guarded by a `ReentrantLock` with `notEmpty` and `notFull` conditions | `TaskQueue`: a buffered channel; `AddTask` / `GetTask` |
| Worker management | `ExecutorService` fixed thread pool, named threads | goroutines and a `sync.WaitGroup` |
| Shared output resource | `ResultStore` with `synchronized` methods | `ResultStore` owned by one collector goroutine fed by a channel |
| No deadlock, safe termination | `close()` signals every waiter; `getTask()` throws `QueueClosedException` once closed and empty; `shutdown()` and `awaitTermination()` | `Close()` closes the channel; `GetTask` returns `ErrQueueClosed`; `wg.Wait()` before closing the results channel |
| No missed or repeated tasks | checked after every run and in tests | checked after every run and in tests, with the race detector |
| Exception handling | `try`/`catch` for `InterruptedException`, `IOException`, `NoSuchFileException`, `ProcessingException`, `QueueClosedException`; try-with-resources | returned `error` values, `errors.Is` / `errors.As`, `defer` for closing files, `recover` in workers |
| Cancellation | Stop button or `cancel()`: `shutdownNow()` interrupts workers | Stop button or Ctrl+C: `context` cancellation |
| Logging | `java.util.logging` with a custom formatter, console and `logs/java-run.log` | a small mutex-guarded logger, console and `logs/go-run.log` |

Log lines have the same shape in both languages:

```
14:02:31.118 [worker-2] INFO  completed task 7 in 64 ms
14:02:31.204 [worker-4] ERROR task 21 failed: value must be from 1 to 1000000000000 (value "-42")
```

## Running

Requirements: a JDK 17 or newer, and Go 1.21 or newer (`brew install go` on
macOS). Nothing else is downloaded.

```bash
./run_tests.sh               # unit tests in both languages
./run_both.sh                # console run in both languages, then a diff of the results
```

Interfaces:

```bash
cd java && ./dataproc        # Java: Swing window
cd go && go run .            # Go: dashboard in the browser at http://127.0.0.1:8080
```

In either interface, choose the number of workers, the queue capacity and a
delay scale (higher is slower and easier to watch), then press Start. Each
worker has a lane showing the task it is processing. The queue depth, the
results table and the log update live. Stop cancels a run in progress.

Console runs:

```bash
cd java && ./dataproc run --workers 4 --capacity 8 --scale 1
cd go   && go run . run -workers 4 -capacity 8 -scale 1
```

Options: workers (1 to 64), queue capacity, delay scale, `tasks` (input file)
and `output` (results file). Output goes to `results/` and logs to `logs/`
inside each language's folder.

Error handling can be seen directly:

```bash
cd java && ./dataproc run --tasks ../data/missing.txt     # missing input file
cd go   && go run . run -tasks ../data/missing.txt
```

## Tests

| Suite | Command | Cases |
|---|---|---|
| Java | `./run_tests.sh` (plain `main`, no framework) | 11 |
| Go | `cd go && go test -race -v` | 11 |

The suites mirror each other: factoring, invalid values, a closed empty queue,
draining a closed queue, adding to a closed queue, waking a blocked worker on
close, 8 producers and 8 consumers moving 4,000 tasks through a capacity-3
queue with nothing lost or repeated, a full run, a missing input file, an
unwritable output location, and cancellation. The Go suite runs under the
race detector, which fails a test on any unsynchronized memory access. It
needs cgo, which macOS has through the Xcode command line tools; on Windows it
needs a C compiler, so `run_tests.sh` falls back to a plain run.

## Layout

```
data/tasks.txt              shared input, 40 tasks
java/src/dataproc/          TaskQueue, Engine, Processor, ResultStore, Log, Main
java/src/dataproc/gui/      Swing window
java/test/dataproc/         unit tests
java/dataproc               build-and-run script
go/                         queue.go, engine.go, processor.go, results.go,
                            logging.go, main.go, server.go, engine_test.go
go/web/index.html           dashboard page, embedded in the binary
run_tests.sh                both test suites
run_both.sh                 both console runs and a diff
```
