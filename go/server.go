package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync"
)

//go:embed web
var webFiles embed.FS

// hub fans events out to every open browser tab over server-sent events
type hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func (h *hub) add() chan []byte {
	ch := make(chan []byte, 256)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) remove(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

// send never blocks: a tab that falls behind loses an event rather than
// stalling the workers
func (h *hub) send(kind string, data any) {
	msg, err := json.Marshal(map[string]any{"type": kind, "data": data})
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

// runner allows one run at a time and remembers how to cancel it
type runner struct {
	mu     sync.Mutex
	cancel context.CancelFunc
}

type runRequest struct {
	Workers  int     `json:"workers"`
	Capacity int     `json:"capacity"`
	Scale    float64 `json:"scale"`
	Tasks    string  `json:"tasks"`
}

func serve(addr string, openBrowser bool) int {
	log := NewLogger("logs/go-run.log", true)
	defer log.Close()

	h := &hub{clients: map[chan []byte]struct{}{}}
	log.SetHook(func(line string) { h.send("log", line) })
	r := &runner{}

	static, err := fs.Sub(webFiles, "web")
	if err != nil {
		log.Error("main", err.Error())
		return 1
	}
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(static)))

	mux.HandleFunc("/api/events", func(w http.ResponseWriter, req *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		ch := h.add()
		defer h.remove(ch)
		for {
			select {
			case msg := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", msg)
				flusher.Flush()
			case <-req.Context().Done():
				return
			}
		}
	})

	mux.HandleFunc("/api/run", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var body runRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "bad request body", http.StatusBadRequest)
			return
		}
		if body.Tasks == "" {
			body.Tasks = "../data/tasks.txt"
		}
		r.mu.Lock()
		if r.cancel != nil {
			r.mu.Unlock()
			http.Error(w, "a run is already in progress", http.StatusConflict)
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		r.cancel = cancel
		r.mu.Unlock()

		cfg := Config{TasksFile: body.Tasks, ResultsFile: "results/go-results.txt",
			Workers: body.Workers, QueueCapacity: body.Capacity, DelayScale: body.Scale}
		tasks, _, _ := LoadTasks(cfg.TasksFile, &Logger{out: discard{}})
		h.send("start", map[string]any{"workers": cfg.Workers, "capacity": cfg.QueueCapacity, "total": len(tasks)})

		go func() {
			defer func() {
				r.mu.Lock()
				r.cancel = nil
				r.mu.Unlock()
				cancel()
			}()
			summary, err := Run(ctx, cfg, log, Events{
				WorkerStarted: func(name string) { h.send("worker", map[string]any{"worker": name, "state": "waiting"}) },
				TaskStarted: func(name string, t Task) {
					h.send("task", map[string]any{"worker": name, "id": t.ID, "value": t.Value})
				},
				TaskFinished: func(name string, res Result) {
					h.send("result", map[string]any{"worker": name, "id": res.Task.ID, "value": res.Task.Value,
						"ok": res.OK, "factors": res.Factors, "message": res.Message, "ms": res.Millis})
				},
				WorkerStopped: func(name string, n int) {
					h.send("worker", map[string]any{"worker": name, "state": "finished", "processed": n})
				},
				QueueDepth: func(d, c int) { h.send("queue", map[string]any{"depth": d, "capacity": c}) },
			})
			if err != nil {
				msg := err.Error()
				var pathErr *fs.PathError
				if errors.Is(err, fs.ErrNotExist) && errors.As(err, &pathErr) {
					msg = "task file not found: " + pathErr.Path
				}
				log.Error("main", msg)
				h.send("error", msg)
				return
			}
			h.send("done", map[string]any{
				"processed": summary.Processed, "ok": summary.OK, "failed": summary.Failed,
				"missing": len(summary.Missing), "duplicates": len(summary.Duplicates),
				"cancelled": summary.Cancelled, "complete": summary.Complete(), "ms": summary.ElapsedMillis,
			})
		}()
		w.WriteHeader(http.StatusAccepted)
	})

	mux.HandleFunc("/api/stop", func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		cancel := r.cancel
		r.mu.Unlock()
		if cancel != nil {
			log.Warn("main", "stop requested from the dashboard")
			cancel()
		}
		w.WriteHeader(http.StatusNoContent)
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("main", "cannot listen on "+addr+": "+err.Error())
		return 1
	}
	url := "http://" + ln.Addr().String() + "/"
	log.Info("main", "dashboard running at "+url+" (ctrl+c to stop)")
	if openBrowser {
		open(url)
	}
	if err := http.Serve(ln, mux); err != nil {
		log.Error("main", err.Error())
		return 1
	}
	return 0
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// open shows the dashboard in the default browser on macos, windows or linux
func open(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	cmd.Stdout, cmd.Stderr = nil, os.Stderr
	_ = cmd.Start()
}
