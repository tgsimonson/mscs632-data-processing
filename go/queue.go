package main

import (
	"context"
	"errors"
	"sync"
)

// ErrQueueClosed is returned when adding to a closed queue, or taking from a
// closed queue that has been emptied
var ErrQueueClosed = errors.New("queue is closed")

// TaskQueue is the shared queue. A buffered channel is already safe for many
// goroutines, so the channel is the queue: a send blocks while the buffer is
// full and a receive blocks while it is empty. The mutex only protects the
// closed flag, so that a late AddTask returns an error instead of panicking on
// a send to a closed channel
type TaskQueue struct {
	tasks  chan Task
	mu     sync.RWMutex
	closed bool
	once   sync.Once
}

func NewTaskQueue(capacity int) (*TaskQueue, error) {
	if capacity < 1 {
		return nil, errors.New("queue capacity must be at least 1")
	}
	return &TaskQueue{tasks: make(chan Task, capacity)}, nil
}

// AddTask waits while the queue is full. It returns ErrQueueClosed after
// Close, and the context's error if the run is cancelled while waiting
func (q *TaskQueue) AddTask(ctx context.Context, t Task) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrQueueClosed
	}
	select {
	case q.tasks <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetTask waits while the queue is empty. Once the queue is closed and
// drained it returns ErrQueueClosed, which tells a worker there is no more work
func (q *TaskQueue) GetTask(ctx context.Context) (Task, error) {
	select {
	case t, ok := <-q.tasks:
		if !ok {
			return Task{}, ErrQueueClosed
		}
		return t, nil
	case <-ctx.Done():
		return Task{}, ctx.Err()
	}
}

// Close stops new tasks; tasks already queued are still handed out. Taking
// the write lock waits for any AddTask in progress, so the channel is never
// closed underneath a send. sync.Once makes a second Close harmless
func (q *TaskQueue) Close() {
	q.once.Do(func() {
		q.mu.Lock()
		q.closed = true
		close(q.tasks)
		q.mu.Unlock()
	})
}

func (q *TaskQueue) Len() int      { return len(q.tasks) }
func (q *TaskQueue) Capacity() int { return cap(q.tasks) }
