package dataproc;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

// a bounded queue shared by one producer and many workers
// one ReentrantLock guards the deque, and two conditions let threads wait
// without spinning: workers wait on notEmpty, the producer waits on notFull
// every wait is in a loop that rechecks its condition, because a thread can wake
// without the condition being true; close() wakes every waiter, so no thread is
// left blocked forever once the work runs out
public final class TaskQueue {

    private final Deque<Task> tasks = new ArrayDeque<>();
    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private boolean closed = false;

    public TaskQueue(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("queue capacity must be at least 1");
        }
        this.capacity = capacity;
    }

    // adds a task, waiting while the queue is full
    public void addTask(Task task) throws InterruptedException, QueueClosedException {
        lock.lockInterruptibly();
        try {
            while (tasks.size() == capacity && !closed) {
                notFull.await();
            }
            if (closed) {
                throw new QueueClosedException("cannot add task " + task.id() + ": queue is closed");
            }
            tasks.addLast(task);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    // takes the next task, waiting while the queue is empty
    // once the queue is closed and empty it throws QueueClosedException instead
    // of waiting, which is how a worker learns that all work has been handed out
    public Task getTask() throws InterruptedException, QueueClosedException {
        lock.lockInterruptibly();
        try {
            while (tasks.isEmpty() && !closed) {
                notEmpty.await();
            }
            if (tasks.isEmpty()) {
                throw new QueueClosedException("queue is closed and empty");
            }
            Task task = tasks.removeFirst();
            notFull.signal();
            return task;
        } finally {
            lock.unlock();
        }
    }

    // stops new tasks from being added; workers still drain what is left
    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return tasks.size();
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        return capacity;
    }
}
