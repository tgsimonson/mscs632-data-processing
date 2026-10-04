package dataproc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// runs one batch: a producer fills the shared queue from the task file while a
// fixed pool of worker threads drains it, processes each task and records the
// result in the shared result store
public final class Engine {

    public record Config(Path tasksFile, Path resultsFile, int workers, int queueCapacity,
                         double delayScale) {

        public Config {
            if (workers < 1 || workers > 64) {
                throw new IllegalArgumentException("workers must be from 1 to 64");
            }
            if (delayScale < 0) {
                throw new IllegalArgumentException("delay scale cannot be negative");
            }
        }
    }

    // the task file after parsing: tasks plus a count of lines that were skipped
    record Loaded(List<Task> tasks, int skipped) {
    }

    private volatile ExecutorService pool;
    private volatile TaskQueue queue;
    private volatile boolean cancelled;

    // reads id,value lines; blank lines and # comments are ignored, and a line
    // that is not id,value is logged and skipped rather than failing the run
    static Loaded loadTasks(Path file) throws IOException {
        List<Task> tasks = new ArrayList<>();
        int skipped = 0;
        int lineNo = 0;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            lineNo++;
            String text = line.trim();
            if (text.isEmpty() || text.startsWith("#")) {
                continue;
            }
            int comma = text.indexOf(',');
            try {
                if (comma < 1) {
                    throw new NumberFormatException("no comma");
                }
                int id = Integer.parseInt(text.substring(0, comma).trim());
                tasks.add(new Task(id, text.substring(comma + 1).trim()));
            } catch (NumberFormatException e) {
                skipped++;
                Log.warn("skipping line " + lineNo + " of " + file.getFileName() + ": expected id,value");
            }
        }
        return new Loaded(tasks, skipped);
    }

    // stops a run in progress: no more tasks are handed out and workers in the
    // middle of a task are interrupted
    public void cancel() {
        cancelled = true;
        TaskQueue q = queue;
        if (q != null) {
            q.close();
        }
        ExecutorService p = pool;
        if (p != null) {
            p.shutdownNow();
        }
    }

    public Summary run(Config config, EngineListener listener) throws IOException, InterruptedException {
        cancelled = false;
        long started = System.nanoTime();
        Loaded loaded = loadTasks(config.tasksFile());
        Log.info("loaded " + loaded.tasks().size() + " tasks from " + config.tasksFile().getFileName()
                + "; starting " + config.workers() + " workers, queue capacity " + config.queueCapacity());

        TaskQueue q = new TaskQueue(config.queueCapacity());
        queue = q;
        Map<String, Integer> perWorker = new ConcurrentHashMap<>();

        try (ResultStore store = new ResultStore(config.resultsFile())) {
            ExecutorService p = Executors.newFixedThreadPool(config.workers(), namedThreads());
            pool = p;
            for (int i = 0; i < config.workers(); i++) {
                p.execute(() -> work(q, store, config.delayScale(), listener, perWorker));
            }

            produce(q, loaded.tasks(), listener);

            // no new work will be submitted; wait for the workers to drain the queue
            p.shutdown();
            if (!p.awaitTermination(10, TimeUnit.MINUTES)) {
                Log.error("workers did not finish within 10 minutes; interrupting them");
                p.shutdownNow();
            }

            store.writeResults(loaded.tasks().size(), cancelled);
            Summary summary = summarize(loaded, store.snapshot(), perWorker,
                    (System.nanoTime() - started) / 1_000_000);
            Log.info("results written to " + config.resultsFile());
            return summary;
        } finally {
            pool = null;
            queue = null;
        }
    }

    // the producer runs on the calling thread; addTask blocks while the queue
    // is full, so the producer can never run far ahead of the workers
    private void produce(TaskQueue q, List<Task> tasks, EngineListener listener) throws InterruptedException {
        try {
            for (Task task : tasks) {
                q.addTask(task);
                listener.queueDepth(q.size(), q.capacity());
            }
            Log.info("producer queued all " + tasks.size() + " tasks");
        } catch (QueueClosedException e) {
            Log.warn("producer stopped early: " + e.getMessage());
        } finally {
            // closing is what lets the workers finish: once the queue is empty
            // getTask throws QueueClosedException instead of waiting forever
            q.close();
        }
    }

    // the loop each worker thread runs until the queue is closed and empty
    private static void work(TaskQueue q, ResultStore store, double scale, EngineListener listener,
                             Map<String, Integer> perWorker) {
        String name = Thread.currentThread().getName();
        int processed = 0;
        Log.info("started");
        listener.workerStarted(name);
        try {
            while (true) {
                Task task;
                try {
                    task = q.getTask();
                } catch (QueueClosedException e) {
                    break; // no more work: the normal way out
                }
                listener.queueDepth(q.size(), q.capacity());
                listener.taskStarted(name, task);

                long t0 = System.nanoTime();
                Result result;
                try {
                    result = Processor.process(task, name, scale);
                    Log.info("completed task " + task.id() + " in " + result.millis() + " ms");
                } catch (ProcessingException e) {
                    result = Result.failure(task, name, e.getMessage(), (System.nanoTime() - t0) / 1_000_000);
                    Log.error("task " + task.id() + " failed: " + e.getMessage() + " (value \"" + task.value() + "\")");
                } catch (RuntimeException e) {
                    // an unexpected bug in processing fails the task, not the worker
                    result = Result.failure(task, name, "unexpected error: " + e, (System.nanoTime() - t0) / 1_000_000);
                    Log.error("task " + task.id() + " hit an unexpected error: " + e);
                }

                try {
                    store.add(result);
                } catch (IOException e) {
                    Log.error("could not write the result of task " + task.id() + ": " + e.getMessage());
                }
                processed++;
                listener.taskFinished(name, result);
            }
        } catch (InterruptedException e) {
            // cancelled: restore the flag so the pool sees it, then stop
            Thread.currentThread().interrupt();
            Log.warn("interrupted, stopping");
        } finally {
            perWorker.put(name, processed);
            Log.info("finished after " + processed + " tasks");
            listener.workerStopped(name, processed);
        }
    }

    private static ThreadFactory namedThreads() {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, "worker-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private Summary summarize(Loaded loaded, List<Result> results, Map<String, Integer> perWorker, long millis) {
        Map<Integer, Integer> seen = new HashMap<>();
        for (Result r : results) {
            seen.merge(r.task().id(), 1, Integer::sum);
        }
        List<Integer> missing = new ArrayList<>();
        for (Task t : loaded.tasks()) {
            if (!seen.containsKey(t.id())) {
                missing.add(t.id());
            }
        }
        List<Integer> duplicates = new ArrayList<>();
        seen.forEach((id, count) -> {
            if (count > 1) {
                duplicates.add(id);
            }
        });
        Collections.sort(duplicates);
        int ok = (int) results.stream().filter(Result::ok).count();
        return new Summary(loaded.tasks().size(), loaded.skipped(), results.size(), ok,
                results.size() - ok, missing, duplicates, new TreeMap<>(perWorker), millis, cancelled);
    }
}
