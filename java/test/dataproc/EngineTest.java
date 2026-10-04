package dataproc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// unit tests without a framework, so they run with only a jdk:
//   java -cp out:out-test dataproc.EngineTest
// the cases match go/engine_test.go one for one
public final class EngineTest {

    private interface Case {
        void run() throws Exception;
    }

    private static int passed;
    private static int failed;
    private static Path dir;

    private EngineTest() {
    }

    private static void check(String name, Case body) {
        try {
            body.run();
            passed++;
            System.out.println("ok     " + name);
        } catch (Throwable t) {
            failed++;
            System.out.println("FAILED " + name + "\n       " + t);
        }
    }

    private static void expect(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static Path write(String name, String text) throws IOException {
        Path file = dir.resolve(name);
        Files.writeString(file, text);
        return file;
    }

    public static void main(String[] args) throws Exception {
        dir = Files.createTempDirectory("dataproc-java-test");
        Log.configure(dir.resolve("test.log"), false);

        check("factor and digit sum", () -> {
            expect(Processor.factor(360).equals("2^3*3^2*5"), Processor.factor(360));
            expect(Processor.factor(97).equals("97"), "prime");
            expect(Processor.factor(1).equals("1"), "one");
            expect(Processor.factor(600851475143L).equals("71*839*1471*6857"), "large");
            expect(Processor.digitSum(123456) == 21, "digit sum");
        });

        check("invalid values raise ProcessingException", () -> {
            for (String bad : new String[] {"abc", "-42", "0", "1000000000001"}) {
                try {
                    Processor.process(new Task(1, bad), "t", 0);
                    throw new AssertionError("no exception for " + bad);
                } catch (ProcessingException expected) {
                    // the case passes
                }
            }
        });

        check("getTask on a closed, empty queue throws instead of blocking", () -> {
            TaskQueue q = new TaskQueue(2);
            q.close();
            try {
                q.getTask();
                throw new AssertionError("no exception");
            } catch (QueueClosedException expected) {
                // the case passes
            }
        });

        check("a closed queue still hands out the tasks already in it", () -> {
            TaskQueue q = new TaskQueue(4);
            q.addTask(new Task(1, "1"));
            q.addTask(new Task(2, "2"));
            q.close();
            expect(q.getTask().id() == 1 && q.getTask().id() == 2, "drain order");
        });

        check("addTask on a closed queue throws", () -> {
            TaskQueue q = new TaskQueue(4);
            q.close();
            try {
                q.addTask(new Task(1, "1"));
                throw new AssertionError("no exception");
            } catch (QueueClosedException expected) {
                // the case passes
            }
        });

        check("close wakes a worker blocked on an empty queue", () -> {
            TaskQueue q = new TaskQueue(1);
            CountDownLatch woke = new CountDownLatch(1);
            Thread t = new Thread(() -> {
                try {
                    q.getTask();
                } catch (QueueClosedException e) {
                    woke.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            t.start();
            Thread.sleep(50);
            q.close();
            expect(woke.await(2, TimeUnit.SECONDS), "worker was not woken by close");
        });

        check("8 producers and 8 consumers on a capacity-3 queue lose and repeat nothing", () -> {
            TaskQueue q = new TaskQueue(3);
            List<Integer> taken = Collections.synchronizedList(new ArrayList<>());
            ExecutorService pool = Executors.newFixedThreadPool(16);
            CountDownLatch producersDone = new CountDownLatch(8);
            for (int p = 0; p < 8; p++) {
                int base = p * 1000;
                pool.execute(() -> {
                    try {
                        for (int i = 0; i < 500; i++) {
                            q.addTask(new Task(base + i, "1"));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        producersDone.countDown();
                    }
                });
            }
            for (int c = 0; c < 8; c++) {
                pool.execute(() -> {
                    try {
                        while (true) {
                            taken.add(q.getTask().id());
                        }
                    } catch (QueueClosedException e) {
                        // drained
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            producersDone.await();
            q.close();
            pool.shutdown();
            expect(pool.awaitTermination(10, TimeUnit.SECONDS), "pool did not terminate");
            Set<Integer> unique = new HashSet<>(taken);
            expect(taken.size() == 4000, "taken " + taken.size());
            expect(unique.size() == 4000, "repeated ids");
        });

        check("a full run processes every task exactly once and records the failures", () -> {
            Path tasks = write("tasks.txt", "# test\n1,12\n2,abc\n3,97\n\nbad line\n4,0\n5,100\n");
            Path out = dir.resolve("out/results.txt");
            Summary s = new Engine().run(new Engine.Config(tasks, out, 3, 2, 0), EngineListener.NONE);
            expect(s.loaded() == 5 && s.skippedLines() == 1, "loaded " + s.loaded() + " skipped " + s.skippedLines());
            expect(s.ok() == 3 && s.failed() == 2, "ok " + s.ok() + " failed " + s.failed());
            expect(s.complete(), s.report());
            List<String> lines = Files.readAllLines(out);
            expect(lines.get(0).equals("# results: 5 tasks, 3 ok, 2 failed"), lines.get(0));
            expect(lines.get(2).equals("id=0002 value=abc status=ERROR message=value is not a whole number"), lines.get(2));
            expect(Files.readAllLines(dir.resolve("out/results-completion.log")).size() == 5, "completion log lines");
        });

        check("a missing task file raises NoSuchFileException", () -> {
            try {
                new Engine().run(new Engine.Config(dir.resolve("missing.txt"), dir.resolve("r.txt"), 2, 2, 0),
                        EngineListener.NONE);
                throw new AssertionError("no exception");
            } catch (NoSuchFileException expected) {
                // the case passes
            }
        });

        check("an unwritable output location raises IOException", () -> {
            Path blocker = write("blocker", "a file, not a folder");
            try {
                new Engine().run(new Engine.Config(write("t.txt", "1,2\n"), blocker.resolve("r.txt"), 2, 2, 0),
                        EngineListener.NONE);
                throw new AssertionError("no exception");
            } catch (IOException expected) {
                // the case passes
            }
        });

        check("cancel stops the workers and reports the run as cancelled", () -> {
            StringBuilder big = new StringBuilder();
            for (int i = 1; i <= 200; i++) {
                big.append(i).append(",1000\n");
            }
            Path tasks = write("big.txt", big.toString());
            Engine engine = new Engine();
            Thread canceller = new Thread(() -> {
                try {
                    Thread.sleep(150);
                } catch (InterruptedException ignored) {
                    // not expected in this test
                }
                engine.cancel();
            });
            canceller.start();
            Summary s = engine.run(new Engine.Config(tasks, dir.resolve("c.txt"), 2, 4, 1.0), EngineListener.NONE);
            expect(s.cancelled(), "not cancelled");
            expect(s.processed() < 200, "processed everything");
            expect(s.duplicates().isEmpty(), "repeated ids after cancel");
        });

        System.out.println();
        System.out.println("tests " + (passed + failed) + ", passed " + passed + ", failed " + failed);
        System.exit(failed == 0 ? 0 : 1);
    }
}
