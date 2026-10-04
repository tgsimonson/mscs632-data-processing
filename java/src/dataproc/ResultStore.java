package dataproc;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

// the shared output resource
// workers call add() concurrently; a synchronized method lets only one of them
// in at a time, so the results list and the completion log are never written by
// two threads at once
// two files come out of a run
// the completion log is appended to as tasks finish, in whatever order the
// workers happen to finish
// the results file is written once at the end, sorted by task id, so it is the
// same on every run
public final class ResultStore implements Closeable {

    private final List<Result> results = new ArrayList<>();
    private final Path resultsFile;
    private final BufferedWriter completionLog;

    public ResultStore(Path resultsFile) throws IOException {
        this.resultsFile = resultsFile;
        Path parent = resultsFile.toAbsolutePath().getParent();
        try {
            Files.createDirectories(parent);
        } catch (java.nio.file.FileAlreadyExistsException e) {
            // a file already has the name the output folder needs
            throw new IOException("create output folder: " + e.getFile() + ": not a directory", e);
        }
        Path logFile = parent.resolve(baseName(resultsFile) + "-completion.log");
        this.completionLog = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8);
    }

    private static String baseName(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    // records a result; safe to call from any number of worker threads
    public synchronized void add(Result result) throws IOException {
        results.add(result);
        completionLog.write(result.completionLine());
        completionLog.newLine();
        completionLog.flush();
    }

    // a copy of the results so far, in completion order
    public synchronized List<Result> snapshot() {
        return new ArrayList<>(results);
    }

    // writes the sorted results file
    public synchronized void writeResults(int total, boolean cancelled) throws IOException {
        List<Result> sorted = new ArrayList<>(results);
        sorted.sort(Comparator.comparingInt(r -> r.task().id()));
        long ok = sorted.stream().filter(Result::ok).count();
        try (BufferedWriter out = Files.newBufferedWriter(resultsFile, StandardCharsets.UTF_8)) {
            out.write("# results: " + total + " tasks, " + ok + " ok, " + (sorted.size() - ok)
                    + " failed" + (cancelled ? ", run cancelled" : ""));
            out.newLine();
            for (Result r : sorted) {
                out.write(r.resultLine());
                out.newLine();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        completionLog.close();
    }
}
