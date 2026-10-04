package dataproc;

import java.util.List;
import java.util.Map;

// what a run produced, checked for missed and repeated tasks
public record Summary(int loaded, int skippedLines, int processed, int ok, int failed,
                      List<Integer> missing, List<Integer> duplicates,
                      Map<String, Integer> perWorker, long elapsedMillis, boolean cancelled) {

    public boolean complete() {
        return missing.isEmpty() && duplicates.isEmpty() && !cancelled;
    }

    public String report() {
        StringBuilder out = new StringBuilder();
        out.append("tasks loaded:      ").append(loaded).append('\n');
        if (skippedLines > 0) {
            out.append("lines skipped:     ").append(skippedLines).append('\n');
        }
        out.append("tasks processed:   ").append(processed).append(" (").append(ok)
                .append(" ok, ").append(failed).append(" failed)\n");
        out.append("missed tasks:      ").append(missing.isEmpty() ? "none" : missing).append('\n');
        out.append("repeated tasks:    ").append(duplicates.isEmpty() ? "none" : duplicates).append('\n');
        out.append("per worker:        ").append(perWorker).append('\n');
        out.append("elapsed:           ").append(elapsedMillis).append(" ms\n");
        out.append("result:            ").append(cancelled ? "CANCELLED"
                : complete() ? "COMPLETE, every task processed exactly once" : "INCOMPLETE");
        return out.toString();
    }
}
