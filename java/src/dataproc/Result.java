package dataproc;

// the outcome of one task, whether it succeeded or failed
public record Result(Task task, String worker, boolean ok, String factors, int digitSum,
                     String message, long millis) {

    static Result success(Task task, String worker, String factors, int digitSum, long millis) {
        return new Result(task, worker, true, factors, digitSum, null, millis);
    }

    static Result failure(Task task, String worker, String message, long millis) {
        return new Result(task, worker, false, null, 0, message, millis);
    }

    // the line written to the sorted results file
    // it leaves out the worker and the timing, which change from run to run, so
    // that two runs, or the Java and Go versions, produce the same file
    String resultLine() {
        String id = String.format("%04d", task.id());
        if (ok) {
            return "id=" + id + " value=" + task.value() + " status=OK factors=" + factors
                    + " digitsum=" + digitSum;
        }
        return "id=" + id + " value=" + task.value() + " status=ERROR message=" + message;
    }

    // the line appended to the completion log as each task finishes
    String completionLine() {
        return worker + " " + resultLine() + " ms=" + millis;
    }
}
