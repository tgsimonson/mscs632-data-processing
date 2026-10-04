package dataproc;

// progress callbacks for the window; the console run uses the no-op default.
// every method is called on the thread where the event happened, so a gui
// must hand the update to its own event thread
public interface EngineListener {

    EngineListener NONE = new EngineListener() {
    };

    default void workerStarted(String worker) {
    }

    default void taskStarted(String worker, Task task) {
    }

    default void taskFinished(String worker, Result result) {
    }

    default void workerStopped(String worker, int processed) {
    }

    default void queueDepth(int depth, int capacity) {
    }
}
