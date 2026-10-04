package dataproc;

// thrown by TaskQueue when a worker asks for a task after the queue has been
// closed and emptied, or a producer adds to a closed queue
// for a worker it is the normal signal that there is no more work, so workers
// catch it and stop
public class QueueClosedException extends Exception {
    public QueueClosedException(String message) {
        super(message);
    }
}
