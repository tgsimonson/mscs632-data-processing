package dataproc;

// a task whose data cannot be processed, such as a value that is not a positive
// number
public class ProcessingException extends Exception {
    public ProcessingException(String message) {
        super(message);
    }
}
