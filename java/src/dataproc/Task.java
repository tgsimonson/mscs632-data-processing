package dataproc;

// one unit of work from the input file: an id and the raw value text
// the value stays a string so that validating it is part of processing, where a
// bad value becomes a logged error instead of a crash while loading
public record Task(int id, String value) {
}
