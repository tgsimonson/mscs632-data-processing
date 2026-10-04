package dataproc;

// the work each task represents: factor the value into primes and add up its
// digits
// a sleep before the work stands in for a slow computation, so that several
// workers are busy at the same time
public final class Processor {

    static final long MAX_VALUE = 1_000_000_000_000L;

    private Processor() {
    }

    // milliseconds of simulated work for a task, before scaling
    static long baseDelay(Task task) {
        try {
            long v = Long.parseLong(task.value().trim());
            return 20 + Math.floorMod(v, 50);
        } catch (NumberFormatException e) {
            return 20;
        }
    }

    // processes one task
    // throws ProcessingException for a value that is not a whole number from 1
    // to one trillion, and InterruptedException if the worker is interrupted
    // during the simulated delay
    public static Result process(Task task, String worker, double delayScale)
            throws ProcessingException, InterruptedException {
        long started = System.nanoTime();
        Thread.sleep(Math.round(baseDelay(task) * delayScale));

        long value;
        try {
            value = Long.parseLong(task.value().trim());
        } catch (NumberFormatException e) {
            throw new ProcessingException("value is not a whole number");
        }
        if (value < 1 || value > MAX_VALUE) {
            throw new ProcessingException("value must be from 1 to 1000000000000");
        }

        long millis = (System.nanoTime() - started) / 1_000_000;
        return Result.success(task, worker, factor(value), digitSum(value), millis);
    }

    // prime factors written as 2^3*3^2*5, or 1 for the value 1
    static String factor(long value) {
        if (value == 1) {
            return "1";
        }
        StringBuilder out = new StringBuilder();
        long n = value;
        for (long p = 2; p * p <= n; p++) {
            int power = 0;
            while (n % p == 0) {
                n /= p;
                power++;
            }
            if (power > 0) {
                append(out, p, power);
            }
        }
        if (n > 1) {
            append(out, n, 1);
        }
        return out.toString();
    }

    private static void append(StringBuilder out, long prime, int power) {
        if (out.length() > 0) {
            out.append('*');
        }
        out.append(prime);
        if (power > 1) {
            out.append('^').append(power);
        }
    }

    static int digitSum(long value) {
        int sum = 0;
        for (long n = value; n > 0; n /= 10) {
            sum += (int) (n % 10);
        }
        return sum;
    }
}
