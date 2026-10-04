package dataproc;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

// entry point. with no arguments it opens the window; "run" processes the
// task file on the console:
//   run [--workers N] [--capacity N] [--scale X] [--tasks FILE] [--output FILE]
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 0 || args[0].equals("gui")) {
            dataproc.gui.App.launch();
            return;
        }
        if (!args[0].equals("run")) {
            System.err.println("usage: dataproc [gui] | run [--workers N] [--capacity N] [--scale X]"
                    + " [--tasks FILE] [--output FILE]");
            System.exit(1);
        }
        System.exit(runConsole(args));
    }

    static int runConsole(String[] args) {
        int workers = 4;
        int capacity = 8;
        double scale = 1.0;
        Path tasks = Path.of("../data/tasks.txt");
        Path output = Path.of("results/java-results.txt");
        try {
            for (int i = 1; i < args.length; i++) {
                String value = i + 1 < args.length ? args[i + 1] : null;
                switch (args[i]) {
                    case "--workers" -> workers = Integer.parseInt(require(value, args[i]));
                    case "--capacity" -> capacity = Integer.parseInt(require(value, args[i]));
                    case "--scale" -> scale = Double.parseDouble(require(value, args[i]));
                    case "--tasks" -> tasks = Path.of(require(value, args[i]));
                    case "--output" -> output = Path.of(require(value, args[i]));
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
                i++;
            }
        } catch (IllegalArgumentException e) {
            // NumberFormatException is a subclass, so a bad number lands here too
            System.err.println("error: " + e.getMessage());
            return 1;
        }

        Log.configure(Path.of("logs/java-run.log"), true);
        try {
            Engine.Config config = new Engine.Config(tasks, output, workers, capacity, scale);
            Summary summary = new Engine().run(config, EngineListener.NONE);
            System.out.println();
            System.out.println(summary.report());
            return summary.complete() ? 0 : 2;
        } catch (NoSuchFileException e) {
            Log.error("task file not found: " + e.getFile());
            return 3;
        } catch (IOException e) {
            Log.error("file error: " + e.getMessage());
            return 3;
        } catch (IllegalArgumentException e) {
            Log.error(e.getMessage());
            return 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Log.error("interrupted before the run finished");
            return 4;
        }
    }

    private static String require(String value, String option) {
        if (value == null) {
            throw new IllegalArgumentException(option + " needs a value");
        }
        return value;
    }
}
