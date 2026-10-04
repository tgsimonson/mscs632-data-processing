package dataproc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

// logging setup on java.util.logging: every line carries the time, the thread
// name and the level, and goes to the console and to logs/java-run.log
public final class Log {

    public static final Logger LOGGER = Logger.getLogger("dataproc");
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Log() {
    }

    // one line per record: 14:02:31.118 [worker-2] INFO  completed task 7
    public static final class LineFormatter extends Formatter {
        @Override
        public String format(LogRecord r) {
            String level = r.getLevel() == Level.SEVERE ? "ERROR"
                    : r.getLevel() == Level.WARNING ? "WARN" : r.getLevel().getName();
            String thread = Thread.currentThread().getName();
            return String.format("%s [%s] %-5s %s%n",
                    LocalTime.now().format(CLOCK), thread, level, r.getMessage());
        }
    }

    // replaces the default handlers with the console and the log file; a log
    // file that cannot be opened is reported and the run continues on the
    // console alone
    public static void configure(Path logFile, boolean console) {
        LOGGER.setUseParentHandlers(false);
        for (Handler h : LOGGER.getHandlers()) {
            LOGGER.removeHandler(h);
            h.close();
        }
        LineFormatter formatter = new LineFormatter();
        if (console) {
            ConsoleHandler ch = new ConsoleHandler();
            ch.setFormatter(formatter);
            LOGGER.addHandler(ch);
        }
        try {
            Files.createDirectories(logFile.toAbsolutePath().getParent());
            FileHandler fh = new FileHandler(logFile.toString(), false);
            fh.setFormatter(formatter);
            LOGGER.addHandler(fh);
        } catch (IOException e) {
            if (!console) {
                ConsoleHandler ch = new ConsoleHandler();
                ch.setFormatter(formatter);
                LOGGER.addHandler(ch);
            }
            LOGGER.warning("cannot open log file " + logFile + ": " + e.getMessage());
        }
    }

    public static void info(String message) {
        LOGGER.info(message);
    }

    public static void warn(String message) {
        LOGGER.warning(message);
    }

    public static void error(String message) {
        LOGGER.severe(message);
    }
}
