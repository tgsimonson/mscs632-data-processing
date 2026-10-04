package dataproc.gui;

import dataproc.Engine;
import dataproc.EngineListener;
import dataproc.Log;
import dataproc.Result;
import dataproc.Summary;
import dataproc.Task;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.awt.GridLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;

// the swing window: run controls, one lane per worker thread, the queue depth,
// a results table and the live log. the engine runs on a swingworker thread;
// every engine callback is handed to the event dispatch thread before it
// touches a component
public final class App extends JFrame {

    // graphite background, lime accent for success, blue for work in progress
    static final Color BG = new Color(0x11, 0x14, 0x18);
    static final Color PANEL = new Color(0x1A, 0x1F, 0x26);
    static final Color LINE = new Color(0x2A, 0x31, 0x3B);
    static final Color TEXT = new Color(0xE6, 0xEA, 0xF0);
    static final Color MUTED = new Color(0x8A, 0x94, 0xA3);
    static final Color OK = new Color(0x9B, 0xE1, 0x5D);
    static final Color BUSY = new Color(0x6C, 0xB6, 0xFF);
    static final Color ERROR = new Color(0xFF, 0x7A, 0x7A);
    static final Font BODY = new Font(Font.SANS_SERIF, Font.PLAIN, 13);
    static final Font BOLD = new Font(Font.SANS_SERIF, Font.BOLD, 13);
    static final Font MONO = new Font(Font.MONOSPACED, Font.PLAIN, 12);

    private final JSpinner workers = new JSpinner(new SpinnerNumberModel(4, 1, 16, 1));
    private final JSpinner capacity = new JSpinner(new SpinnerNumberModel(6, 1, 32, 1));
    private final JSpinner scale = new JSpinner(new SpinnerNumberModel(6.0, 0.0, 20.0, 1.0));
    private final JButton start = new JButton("Start");
    private final JButton stop = new JButton("Stop");
    private final JLabel tasksLabel = muted("");
    private final JPanel lanes = new JPanel();
    private final Map<String, Lane> laneByWorker = new LinkedHashMap<>();
    private final JProgressBar queueBar = new JProgressBar();
    private final JProgressBar doneBar = new JProgressBar();
    private final DefaultTableModel results = new DefaultTableModel(
            new String[] {"ID", "Value", "Status", "Factors or error", "Worker", "ms"}, 0) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };
    private final JTextArea log = new JTextArea();
    private final JLabel summary = muted("Press Start to process the task file.");

    private Path tasksFile = Path.of("../data/tasks.txt");
    private Engine engine;
    private int total;

    // one row per worker thread: its name, what it is doing, and its task count
    private static final class Lane extends JPanel {
        final JLabel state = new JLabel("idle");
        final JLabel count = new JLabel("0 tasks");
        int done;

        Lane(String name) {
            super(new BorderLayout(12, 0));
            setBackground(BG);
            setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createMatteBorder(0, 3, 0, 0, MUTED),
                    BorderFactory.createEmptyBorder(6, 10, 6, 10)));
            JLabel label = new JLabel(name);
            label.setFont(BOLD);
            label.setForeground(TEXT);
            label.setPreferredSize(new Dimension(80, 20));
            state.setFont(MONO);
            state.setForeground(MUTED);
            count.setFont(BODY);
            count.setForeground(MUTED);
            add(label, BorderLayout.WEST);
            add(state, BorderLayout.CENTER);
            add(count, BorderLayout.EAST);
            setMaximumSize(new Dimension(Integer.MAX_VALUE, 34));
        }

        void accent(Color c) {
            setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createMatteBorder(0, 3, 0, 0, c),
                    BorderFactory.createEmptyBorder(6, 10, 6, 10)));
        }
    }

    public static void launch() {
        if (GraphicsEnvironment.isHeadless()) {
            System.err.println("no display available; use: dataproc run");
            System.exit(1);
        }
        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
            } catch (Exception ignored) {
                // the default look is fine if this fails
            }
            new App().setVisible(true);
        });
    }

    private App() {
        super("Data Processing System (Java)");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setSize(1180, 780);
        setMinimumSize(new Dimension(980, 640));
        setLocationRelativeTo(null);

        JPanel root = new JPanel(new BorderLayout(0, 12));
        root.setBackground(BG);
        root.setBorder(BorderFactory.createEmptyBorder(16, 18, 12, 18));
        root.add(header(), BorderLayout.NORTH);
        root.add(body(), BorderLayout.CENTER);
        summary.setFont(BOLD);
        root.add(summary, BorderLayout.SOUTH);
        setContentPane(root);

        // log lines from every thread also land in the window's log pane
        Log.configure(Path.of("logs/java-run.log"), false);
        Log.LOGGER.addHandler(new Handler() {
            private final java.util.logging.Formatter f = new Log.LineFormatter();

            @Override
            public void publish(LogRecord r) {
                String line = f.format(r);
                SwingUtilities.invokeLater(() -> {
                    log.append(line);
                    log.setCaretPosition(log.getDocument().getLength());
                });
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        });

        start.addActionListener(e -> startRun());
        stop.addActionListener(e -> {
            if (engine != null) {
                Log.warn("stop requested from the window");
                engine.cancel();
            }
        });
        stop.setEnabled(false);
        updateTasksLabel();
        workers.addChangeListener(e -> {
            if (start.isEnabled()) {
                showIdleLanes((Integer) workers.getValue());
            }
        });
        showIdleLanes((Integer) workers.getValue());
        queueBar.setString("0 / " + capacity.getValue());
        doneBar.setString("0 / 0");
    }

    // one idle lane per worker that the next run will start
    private void showIdleLanes(int n) {
        lanes.removeAll();
        laneByWorker.clear();
        for (int i = 1; i <= n; i++) {
            Lane lane = new Lane("worker-" + i);
            laneByWorker.put("worker-" + i, lane);
            lanes.add(lane);
        }
        lanes.revalidate();
        lanes.repaint();
    }

    private static JLabel muted(String text) {
        JLabel l = new JLabel(text);
        l.setFont(BODY);
        l.setForeground(MUTED);
        return l;
    }

    private static JPanel card(String title) {
        JPanel p = new JPanel(new BorderLayout(0, 8));
        p.setBackground(PANEL);
        p.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(LINE), BorderFactory.createEmptyBorder(10, 12, 12, 12)));
        JLabel t = new JLabel(title);
        t.setFont(BOLD);
        t.setForeground(TEXT);
        p.add(t, BorderLayout.NORTH);
        return p;
    }

    private JPanel header() {
        JPanel h = new JPanel(new BorderLayout());
        h.setOpaque(false);
        JPanel titles = new JPanel(new GridLayout(2, 1));
        titles.setOpaque(false);
        JLabel title = new JLabel("Data Processing System");
        title.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 22));
        title.setForeground(TEXT);
        titles.add(title);
        titles.add(muted("Java · ExecutorService worker pool · ReentrantLock queue · synchronized result store"));
        h.add(titles, BorderLayout.WEST);

        JPanel controls = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 4));
        controls.setOpaque(false);
        controls.add(muted("Workers"));
        controls.add(workers);
        controls.add(muted("Queue capacity"));
        controls.add(capacity);
        controls.add(muted("Delay scale"));
        controls.add(scale);
        style(start, OK, BG);
        style(stop, ERROR, BG);
        controls.add(start);
        controls.add(stop);
        h.add(controls, BorderLayout.EAST);

        JPanel fileRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
        fileRow.setOpaque(false);
        fileRow.add(muted("Task file:"));
        fileRow.add(tasksLabel);
        JButton choose = new JButton("Choose...");
        style(choose, LINE, TEXT);
        choose.addActionListener(e -> {
            JFileChooser fc = new JFileChooser(tasksFile.toAbsolutePath().getParent().toFile());
            if (fc.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
                tasksFile = fc.getSelectedFile().toPath();
                updateTasksLabel();
            }
        });
        fileRow.add(choose);
        h.add(fileRow, BorderLayout.SOUTH);
        return h;
    }

    private static void style(JButton b, Color bg, Color fg) {
        b.setBackground(bg);
        b.setForeground(fg);
        b.setFont(BOLD);
        b.setOpaque(true);
        b.setBorderPainted(false);
        b.setFocusPainted(false);
    }

    private void updateTasksLabel() {
        tasksLabel.setText(tasksFile.toAbsolutePath().normalize().toString());
    }

    private JSplitPane body() {
        JPanel left = new JPanel(new BorderLayout(0, 12));
        left.setOpaque(false);

        JPanel workerCard = card("Worker threads");
        lanes.setLayout(new BoxLayout(lanes, BoxLayout.Y_AXIS));
        lanes.setBackground(PANEL);
        JScrollPane laneScroll = new JScrollPane(lanes);
        laneScroll.setBorder(null);
        laneScroll.getViewport().setBackground(PANEL);
        workerCard.add(laneScroll, BorderLayout.CENTER);

        JPanel meters = new JPanel(new GridLayout(4, 1, 0, 4));
        meters.setOpaque(false);
        meters.add(muted("Shared queue depth"));
        bar(queueBar, BUSY);
        meters.add(queueBar);
        meters.add(muted("Tasks finished"));
        bar(doneBar, OK);
        meters.add(doneBar);
        workerCard.add(meters, BorderLayout.SOUTH);
        left.add(workerCard, BorderLayout.CENTER);

        JPanel logCard = card("Log");
        log.setEditable(false);
        log.setLineWrap(true);
        log.setWrapStyleWord(true);
        log.setFont(MONO);
        log.setBackground(BG);
        log.setForeground(TEXT);
        log.setBorder(BorderFactory.createEmptyBorder(6, 8, 6, 8));
        JScrollPane logScroll = new JScrollPane(log);
        logScroll.setBorder(BorderFactory.createLineBorder(LINE));
        logScroll.setPreferredSize(new Dimension(400, 220));
        logCard.add(logScroll, BorderLayout.CENTER);
        left.add(logCard, BorderLayout.SOUTH);

        JPanel resultCard = card("Results");
        JTable table = new JTable(results);
        table.setFont(MONO);
        table.setRowHeight(22);
        table.setBackground(BG);
        table.setForeground(TEXT);
        table.setGridColor(LINE);
        table.getTableHeader().setFont(BOLD);
        table.getTableHeader().setBackground(PANEL);
        table.getTableHeader().setForeground(MUTED);
        table.setDefaultRenderer(Object.class, new DefaultTableCellRenderer() {
            @Override
            public Component getTableCellRendererComponent(JTable t, Object v, boolean s, boolean f, int row, int col) {
                Component c = super.getTableCellRendererComponent(t, v, s, f, row, col);
                c.setBackground(s ? LINE : BG);
                boolean error = "ERROR".equals(t.getValueAt(row, 2));
                c.setForeground(col == 2 ? (error ? App.ERROR : OK) : error && col == 3 ? App.ERROR : TEXT);
                return c;
            }
        });
        int[] widths = {50, 120, 70, 300, 80, 50};
        for (int i = 0; i < widths.length; i++) {
            table.getColumnModel().getColumn(i).setPreferredWidth(widths[i]);
        }
        JScrollPane tableScroll = new JScrollPane(table);
        tableScroll.setBorder(BorderFactory.createLineBorder(LINE));
        tableScroll.getViewport().setBackground(BG);
        resultCard.add(tableScroll, BorderLayout.CENTER);

        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, left, resultCard);
        split.setResizeWeight(0.48);
        split.setBorder(null);
        split.setDividerSize(10);
        split.setBackground(BG);
        // a plain divider instead of the metal look's dotted one
        javax.swing.plaf.basic.BasicSplitPaneUI ui = new javax.swing.plaf.basic.BasicSplitPaneUI();
        split.setUI(ui);
        split.setBorder(null);
        ui.getDivider().setBorder(null);
        ui.getDivider().setBackground(BG);
        return split;
    }

    private static void bar(JProgressBar b, Color c) {
        b.setForeground(c);
        b.setBackground(BG);
        b.setBorderPainted(false);
        b.setStringPainted(true);
        b.setFont(BODY);
        b.setPreferredSize(new Dimension(100, 18));
    }

    // ------------------------------------------------------------- running

    private void startRun() {
        int n = (Integer) workers.getValue();
        int cap = (Integer) capacity.getValue();
        double s = (Double) scale.getValue();

        results.setRowCount(0);
        log.setText("");
        showIdleLanes(n);
        queueBar.setMaximum(cap);
        queueBar.setValue(0);
        queueBar.setString("0 / " + cap);
        try {
            total = (int) Files.readAllLines(tasksFile).stream()
                    .map(String::trim).filter(l -> !l.isEmpty() && !l.startsWith("#")).count();
        } catch (Exception e) {
            total = 0;
        }
        doneBar.setMaximum(Math.max(total, 1));
        doneBar.setValue(0);
        doneBar.setString("0 / " + total);
        summary.setForeground(MUTED);
        summary.setText("Running " + n + " workers...");
        start.setEnabled(false);
        stop.setEnabled(true);

        engine = new Engine();
        Engine.Config config = new Engine.Config(tasksFile, Path.of("results/java-results.txt"), n, cap, s);
        EngineListener listener = new GuiListener();

        new SwingWorker<Summary, Void>() {
            @Override
            protected Summary doInBackground() throws Exception {
                // this thread runs the producer loop, so name it for the log
                Thread.currentThread().setName("producer");
                return engine.run(config, listener);
            }

            @Override
            protected void done() {
                start.setEnabled(true);
                stop.setEnabled(false);
                try {
                    Summary sum = get();
                    summary.setForeground(sum.complete() ? OK : ERROR);
                    summary.setText(String.format(
                            "%s  ·  %d processed (%d ok, %d failed)  ·  missed: %s  ·  repeated: %s  ·  %d ms",
                            sum.cancelled() ? "Cancelled" : sum.complete() ? "Complete" : "Incomplete",
                            sum.processed(), sum.ok(), sum.failed(),
                            sum.missing().isEmpty() ? "none" : sum.missing().size(),
                            sum.duplicates().isEmpty() ? "none" : sum.duplicates().size(),
                            sum.elapsedMillis()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    String message = cause instanceof java.nio.file.NoSuchFileException nf
                            ? "task file not found: " + nf.getFile() : String.valueOf(cause);
                    Log.error(message);
                    summary.setForeground(ERROR);
                    summary.setText("Error: " + message);
                }
            }
        }.execute();
    }

    // forwards engine events to the event dispatch thread
    private final class GuiListener implements EngineListener {
        @Override
        public void workerStarted(String worker) {
            SwingUtilities.invokeLater(() -> lane(worker).state.setText("waiting for a task"));
        }

        @Override
        public void taskStarted(String worker, Task task) {
            SwingUtilities.invokeLater(() -> {
                Lane l = lane(worker);
                l.state.setText("processing task " + task.id() + " (" + task.value() + ")");
                l.state.setForeground(BUSY);
                l.accent(BUSY);
            });
        }

        @Override
        public void taskFinished(String worker, Result r) {
            SwingUtilities.invokeLater(() -> {
                Lane l = lane(worker);
                l.done++;
                l.count.setText(l.done + (l.done == 1 ? " task" : " tasks"));
                l.state.setText("waiting for a task");
                l.state.setForeground(MUTED);
                l.accent(r.ok() ? OK : ERROR);
                results.addRow(new Object[] {r.task().id(), r.task().value(), r.ok() ? "OK" : "ERROR",
                        r.ok() ? r.factors() : r.message(), worker, r.millis()});
                doneBar.setValue(results.getRowCount());
                doneBar.setString(results.getRowCount() + " / " + total);
            });
        }

        @Override
        public void workerStopped(String worker, int processed) {
            SwingUtilities.invokeLater(() -> {
                Lane l = lane(worker);
                l.state.setText("finished");
                l.state.setForeground(MUTED);
                l.accent(MUTED);
            });
        }

        @Override
        public void queueDepth(int depth, int cap) {
            SwingUtilities.invokeLater(() -> {
                queueBar.setValue(depth);
                queueBar.setString(depth + " / " + cap);
            });
        }

        private Lane lane(String worker) {
            return laneByWorker.computeIfAbsent(worker, w -> {
                Lane l = new Lane(w);
                lanes.add(l);
                lanes.revalidate();
                return l;
            });
        }
    }
}
