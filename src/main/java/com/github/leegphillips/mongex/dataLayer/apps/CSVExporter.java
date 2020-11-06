package com.github.leegphillips.mongex.dataLayer.apps;

import com.github.leegphillips.mongex.dataLayer.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.TimeFrame;
import com.github.leegphillips.mongex.dataLayer.Utils;
import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.processors.*;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class CSVExporter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CSVExporter.class);

    private static final int QUEUE_SIZE = 4096;
    private static final Classification CLOSE = new Classification(false, null);

    private static final ExecutorService SERVICE = Executors.newCachedThreadPool();
    private static final ScheduledExecutorService TIMED = Executors.newSingleThreadScheduledExecutor();

    private static final File[] FILES = Utils.getFiles();
    private static final CurrencyPair[] PAIRS = Arrays.stream(FILES).map(CurrencyPair::get)
            .distinct()
            .toArray(CurrencyPair[]::new);

    private final long start = System.currentTimeMillis();

    private final CurrencyPair pair;
    private final TimeFrame tf;

    public static void main(String[] args) {
        CurrencyPair pair = CurrencyPair.get(args[0]);
        TimeFrame tf = TimeFrame.get(args[1]);
        new CSVExporter(pair, tf).run();
    }

    public CSVExporter(CurrencyPair pair, TimeFrame tf) {
        this.pair = pair;
        this.tf = tf;
    }

    @Override
    public void run() {
        Map<CurrencyPair, TickReader> readers = Arrays.stream(PAIRS)
                .collect(toMap(Function.identity(), TickReader::new));
        readers.values().forEach(SERVICE::execute);

        Map<CurrencyPair, TickTimeFrameFilter> filters = readers.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> new TickTimeFrameFilter(tf, entry.getValue())));
        filters.values().forEach(SERVICE::execute);

        Map<CurrencyPair, TickPadder> padders = filters.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> new TickPadder(tf, entry.getValue())));
        padders.values().forEach(SERVICE::execute);

        Map<CurrencyPair, TickMATracker> trackers = padders.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> new TickMATracker(entry.getValue())));
        trackers.values().forEach(SERVICE::execute);

        StateAggregator aggregator = new StateAggregator(trackers.entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
        SERVICE.execute(aggregator);

        FullStateTracker fullStateTracker = new FullStateTracker(aggregator);
        SERVICE.execute(fullStateTracker);

        Classifier classifier = new Classifier(fullStateTracker);
        SERVICE.execute(classifier);

        Writer writer = new Writer(classifier);
        SERVICE.execute(writer);

        TIMED.scheduleAtFixedRate(new Monitor(writer), 5, 5, TimeUnit.SECONDS);
    }

    private class Monitor implements Runnable {

        private final Writer writer;

        public Monitor(Writer writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            long duration = (System.currentTimeMillis() - start) / 1000;
            LOG.info("-----------------------------------------------------------------");
            LOG.info("Duration: " + duration + "s");
            LOG.info("Lines: " + writer.getLines());
        }
    }

    private class Classifier extends ArrayBlockingQueue<Classification> implements Runnable {

        private final BlockingQueue<Map<CurrencyPair, State>> input;

        public Classifier(BlockingQueue<Map<CurrencyPair, State>> input) {
            super(QUEUE_SIZE);
            this.input = input;
        }

        @Override
        public void run() {
            try {
                Map<CurrencyPair, State> current = input.take();
                Map<CurrencyPair, State> next = input.take();
                while (next != CLOSE) {
                    BigDecimal currentValue = current.get(pair).getValues().get(1);
                    BigDecimal nextValue = next.get(pair).getValues().get(1);

                    // TODO only publish if there is a change
                    // TODO only publish if current has a non-zero value
                    put(new Classification(currentValue.compareTo(nextValue) < 0, new ArrayList<>(current.values())));

                    current = next;
                    next = input.take();
                }
                put(CLOSE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @ToString
    private static class Classification {
        private final boolean up;
        private final List<State> states;

        public Classification(boolean up, List<State> states) {
            this.up = up;
            this.states = states;
        }

        public String toCSV() {
            StringBuilder result = new StringBuilder();
            LocalDateTime timestamp = states.stream()
                    .map(State::getTimestamp)
                    .filter(ts -> !ts.isEqual(LocalDateTime.MIN))
                    .findFirst().orElseThrow(IllegalStateException::new);
            result.append(timestamp);
            result.append(", ");
            result.append(up);
            result.append(", ");
            for (State state : states) {
                // TODO check ordering of values
                for (BigDecimal value : state.getValues().values()) {
                    result.append(value.toPlainString());
                    result.append(", ");
                }
            }
            return result.toString();
        }
    }

    private class Writer implements Runnable {

        private final BlockingQueue<Classification> input;
        private final AtomicLong counter = new AtomicLong(0);

        public Writer(BlockingQueue<Classification> input) {
            this.input = input;
        }

        @Override
        public void run() {
            File output = new File("output.csv");
            output.delete();
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter(output, true));
                Classification classification = input.take();
                while (classification != CLOSE) {
                    bw.write(classification.toCSV());
                    bw.newLine();
                    counter.incrementAndGet();
                    classification = input.take();
                }
                bw.close();
                SERVICE.shutdown();
                TIMED.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public long getLines() {
            return counter.get();
        }
    }
}
