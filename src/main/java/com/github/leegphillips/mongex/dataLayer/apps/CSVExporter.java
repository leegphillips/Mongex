package com.github.leegphillips.mongex.dataLayer.apps;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.TimeFrame;
import com.github.leegphillips.mongex.dataLayer.processors.*;
import com.github.leegphillips.mongex.dataLayer.utils.PropertiesSingleton;
import com.github.leegphillips.mongex.dataLayer.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.github.leegphillips.mongex.dataLayer.utils.Constants.QUEUE_SIZE;
import static com.github.leegphillips.mongex.dataLayer.utils.PropertiesSingleton.CSV_LOCATION;
import static java.util.stream.Collectors.toMap;

public class CSVExporter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CSVExporter.class);

    private static final ExecutorService SERVICE = Executors.newCachedThreadPool();
    private static final ScheduledExecutorService TIMED = Executors.newSingleThreadScheduledExecutor();

    private static final File[] FILES = Utils.getFiles();
    private static final CurrencyPair[] PAIRS = Arrays.stream(FILES).map(CurrencyPair::get)
            .distinct()
            .toArray(CurrencyPair[]::new);

    private final long start = System.currentTimeMillis();

    private final File train;
    private final File eval;

    private final CurrencyPair pair;
    private final TimeFrame tf;
    private final String pairMatch;

    public CSVExporter(CurrencyPair pair, TimeFrame tf, String pairMatch) {
        this.pair = pair;
        this.tf = tf;
        this.pairMatch = pairMatch;
        this.train = new File(PropertiesSingleton.getInstance().getProperty(CSV_LOCATION) + pair.getLabel() + "-" + tf.getLabel() + (pairMatch == null ? "" : "-{" + pairMatch + "}") + "-train.csv");
        this.eval = new File(PropertiesSingleton.getInstance().getProperty(CSV_LOCATION) + pair.getLabel() + "-" + tf.getLabel() + (pairMatch == null ? "" : "-{" + pairMatch + "}") + "-eval.csv");
    }

    public static void main(String[] args) {
        CurrencyPair pair = CurrencyPair.get(args[0]);
        TimeFrame tf = TimeFrame.get(args[1]);
        new CSVExporter(pair, tf, args.length > 2 ? args[2] : null).run();
    }

    @Override
    public void run() {
        train.delete();
        eval.delete();

        LocalDate base = Utils.file2Date(Arrays.stream(FILES)
                .filter(file -> file.getName().contains(pair.getLabel()))
                .sorted(Comparator.comparing(File::getName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No file found for " + pair.getLabel())));

        Map<CurrencyPair, TickReader> readers = Arrays.stream(PAIRS)
                .filter(pair -> pairMatch == null ? true : pair.getLabel().contains(pairMatch))
                .collect(toMap(Function.identity(), pair -> new TickReader(pair, base)));
        readers.values().forEach(SERVICE::execute);

        Map<CurrencyPair, TickTimeFrameFilter> filters = readers.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> new TickTimeFrameFilter(tf, entry.getValue())));
        filters.values().forEach(SERVICE::execute);

        Map<CurrencyPair, TickPadder> padders = filters.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> new TickPadder(tf, entry.getValue())));
        padders.values().forEach(SERVICE::execute);

        Map<CurrencyPair, StateTracker> trackers = padders.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> new StateTracker(entry.getValue())));
        trackers.values().forEach(SERVICE::execute);

        StateAggregator aggregator = new StateAggregator(trackers.entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
        SERVICE.execute(aggregator);

        FullStateTracker fullStateTracker = new FullStateTracker(pairMatch, aggregator);
        SERVICE.execute(fullStateTracker);

        Classifier classifier = new Classifier(pair, fullStateTracker);
        SERVICE.execute(classifier);

        CSVWriter writer = new CSVWriter(train, eval, classifier);
        SERVICE.execute(writer);

        TIMED.scheduleAtFixedRate(new Monitor(readers, filters, padders, trackers, aggregator, fullStateTracker, classifier, writer), 5, 5, TimeUnit.SECONDS);
    }

    private class Monitor implements Runnable {

        private final Map<CurrencyPair, TickReader> readers;
        private final Map<CurrencyPair, TickTimeFrameFilter> filters;
        private final Map<CurrencyPair, StateTracker> trackers;
        private final Map<CurrencyPair, TickPadder> padders;
        private final StateAggregator aggregator;
        private final FullStateTracker fullStateTracker;
        private final Classifier classifier;
        private final CSVWriter writer;

        public Monitor(Map<CurrencyPair, TickReader> readers, Map<CurrencyPair, TickTimeFrameFilter> filters, Map<CurrencyPair, TickPadder> padders, Map<CurrencyPair, StateTracker> trackers, StateAggregator aggregator, FullStateTracker fullStateTracker, Classifier classifier, CSVWriter writer) {
            this.readers = readers;
            this.filters = filters;
            this.padders = padders;
            this.trackers = trackers;
            this.aggregator = aggregator;
            this.fullStateTracker = fullStateTracker;
            this.classifier = classifier;
            this.writer = writer;
        }

        @Override
        public void run() {
            long duration = (System.currentTimeMillis() - start) / 1000;
            long lines = writer.getLines();
            long rate = lines / duration;
            int filesRemaining = readers.values().stream().mapToInt(TickReader::getFilesRemaining).sum();
            LOG.info("-----------------------------------------------------------------");
            LOG.info("Duration: " + duration + "s");
            LOG.info("Lines: " + lines);
            LOG.info("Rate: " + rate + " lines/s");
            LOG.info("Remaining files: " + filesRemaining);
            LOG.info("File size: " + writer.getFileSize() / 1024 + "kb");
            LOG.info("Last: " + writer.getLast());
            LOG.info("Filtered: " + filters.values().stream().mapToLong(TickTimeFrameFilter::getFiltered).sum());
            LOG.info("Padded: " + padders.values().stream().mapToInt(TickPadder::getPadding).sum());
            LOG.info("Queues:"
                    + " Readers: " + readers.values().stream().mapToInt(reader -> QUEUE_SIZE - reader.remainingCapacity()).sum()
                    + " Filters: " + filters.values().stream().mapToInt(filter -> QUEUE_SIZE - filter.remainingCapacity()).sum()
                    + " Padders: " + padders.values().stream().mapToInt(padder -> QUEUE_SIZE - padder.remainingCapacity()).sum()
                    + " Trackers: " + trackers.values().stream().mapToInt(tracker -> QUEUE_SIZE - tracker.remainingCapacity()).sum()
                    + " Aggregator: " + (QUEUE_SIZE - aggregator.remainingCapacity())
                    + " Full state: " + (QUEUE_SIZE - fullStateTracker.remainingCapacity())
                    + " Classifier: " + (QUEUE_SIZE - classifier.remainingCapacity()));

            if (filesRemaining == 0) {
                SERVICE.shutdown();
                TIMED.shutdown();
                System.exit(0);
            }
        }
    }
}
