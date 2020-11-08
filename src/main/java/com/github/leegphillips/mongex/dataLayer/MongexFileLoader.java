package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.processors.StateTracker;
import com.github.leegphillips.mongex.dataLayer.processors.TickPadder;
import com.github.leegphillips.mongex.dataLayer.processors.TickReader;
import com.github.leegphillips.mongex.dataLayer.processors.TickTimeFrameFilter;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.leegphillips.mongex.dataLayer.dao.State.END;
import static java.util.stream.Collectors.toList;

public class MongexFileLoader implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MongexFileLoader.class);

    private static final int QUEUE_SIZE = 4096;
    private static final int MONGO_BATCH_SIZE = 4096;

    private static final File[] FILES = Utils.getFiles();
    private static final CurrencyPair[] PAIRS = Arrays.stream(FILES).map(CurrencyPair::get)
            .distinct()
            .toArray(CurrencyPair[]::new);

    private static final ExecutorService SERVICE = Executors.newCachedThreadPool();
    private static final ScheduledExecutorService TIMED = Executors.newSingleThreadScheduledExecutor();

    private final long start = System.currentTimeMillis();
    private final AtomicInteger filesCompleted = new AtomicInteger(0);
    private final AtomicInteger running = new AtomicInteger();

    private final TimeFrame tf;

    public MongexFileLoader(TimeFrame tf) {
        this.tf = tf;
    }

    public static void main(String[] args) {
        TimeFrame tf = TimeFrame.get(args[0]);
        new MongexFileLoader(tf).run();
    }

    @Override
    public void run() {
        List<TickReader> readers = Arrays.stream(PAIRS)
                .map(TickReader::new)
                .collect(toList());
        readers.forEach(SERVICE::execute);
        running.set(readers.size());

        List<TickTimeFrameFilter> filters = readers.stream()
                .map(reader -> new TickTimeFrameFilter(tf, reader))
                .collect(toList());
        filters.forEach(SERVICE::execute);

        List<TickPadder> padders = filters.stream()
                .map(filter -> new TickPadder(tf, filter))
                .collect(toList());
        padders.forEach(SERVICE::execute);

        List<StateTracker> trackers = padders.stream()
                .map(StateTracker::new)
                .collect(toList());
        trackers.forEach(SERVICE::execute);

        List<MongoWriter> writers = trackers.stream()
                .map(MongoWriter::new)
                .collect(toList());
        writers.forEach(SERVICE::execute);

        TIMED.scheduleAtFixedRate(new Monitor(writers, trackers, padders, filters, readers), 50, 5, TimeUnit.SECONDS);
    }

    private class Monitor implements Runnable {

        private final List<MongoWriter> writers;
        private final List<StateTracker> trackers;
        private final List<TickPadder> padders;
        private final List<TickTimeFrameFilter> filters;
        private final List<TickReader> readers;

        private Monitor(List<MongoWriter> writers, List<StateTracker> trackers, List<TickPadder> padders, List<TickTimeFrameFilter> filters, List<TickReader> readers) {
            this.writers = writers;
            this.trackers = trackers;
            this.padders = padders;
            this.filters = filters;
            this.readers = readers;
        }

        @Override
        public void run() {
            long duration = (System.currentTimeMillis() - start) / 1000;
            int recordCount = writers.stream().mapToInt(MongoWriter::getRecordsCount).sum();
            long rate = recordCount / duration;
            LOG.info("-----------------------------------------------------------------");
            LOG.info("Duration: " + duration + "s");
            LOG.info("Records: " + recordCount);
            LOG.info("Rate: " + rate + " record/s");
            LOG.info("Files: " + filesCompleted.get());
            LOG.info("Running: " + running.get());
            LOG.info("Filtered: " + filters.stream().mapToLong(TickTimeFrameFilter::getFiltered).sum());
            LOG.info("Padded: " + padders.stream().mapToInt(TickPadder::getPadding).sum());
            LOG.info("Queues:"
                    + " Filters: " + readers.stream().mapToInt(reader -> QUEUE_SIZE - reader.remainingCapacity()).sum()
                    + " Padders: " + filters.stream().mapToInt(filter -> QUEUE_SIZE - filter.remainingCapacity()).sum()
                    + " Trackers: " + padders.stream().mapToInt(padder -> QUEUE_SIZE - padder.remainingCapacity()).sum()
                    + " Writers: " + trackers.stream().mapToInt(tracker -> QUEUE_SIZE - tracker.remainingCapacity()).sum());
        }
    }

    private class MongoWriter implements Runnable {

        private final BlockingQueue<State> input;
        private final AtomicInteger recordsCount = new AtomicInteger(0);

        public MongoWriter(BlockingQueue<State> input) {
            this.input = input;
        }

        @Override
        public void run() {
            try {
                List<Document> updates = new ArrayList<>();
                State tick = input.take();
                MongoCollection<Document> stream = DatabaseFactory.getStream(tick.getPair(), tf);
                while (tick != END) {
                    updates.add(tick.toDocument());
                    if (updates.size() == MONGO_BATCH_SIZE) {
                        stream.insertMany(updates);
                        recordsCount.addAndGet(updates.size());
                        updates = new ArrayList<>();
                    }
                    tick = input.take();
                }
                stream.insertMany(updates);
                recordsCount.addAndGet(updates.size());

                if (running.decrementAndGet() == 0) {
                    SERVICE.shutdown();
                    TIMED.shutdown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public int getRecordsCount() {
            return recordsCount.get();
        }
    }
}
