package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.github.leegphillips.mongex.dataLayer.ma.MovingAverage;
import com.github.leegphillips.mongex.dataLayer.ma.SimpleMovingAverage;
import com.mongodb.client.MongoCollection;
import lombok.ToString;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.github.leegphillips.mongex.dataLayer.dao.State.END;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class MongexFileLoader implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MongexFileLoader.class);

    public static final int[] MA_SIZES = new int[]{1, 2, 8, 34, 144, 610, 2584};

    private static final int QUEUE_SIZE = 4096;
    private static final int BUFFER_SIZE = 4096;
    private static final int MONGO_BATCH_SIZE = 4096;

    private static final String CSV_SUFFIX = ".csv";

    private static final File[] FILES = Utils.getFiles();
    private static final CurrencyPair[] PAIRS = Arrays.stream(FILES).map(CurrencyPair::new)
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
                .map(TickTimeFrameFilter::new)
                .collect(toList());
        filters.forEach(SERVICE::execute);

        List<TickPadder> padders = filters.stream()
                .map(TickPadder::new)
                .collect(toList());
        padders.forEach(SERVICE::execute);

        List<TickMATracker> trackers = padders.stream()
                .map(TickMATracker::new)
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
        private final List<TickMATracker> trackers;
        private final List<TickPadder> padders;
        private final List<TickTimeFrameFilter> filters;
        private final List<TickReader> readers;

        private Monitor(List<MongoWriter> writers, List<TickMATracker> trackers, List<TickPadder> padders, List<TickTimeFrameFilter> filters, List<TickReader> readers) {
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
                    + " Trackers: " + trackers.stream().mapToInt(tracker -> QUEUE_SIZE - tracker.remainingCapacity()).sum()
                    + " Padders: " + padders.stream().mapToInt(padder -> QUEUE_SIZE - padder.remainingCapacity()).sum()
                    + " Filters: " + filters.stream().mapToInt(filter -> QUEUE_SIZE - filter.remainingCapacity()).sum()
                    + " Readers: " + readers.stream().mapToInt(reader -> QUEUE_SIZE - reader.remainingCapacity()).sum());
        }
    }

    private class TickReader extends ArrayBlockingQueue<Tick> implements Runnable {

        private final CurrencyPair pair;

        private TickReader(CurrencyPair pair) {
            super(QUEUE_SIZE);
            this.pair = pair;
        }

        @Override
        public void run() {
            List<File> filesForPair = Arrays.stream(FILES)
                    .filter(file -> file.getName().contains(pair.getLabel()))
                    .collect(toList());

            LOG.info(pair.getLabel());

            try {
                for (File zip : filesForPair) {
                    try {
                        ZipInputStream zis = new ZipInputStream(new FileInputStream(zip));
                        BufferedReader br = new BufferedReader(new InputStreamReader(zis), BUFFER_SIZE);

                        ZipEntry zipEntry = zis.getNextEntry();
                        while (zipEntry != null) {
                            if (zipEntry.getName().endsWith(CSV_SUFFIX)) {
                                break;
                            }
                            zipEntry = zis.getNextEntry();
                        }

                        String line = br.readLine();
                        while (line != null) {
                            String[] values = line.split(",");
                            put(Tick.create(pair, values[0], values[1], values[2]));
                            line = br.readLine();
                        }
                        br.close();
                        filesCompleted.incrementAndGet();
                    } catch (IOException e) {
                        throw new UncheckedIOException(zip.getName(), e);
                    }
                }
                put(Tick.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private class TickTimeFrameFilter extends ArrayBlockingQueue<Tick> implements Runnable {

        private final BlockingQueue<Tick> input;
        private final AtomicLong filtered = new AtomicLong(0);

        public TickTimeFrameFilter(BlockingQueue<Tick> input) {
            super(QUEUE_SIZE);
            this.input = input;
        }

        @Override
        public void run() {
            try {
                Tick current = input.take();
                LocalDateTime ceiling = tf.ceiling(current.getTimestamp());
                Tick next = input.take();
                while (next != Tick.POISON) {

                    if (next.getTimestamp().compareTo(ceiling) > 0) {
                        put(new Tick(current.getPair(), ceiling, current.getAsk(), current.getBid()));
                        ceiling = tf.ceiling(next.getTimestamp());
                    } else {
                        filtered.incrementAndGet();
                    }
                    current = next;

                    next = input.take();
                }
                put(Tick.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public long getFiltered() {
            return filtered.get();
        }
    }

    private class TickPadder extends ArrayBlockingQueue<Tick> implements Runnable {

        private final BlockingQueue<Tick> input;
        private final AtomicInteger padding = new AtomicInteger(0);

        public TickPadder(BlockingQueue<Tick> input) {
            super(QUEUE_SIZE);
            this.input = input;
        }

        @Override
        public void run() {
            try {
                Tick current = input.take();
                put(current);
                Tick next = input.take();
                while (next != Tick.POISON) {
                    LocalDateTime nextTF = tf.next(current.getTimestamp());
                    while (nextTF.isBefore(next.getTimestamp())) {
                        put(new Tick(current.getPair(), nextTF, current.getBid(), current.getAsk()));
                        nextTF = tf.next(nextTF);
                        padding.incrementAndGet();
                    }
                    put(next);
                    current = next;
                    next = input.take();
                }
                put(Tick.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public int getPadding() {
            return padding.get();
        }
    }

    @ToString
    private class TickMATracker extends ArrayBlockingQueue<State> implements Runnable {

        private final BlockingQueue<Tick> input;
        private final List<SimpleMovingAverage> sMAs = Arrays.stream(MA_SIZES).mapToObj(SimpleMovingAverage::new).collect(toList());

        public TickMATracker(BlockingQueue<Tick> input) {
            super(QUEUE_SIZE);
            this.input = input;
        }

        @Override
        public void run() {
            try {
                Tick tick = input.take();
                while (tick != Tick.POISON) {
                    BigDecimal mid = tick.getMid();
                    sMAs.parallelStream().forEach(ma -> ma.add(mid));
                    put(new State(tick.getPair(), tick.getTimestamp(), sMAs.parallelStream().collect(toMap(MovingAverage::getSize, SimpleMovingAverage::getValue))));
                    tick = input.take();
                }
                put(END);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
