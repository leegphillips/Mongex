package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.leegphillips.mongex.dataLayer.MongexFileLoader.MA_SIZES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class MongexFullStateWriter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MongexFullStateWriter.class);

    private static final String VALUES_ATTR = "values";

    private static final ExecutorService SERVICE = Executors.newCachedThreadPool();
    private static final ScheduledExecutorService TIMED = Executors.newSingleThreadScheduledExecutor();

    private static final int QUEUE_SIZE = 32;
    private static final Document END = new Document();
    private static final List<State> CLOSE = new ArrayList<>();

    private final long start = System.currentTimeMillis();

    private final TimeFrame tf;

    public static void main(String[] args) {
        TimeFrame tf = TimeFrame.get(args[0]);
        new MongexFullStateWriter(tf).run();
    }

    public MongexFullStateWriter(TimeFrame tf) {
        this.tf = tf;
    }

    @Override
    public void run() {
        Map<CurrencyPair, MongoReader> readers = DatabaseFactory.getIndividualStreams(tf).entrySet().stream()
                .collect(toMap(entry -> CurrencyPair.get(entry.getKey()), entry -> new MongoReader(entry.getValue())));
        readers.values().forEach(SERVICE::execute);

        Map<CurrencyPair, Convertor> convertors = readers.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> new Convertor(entry.getValue())));
        convertors.values().forEach(SERVICE::execute);

        Aggregator aggregator = new Aggregator(convertors.entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
        SERVICE.execute(aggregator);

        FullState fullState = new FullState(aggregator);
        SERVICE.execute(fullState);

        MongoWriter writer = new MongoWriter(fullState);
        SERVICE.execute(writer);

        TIMED.scheduleAtFixedRate(new Monitor(readers, convertors, aggregator, fullState, writer), 5, 5, TimeUnit.SECONDS);
    }

    private class Monitor implements Runnable {

        private final List<MongoReader> readers;
        private final List<Convertor> convertors;
        private final Aggregator aggregator;
        private final FullState fullState;
        private final MongoWriter writer;

        public Monitor(Map<CurrencyPair, MongoReader> readers, Map<CurrencyPair, Convertor> convertors, Aggregator aggregator, FullState fullState, MongoWriter writer) {
            this.readers = new ArrayList<>(readers.values());
            this.convertors = new ArrayList<>(convertors.values());
            this.aggregator = aggregator;
            this.fullState = fullState;
            this.writer = writer;
        }

        @Override
        public void run() {
            long duration = (System.currentTimeMillis() - start) / 1000;
            long records = writer.getRecords();
            long rate = records/duration;
            LOG.info("-----------------------------------------------------------------");
            LOG.info("Duration: " + duration + "s");
            LOG.info("Records: " + records);
            LOG.info("Rate: " + rate + " record/s");
            LOG.info("Remaining: " + readers.stream().mapToLong(MongoReader::getRemaining).sum());
            LOG.info("Last: " + writer.getLast());
            LOG.info("Queues:"
                    + " Convertors: " + readers.stream().mapToInt(tracker -> QUEUE_SIZE - tracker.remainingCapacity()).sum()
                    + " Aggregator: " + convertors.stream().mapToInt(tracker -> QUEUE_SIZE - tracker.remainingCapacity()).sum()
                    + " Full state: " + (QUEUE_SIZE - aggregator.remainingCapacity())
                    + " Writer: " + (QUEUE_SIZE - fullState.remainingCapacity()));
        }
    }

    private static class MongoReader extends ArrayBlockingQueue<Document> implements Runnable {

        private final AtomicLong remaining = new AtomicLong();
        private final MongoCollection<Document> stream;

        public MongoReader(MongoCollection<Document> stream) {
            super(QUEUE_SIZE);
            this.stream = stream;
        }

        @Override
        public void run() {
            try {
                remaining.set(stream.countDocuments());
                for (Document doc : stream.find()) {
                    put(doc);
                    remaining.decrementAndGet();
                }
                put(END);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public long getRemaining() {
            return remaining.get();
        }
    }

    private static class Convertor extends WrappedBlockingQueue<State> implements Runnable {

        private final BlockingQueue<Document> input;

        public Convertor(BlockingQueue<Document> input) {
            super(QUEUE_SIZE);
            this.input = input;
        }

        @Override
        public void run() {
            try {
                Document doc = input.take();
                while (doc != END) {
                    put(State.fromDocument(doc));
                    doc = input.take();
                }
                put(State.END);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class Aggregator extends ArrayBlockingQueue<List<State>> implements Runnable {

        private final Map<CurrencyPair, WrappedBlockingQueue<State>> inputs;

        public Aggregator(Map<CurrencyPair, WrappedBlockingQueue<State>> inputs) {
            super(QUEUE_SIZE);
            this.inputs = new HashMap<>(inputs);
        }

        @Override
        public void run() {
            try {
                Map<CurrencyPair, State> nexts = inputs.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().take()));

                while (nexts.size() > 0) {
                    List<CurrencyPair> finished = nexts.entrySet().stream()
                            .filter(entry -> entry.getValue().equals(State.END))
                            .map(Map.Entry::getKey)
                            .collect(toList());

                    finished.forEach(inputs::remove);
                    finished.forEach(nexts::remove);

                    State first = nexts.values().stream()
                            .min(Comparator.comparing(State::getTimestamp))
                            .orElseThrow(IllegalStateException::new);

                    List<State> same = nexts.values().stream()
                            .filter(state -> state.getTimestamp().isEqual(first.getTimestamp()))
                            .collect(toList());

                    same.stream()
                            .filter(state -> nexts.containsKey(state.getPair()))
                            .forEach(state -> nexts.put(state.getPair(), inputs.get(state.getPair()).take()));

                    put(same);
                }
                put(CLOSE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class FullState extends ArrayBlockingQueue<List<State>> implements Runnable {

        private final BlockingQueue<List<State>> input;

        public FullState(BlockingQueue<List<State>> input) {
            super(QUEUE_SIZE);
            this.input = input;
        }

        @Override
        public void run() {
            Map<CurrencyPair, State> full = Utils.getAllCurrencies()
                    .collect(toMap(pair -> pair, pair -> new State(pair, LocalDateTime.MIN, Arrays.stream(MA_SIZES)
                            .boxed()
                            .collect(toMap(entry -> entry, entry -> BigDecimal.ZERO))), (o1, o2) -> o1, TreeMap::new));
            try {
                List<State> update = input.take();
                while (update != CLOSE) {
                    update.forEach(state -> full.replace(state.getPair(), state));

                    put(new ArrayList<>(full.values()));

                    update = input.take();
                }
                put(CLOSE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private class MongoWriter implements Runnable {

        private final BlockingQueue<List<State>> input;
        private final AtomicLong records = new AtomicLong(0);
        private String last = "";

        public MongoWriter(BlockingQueue<List<State>> input) {
            this.input = input;
        }

        @Override
        public void run() {
            MongoCollection<Document> stream = DatabaseFactory.getMainStream(tf);
            try {
                List<State> state = input.take();
                List<Document> toInsert = new ArrayList<>();
                while (state != CLOSE) {
                    Document doc = new Document();

                    LocalDateTime timestamp = state.stream()
                            .filter(s -> !s.getTimestamp().isEqual(LocalDateTime.MIN))
                            .findFirst().orElseThrow(IllegalStateException::new)
                            .getTimestamp();

                    doc.append(Candle.TIMESTAMP_ATTR_NAME, timestamp);

                    doc.append(VALUES_ATTR, state.stream()
                            .collect(toMap(s -> s.getPair().getLabel(), s -> s.getValues().entrySet().stream().collect(toMap(entry -> entry.getKey().toString(), entry -> entry.getValue())), (o1, o2) -> o1, TreeMap::new)));

                    toInsert.add(doc);
                    records.incrementAndGet();
                    if (toInsert.size() == QUEUE_SIZE) {
                        stream.insertMany(toInsert);
                        toInsert = new ArrayList<>();
                        last = timestamp.toString();
                    }

                    state = input.take();
                }
                stream.insertMany(toInsert);
                SERVICE.shutdown();
                TIMED.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public long getRecords() {
            return records.get();
        }

        public String getLast() {
            return last;
        }
    }

    private static class WrappedBlockingQueue<T> extends ArrayBlockingQueue<T> {
        public WrappedBlockingQueue(int capacity) {
            super(capacity);
        }

        @Override
        public T take() {
            try {
                return super.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException();
            }
        }
    }
}
