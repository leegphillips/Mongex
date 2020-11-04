package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.dao.State;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.leegphillips.mongex.dataLayer.MongexFileLoader.MA_SIZES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class MongexFullStateWriter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MongexFullStateWriter.class);

    private static final ExecutorService SERVICE = Executors.newCachedThreadPool();
    private static final ScheduledExecutorService TIMED = Executors.newSingleThreadScheduledExecutor();

    private static final int QUEUE_SIZE = 4096;
    private static final Document END = new Document();
    private static final List<State> CLOSE = new ArrayList<>();

    private final long start = System.currentTimeMillis();
    private final AtomicInteger lines = new AtomicInteger(0);

    private final CurrencyPair pair;
    private final TimeFrame tf;

    public static void main(String[] args) {
        CurrencyPair pair = new CurrencyPair(args[0]);
        TimeFrame tf = TimeFrame.get(args[1]);
        new MongexFullStateWriter(pair, tf).run();
    }

    public MongexFullStateWriter(CurrencyPair pair, TimeFrame tf) {
        this.pair = pair;
        this.tf = tf;
    }

    @Override
    public void run() {
        Map<CurrencyPair, MongoReader> readers = DatabaseFactory.getStreams(tf).entrySet().stream()
                .collect(toMap(entry -> new CurrencyPair(entry.getKey()), entry -> new MongoReader(entry.getValue())));
        readers.values().forEach(SERVICE::execute);

        Map<CurrencyPair, Convertor> convertors = readers.entrySet().stream()
                .collect(toMap(entry -> entry.getKey(), entry -> new Convertor(entry.getValue())));
        convertors.values().forEach(SERVICE::execute);

        Aggregator aggregator = new Aggregator(convertors.entrySet().stream().collect(toMap(entry -> entry.getKey(), entry -> entry.getValue())));
        SERVICE.execute(aggregator);

        FullState fullState = new FullState(aggregator);
        SERVICE.execute(fullState);

        FileCSVWriter writer = new FileCSVWriter(fullState);
        SERVICE.execute(writer);

//        FileCSVWriter writer = new FileCSVWriter(reader);
//        SERVICE.execute(writer);

//        TIMED.scheduleAtFixedRate(new Monitor(reader, writer), 5, 5, TimeUnit.SECONDS);
    }

    private class Monitor implements Runnable {

        private final MongoReader reader;
        private final FileCSVWriter writer;

        public Monitor(MongoReader reader, FileCSVWriter writer) {
            this.reader = reader;
            this.writer = writer;
        }

        @Override
        public void run() {
            long duration = (System.currentTimeMillis() - start) / 1000;
            LOG.info("-----------------------------------------------------------------");
            LOG.info("Duration: " + duration + "s");
            LOG.info("Lines: " + lines.get());
            LOG.info("Remaining: " + reader.getRemaining());
            LOG.info("Queues:"
                    + " Reader: " + (QUEUE_SIZE - reader.remainingCapacity()));

        }
    }

    private class MongoReader extends ArrayBlockingQueue<Document> implements Runnable {

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

    private class Convertor extends WrappedBlockingQueue<State> implements Runnable {

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

    private class Aggregator extends ArrayBlockingQueue<List<State>> implements Runnable {

        private final Map<CurrencyPair, WrappedBlockingQueue<State>> inputs;

        public Aggregator(Map<CurrencyPair, WrappedBlockingQueue<State>> inputs) {
            super(QUEUE_SIZE);
            this.inputs = new HashMap<>(inputs);
        }

        @Override
        public void run() {
            try {
                Map<CurrencyPair, State> nexts = inputs.entrySet().stream()
                        .collect(toMap(entry -> entry.getKey(), entry -> entry.getValue().take()));

                while (nexts.size() > 0) {
                    List<CurrencyPair> finished = nexts.entrySet().stream()
                            .filter(entry -> entry.getValue().equals(State.END))
                            .map(Map.Entry::getKey)
                            .collect(toList());

                    finished.forEach(inputs::remove);
                    finished.forEach(nexts::remove);

                    State first = nexts.values().stream()
                            .sorted(Comparator.comparing(State::getTimestamp))
                            .findFirst().orElseThrow(IllegalStateException::new);

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

    private class FullState extends ArrayBlockingQueue<List<State>> implements Runnable {

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

    private class FileCSVWriter implements Runnable {

        private final BlockingQueue<List<State>> input;

        public FileCSVWriter(BlockingQueue<List<State>> input) {
            this.input = input;
        }
        @Override
        public void run() {
            try {
                BufferedWriter bw = new BufferedWriter(new java.io.FileWriter("output.csv", true));
                List<State> fullState = input.take();
                while (fullState != CLOSE) {
                    for (State state : fullState) {
                        for (BigDecimal value : state.getValues().values()) {
                            bw.write(value.toPlainString());
                            bw.write(", ");
                        }
                    }
                    lines.incrementAndGet();
                    bw.newLine();

                    fullState = input.take();
                }
                bw.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new UncheckedIOException("", e);
            }
            SERVICE.shutdown();
            TIMED.shutdown();
        }
    }

//    private class FileCSVWriter implements Runnable {
//
//        private final BlockingQueue<List<State>> input;
//
//        public FileCSVWriter(BlockingQueue<List<State>> input) {
//            this.input = input;
//        }
//
//        @Override
//        public void run() {
//            try {
//                BufferedWriter bw = new BufferedWriter(new java.io.FileWriter("output.csv", true));
//                Document current = input.take();
//                Document next = input.take();
//                while (next != END) {
//                    List<Integer> keys = current.keySet().stream()
//                            .filter(key -> !key.equals(Candle.TIMESTAMP_ATTR_NAME))
//                            .filter(key -> !key.equals("_id"))
//                            .map(Integer::valueOf)
//                            .sorted()
//                            .collect(toList());
//
//                    for (Integer key : keys) {
//                        bw.write(current.get(key.toString(), Decimal128.class).bigDecimalValue().toPlainString());
//                        bw.write(", ");
//                    }
//
//                    bw.write("" + next.get("1", Decimal128.class).compareTo(current.get("1", Decimal128.class)));
//                    bw.write(", ");
//
//                    bw.newLine();
//                    current = next;
//                    next = input.take();
//                }
//                bw.close();
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            } catch (IOException e) {
//                throw new UncheckedIOException("", e);
//            }
//            SERVICE.shutdown();
//            TIMED.shutdown();
//        }
//    }

    private class WrappedBlockingQueue<T> extends ArrayBlockingQueue<T> {
        public WrappedBlockingQueue(int capacity) {
            super(capacity);
        }

        @Override
        public T take() {
            try {
                return super.take();
            } catch (InterruptedException e) {
                // TODO implement UncheckedInterruptedException
                throw new RuntimeException(e);
            }
        }
    }
}
