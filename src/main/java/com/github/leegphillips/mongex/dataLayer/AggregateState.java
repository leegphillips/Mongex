package com.github.leegphillips.mongex.dataLayer;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class AggregateState extends ArrayBlockingQueue<AggregateState.FlatState> {

    private final static Logger LOG = LoggerFactory.getLogger(AggregateState.class);

    private final static int SIZE = 256;

    private final AtomicInteger counter = new AtomicInteger(0);
    private final BlockingQueue<Change> input;

    public AggregateState(BlockingQueue<Change> input) {
        super(SIZE);
        this.input = input;
        new Thread(new Worker(), getClass().getSimpleName()).start();
    }

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<FlatState> input = new AggregateState(new TimeFrameMarketStateIterable(TimeFrame.ONE_DAY));
        FlatState state = input.take();
        while (state != FlatState.POISON) {
            state = input.take();
        }
    }

    static class FlatState {
        final static FlatState POISON = new FlatState(null, null);

        private final LocalDateTime timestamp;
        private final Map<CurrencyPair, Map<Integer, BigDecimal>> values;

        FlatState(LocalDateTime timestamp, Map<CurrencyPair, Map<Integer, BigDecimal>> values) {
            this.timestamp = timestamp;
            this.values = values;
        }

        @Override
        public String toString() {
            return timestamp.plusNanos(1).toString();
        }

        public Document toDocument() {
            Document doc = new Document();

            doc.put(Candle.TIMESTAMP_ATTR_NAME, timestamp.toString());

            for (Map.Entry<CurrencyPair, Map<Integer, BigDecimal>> entry : values.entrySet()) {
                Map<String, BigDecimal> copy = entry.getValue().entrySet()
                        .stream()
                        .collect(toMap(a -> a.getKey().toString(), Map.Entry::getValue));

                doc.put(entry.getKey().getLabel(), copy);
            }

            return doc;
        }
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                Map<CurrencyPair, StreamState> states = Utils.getAllCurrencies().map(StreamState::new).collect(toMap(StreamState::getPair, identity()));
                Change change = input.take();

                while (change != Change.POISON) {

                    change.getDeltas().values().stream()
                            .filter(delta -> delta.getValue().compareTo(BigDecimal.ZERO) > 0)
                            .forEach(delta -> states.get(delta.getPair()).update(delta));

                    Map<CurrencyPair, Map<Integer, BigDecimal>> snapshot = new TreeMap<>(states.values().stream()
                            .collect(toMap(StreamState::getPair, StreamState::getSnapshot)));

                    FlatState flatState = new FlatState(change.getTimestamp(), snapshot);

                    LOG.trace(flatState.toString());
                    put(flatState);

                    change = input.take();
                }
                put(FlatState.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
