package com.github.leegphillips.mongex.dataLayer;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class AggregateState implements Iterable<AggregateState.FlatState>, Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(AggregateState.class);

    private final TimeFrameMarketStateIterable changes;
    private final TimeFrame tf;
    private final Iterator<Change> iterator;
    private final Map<CurrencyPair, StreamState> states;

    private final AtomicInteger counter = new AtomicInteger(0);

    public AggregateState(TimeFrame tf) {
        this.changes = new TimeFrameMarketStateIterable(tf);
        this.tf = tf;
        this.iterator = changes.iterator();
        states = Utils.getAllCurrencies().map(StreamState::new).collect(toMap(StreamState::getPair, identity()));
    }

    @Override
    public void close() {
        changes.close();
    }

    @Override
    public Iterator<FlatState> iterator() {
        return new Iterator<FlatState>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public FlatState next() {
                Change change = iterator.next();

                change.getDeltas().values().stream()
                        .filter(delta -> delta.getValue().compareTo(BigDecimal.ZERO) > 0)
                        .forEach(delta -> states.get(delta.getPair()).update(delta));

                Map<CurrencyPair, Map<Integer, BigDecimal>> snapshot = new TreeMap<>(states.values().stream()
                        .collect(toMap(StreamState::getPair, StreamState::getSnapshot)));

                FlatState flatState = new FlatState(change.getTimestamp(), snapshot);

                if (counter.incrementAndGet() % 100 == 0)
                    LOG.info(flatState.toString());

                return flatState;
            }
        };
    }

    class FlatState {
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
            doc.put(TimeFrame.ATTR_NAME, tf.getLabel());

            for (Map.Entry<CurrencyPair, Map<Integer, BigDecimal>> entry : values.entrySet()) {
                Map<String, BigDecimal> copy = entry.getValue().entrySet()
                                                    .stream()
                                                    .collect(toMap(a -> a.getKey().toString(), Map.Entry::getValue));

                doc.put(entry.getKey().getLabel(), copy);
            }

            return doc;
        }
    }

    public static void main(String[] args) {
        new AggregateState(TimeFrame.ONE_DAY).iterator().forEachRemaining(state -> System.out.println(state));
    }
}
