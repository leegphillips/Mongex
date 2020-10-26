package com.github.leegphillips.mongex.dataLayer;

import java.io.Closeable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class AggregateState implements Iterable<AggregateState.FlatState>, Closeable {

    private final TimeFrameMarketStateIterable changes;
    private final Iterator<Change> iterator;
    private final Map<CurrencyPair, StreamState> states;

    public AggregateState(TimeFrame tf) {
        this.changes = new TimeFrameMarketStateIterable(tf);
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

                change.getDeltas().stream()
                        .filter(delta -> delta.getValue().compareTo(BigDecimal.ZERO) > 0)
                        .forEach(delta -> states.get(delta.getPair()).update(delta));

                Map<CurrencyPair, Map<Integer, BigDecimal>> snapshot = states.values().stream()
                        .collect(toMap(StreamState::getPair, StreamState::getSnapshot));

                return new FlatState(change.getTimestamp(), snapshot);
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
            return timestamp.plusNanos(1) +
                    " states=" + values;
        }
    }

    public static void main(String[] args) {
        new AggregateState(TimeFrame.ONE_DAY).iterator().forEachRemaining(state -> System.out.println(state));
    }
}
