package com.github.leegphillips.mongex.dataLayer;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class AggregateState {

    private final Map<CurrencyPair, StreamState> states;
    private LocalDateTime timestamp;
    private AtomicInteger counter = new AtomicInteger(0);

    public AggregateState() {
        states = Utils.getAllCurrencies().map(StreamState::new).collect(toMap(StreamState::getPair, Function.identity()));
    }

    public static void main(String[] args) {
        AggregateState aggregateState = new AggregateState();
        new TimeFrameMarketStateIterable(TimeFrame.ONE_HOUR).forEach(aggregateState::update);
    }

    private void update(Change change) {
        timestamp = change.getTimestamp();
        change.getDeltas().stream()
                .filter(delta -> delta.getValue().compareTo(BigDecimal.ZERO) > 0)
                .forEach(delta -> states.get(delta.getPair()).update(delta));
        System.out.println(counter.incrementAndGet() + " " + this);
    }

    @Override
    public String toString() {
        return timestamp.plusNanos(1) +
                " states=" + states.values();
    }
}
