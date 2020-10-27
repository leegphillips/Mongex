package com.github.leegphillips.mongex.dataLayer;

import java.io.Closeable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.function.Function;

import static com.github.leegphillips.mongex.dataLayer.Change.coalesce;
import static java.util.stream.Collectors.toMap;

public class TimeFrameMarketStateIterable implements Iterable<Change>, Closeable {

    private final MarketStateIterable changes;
    private final Iterator<Change> iterator;
    private final TimeFrame tf;

    private Change state;
    private LocalDateTime currentBlockEnd;
    private Change next;

    public TimeFrameMarketStateIterable(TimeFrame tf) {
        this.tf = tf;
        changes = new MarketStateIterable();
        iterator = changes.iterator();

        state = new Change(LocalDateTime.MIN, Utils.getAllCurrencies()
                .map(pair -> new Delta(pair, BigDecimal.ZERO))
                .collect(toMap(Delta::getPair, Function.identity())));

        currentBlockEnd = tf.ceiling(state.getTimestamp());
        next = iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void close() {
        changes.close();
    }

    @Override
    public Iterator<Change> iterator() {

        return new Iterator<Change>() {
            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Change next() {
                while (next.getTimestamp().isBefore(currentBlockEnd)) {
                    state = coalesce(state, next);
                    next = iterator.hasNext() ? iterator.next() : null;
                }
                state.setTimestamp(currentBlockEnd);
                currentBlockEnd = tf.next(currentBlockEnd);
                return state;
            }
        };
    }
}
