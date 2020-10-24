package com.github.leegphillips.mongex.dataLayer;

import java.io.Closeable;
import java.time.LocalDateTime;
import java.util.Iterator;

import static com.github.leegphillips.mongex.dataLayer.Change.coalesce;

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
        state = iterator.hasNext() ? iterator.next() : null;
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
                return iterator.hasNext();
            }

            @Override
            public Change next() {
                while (next != null & next.getTimestamp().isBefore(currentBlockEnd)) {
                    state = coalesce(state, next);
                    next = iterator.hasNext() ? iterator.next() : null;
                }
                state.setTimestamp(currentBlockEnd);
                currentBlockEnd = tf.next(currentBlockEnd);
                return state;
            }
        };
    }

    public static void main(String[] args) {
        new TimeFrameMarketStateIterable(TimeFrame.ONE_WEEK).iterator().forEachRemaining(state -> System.out.println(state));
    }
}
