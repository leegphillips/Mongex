package com.github.leegphillips.mongex.dataLayer;

import java.io.Closeable;
import java.util.Iterator;

import static java.util.Collections.singletonList;

public class MarketStateIterable implements Iterable<Change>, Closeable {

    private final ChangeIterable changes;
    private final Iterator<Change> iterator;

    private Change state;

    public MarketStateIterable() {
        changes = new ChangeIterable();
        iterator = changes.iterator();
        state = iterator.hasNext() ? iterator.next() : null;
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
                return Change.coalesce(state, singletonList(iterator.next()));
            }
        };
    }
}
