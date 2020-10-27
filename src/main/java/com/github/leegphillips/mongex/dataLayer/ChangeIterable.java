package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ChangeIterable implements Iterable<Change>, Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(ChangeIterable.class);

    private final TickMarketIterable ticks;
    private final Iterator<Tick> iterator;

    private Tick next;

    public ChangeIterable() {
        ticks = new TickMarketIterable();
        iterator = ticks.iterator();
        next = iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void close() {
        ticks.close();
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
                LocalDateTime timestamp = next.getTimestamp();
                Map<CurrencyPair, Delta> deltas = new TreeMap<>();
                deltas.put(next.getPair(), new Delta(next.getPair(), next.getMid()));
                while (iterator.hasNext()) {
                    next = iterator.next();
                    if (next.getTimestamp().compareTo(timestamp) == 0) {
                        Delta prev = deltas.put(next.getPair(), new Delta(next.getPair(), next.getMid()));
                        if (prev != null)
                            LOG.trace("Duplicate: " + prev);
                    } else {
                        break;
                    }
                }
                return new Change(timestamp, deltas);
            }
        };
    }

    public static void main(String[] args) throws IOException {
        AtomicInteger count = new AtomicInteger(0);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("object.data"));
        new ChangeIterable().iterator().forEachRemaining(delta -> {
            try {
                objectOutputStream.writeObject(delta);
                if (count.incrementAndGet() % 100000 == 0) {
                    objectOutputStream.flush();
                    System.out.println(count.get() + "   " + delta);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
