package com.github.leegphillips.mongex.dataLayer;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ChangeIterable implements Iterable<Change>, Closeable {

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
                List<Delta> deltas = new ArrayList<>();
                deltas.add(new Delta(next.getPair(), next.getMid()));
                while (iterator.hasNext()) {
                    next = iterator.next();
                    if (next.getTimestamp().compareTo(timestamp) == 0) {
                        deltas.add(new Delta(next.getPair(), next.getMid()));
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
