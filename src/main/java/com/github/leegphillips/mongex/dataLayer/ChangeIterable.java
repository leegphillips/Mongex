package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

public class ChangeIterable extends ArrayBlockingQueue<Change> {
    private final static Logger LOG = LoggerFactory.getLogger(ChangeIterable.class);

    private final static int SIZE = 256;

    public ChangeIterable() {
        super(SIZE);
        new Thread(new Worker(), getClass().getSimpleName()).start();
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                TickMarketIterable ticks = new TickMarketIterable();
                Tick tick = ticks.take();
                LocalDateTime timestamp = tick.getTimestamp();
                Map<CurrencyPair, Delta> deltas = new TreeMap<>();
                while (tick != Tick.POISON) {
                    if (tick.getTimestamp().compareTo(timestamp) > 0) {
                        Change change = new Change(timestamp, deltas);
                        LOG.trace(change.toString());
                        put(change);
                        timestamp = tick.getTimestamp();
                        deltas = new TreeMap<>();
                        deltas.put(tick.getPair(), new Delta(tick.getPair(), tick.getMid()));
                    }
                    Delta prev = deltas.put(tick.getPair(), new Delta(tick.getPair(), tick.getMid()));
                    if (prev != null)
                        LOG.trace("Duplicate: " + prev);

                    tick = ticks.take();
                }
                put(Change.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ChangeIterable changes = new ChangeIterable();
        Change change = changes.take();
        while (change != Change.POISON) {
            change = changes.take();
        }
    }
}
