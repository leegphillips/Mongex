package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

import static java.util.Collections.singletonList;

public class MarketStateIterable extends ArrayBlockingQueue<Change> {
    private final static Logger LOG = LoggerFactory.getLogger(MarketStateIterable.class);

    private final static int SIZE = 256;

    public MarketStateIterable() {
        super(SIZE);
        new Thread(new Worker(), getClass().getSimpleName()).start();
    }

    public static void main(String[] args) throws InterruptedException {
        MarketStateIterable changes = new MarketStateIterable();
        Change change = changes.take();
        while (change != Change.POISON) {
            change = changes.take();
        }
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                ChangeIterable changes = new ChangeIterable();
                Change next = changes.take();
                Change state = next;
                while (next != Change.POISON) {
                    state = Change.coalesce(state, singletonList(changes.take()));
                    LOG.trace(state.toString());
                    put(state);
                    next = changes.take();
                }
                put(Change.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
