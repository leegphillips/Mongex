package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.Tick;
import com.github.leegphillips.mongex.dataLayer.TimeFrame;

import java.time.LocalDateTime;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class TickTimeFrameFilter extends ArrayBlockingQueue<Tick> implements Runnable {
    private static final int QUEUE_SIZE = 4096;

    private final TimeFrame tf;
    private final BlockingQueue<Tick> input;
    private final AtomicLong filtered = new AtomicLong(0);

    public TickTimeFrameFilter(TimeFrame tf, BlockingQueue<Tick> input) {
        super(QUEUE_SIZE);
        this.tf = tf;
        this.input = input;
    }

    @Override
    public void run() {
        try {
            Tick current = input.take();
            LocalDateTime ceiling = tf.ceiling(current.getTimestamp());
            Tick next = input.take();
            while (next != Tick.POISON) {

                if (next.getTimestamp().compareTo(ceiling) > 0) {
                    put(new Tick(current.getPair(), ceiling, current.getAsk(), current.getBid()));
                    ceiling = tf.ceiling(next.getTimestamp());
                } else {
                    filtered.incrementAndGet();
                }
                current = next;

                next = input.take();
            }
            put(Tick.POISON);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long getFiltered() {
        return filtered.get();
    }
}
