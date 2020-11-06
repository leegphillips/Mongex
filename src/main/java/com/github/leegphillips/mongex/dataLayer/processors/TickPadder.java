package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.Tick;
import com.github.leegphillips.mongex.dataLayer.TimeFrame;

import java.time.LocalDateTime;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TickPadder extends ArrayBlockingQueue<Tick> implements Runnable {
    private static final int QUEUE_SIZE = 4096;

    private final TimeFrame tf;
    private final BlockingQueue<Tick> input;
    private final AtomicInteger padding = new AtomicInteger(0);

    public TickPadder(TimeFrame tf, BlockingQueue<Tick> input) {
        super(QUEUE_SIZE);
        this.tf = tf;
        this.input = input;
    }

    @Override
    public void run() {
        try {
            Tick current = input.take();
            put(current);
            Tick next = input.take();
            while (next != Tick.POISON) {
                LocalDateTime nextTF = tf.next(current.getTimestamp());
                while (nextTF.isBefore(next.getTimestamp())) {
                    put(new Tick(current.getPair(), nextTF, current.getBid(), current.getAsk()));
                    nextTF = tf.next(nextTF);
                    padding.incrementAndGet();
                }
                put(next);
                current = next;
                next = input.take();
            }
            put(Tick.POISON);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int getPadding() {
        return padding.get();
    }
}