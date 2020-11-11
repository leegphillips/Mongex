package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.Tick;
import com.github.leegphillips.mongex.dataLayer.dao.TimeFrame;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

public class TickPadder extends WrappedBlockingQueue<Tick> implements Runnable {
    private final TimeFrame tf;
    private final WrappedBlockingQueue<Tick> input;
    private final AtomicInteger padding = new AtomicInteger(0);

    public TickPadder(TimeFrame tf, WrappedBlockingQueue<Tick> input) {
        this.tf = tf;
        this.input = input;
    }

    @Override
    public void run() {
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
    }

    public int getPadding() {
        return padding.get();
    }
}