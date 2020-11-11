package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.Tick;
import com.github.leegphillips.mongex.dataLayer.dao.TimeFrame;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

public class TickTimeFrameFilter extends WrappedBlockingQueue<Tick> implements Runnable {

    private final TimeFrame tf;
    private final WrappedBlockingQueue<Tick> input;
    private final AtomicLong filtered = new AtomicLong(0);

    public TickTimeFrameFilter(TimeFrame tf, WrappedBlockingQueue<Tick> input) {
        this.tf = tf;
        this.input = input;
    }

    @Override
    public void run() {
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
    }

    public long getFiltered() {
        return filtered.get();
    }
}
