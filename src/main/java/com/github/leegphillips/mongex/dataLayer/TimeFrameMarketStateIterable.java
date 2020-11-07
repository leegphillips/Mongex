package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.github.leegphillips.mongex.dataLayer.Change.coalesce;

public class TimeFrameMarketStateIterable extends ArrayBlockingQueue<Change> {
    private final static Logger LOG = LoggerFactory.getLogger(TimeFrameMarketStateIterable.class);

    private final static int SIZE = 256;

    private final TimeFrame tf;

    public TimeFrameMarketStateIterable(TimeFrame tf) {
        super(SIZE);
        this.tf = tf;
        new Thread(new Worker(), getClass().getSimpleName()).start();
    }

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<Change> input = new TimeFrameMarketStateIterable(TimeFrame.FIVE_MINUTES);
        Change change = input.take();
        while (change != Change.POISON) {
            change = input.take();
        }
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                BlockingQueue<Change> input = new MarketStateIterable();
                Change change = input.take();
                List<Change> changes = new ArrayList<>();
                LocalDateTime currentBlockEnd = tf.ceiling(change.getTimestamp());
                while (change != Change.POISON) {
                    if (!change.getTimestamp().isBefore(currentBlockEnd)) {
                        change = coalesce(change, changes);
                        change.setTimestamp(currentBlockEnd);
                        LOG.trace(change.toString());
                        put(change);
                        changes = new ArrayList<>();
                        currentBlockEnd = tf.next(currentBlockEnd);
                    }
                    changes.add(change);
                    change = input.take();
                }
                put(Change.POISON);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
