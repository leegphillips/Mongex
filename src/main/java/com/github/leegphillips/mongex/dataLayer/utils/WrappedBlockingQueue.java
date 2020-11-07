package com.github.leegphillips.mongex.dataLayer.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

import static com.github.leegphillips.mongex.dataLayer.utils.Constants.QUEUE_SIZE;

public class WrappedBlockingQueue<T> extends ArrayBlockingQueue<T> {
    private static final Logger LOG = LoggerFactory.getLogger(WrappedBlockingQueue.class);

    public WrappedBlockingQueue() {
        super(QUEUE_SIZE);
    }

    @Override
    public T take() {
        try {
            return super.take();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
            System.exit(-1);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(T t) {
        try {
            super.put(t);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
            System.exit(-2);
            throw new RuntimeException(e);
        }
    }
}
