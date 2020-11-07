package com.github.leegphillips.mongex.dataLayer.utils;

import java.util.concurrent.ArrayBlockingQueue;

import static com.github.leegphillips.mongex.dataLayer.utils.Constants.QUEUE_SIZE;

public class WrappedBlockingQueue<T> extends ArrayBlockingQueue<T> {
    public WrappedBlockingQueue() {
        super(QUEUE_SIZE);
    }

    @Override
    public T take() {
        try {
            return super.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(T t) {
        try {
            super.put(t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
