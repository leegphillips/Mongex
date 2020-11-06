package com.github.leegphillips.mongex.dataLayer.utils;

import java.util.concurrent.ArrayBlockingQueue;

public class WrappedBlockingQueue<T> extends ArrayBlockingQueue<T> {
    public WrappedBlockingQueue(int capacity) {
        super(capacity);
    }

    @Override
    public T take() {
        try {
            return super.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
