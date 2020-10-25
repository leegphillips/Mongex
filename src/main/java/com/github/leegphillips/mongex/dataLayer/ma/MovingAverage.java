package com.github.leegphillips.mongex.dataLayer.ma;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;

public abstract class MovingAverage {

    protected final Deque<BigDecimal> window;
    protected final int size;
    private final String name;

    public MovingAverage(String name, int size) {
        window = new ArrayDeque<>(size + 1);
        this.name = name;
        this.size = size;
    }

    public void add(BigDecimal value) {
        window.add(value);
        if (window.size() > size)
            window.removeFirst();
    }

    public String getName() {
        return name;
    }

    public abstract BigDecimal getValue();

    @Override
    public String toString() {
        return "" + size +
                ":" + getValue();
    }
}
