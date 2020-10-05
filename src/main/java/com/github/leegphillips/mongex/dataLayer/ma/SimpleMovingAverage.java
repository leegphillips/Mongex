package com.github.leegphillips.mongex.dataLayer.ma;

import java.math.BigDecimal;

public class SimpleMovingAverage extends MovingAverage {

    public SimpleMovingAverage(int size) {
        super("sma" + size, size);
    }

    @Override
    public BigDecimal getValue() {
        if (window.size() < size)
            return BigDecimal.ZERO;

        return window.stream()
                .reduce(BigDecimal::add)
                .get()
                .divide(BigDecimal.valueOf(size));
    }
}
