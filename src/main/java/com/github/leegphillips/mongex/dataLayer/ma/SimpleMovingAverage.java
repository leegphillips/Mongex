package com.github.leegphillips.mongex.dataLayer.ma;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class SimpleMovingAverage extends MovingAverage {

    public static final String ATTR_NAME = "sma ";

    public SimpleMovingAverage(int size) {
        super(ATTR_NAME + size, size);
    }

    @Override
    public BigDecimal getValue() {
        if (window.size() < size)
            return BigDecimal.ZERO;

        return window.stream()
                .reduce(BigDecimal::add)
                .get()
                .divide(BigDecimal.valueOf(size), RoundingMode.HALF_EVEN);
    }
}
