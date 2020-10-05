package com.github.leegphillips.mongex.dataLayer;

import lombok.NonNull;
import org.bson.Document;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayDeque;

public class SMAWindow {
    private final ArrayDeque<BigDecimal> window = new ArrayDeque<>();

    private final CandleSpecification specification;
    private final int[] sMAS;

    private final int windowSize;

    public SMAWindow(@NonNull CandleSpecification specification, @NonNull int[] sMAS) {
        this.specification = specification;
        this.sMAS = sMAS;

        if (sMAS.length == 0)
            throw new IllegalArgumentException("SMA's cannot be empty");

        // it is essential that this array is sorted
        for (int i = 1, last = sMAS[0]; i <= sMAS.length - 1; i++) {
            int current = sMAS[i];
            if (last > current)
                throw new IllegalArgumentException("SMA's must be ordered lowest to highest");
            last = current;
        }

        windowSize = sMAS[sMAS.length - 1] * specification.getEventsPerDay();
    }

    public void add(Document candle) {
        LocalDate current = LocalDate.parse(candle.getString("date"));

    }
}
