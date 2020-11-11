package com.github.leegphillips.mongex.dataLayer.dao;

import lombok.NonNull;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.function.UnaryOperator;

@SuppressWarnings("unused")
public enum TimeFrame {
    FIVE_SECONDS("5s", timestamp -> timestamp.plusSeconds(5)),
    TEN_SECONDS("10s", timestamp -> timestamp.plusSeconds(10)),
    THIRTY_SECONDS("30s", timestamp -> timestamp.plusSeconds(30)),
    ONE_MINUTE("1m", timestamp -> timestamp.plusMinutes(1)),
    FIVE_MINUTES("5m", timestamp -> timestamp.plusMinutes(5)),
    TEN_MINUTES("10m", timestamp -> timestamp.plusMinutes(10)),
    FIFTEEN_MINUTES("15m", timestamp -> timestamp.plusMinutes(15)),
    THIRTY_MINUTES("30m", timestamp -> timestamp.plusMinutes(30)),
    ONE_HOUR("1h", timestamp -> timestamp.plusHours(1)),
    THREE_HOURS("3h", timestamp -> timestamp.plusHours(3)),
    FOUR_HOURS("4h", timestamp -> timestamp.plusHours(4)),
    EIGHT_HOURS("8h", timestamp -> timestamp.plusHours(8)),
    ONE_DAY("1D", timestamp -> timestamp.plusDays(1)),
    ONE_WEEK("1W", timestamp -> timestamp.plusWeeks(1)),
    ONE_MONTH("1M", timestamp -> timestamp.plusMonths(1));

    public static final String ATTR_NAME = "timeframe";

    private final String label;
    private final UnaryOperator<LocalDateTime> next;

    TimeFrame(String label, UnaryOperator<LocalDateTime> next) {
        this.label = label;
        this.next = next;
    }

    public static TimeFrame get(@NonNull String label) {
        return Arrays.stream(TimeFrame.class.getEnumConstants())
                .filter(tf -> tf.label.equals(label))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Searching for an non-existent value: " + label));
    }

    public String getLabel() {
        return label;
    }

    public LocalDateTime next(LocalDateTime timestamp) {
        return next.apply(timestamp);
    }

    @Override
    public String toString() {
        return label;
    }

    public LocalDateTime floor(LocalDateTime timestamp) {
        LocalDateTime base = timestamp.minusMonths(1).truncatedTo(ChronoUnit.DAYS);
        LocalDateTime next = next(base);
        while (base.compareTo(timestamp) <= 0) {
            if (next.isAfter(timestamp))
                return base;
            base = next;
            next = next(next);
        }
        throw new IllegalStateException();
    }

    public LocalDateTime ceiling(LocalDateTime timestamp) {
        return next(floor(timestamp)).minusNanos(1);
    }
}
