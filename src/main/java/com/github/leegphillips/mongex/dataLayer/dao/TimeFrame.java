package com.github.leegphillips.mongex.dataLayer.dao;

import lombok.NonNull;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.function.UnaryOperator;

@SuppressWarnings("unused")
public enum TimeFrame {
    ONE_SECOND("1s", timestamp -> timestamp.plusSeconds(1), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.SECONDS)),
    FIVE_SECONDS("5s", timestamp -> timestamp.plusSeconds(5), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.MINUTES)),
    TEN_SECONDS("10s", timestamp -> timestamp.plusSeconds(10), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.MINUTES)),
    THIRTY_SECONDS("30s", timestamp -> timestamp.plusSeconds(30), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.MINUTES)),
    ONE_MINUTE("1m", timestamp -> timestamp.plusMinutes(1), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.HOURS)),
    FIVE_MINUTES("5m", timestamp -> timestamp.plusMinutes(5), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.HOURS)),
    TEN_MINUTES("10m", timestamp -> timestamp.plusMinutes(10), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.HOURS)),
    FIFTEEN_MINUTES("15m", timestamp -> timestamp.plusMinutes(15), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.HOURS)),
    THIRTY_MINUTES("30m", timestamp -> timestamp.plusMinutes(30), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.HOURS)),
    ONE_HOUR("1h", timestamp -> timestamp.plusHours(1), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.DAYS)),
    THREE_HOURS("3h", timestamp -> timestamp.plusHours(3), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.DAYS)),
    FOUR_HOURS("4h", timestamp -> timestamp.plusHours(4), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.DAYS)),
    EIGHT_HOURS("8h", timestamp -> timestamp.plusHours(8), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.DAYS)),
    ONE_DAY("1D", timestamp -> timestamp.plusDays(1), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.WEEKS)),
    ONE_WEEK("1W", timestamp -> timestamp.plusWeeks(1), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.MONTHS)),
    ONE_MONTH("1M", timestamp -> timestamp.plusMonths(1), timestamp -> timestamp.minusNanos(1).truncatedTo(ChronoUnit.YEARS));

    public static final String ATTR_NAME = "timeframe";

    private final String label;
    private final UnaryOperator<LocalDateTime> next;
    private final UnaryOperator<LocalDateTime> base;

    TimeFrame(String label, UnaryOperator<LocalDateTime> next, UnaryOperator<LocalDateTime> base) {
        this.label = label;
        this.next = next;
        this.base = base;
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
        LocalDateTime base = this.base.apply(timestamp);
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
