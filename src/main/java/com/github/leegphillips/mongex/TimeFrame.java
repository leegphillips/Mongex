package com.github.leegphillips.mongex;

import lombok.NonNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.UnaryOperator;

@SuppressWarnings("unused")
public enum TimeFrame {
    FIVE_SECONDS("5s", timestamp -> timestamp.plusSeconds(5), null, null),
    TEN_SECONDS("10s", timestamp -> timestamp.plusSeconds(10), null, null),
    THIRTY_SECONDS("30s", timestamp -> timestamp.plusSeconds(30), null, null),
    ONE_MINUTE("1m", timestamp -> timestamp.plusMinutes(1), null, null),
    FIVE_MINUTES("5m", timestamp -> timestamp.plusMinutes(5), null, null),
    TEN_MINUTES("10m", timestamp -> timestamp.plusMinutes(10), null, null),
    FIFTEEN_MINUTES("15m", timestamp -> timestamp.plusMinutes(15), null, null),
    THIRTY_MINUTES("30m", timestamp -> timestamp.plusMinutes(30), null, null),
    ONE_HOUR("1h", timestamp -> timestamp.plusHours(1), null, null),
    THREE_HOURS("3h", timestamp -> timestamp.plusHours(3), null, null),
    FOUR_HOURS("4h", timestamp -> timestamp.plusHours(4), null, null),
    EIGHT_HOURS("8h", timestamp -> timestamp.plusHours(8), null, null),
    ONE_DAY("1D", timestamp -> timestamp.plusDays(1), null, null),
    ONE_WEEK("1W", timestamp -> timestamp.plusWeeks(1), null, null),
    ONE_MONTH("1M", timestamp -> timestamp.plusMonths(1), null, null);

    public static final String ATTR_NAME = "timeframe";

    private final String label;
    private final UnaryOperator<LocalDateTime> next;
    private final UnaryOperator<LocalDateTime> floor;
    private final UnaryOperator<LocalDateTime> ceiling;

    TimeFrame(String label, UnaryOperator<LocalDateTime> next,
              UnaryOperator<LocalDateTime> floor, UnaryOperator<LocalDateTime> ceiling) {
        this.label = label;
        this.next = next;
        this.floor = floor;
        this.ceiling = ceiling;
    }

    public String getLabel() {
        return label;
    }

    public LocalDateTime next(LocalDateTime timestamp) {
        return next.apply(timestamp);
    }

    public LocalDateTime floor(LocalDateTime timestamp) {
        return floor.apply(timestamp);
    }

    public LocalDateTime ceiling(LocalDateTime timestamp) {
        return ceiling.apply(timestamp);
    }

    // :(
    public static TimeFrame get(@NonNull String label) {
        Optional<TimeFrame> t = Arrays.stream(TimeFrame.class.getEnumConstants())
                .filter(tf -> tf.label.equals(label))
                .findFirst();
        if (t.isPresent())
            return t.get();
        throw new IllegalArgumentException("Searching for an non-existent value: " + label);
    }

    @Override
    public String toString() {
        return label;
    }
}
