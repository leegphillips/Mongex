package com.github.leegphillips.mongex;

import lombok.NonNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Optional;
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
    private final UnaryOperator<LocalDateTime> operator;

    TimeFrame(String label, UnaryOperator<LocalDateTime> operator) {
        this.label = label;
        this.operator = operator;
    }

    public String getLabel() {
        return label;
    }

    public LocalDateTime next(LocalDateTime timestamp) {
        return operator.apply(timestamp);
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
