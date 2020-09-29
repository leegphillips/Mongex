package org.github.leegphillips.mongex;

import lombok.NonNull;
import lombok.ToString;

import java.util.Arrays;
import java.util.Optional;

@SuppressWarnings("unused")
@ToString
public enum TimeFrame {
    FIVE_SECONDS("5s"),
    TEN_SECONDS("10s"),
    THIRTY_SECONDS("30s"),
    ONE_MINUTE("1m"),
    FIVE_MINUTES("5m"),
    TEN_MINUTES("10m"),
    FIFTEEN_MINUTES("15m"),
    THIRTY_MINUTES("30m"),
    ONE_HOUR("1h"),
    THREE_HOURS("3h"),
    FOUR_HOURS("4h"),
    EIGHT_HOURS("8h"),
    ONE_DAY("1D"),
    ONE_WEEK("1W"),
    ONE_MONTH("1M");

    public static final String ATTR_NAME = "timeframe";

    private final String label;

    TimeFrame(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
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
}
