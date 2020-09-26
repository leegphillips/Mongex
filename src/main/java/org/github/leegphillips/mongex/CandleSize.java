package org.github.leegphillips.mongex;

import lombok.ToString;

@SuppressWarnings("unused")
@ToString
public enum CandleSize {
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

    private final String label;

    CandleSize(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }
}
