package org.github.leegphillips.mongex;

public enum CandleSize {
    FIVE_SECONDS(5 * 1000, "5S"),
    TEN_SECONDS(10 * 1000, "10S"),
    THIRTY_SECONDS(30 * 1000, "30S"),
    ONE_MINUTE(1 * 60 * 1000, "1M"),
    FIVE_MINUTES(5 * 60 * 1000, "5M"),
    TEN_MINUTES(10 * 60 * 1000, "10M"),
    FIFTEEN_MINUTES(15 * 60 * 1000, "15M"),
    THIRTY_MINUTES(30 * 60 * 1000, "30M"),
    ONE_HOUR(1 * 60 * 60 * 1000, "1H"),
    THREE_HOURS(3 * 60 * 60 * 1000, "3H"),
    FOUR_HOURS(4 * 60 * 60 * 1000, "4H"),
    EIGHT_HOURS(8 * 60 * 60 * 1000, "8H"),
    ONE_DAY(1 * 24 * 60 * 60 * 1000, "1D"),
    ONE_WEEK(7 * 24 * 60 * 60 * 1000, "1W"),

    // don't use
    ONE_MONTH(-1, "");

    public final long millis;
    private final String label;

    CandleSize(long millis, String label) {
        this.millis = millis;
        this.label = label;
    }
}
