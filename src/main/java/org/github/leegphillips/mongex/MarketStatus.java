package org.github.leegphillips.mongex;

public enum MarketStatus {
    OPEN("Open"),
    CLOSED("Closed"),
    NOT_STARTED("Not started");

    private final String label;

    MarketStatus(String label) {
        this.label = label;
    }
}
