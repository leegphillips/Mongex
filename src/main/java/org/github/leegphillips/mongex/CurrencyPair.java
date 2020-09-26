package org.github.leegphillips.mongex;

import lombok.NonNull;
import lombok.ToString;

import static java.lang.String.format;

@ToString
public class CurrencyPair {
    private final String label;

    public CurrencyPair(@NonNull String label) {
        if (label.length() != 6)
            throw new IllegalArgumentException(format("Currency pair label must be 6 characters: %s", label));
        this.label = label;
    }

    public String getLabel() {
        return label;
    }
}
