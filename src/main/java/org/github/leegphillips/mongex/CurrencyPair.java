package org.github.leegphillips.mongex;

import lombok.NonNull;
import lombok.ToString;

import java.util.Objects;

import static java.lang.String.format;

@ToString
public class CurrencyPair {
    public static final String ATTR_NAME = "pair";

    private final String label;

    public CurrencyPair(@NonNull String label) {
        if (label.length() != 6)
            throw new IllegalArgumentException(format("Currency pair label must be 6 characters: %s", label));
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CurrencyPair that = (CurrencyPair) o;
        return label.equals(that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label);
    }
}
