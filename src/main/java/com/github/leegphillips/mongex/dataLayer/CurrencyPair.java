package com.github.leegphillips.mongex.dataLayer;

import lombok.NonNull;
import lombok.ToString;

import java.io.File;
import java.io.Serializable;
import java.util.Objects;

import static java.lang.String.format;

@ToString
// TODO change this to a list of static references
public class CurrencyPair implements Serializable, Comparable<CurrencyPair> {
    public static final String ATTR_NAME = "pair";

    private final String label;

    public CurrencyPair(@NonNull String label) {
        if (label.length() != 6)
            throw new IllegalArgumentException(format("Currency pair label must be 6 characters: %s", label));
        this.label = label;
    }

    public CurrencyPair(@NonNull File file) {
        this(file.getName().substring(19, 25));
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

    @Override
    public int compareTo(CurrencyPair o) {
        return label.compareTo(o.getLabel());
    }
}
