package com.github.leegphillips.mongex.dataLayer.dao;

import lombok.NonNull;
import lombok.ToString;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static java.lang.String.format;

@ToString
public class CurrencyPair implements Serializable, Comparable<CurrencyPair> {
    public static final String ATTR_NAME = "pair";

    private static final Map<String, CurrencyPair> PAIRS = new TreeMap<>();

    private final String label;

    private CurrencyPair(@NonNull String label) {
        this.label = label;
    }

    public static CurrencyPair get(@NonNull String label) {
        if (label.length() != 6)
            throw new IllegalArgumentException(format("Currency pair label must be 6 characters: %s", label));

        CurrencyPair pair = PAIRS.get(label);
        if (pair == null) {
            pair = new CurrencyPair(label);
            PAIRS.put(label, pair);
        }
        return pair;
    }

    public static CurrencyPair get(@NonNull File file) {
        return get(file.getName().substring(19, 25));
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
