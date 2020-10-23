package com.github.leegphillips.mongex.dataLayer;

import java.io.Serializable;
import java.math.BigDecimal;

public class Delta implements Serializable {
    private final CurrencyPair pair;
    private final BigDecimal value;

    public Delta(CurrencyPair pair, BigDecimal value) {
        this.pair = pair;
        this.value = value;
    }

    public CurrencyPair getPair() {
        return pair;
    }

    public BigDecimal getValue() {
        return value;
    }

    @Override
    public String toString() {
        return pair.getLabel() + ":" + value;
    }
}
