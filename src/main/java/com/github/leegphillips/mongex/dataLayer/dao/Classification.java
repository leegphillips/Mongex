package com.github.leegphillips.mongex.dataLayer.dao;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;

public class Classification {
    public static final Classification CLOSE = new Classification(false, EMPTY_LIST);

    private final boolean up;
    private final List<State> states;
    private final LocalDateTime timestamp;

    public Classification(boolean up, List<State> states) {
        this.up = up;
        this.states = states;
        this.timestamp = states.stream()
                .map(State::getTimestamp)
                .filter(ts -> !ts.isEqual(LocalDateTime.MIN))
                .findFirst().orElse(LocalDateTime.MIN);
    }

    public String toHeaders() {
        StringBuilder result = new StringBuilder();

// remove until we can work out how to pop from data set
//        result.append("Timestamp,");
        result.append("Next");

        for (State state : states) {
            for (Integer i : state.getValues().keySet()) {
                result.append(",");
                result.append(state.getPair().getLabel());
                result.append("SMA");
                result.append(i.toString());
            }
        }

        return result.toString();
    }

    public String toCSV() {
        StringBuilder result = new StringBuilder();

// remove until we can work out how to pop from data set
//        result.append(timestamp);
//        result.append(", ");
        result.append(up ? "1" : "0");
        for (State state : states) {
            for (BigDecimal value : state.getValues().values()) {
                result.append(", ");
                result.append(value.toPlainString());
            }
        }
        return result.toString();
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }
}

