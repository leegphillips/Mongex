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

        result.append("Timestamp, ");
        result.append("Next, ");

        for (State state : states) {
            for (Integer i : state.getValues().keySet()) {
                result.append(state.getPair().getLabel());
                result.append(" SMA");
                result.append(i.toString());
                result.append(", ");
            }
        }

        return result.toString();
    }

    public String toCSV() {
        StringBuilder result = new StringBuilder();

        result.append(timestamp);
        result.append(", ");
        result.append(up ? "1" : "0");
        result.append(", ");
        for (State state : states) {
            for (BigDecimal value : state.getValues().values()) {
                result.append(value.toPlainString());
                result.append(", ");
            }
        }
        return result.toString();
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }
}

