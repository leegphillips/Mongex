package com.github.leegphillips.mongex.dataLayer.dao;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public class Classification {
    public static final Classification CLOSE = new Classification(false, null);

    private final boolean up;
    private final List<State> states;

    public Classification(boolean up, List<State> states) {
        this.up = up;
        this.states = states;
    }

    public String toCSV() {
        StringBuilder result = new StringBuilder();
        LocalDateTime timestamp = states.stream()
                .map(State::getTimestamp)
                .filter(ts -> !ts.isEqual(LocalDateTime.MIN))
                .findFirst().orElseThrow(IllegalStateException::new);
        result.append(timestamp);
        result.append(", ");
        result.append(up);
        result.append(", ");
        for (State state : states) {
            // TODO check ordering of values
            for (BigDecimal value : state.getValues().values()) {
                result.append(value.toPlainString());
                result.append(", ");
            }
        }
        return result.toString();
    }
}

