package com.github.leegphillips.mongex.dataLayer;


import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@ToString
public class Change implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Change.class);

    private static final Set<CurrencyPair> ALL = Utils.getAllCurrencies().collect(Collectors.toSet());

    private LocalDateTime timestamp;
    private final Map<CurrencyPair, Delta> deltas;

    public Change(LocalDateTime timestamp, Map<CurrencyPair, Delta> deltas) {
        this.timestamp = timestamp;
        this.deltas = deltas;
    }

    public static Change coalesce(Change current, List<Change> latest) {
        Set<CurrencyPair> remaining = new HashSet<>(ALL);

        Map<CurrencyPair, Delta> updated = new TreeMap<>();
        if (!latest.isEmpty()) {
            for (int i = latest.size() - 1; i >= 0; i--) {
                Change tail = latest.get(i);
                for (Delta delta : tail.deltas.values()) {
                    if (remaining.contains(delta.getPair())) {
                        updated.put(delta.getPair(), delta);
                        remaining.remove(delta.getPair());
                    }
                    if (remaining.size() == 0) {
                        if (i > 0)
                            LOG.info("Finish early optimisation: " + i);
                        break;
                    }
                }
            }
        }

        current.deltas.values().stream()
                .filter(delta -> !updated.containsKey(delta.getPair()))
                .forEach(delta -> updated.put(delta.getPair(), delta));

        return new Change(latest.isEmpty() ? null : latest.get(latest.size() - 1).timestamp, updated);
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public Map<CurrencyPair, Delta> getDeltas() {
        return deltas;
    }
}
