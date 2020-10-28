package com.github.leegphillips.mongex.dataLayer;


import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;

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

        List<Delta> deltas = concat(latest.stream(), of(current))
                .sorted((o1, o2) -> o1.timestamp.compareTo(o2.timestamp) * -1)
                .map(Change::getDeltas)
                .map(Map::values)
                .flatMap(Collection::stream)
                .collect(toList());

        Map<CurrencyPair, Delta> updated = new TreeMap<>();

        for (int i = 0; i < deltas.size(); i++) {
            Delta delta = deltas.get(i);
            if (remaining.contains(delta.getPair())) {
                updated.put(delta.getPair(), delta);
                remaining.remove(delta.getPair());
                if (remaining.size() == 0) {
                    LOG.info("Finish early optimisation: " + (deltas.size() - i));
                    break;
                }
            }
        }

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
