package com.github.leegphillips.mongex.dataLayer;


import lombok.ToString;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.TreeMap;

@ToString
public class Change implements Serializable {
    private LocalDateTime timestamp;
    private Map<CurrencyPair, Delta> deltas;

    public Change(LocalDateTime timestamp, Map<CurrencyPair, Delta> deltas) {
        this.timestamp = timestamp;
        this.deltas = deltas;
    }

    public static Change coalesce(Change current, Change latest) {
        LocalDateTime timestamp = latest.timestamp;
        Map<CurrencyPair, Delta> deltas = new TreeMap<>(latest.deltas);
        current.deltas.values().stream()
                .filter(delta -> !deltas.containsKey(delta.getPair()))
                .forEach(delta -> deltas.put(delta.getPair(), delta));
        return new Change(timestamp, deltas);
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
