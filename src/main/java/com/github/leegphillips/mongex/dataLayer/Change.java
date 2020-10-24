package com.github.leegphillips.mongex.dataLayer;

import lombok.ToString;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

@ToString
public class Change implements Serializable {
    private LocalDateTime timestamp;
    private List<Delta> deltas;

    public Change(LocalDateTime timestamp, Delta first) {
        this.timestamp = timestamp;
        this.deltas = new ArrayList<>();
        deltas.add(first);
    }

    private Change(LocalDateTime timestamp, List<Delta> deltas) {
        this.timestamp = timestamp;
        this.deltas = deltas;
        this.deltas.sort(comparing(o -> o.getPair().getLabel()));
    }

    public void add(Delta delta) {
        if (!deltas.stream().anyMatch(delta1 -> delta.getPair().equals(delta1.getPair()))) {
            deltas.add(delta);
        }
    }

    public static Change coalesce(LocalDateTime timestamp, Change current, Change latest) {
        List<Delta> deltas = new ArrayList<>();
        deltas.addAll(latest.deltas);
        deltas.addAll(current.deltas.stream().filter(d1 -> !deltas.stream().anyMatch(d2 -> d2.getPair().equals(d1.getPair()))).collect(toList()));
        return new Change(timestamp, deltas);
    }
    public static Change coalesce(Change current, Change latest) {
        LocalDateTime timestamp = latest.timestamp;
        return coalesce(timestamp, current, latest);
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
