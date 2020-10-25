package com.github.leegphillips.mongex.dataLayer;


import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

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

    public List<Delta> getDeltas() {
        return deltas;
    }

    @Override
    public String toString() {
        deltas.sort(comparing(o -> o.getPair().getLabel()));
        return "Change{" +
                "timestamp=" + timestamp +
                ", deltas(" + deltas.size() + ")=" + deltas +
                '}';
    }

    public String toCSV() {
        StringBuilder builder = new StringBuilder();
        builder.append(timestamp);
        builder.append(", ");
//        for (Delta d : deltas) {
//            builder.append(d.getValue());
//            builder.append(", ");
//        }

        Delta eurusd = deltas.stream().filter(delta -> delta.getPair().getLabel().equals("EURUSD")).findFirst().orElseThrow(IllegalStateException::new);
        builder.append(eurusd.getValue());
        builder.append(", ");

        Delta usdchf = deltas.stream().filter(delta -> delta.getPair().getLabel().equals("USDCHF")).findFirst().orElseThrow(IllegalStateException::new);
        builder.append(usdchf.getValue());
        builder.append(", ");

        builder.append("\n");
        return builder.toString();
    }
}
