package com.github.leegphillips.mongex.dataLayer;

import lombok.ToString;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Comparator.comparing;

@ToString
public class Change implements Serializable {
    private LocalDateTime timestamp;
    private List<Delta> deltas;

    public Change(LocalDateTime timestamp, Delta first) {
        this.timestamp = timestamp;
        this.deltas = new ArrayList<>();
        deltas.add(first);
    }

    public void add(Delta delta) {
        if (!deltas.stream().anyMatch(delta1 -> delta.getPair().equals(delta1))) {
            deltas.add(delta);
        }
    }

    public void coalesce(Change latest) {
        timestamp = latest.timestamp;
        deltas.replaceAll(delta -> latest.deltas.stream()
                                                    .filter(d -> delta.getPair().equals(d.getPair()))
                                                    .findFirst()
                                                    .orElse(delta));

        List<Delta> toAdd = new ArrayList<>();
        for (Delta d1 : latest.deltas) {
            boolean add = true;
            for (Delta d2 : deltas) {
                if (d1.getPair().getLabel().equals(d2.getPair().getLabel())) {
                    add = false;
                    break;
                }
            }
            if (add)
                toAdd.add(d1);
        }
        deltas.addAll(toAdd);

        deltas.sort(comparing(o -> o.getPair().getLabel()));
    }
}
