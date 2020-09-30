package org.github.leegphillips.mongex;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class CandleEnricher {
    private static final Logger LOG = LoggerFactory.getLogger(CandleEnricher.class);

    private final TimeFrame timeFrame;

    public CandleEnricher(TimeFrame timeFrame) {
        this.timeFrame = timeFrame;
    }

    public List<Candle> enrich(@NonNull List<Candle> originals, LocalDateTime batchCeiling) {
        List<Candle> additions = new ArrayList<>();

        Candle previous = originals.get(0);
        for (Candle current : originals.subList(1, originals.size() - 1)) {
            LocalDateTime target = timeFrame.next(previous.getTimestamp());
            if (target.isBefore(current.getTimestamp())) {

            }
        }

        return concat(originals.stream(), additions.stream()).collect(toList());
    }
}
