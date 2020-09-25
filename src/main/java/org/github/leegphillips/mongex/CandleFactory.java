package org.github.leegphillips.mongex;

import lombok.NonNull;
import org.apache.commons.csv.CSVRecord;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class CandleFactory {
    public Candle create(@NonNull List<CSVRecord> records, @NonNull String pair, @NonNull String tickSize, @NonNull LocalDateTime batchCeiling) {
        if (pair.isEmpty() || tickSize.isEmpty())
            throw new IllegalArgumentException(pair + " " + tickSize);

        List<Tick> tickList = records.stream().map(Tick::new).collect(toList());
        Optional<Tick> head = tickList.stream().findFirst();
        if (head.isPresent()) {
            return tickList.stream()
                    .skip(1)
                    .reduce(new Candle(tickSize, pair, batchCeiling, head.get()), Candle::addTick, Candle::combiner);
        } else {
            throw new IllegalArgumentException("Candles must comprise at least one tick: " + pair + " " + batchCeiling);
        }
    }
}
