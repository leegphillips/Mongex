package org.github.leegphillips.mongex;

import lombok.NonNull;
import org.apache.commons.csv.CSVRecord;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public class CandleFactory {
    public Candle create(@NonNull List<CSVRecord> records, @NonNull String pair, @NonNull String tickSize, @NonNull LocalDateTime batchCeiling) {

        // add check that tick isn't past batch ceiling

        TickFacade first = new TickFacade(records.get(0));
        BigDecimal open = first.getMid();
        BigDecimal close = new TickFacade(records.get(records.size() - 1)).getMid();
        BigDecimal low = first.getMid();
        BigDecimal high = first.getMid();
        for (CSVRecord record : records.size() > 1 ? records.subList(1, records.size() - 1) : records) {
            TickFacade tick = new TickFacade(record);
            low = tick.getMid().min(low);
            high = tick.getMid().max(high);
        }

        return new Candle(tickSize, pair, batchCeiling, open, high, low, close, records.size());
    }
}
