package org.github.leegphillips.mongex;

import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import java.math.BigDecimal;
import java.util.List;

public class CandleFactory {
    public Document create(List<CSVRecord> records) {
        TickFacade first = new TickFacade(records.get(0));
        BigDecimal open = first.getMid();
        BigDecimal close = new TickFacade(records.get(records.size() - 1)).getMid();
        long timestamp = first.getTimestamp();
        BigDecimal low = first.getMid();
        BigDecimal high = first.getMid();
        for (CSVRecord record : records.size() > 1 ? records.subList(1, records.size() - 1) : records) {
            TickFacade tick = new TickFacade(record);
            low = tick.getMid().min(low);
            high = tick.getMid().max(high);
        }

        Document candle = new Document();
        candle.append("timestamp", timestamp);
        candle.append("open", open);
        candle.append("high", high);
        candle.append("low", low);
        candle.append("close", close);
        candle.append("tick count", records.size());

        return candle;
    }
}
