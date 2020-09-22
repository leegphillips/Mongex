package org.github.leegphillips.mongex;

import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CandleFactory {
    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    // different to the other one
    private static final SimpleDateFormat STRING_TO_DATE = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    public Document create(List<CSVRecord> records, String pair) throws ParseException {
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
        Date tickDate = STRING_TO_DATE.parse(Long.toString(first.getTimestamp()));
        candle.append("pair", pair);
        candle.append("date", FORMATTER.format(tickDate));
        candle.append("open", open);
        candle.append("high", high);
        candle.append("low", low);
        candle.append("close", close);
        candle.append("tick count", records.size());

        return candle;
    }
}
