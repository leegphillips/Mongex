package com.github.leegphillips.mongex;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class CandleBatcher implements Callable<List<Candle>> {

    private final CurrencyPair pair;
    private final CandleSpecification candleSpecification;
    private final CSVParser records;

    public CandleBatcher(CurrencyPair pair, CandleSpecification candleSpecification, CSVParser records) {
        this.pair = pair;
        this.candleSpecification = candleSpecification;
        this.records = records;
    }

    @Override
    public List<Candle> call() {
        List<Candle> candles = new ArrayList<>();
        List<Tick> batch = new ArrayList<>();
        LocalDateTime batchFloor = null;
        LocalDateTime batchCeiling = null;
        TimeFrame tickSize = candleSpecification.getTickSize();

        for (CSVRecord record : records) {
            Tick tick = Tick.create(record);
            LocalDateTime time = tick.getTimestamp();
            if (batchFloor == null) {
                batchFloor = candleSpecification.getFloor(tick.getTimestamp());
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }

            if (!time.isBefore(batchCeiling)) {
                candles.add(Candle.create(batch, pair, tickSize, batchCeiling));
                batch = new ArrayList<>();
                batchFloor = candleSpecification.getFloor(time);
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }
            batch.add(tick);
        }
        candles.add(Candle.create(batch, pair, tickSize, batchCeiling));

        return candles;
    }
}
