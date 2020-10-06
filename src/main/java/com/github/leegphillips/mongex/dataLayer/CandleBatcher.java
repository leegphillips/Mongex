package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.ma.SimpleMovingAverage;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class CandleBatcher implements Callable<List<Candle>> {

    private final CurrencyPair pair;
    private final CandleSpecification candleSpecification;
    private final Iterable<Tick> ticks;
    private final List<SimpleMovingAverage> sMAs;

    public CandleBatcher(CurrencyPair pair, CandleSpecification candleSpecification, Iterable<Tick> ticks, List<SimpleMovingAverage> sMAs) {
        this.pair = pair;
        this.candleSpecification = candleSpecification;
        this.ticks = ticks;
        this.sMAs = sMAs;
    }

    @Override
    public List<Candle> call() {
        List<Candle> candles = new ArrayList<>();
        List<Tick> batch = new ArrayList<>();
        LocalDateTime batchFloor = null;
        LocalDateTime batchCeiling = null;
        TimeFrame tickSize = candleSpecification.getTickSize();

        for (Tick tick : ticks) {
            LocalDateTime time = tick.getTimestamp();
            if (batchFloor == null) {
                batchFloor = candleSpecification.getFloor(tick.getTimestamp());
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }

            if (time.compareTo(batchCeiling) > 0) {
                candles.add(Candle.create(batch, pair, tickSize, batchCeiling, sMAs));
                batch = new ArrayList<>();
                batchFloor = candleSpecification.getFloor(time);
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }
            batch.add(tick);
        }
        candles.add(Candle.create(batch, pair, tickSize, batchCeiling, sMAs));

        return candles;
    }
}
