package org.github.leegphillips.mongex;

import lombok.NonNull;
import lombok.ToString;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static java.lang.String.format;

@ToString
public class Candle {
    private static final Logger LOG = LoggerFactory.getLogger(Candle.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private final CandleSize duration;
    private final CurrencyPair pair;
    private final LocalDateTime timestamp;
    private final BigDecimal open;
    private final BigDecimal high;
    private final BigDecimal low;
    private final BigDecimal close;
    private final int tickCount;
    private final int errorCount;
    private final long duplicatesCount;


    private Candle(CandleSize duration, CurrencyPair pair, LocalDateTime timestamp, BigDecimal open, BigDecimal high,
                   BigDecimal low, BigDecimal close, int tickCount, long duplicatesCount, int errorCount) {
        this.duration = duration;
        this.pair = pair;
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.tickCount = tickCount;
        this.duplicatesCount = duplicatesCount;
        this.errorCount = errorCount;
    }

    public static Candle create(@NonNull List<Tick> ticks, @NonNull CurrencyPair pair,
                                @NonNull CandleSize tickSize, @NonNull LocalDateTime batchCeiling) {
        long duplicates = ticks.size() - ticks.stream().map(Tick::getTimestamp).distinct().count();

        Candle candle = ticks.parallelStream()
                .map(tick -> tickValidator(tick, batchCeiling))
                .map(tick -> tickMapper(tickSize, pair, tick))
                .reduce(Candle::combiner)
                .orElseThrow(() -> new IllegalStateException(format("Candles need at least one tick %s %s %s", pair.getLabel(), ticks.size(), batchCeiling)));

        return new Candle(candle.duration, candle.pair, batchCeiling, candle.open, candle.high, candle.low,
                candle.close, candle.tickCount, duplicates, candle.errorCount);
    }

    private static Tick tickValidator(Tick tick, LocalDateTime batchCeiling) {
        if (tick.getTimestamp().isAfter(batchCeiling))
            throw new IllegalStateException(format("Tick is outside the range of Candle %s %s", batchCeiling, tick.getTimestamp()));
        return tick;
    }

    private static Candle tickMapper(CandleSize duration, CurrencyPair pair, Tick tick) {
        return new Candle(duration, pair, tick.getTimestamp(), tick.getMid(), tick.getMid(), tick.getMid(), tick.getMid(),
                1, 0, tick.isError() ? 1 : 0);
    }

    private static Candle combiner(Candle c1, Candle c2) {
        if (c1.duration != c2.duration)
            throw new IllegalStateException("Cannot combine candles with different durations:" + c1 + " " + c2);

        if (!c1.pair.getLabel().contentEquals(c2.pair.getLabel()))
            throw new IllegalStateException("Cannot combine candles from different pairs:" + c1 + " " + c2);

        int tickCount = c1.tickCount + c2.tickCount;
        int errorCount = c1.errorCount + c2.errorCount;
        BigDecimal low = c1.low.min(c2.low);
        BigDecimal high = c1.high.max(c2.high);
        boolean timestampCollision = c1.timestamp.isEqual(c2.timestamp);
        boolean c1IsBefore = c1.timestamp.isBefore(c2.timestamp);
        LocalDateTime timestamp = c1IsBefore ? c2.timestamp : c1.timestamp;
        BigDecimal open = timestampCollision || c1IsBefore ? c1.open : c2.open;
        BigDecimal close = timestampCollision ? c1.close : c1IsBefore ? c2.close : c1.close;
        long duplicates = c1.duplicatesCount + c2.duplicatesCount;
        if (timestampCollision)
            duplicates++;

        return new Candle(c1.duration, c1.pair, timestamp, open, high, low, close, tickCount, duplicates, errorCount);
    }

    public Document toDocument() {
        Document result = new Document();

        result.append("duration", duration.getLabel());
        result.append("pair", pair.getLabel());
        result.append("timestamp", FORMATTER.format(timestamp));
        result.append("open", open);
        result.append("high", high);
        result.append("low", low);
        result.append("close", close);
        result.append("tick count", tickCount);
        result.append("duplicate count", duplicatesCount);
        result.append("error count", errorCount);

        return result;
    }

    public CandleSize getDuration() {
        return duration;
    }

    public CurrencyPair getPair() {
        return pair;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public BigDecimal getOpen() {
        return open;
    }

    public BigDecimal getHigh() {
        return high;
    }

    public BigDecimal getLow() {
        return low;
    }

    public BigDecimal getClose() {
        return close;
    }

    public int getTickCount() {
        return tickCount;
    }

    public long getDuplicatesCount() {
        return duplicatesCount;
    }

    public int getErrorCount() {
        return errorCount;
    }
}
