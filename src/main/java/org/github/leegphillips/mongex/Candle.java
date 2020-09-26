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
import java.util.Optional;

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

    private Candle(CandleSize duration, CurrencyPair pair, LocalDateTime timestamp, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, int tickCount) {
        this.duration = duration;
        this.pair = pair;
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.tickCount = tickCount;
    }

    public static Optional<Candle> create(@NonNull List<Tick> ticks, @NonNull CurrencyPair pair, @NonNull CandleSize tickSize, @NonNull LocalDateTime batchCeiling) {
        long uniqueTimestamps = ticks.stream().map(Tick::getTimestamp).distinct().count();
        int totalTicks = ticks.size();
        long difference = totalTicks - uniqueTimestamps;
        // too much noise
        if (difference > 50)
            LOG.warn(format("%d duplicate ticks found in %s %s", difference, pair.getLabel(), batchCeiling));

        Optional<Candle> optionalCandle = ticks.parallelStream()
                .map(tick -> tickValidator(tick, batchCeiling))
                .map(tick -> tickMapper(tickSize, pair, tick))
                .reduce(Candle::combiner);

        if (optionalCandle.isPresent()) {
            Candle candle = optionalCandle.get();
            return Optional.of(new Candle(candle.duration, candle.pair, batchCeiling, candle.open, candle.high, candle.low, candle.close, candle.tickCount));
        } else {
            LOG.warn(format("No valid ticks found for Candle %s %s", pair.getLabel(), batchCeiling));
            return optionalCandle;
        }
    }

    private static Tick tickValidator(Tick tick, LocalDateTime batchCeiling) {
        if (tick.getTimestamp().isAfter(batchCeiling))
            throw new IllegalStateException(format("Tick is outside the range of Candle %s %s", batchCeiling, tick.getTimestamp()));
        return tick;
    }

    private static Candle tickMapper(CandleSize duration, CurrencyPair pair, Tick tick) {
        LOG.trace(format("Mapping %s %s %s", duration, pair, tick));
        return new Candle(duration, pair, tick.getTimestamp(), tick.getMid(), tick.getMid(), tick.getMid(), tick.getMid(), 1);
    }

    private static Candle combiner(Candle c1, Candle c2) {
        LOG.trace(format("Combining %s %s", c1, c2));
        if (c1.duration != c2.duration)
            throw new IllegalStateException("Cannot combine candles with different durations:" + c1 + " " + c2);

        if (!c1.pair.getLabel().contentEquals(c2.pair.getLabel()))
            throw new IllegalStateException("Cannot combine candles from different pairs:" + c1 + " " + c2);

        boolean c1IsBefore = c1.timestamp.isBefore(c2.timestamp);
        LocalDateTime timestamp = c1IsBefore ? c2.timestamp : c1.timestamp;
        BigDecimal low = c1.low.min(c2.low);
        BigDecimal open = c1IsBefore ? c1.open : c2.open;
        BigDecimal high = c1.high.max(c2.high);
        BigDecimal close = c1IsBefore ? c2.close : c1.close;
        int tickCount = c1.tickCount + c2.tickCount;

        return new Candle(c1.duration, c1.pair, timestamp, open, high, low, close, tickCount);
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
}
