package com.github.leegphillips.mongex.dataLayer;

import lombok.NonNull;
import lombok.ToString;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static java.lang.String.format;

@ToString
public class Candle {
    private static final Logger LOG = LoggerFactory.getLogger(Candle.class);

    public static final String TIMESTAMP_ATTR_NAME = "timestamp";
    public static final String OPEN_ATTR_NAME = "open";
    public static final String HIGH_ATTR_NAME = "high";
    public static final String LOW_ATTR_NAME = "low";
    public static final String CLOSE_ATTR_NAME = "close";
    public static final String MID_ATTR_NAMR = "mid";
    public static final String TICK_COUNT_ATTR_NAME = "tick count";
    public static final String ERROR_COUNT_ATTR_NAME = "error count";
    public static final String DUPLICATES_COUNT_ATTR_NAME = "duplicates count";
    public static final String INVERSION_COUNT_ATTR_NAME = "inversion count";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private final TimeFrame timeFrame;
    private final CurrencyPair pair;

    // ending timestamp of the candle - close time
    private final LocalDateTime timestamp;
    private final BigDecimal open;
    private final BigDecimal high;
    private final BigDecimal low;
    private final BigDecimal close;
    private final BigDecimal mid;

    // number of ticks used to compose the candle, if 0 then candle is generated from previous candle
    private final int tickCount;

    // number of ticks with errors detected - usually bid/ask == 0
    private final int errorCount;

    // number of ticks where bid/ask needed to be inverted to be +ve
    private final int inversionCount;

    // number of timestamp collisions in composing ticks - if > 0 then may be issues with open/close
    private final int duplicatesCount;

    private Candle(TimeFrame timeFrame, CurrencyPair pair, LocalDateTime timestamp, BigDecimal open, BigDecimal high,
                   BigDecimal low, BigDecimal close, BigDecimal mid, int tickCount, int duplicatesCount, int errorCount,
                   int inversionCount) {
        this.timeFrame = timeFrame;
        this.pair = pair;
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.mid = mid;
        this.tickCount = tickCount;
        this.duplicatesCount = duplicatesCount;
        this.errorCount = errorCount;
        this.inversionCount = inversionCount;
    }

    public static Candle create(@NonNull List<Tick> ticks, @NonNull CurrencyPair pair,
                                @NonNull TimeFrame tickSize, @NonNull LocalDateTime batchCeiling) {
        int duplicates = ticks.size() - (int) ticks.stream().map(Tick::getTimestamp).distinct().count();

        Candle candle = ticks.parallelStream()
                .map(tick -> tickValidator(tick, batchCeiling))
                .map(tick -> tickMapper(tickSize, pair, tick))
                .reduce(Candle::combiner)
                .orElseThrow(() -> new IllegalStateException(format("Candles need at least one tick %s %s %s", pair.getLabel(), ticks.size(), batchCeiling)));

        return new Candle(candle.timeFrame, candle.pair, batchCeiling, candle.open, candle.high, candle.low,
                candle.close, candle.mid, candle.tickCount, duplicates, candle.errorCount, candle.inversionCount);
    }

    public static Candle create(@NonNull Document doc) {
        TimeFrame timeframe = TimeFrame.get(doc.getString(TimeFrame.ATTR_NAME));
        CurrencyPair pair = new CurrencyPair(doc.getString(CurrencyPair.ATTR_NAME));
        LocalDateTime timestamp = LocalDateTime.parse(doc.getString(TIMESTAMP_ATTR_NAME), FORMATTER);
        BigDecimal open = doc.get(OPEN_ATTR_NAME, Decimal128.class).bigDecimalValue();
        BigDecimal high = doc.get(HIGH_ATTR_NAME, Decimal128.class).bigDecimalValue();
        BigDecimal low = doc.get(LOW_ATTR_NAME, Decimal128.class).bigDecimalValue();
        BigDecimal close = doc.get(CLOSE_ATTR_NAME, Decimal128.class).bigDecimalValue();
        BigDecimal mid = doc.get(MID_ATTR_NAMR, Decimal128.class).bigDecimalValue();
        int tickCount = doc.getInteger(TICK_COUNT_ATTR_NAME);
        int duplicatesCount = doc.getInteger(DUPLICATES_COUNT_ATTR_NAME);
        int errorCount = doc.getInteger(ERROR_COUNT_ATTR_NAME);
        int inversionCount = doc.getInteger(INVERSION_COUNT_ATTR_NAME);

        return new Candle(timeframe, pair, timestamp, open, high, low, close, mid, tickCount, duplicatesCount,
                errorCount, inversionCount);
    }

    public static Candle createForNotStarted(TimeFrame timeframe, CurrencyPair pair, LocalDateTime timestamp) {
        return new Candle(timeframe, pair, timestamp, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, 0, 0, 0, 0);
    }


    private static Tick tickValidator(Tick tick, LocalDateTime batchCeiling) {
        if (tick.getTimestamp().isAfter(batchCeiling))
            throw new IllegalStateException(format("Tick is outside the range of Candle %s %s", batchCeiling, tick.getTimestamp()));
        return tick;
    }

    private static Candle tickMapper(TimeFrame duration, CurrencyPair pair, Tick tick) {
        return new Candle(duration, pair, tick.getTimestamp(), tick.getMid(), tick.getMid(), tick.getMid(),
                tick.getMid(), tick.getMid(), tick.isInterpolated() ? 0 : 1, 0, tick.isError() ? 1 : 0, tick.isInverted() ? 1 : 0);
    }

    private static Candle combiner(Candle c1, Candle c2) {
        if (c1.timeFrame != c2.timeFrame)
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
        BigDecimal mid;
        // still a gap where one has tickcount == 0
        if (c1.tickCount == c2.tickCount) {
            mid = c1.mid.add(c2.mid).divide(BigDecimal.valueOf(2));
        } else {
            mid = (c1.mid.multiply(BigDecimal.valueOf(c1.tickCount)).add(c2.mid.multiply(BigDecimal.valueOf(c2.tickCount)))).divide(BigDecimal.valueOf(tickCount), 4, RoundingMode.HALF_EVEN);
        }
        int duplicates = c1.duplicatesCount + c2.duplicatesCount;
        if (timestampCollision)
            duplicates++;
        int inversionCount = c1.inversionCount + c2.inversionCount;

        return new Candle(c1.timeFrame, c1.pair, timestamp, open, high, low, close, mid, tickCount, duplicates, errorCount,
                inversionCount);
    }

    // TODO add tests
    public Candle generateFrom(LocalDateTime nextSlot) {
        return new Candle(timeFrame, pair, nextSlot, close, close, close, close, close, 0, duplicatesCount, errorCount,
                inversionCount);
    }

    public Document toDocument() {
        Document result = new Document();

        result.append(TimeFrame.ATTR_NAME, timeFrame.getLabel());
        result.append(CurrencyPair.ATTR_NAME, pair.getLabel());
        result.append(TIMESTAMP_ATTR_NAME, FORMATTER.format(timestamp));
        result.append(OPEN_ATTR_NAME, Decimal128.parse(open.toPlainString()));
        result.append(HIGH_ATTR_NAME, Decimal128.parse(high.toPlainString()));
        result.append(LOW_ATTR_NAME, Decimal128.parse(low.toPlainString()));
        result.append(CLOSE_ATTR_NAME, Decimal128.parse(close.toPlainString()));
        result.append(MID_ATTR_NAMR, Decimal128.parse(mid.toPlainString()));
        result.append(TICK_COUNT_ATTR_NAME, tickCount);
        result.append(DUPLICATES_COUNT_ATTR_NAME, duplicatesCount);
        result.append(ERROR_COUNT_ATTR_NAME, errorCount);
        result.append(INVERSION_COUNT_ATTR_NAME, inversionCount);

        return result;
    }

    public TimeFrame getTimeFrame() {
        return timeFrame;
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

    public BigDecimal getMid() {
        return mid;
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

    public int getInversionCount() {
        return inversionCount;
    }
}
