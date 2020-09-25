package org.github.leegphillips.mongex;

import lombok.NonNull;
import lombok.ToString;
import org.bson.Document;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@ToString
public class Candle {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private final String duration;
    private final String pair;
    private final LocalDateTime timestamp;
    private final BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private int tickCount = 1;

    public Candle(@NonNull String tickSize, @NonNull String pair, @NonNull LocalDateTime batchCeiling, @NonNull Tick tick) {
        if (tick.getTimestamp().isAfter(batchCeiling))
            throw new IllegalStateException("Tick cannot be after candle end: " + tick + " " + batchCeiling);
        this.duration = tickSize;
        this.pair = pair;
        this.timestamp = batchCeiling;
        this.open = tick.getMid();
        this.high = tick.getMid();
        this.low = tick.getMid();
        this.close = tick.getMid();
    }

    public static Candle combiner(Candle c1, Candle c2) {
        throw new IllegalStateException("Don't call in parallel streams or the close price will be wrong");
    }

    public Candle addTick(Tick tick) {
        if (tick.getTimestamp().isAfter(timestamp))
            throw new IllegalStateException("Tick cannot be after candle end: " + tick + " " + timestamp);
        low = tick.getMid().min(low);
        high = tick.getMid().max(high);
        close = tick.getMid();
        tickCount++;
        return this;
    }

    public Document toDocument() {
        Document result = new Document();

        result.append("duration", duration);
        result.append("pair", pair);
        result.append("timestamp", FORMATTER.format(timestamp));
        result.append("open", open);
        result.append("high", high);
        result.append("low", low);
        result.append("close", close);
        result.append("tick count", tickCount);

        return result;
    }
}
