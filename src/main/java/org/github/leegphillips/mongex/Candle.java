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
    private final BigDecimal high;
    private final BigDecimal low;
    private final BigDecimal close;
    private final int tickCount;

    public Candle(@NonNull String duration, @NonNull String pair, @NonNull LocalDateTime timestamp,
                  @NonNull BigDecimal open, @NonNull BigDecimal high, @NonNull BigDecimal low,
                  @NonNull BigDecimal close, int tickCount) {

        // assign these before assertions so Lombok has values for toString on error
        this.duration = duration;
        this.pair = pair;
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.tickCount = tickCount;

        if (tickCount < 1 ||
                high.compareTo(low) < 0 ||
                open.compareTo(BigDecimal.ZERO) < 0 ||
                high.compareTo(BigDecimal.ZERO) < 0 ||
                low.compareTo(BigDecimal.ZERO) < 0 ||
                close.compareTo(BigDecimal.ZERO) < 0)
            throw new IllegalArgumentException(this.toString());
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
