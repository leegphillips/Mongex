package org.github.leegphillips.mongex;

import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.csv.CSVRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;

@ToString
public class TickFacade {
    private final long timestamp;
    private final BigDecimal bid;
    private final BigDecimal ask;
    private BigDecimal mid;

    public TickFacade(@NonNull CSVRecord record) {

        // assign first for Lombok error printing
        timestamp = Long.parseLong(record.get(0).replaceAll("\\s", ""));
        bid = new BigDecimal(record.get(2).trim());
        ask = new BigDecimal(record.get(1).trim());

        if (bid.compareTo(ask) > 0 || ask.compareTo(BigDecimal.ZERO) < 0 || bid.compareTo(BigDecimal.ZERO) < 0)
            throw new IllegalArgumentException(this.toString());
    }

    public long getTimestamp() {
        return timestamp;
    }

    public BigDecimal getBid() {
        return bid;
    }

    public BigDecimal getAsk() {
        return ask;
    }

    public BigDecimal getMid() {
        // only calculate if called
        if (mid == null) {
            mid = getAsk()
                    .add(getBid())
                    .divide(BigDecimal.valueOf(2), 4, RoundingMode.HALF_EVEN);
        }
        return mid;
    }
}
