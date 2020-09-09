package org.github.leegphillips.mongex;

import org.apache.commons.csv.CSVRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class TickFacade {
    private final long timestamp;
    private String bidInternal;
    private String askInternal;
    private BigDecimal bid;
    private BigDecimal ask;
    private BigDecimal mid;

    public TickFacade(CSVRecord record) {
        timestamp = Long.parseLong(record.get(0).replaceAll("\\s", ""));
        bidInternal = record.get(2).trim();
        askInternal = record.get(1).trim();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public BigDecimal getBid() {
        if (bid == null) {
            bid = new BigDecimal(bidInternal);
            bidInternal = null;
        }
        return bid;
    }

    public BigDecimal getAsk() {
        if (ask == null) {
            ask = new BigDecimal(askInternal);
            askInternal = null;
        }
        return ask;
    }

    public BigDecimal getMid() {
        // only calculate if called
        if (mid == null) {
            mid = getAsk()
                    .add(getBid())
                    .divide(BigDecimal.valueOf(2), RoundingMode.HALF_EVEN);
        }
        return mid;
    }
}
