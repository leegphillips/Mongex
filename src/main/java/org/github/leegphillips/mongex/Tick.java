package org.github.leegphillips.mongex;

import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.csv.CSVRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@ToString
public class Tick {

    private static final DateTimeFormatter STR2DATE = DateTimeFormatter.ofPattern("yyyyMMdd HHmmssSSS");

    private final LocalDateTime timestamp;
    private final BigDecimal bid;
    private final BigDecimal ask;
    private final BigDecimal mid;
    private boolean error = false;

    private Tick(LocalDateTime timestamp, BigDecimal bid, BigDecimal ask, BigDecimal mid) {
        this.timestamp = timestamp;
        this.bid = bid;
        this.ask = ask;
        this.mid = mid;
    }

    public static Tick create(@NonNull CSVRecord record) {
        LocalDateTime timestamp = LocalDateTime.parse(record.get(0), STR2DATE);
        BigDecimal ask = new BigDecimal(record.get(1).trim());
        BigDecimal bid = new BigDecimal(record.get(2).trim());
        Tick tick = new Tick(timestamp,
                bid,
                ask,
                ask.add(bid)
                        .divide(BigDecimal.valueOf(2), 4, RoundingMode.HALF_EVEN));

// the data seems to have an issue whereby the bid and ask get swapped often
// we don't care too much about this because we are interested in the generated mid only
// assuming the values are correct by in the wrong place, we can ignore this check
//        if (tick.bid.compareTo(tick.ask) > 0) {
//            LOG.warn(String.format("%s bid greater than ask %s %s", tick.timestamp, tick.bid, tick.ask));
//            failed = true;
//        }

        tick.error = tick.ask.compareTo(BigDecimal.ZERO) < 0 || tick.bid.compareTo(BigDecimal.ZERO) < 0;

        return tick;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public BigDecimal getBid() {
        return bid;
    }

    public BigDecimal getAsk() {
        return ask;
    }

    public BigDecimal getMid() {
        return mid;
    }

    public boolean isError() {
        return error;
    }
}
