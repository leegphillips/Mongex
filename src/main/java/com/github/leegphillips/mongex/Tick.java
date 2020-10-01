package com.github.leegphillips.mongex;

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
    private final boolean error;
    private final boolean inverted;

    private Tick(LocalDateTime timestamp, BigDecimal bid, BigDecimal ask) {
        this.timestamp = timestamp;
        this.inverted = bid.compareTo(ask) > 0;
        this.bid = inverted ? ask : bid;
        this.ask = inverted ? bid : ask;
        this.mid = ask.add(bid).divide(BigDecimal.valueOf(2), 4, RoundingMode.HALF_EVEN);
        this.error = ask.compareTo(BigDecimal.ZERO) < 0 || bid.compareTo(BigDecimal.ZERO) < 0;
    }

    public static Tick create(@NonNull CSVRecord record) {
        LocalDateTime timestamp = LocalDateTime.parse(record.get(0), STR2DATE);
        BigDecimal ask = new BigDecimal(record.get(1).trim());
        BigDecimal bid = new BigDecimal(record.get(2).trim());
        return new Tick(timestamp, bid, ask);
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

    public boolean isInverted() {
        return inverted;
    }
}