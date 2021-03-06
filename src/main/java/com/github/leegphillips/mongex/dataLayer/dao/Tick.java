package com.github.leegphillips.mongex.dataLayer.dao;

import lombok.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Tick {
    private static final DateTimeFormatter STR2DATE = DateTimeFormatter.ofPattern("yyyyMMdd HHmmssSSS");

    public final static Tick POISON = Tick.create(CurrencyPair.get("POISON"), LocalDateTime.now().format(STR2DATE), "0", "0");

    private final LocalDateTime timestamp;
    private final BigDecimal bid;
    private final BigDecimal ask;
    private final CurrencyPair pair;

    private final boolean error;
    private final boolean inverted;

    public Tick(CurrencyPair pair, LocalDateTime timestamp, BigDecimal bid, BigDecimal ask) {
        this.pair = pair;
        this.timestamp = timestamp;
        this.inverted = bid.compareTo(ask) > 0;
        this.bid = inverted ? ask : bid;
        this.ask = inverted ? bid : ask;
        this.error = ask.compareTo(BigDecimal.ZERO) < 0 || bid.compareTo(BigDecimal.ZERO) < 0;
    }

    public static Tick create(@NonNull CurrencyPair pair, @NonNull String ts, @NonNull String a, @NonNull String b) {
        LocalDateTime timestamp = LocalDateTime.parse(ts, STR2DATE);
        BigDecimal ask = new BigDecimal(a.trim());
        BigDecimal bid = new BigDecimal(b.trim());
        return new Tick(pair, timestamp, bid, ask);
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
        return ask.add(bid).divide(BigDecimal.valueOf(2), 4, RoundingMode.HALF_EVEN);
    }

    public boolean isError() {
        return error;
    }

    public boolean isInverted() {
        return inverted;
    }

    public CurrencyPair getPair() {
        return pair;
    }

    @Override
    public String toString() {
        return "Tick{" +
                "timestamp=" + timestamp +
                ", mid=" + getMid() +
                ", pair=" + pair +
                '}';
    }
}
