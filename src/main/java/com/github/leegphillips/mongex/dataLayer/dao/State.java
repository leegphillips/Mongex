package com.github.leegphillips.mongex.dataLayer.dao;

import org.bson.Document;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class State {
    public static final String INSTRUMENT_ATTR_NAME = "instrument";
    public static final String SMA_ATTR_NAME = "sma";
    public static final State END = new State(null, null, null);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    private final CurrencyPair pair;
    private final LocalDateTime timestamp;
    private final Map<Integer, BigDecimal> values;

    public State(CurrencyPair pair, LocalDateTime timestamp, Map<Integer, BigDecimal> values) {
        this.pair = pair;
        this.timestamp = timestamp;
        this.values = values;
    }

    public static State fromDocument(Document doc) {
        LocalDateTime timestamp = LocalDateTime.parse(doc.getString(Candle.TIMESTAMP_ATTR_NAME), FORMATTER);
        CurrencyPair pair = CurrencyPair.get(doc.getString(INSTRUMENT_ATTR_NAME));

//        Document smas = doc.get(SMA_ATTR_NAME, Document.class);
//        Map<Integer, BigDecimal> values = smas.keySet().stream()
//                .collect(toMap(key -> Integer.valueOf(key), key -> smas.get(key, Decimal128.class).bigDecimalValue(), (o1, o2) -> o1, TreeMap::new));

        Map<Integer, BigDecimal> values = Collections.EMPTY_MAP;

        return new State(pair, timestamp, values);
    }

    public Document toDocument() {
        Document doc = new Document();

        doc.append(Candle.TIMESTAMP_ATTR_NAME, timestamp.format(FORMATTER));
        doc.append(INSTRUMENT_ATTR_NAME, pair.getLabel());
        doc.append(SMA_ATTR_NAME, values.entrySet().stream().collect(toMap(entry -> entry.getKey().toString(), Map.Entry::getValue)));

        return doc;
    }

    public CurrencyPair getPair() {
        return pair;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public Map<Integer, BigDecimal> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "State{" +
                "pair=" + pair +
                ", timestamp=" + timestamp +
                ", values=" + values +
                '}';
    }
}
