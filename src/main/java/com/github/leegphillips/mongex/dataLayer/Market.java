package com.github.leegphillips.mongex.dataLayer;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class Market {
    public static final String MARKET_COUNT_ATTR_NAME = "market count";
    public static final String ATTR_NAME = "market";
    private static final Logger LOG = LoggerFactory.getLogger(Market.class);
    private final List<CurrencyPair> marketComponents;
    private final List<Candle> candles = new ArrayList<>();

    public Market(List<CurrencyPair> marketComponents) {
        this.marketComponents = marketComponents;
    }

    public void add(Candle candle) {
        candles.add(candle);
    }

    public Document toDocument() {
        Candle first = candles.get(0);
        LocalDateTime timestamp = first.getTimestamp();
        TimeFrame timeframe = first.getTimeFrame();

        List<CurrencyPair> existingPairs = candles.stream().map(candle -> candle.getPair()).collect(toList());
        List<CurrencyPair> missingPairs = marketComponents.stream().filter(pair -> !existingPairs.contains(pair)).collect(toList());

        candles.addAll(missingPairs.stream().map(pair -> Candle.createForNotStarted(timeframe, pair, timestamp)).collect(toList()));

        candles.sort(comparing(o -> o.getPair().getLabel()));

        Document doc = new Document();
        doc.append(Candle.TIMESTAMP_ATTR_NAME, candles.get(0).getTimestamp());
        doc.append(TimeFrame.ATTR_NAME, candles.get(0).getTimeFrame().getLabel());
        doc.append(Market.MARKET_COUNT_ATTR_NAME, candles.size());
        doc.append(Market.ATTR_NAME, candles.stream()
                .map(Candle::toDocument)
                .map(this::removeUnwantedFields)
                .collect(toList()));

        return doc;
    }

    private Document removeUnwantedFields(Document doc) {
        doc.remove(Candle.TIMESTAMP_ATTR_NAME);
        doc.remove(TimeFrame.ATTR_NAME);
        return doc;
    }
}
