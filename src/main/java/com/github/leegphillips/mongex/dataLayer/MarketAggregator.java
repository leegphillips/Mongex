package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPairDAO;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

public class MarketAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(MarketAggregator.class);

    public static final String COLLECTION_NAME = "MARKET";

    private final MongoDatabase db;
    private final TimeFrame timeFrame;

    public MarketAggregator(MongoDatabase db, TimeFrame timeFrame) {
        this.db = db;
        this.timeFrame = timeFrame;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        new MarketAggregator(DatabaseFactory.create(), TimeFrame.FIVE_MINUTES).execute();
        LOG.info("Completed in " + (System.currentTimeMillis() - start) / 1000 + "s");
    }

    private void execute() {
        MongoCollection<Document> marketSeries = db.getCollection(COLLECTION_NAME);
        marketSeries.createIndex(new Document(Candle.TIMESTAMP_ATTR_NAME, 1));
        marketSeries.createIndex(new Document(TimeFrame.ATTR_NAME, 1));
        marketSeries.createIndex(new Document(Market.MARKET_COUNT_ATTR_NAME, 1));

        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        FindIterable<Document> candleSeries = candles.find(new Document(TimeFrame.ATTR_NAME, timeFrame.getLabel())).sort(new Document(Candle.TIMESTAMP_ATTR_NAME, 1));

        List<CurrencyPair> pairs = new CurrencyPairDAO().getAll();

        Market market = new Market(pairs);
        LocalDateTime current = null;
        for (Document doc : candleSeries) {
            Candle candle = Candle.create(doc);
            if (current == null) {
                current = candle.getTimestamp();
            } else {
                LocalDateTime timestamp = candle.getTimestamp();
                if (!timestamp.isEqual(current)) {
                    marketSeries.insertOne(market.toDocument());
                    market = new Market(pairs);
                    current = candle.getTimestamp();
                }
            }
            market.add(candle);
        }
        marketSeries.insertOne(market.toDocument());
    }
}
