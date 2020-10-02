package com.github.leegphillips.mongex;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleCandleSeriesChecker {
    private static final Logger LOG = LoggerFactory.getLogger(FullCandleSeriesChecker.class);

    private static final TimeFrame TIME_FRAME = TimeFrame.FIVE_MINUTES;

    private final Properties properties;
    private final MongoDatabase db;

    private final AtomicInteger counter = new AtomicInteger();

    public SingleCandleSeriesChecker(Properties properties, MongoDatabase db) {
        this.properties = properties;
        this.db = db;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        new SingleCandleSeriesChecker(properties, db).execute(args[0]);
        LOG.info("Stream scanned in " + (System.currentTimeMillis() - start) + "ms");
    }

    private void execute(String pair) {
        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        FindIterable<Document> pairSeries = candles.find(new Document(CurrencyPair.ATTR_NAME, pair));
        new CandleSeriesChecker(pairSeries, TIME_FRAME, counter).run();
    }

}
