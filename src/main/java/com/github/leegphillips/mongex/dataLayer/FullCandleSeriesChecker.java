package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPairDAO;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class FullCandleSeriesChecker {
    private static final Logger LOG = LoggerFactory.getLogger(FullCandleSeriesChecker.class);

    private static final TimeFrame TIME_FRAME = TimeFrame.FIVE_MINUTES;

    private final Properties properties;
    private final MongoDatabase db;

    private final AtomicInteger counter = new AtomicInteger();

    public FullCandleSeriesChecker(Properties properties, MongoDatabase db) {
        this.properties = properties;
        this.db = db;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create();
        new FullCandleSeriesChecker(properties, db).execute();
        LOG.info("All streams scanned in " + (System.currentTimeMillis() - start) + "ms");
    }

    private void execute() {
        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        List<CurrencyPair> pairs = new CurrencyPairDAO().getAll();
        counter.set(pairs.size());

        Stream<FindIterable<Document>> pairsSeries = pairs.parallelStream()
                .map(pair -> candles.find(new Document(CurrencyPair.ATTR_NAME, pair.getLabel())
                        .append(TimeFrame.ATTR_NAME, TIME_FRAME.getLabel())));
        pairsSeries
                .map(pairSeries -> new CandleSeriesChecker(pairSeries, TIME_FRAME, counter))
                .forEach(CandleSeriesChecker::run);
    }
}
