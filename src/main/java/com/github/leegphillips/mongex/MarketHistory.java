package com.github.leegphillips.mongex;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

import static com.mongodb.client.model.Projections.exclude;

public class MarketHistory {
    private static final Logger LOG = LoggerFactory.getLogger(MarketHistory.class);

    private static final String COLLECTION_NAME = "MARKET HISTORY";

    // all fibonacci uses too much space
    //private static final int[] VIEWS = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233};
    private static final int[] VIEWS = {1, 8, 144, 2584};

    private final ArrayDeque<Document> window = new ArrayDeque<>();
    private final MongoDatabase db;
    private final TimeFrame timeFrame;

    public MarketHistory(MongoDatabase db, TimeFrame timeFrame) {
        this.db = db;
        this.timeFrame = timeFrame;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        new MarketHistory(DatabaseFactory.create(PropertiesSingleton.getInstance()), TimeFrame.FIVE_MINUTES).execute();
        LOG.info("Completed in " + (System.currentTimeMillis() - start) / 1000 + "s");
    }

    // TODO space issues so change to only start when all are present
    private void execute() {
        // add indexing
        MongoCollection<Document> histories = db.getCollection(COLLECTION_NAME);
        MongoCollection<Document> marketSeries = db.getCollection(MarketAggregator.COLLECTION_NAME);
        FindIterable<Document> series = marketSeries.find().projection(exclude("_id", Market.MARKET_COUNT_ATTR_NAME, TimeFrame.ATTR_NAME));
        int count = 0;
        for (Document doc : series) {
            count++;
            window.add(doc);
            if (window.size() > VIEWS[VIEWS.length - 1]) {
                window.removeLast();
                Document[] markets = window.toArray(new Document[]{});
                Document history = new Document();
                history.append(TimeFrame.ATTR_NAME, timeFrame.getLabel());
                for (int view : VIEWS) {
                    history.append(Integer.valueOf(view).toString(), markets[view - 1]);
                }
                histories.insertOne(history);
                if (count % 1000 == 0) {
                    //LOG.info(window.getFirst().get());
                    LOG.info("Processed: " + count);
                }
            }
        }
    }
}
