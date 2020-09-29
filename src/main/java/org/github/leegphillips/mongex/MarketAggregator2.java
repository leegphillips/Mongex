package org.github.leegphillips.mongex;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarketAggregator2 {
    private static final Logger LOG = LoggerFactory.getLogger(MarketAggregator2.class);

    private static final String COLLECTION_NAME = "MARKET";

    private final MongoDatabase db;
    private final TimeFrame timeFrame;

    public MarketAggregator2(MongoDatabase db, TimeFrame timeFrame) {
        this.db = db;
        this.timeFrame = timeFrame;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        new MarketAggregator2(DatabaseFactory.create(PropertiesSingleton.getInstance()), TimeFrame.FIVE_MINUTES).execute();
        LOG.info("Completed in " + (System.currentTimeMillis() - start) / 1000 + "s");
    }

    private void execute() {
        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        FindIterable<Document> sort = candles.find(new Document(TimeFrame.ATTR_NAME, TimeFrame.FIVE_MINUTES.getLabel())).sort(new Document(Timestamp.ATTR_NAME, 1));
        int i = 0;
        for (Document doc : sort) {
            Candle candle = Candle.create(doc);
            System.out.println(i + " " + candle);
            i++;
        }
    }
}
