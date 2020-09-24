package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.mongodb.client.model.Projections.exclude;
import static org.github.leegphillips.mongex.SortingFactory.EARLIEST;
import static org.github.leegphillips.mongex.SortingFactory.LATEST;

public class MarketAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(MarketAggregator.class);

    private static final String COLLECTION_NAME = "MARKET 1D";

    private final MongoDatabase db;

    public MarketAggregator(MongoDatabase db) {
        this.db = db;
    }

    public static void main(String[] args) {
        new MarketAggregator(DatabaseFactory.create(PropertiesSingleton.getInstance())).execute();
    }

    private void execute() {
        long start = System.currentTimeMillis();
        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        MongoCollection<Document> market = db.getCollection(COLLECTION_NAME);
        market.createIndex(new Document("date", -1));

        LocalDate earliest = LocalDate.parse(candles.find().sort(EARLIEST).limit(1).cursor().next().getString("date"));
        LocalDate latest = LocalDate.parse(candles.find().sort(LATEST).limit(1).cursor().next().getString("date")).plusDays(1);

        for (LocalDate current = earliest; current.isBefore(latest); current = current.plusDays(1)) {
            List<Document> allCandlesThatDay = new ArrayList<>();
            String date = current.toString();
            candles.find(new Document("date", date))
                    .projection(exclude("_id", "date", "timestamp"))
                    .spliterator().forEachRemaining(allCandlesThatDay::add);
            allCandlesThatDay.sort(Comparator.comparing(o -> o.getString("pair")));
            Document entry = new Document("date", date);
            entry.append("market", allCandlesThatDay);
            int marketSize = allCandlesThatDay.size();
            entry.append("market size", marketSize);
            if (marketSize > 0) {
                market.insertOne(entry);
                LOG.info("Date: " + date + " Market size: " + marketSize);
            }
        }
        LOG.info("Completed in " + (System.currentTimeMillis() - start) + "ms");
    }
}
