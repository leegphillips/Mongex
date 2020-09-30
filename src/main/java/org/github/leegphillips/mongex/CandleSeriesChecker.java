package org.github.leegphillips.mongex;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class CandleSeriesChecker {
    private static final Logger LOG = LoggerFactory.getLogger(CandleSeriesChecker.class);

    private static final TimeFrame TIME_FRAME = TimeFrame.FIVE_MINUTES;

    private final Properties properties;
    private final MongoDatabase db;

    private final AtomicInteger counter = new AtomicInteger();

    public CandleSeriesChecker(Properties properties, MongoDatabase db) {
        this.properties = properties;
        this.db = db;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        new CandleSeriesChecker(properties, db).execute();
        LOG.info("All streams scanned in " + (System.currentTimeMillis() - start) + "ms");
    }

    private void execute() {
        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        List<CurrencyPair> pairs = new ArrayList<>();
        candles.distinct(CurrencyPair.ATTR_NAME, String.class)
                .map(CurrencyPair::new)
                .iterator()
                .forEachRemaining(pairs::add);

        pairs.stream()
                .forEach(currencyPair -> System.out.println(currencyPair.getLabel()));

        counter.set(pairs.size());

        Stream<FindIterable<Document>> pairsSeries = pairs.parallelStream()
                .map(pair -> candles.find(new Document(CurrencyPair.ATTR_NAME, pair.getLabel())
                        .append(TimeFrame.ATTR_NAME, TIME_FRAME.getLabel()))
                        .sort(new Document(Candle.TIMESTAMP_ATTR_NAME, 1)));

        pairsSeries
                .map(pairSeries -> new Checker(pairSeries))
                .forEach(Checker::run);
    }

    private class Checker implements Runnable {
        private final FindIterable<Document> pairSeries;

        private Checker(FindIterable<Document> pairSeries) {
            this.pairSeries = pairSeries;
        }

        @Override
        public void run() {
            Candle prev = null;
            for (Document doc : pairSeries) {
                Candle current = Candle.create(doc);
//                if (prev != null && !current.getTimestamp().isEqual(TIME_FRAME.next(prev.getTimestamp())))
//                    throw new IllegalStateException("Illegal gap: " + candle.getPair().getLabel() + " " + prev.getTimestamp() + " " + current.getTimestamp());
                prev = current;
                LOG.info(current.getPair().getLabel() + " " + current.getTimestamp() + " " + counter.get());
            }
            counter.decrementAndGet();
        }
    }
}
