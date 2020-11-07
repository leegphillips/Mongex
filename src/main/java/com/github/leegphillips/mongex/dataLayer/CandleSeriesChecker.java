package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.atomic.AtomicInteger;

public class CandleSeriesChecker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CandleSeriesChecker.class);

    private final FindIterable<Document> pairSeries;
    private final TimeFrame timeframe;
    private final AtomicInteger counter;

    public CandleSeriesChecker(FindIterable<Document> pairSeries, TimeFrame timeframe, AtomicInteger counter) {
        this.pairSeries = pairSeries;
        this.timeframe = timeframe;
        this.counter = counter;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        MongoDatabase db = DatabaseFactory.create();
        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        FindIterable<Document> pairSeries = candles.find(new Document(CurrencyPair.ATTR_NAME, CurrencyPair.get(args[0]).getLabel()));
        new CandleSeriesChecker(pairSeries, TimeFrame.get(args[1]), new AtomicInteger()).run();
        LOG.info("Stream scanned in " + (System.currentTimeMillis() - start) + "ms");
    }

    @Override
    public void run() {
        Candle prev = null;
        for (Document doc : pairSeries) {
            Candle current = Candle.create(doc);
            if (prev != null && !current.getTimestamp().isEqual(timeframe.next(prev.getTimestamp()))) {
                MDC.put("prev", prev.toString());
                MDC.put("current", current.toString());
                LOG.error("Illegal gap");
                break;
            }
            prev = current;
        }
        counter.decrementAndGet();
    }
}
