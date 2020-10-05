package com.github.leegphillips.mongex.ma;

import com.github.leegphillips.mongex.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class MALoader {
    private static final Logger LOG = LoggerFactory.getLogger(MALoader.class);

    private static final String COLLECTION_NAME = "MA's";

    private static final int[] SPARSE_FIBS = {2, 8, 34, 144, 610, 2584};

    private final MongoCollection<Document> candles;
    private final MongoCollection<Document> movingAverages;
    private final TimeFrame timeFrame;

    private final List<SimpleMovingAverage> sMAs = stream(SPARSE_FIBS).mapToObj(SimpleMovingAverage::new).collect(toList());

    private MALoader(MongoDatabase db, TimeFrame timeFrame) {
        this.timeFrame = timeFrame;
        this.candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        this.movingAverages = db.getCollection(COLLECTION_NAME);
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        LOG.info("Starting");
        new MALoader(DatabaseFactory.create(PropertiesSingleton.getInstance()), TimeFrame.FIVE_MINUTES).execute();
        LOG.info("Completed in " + (System.currentTimeMillis() - start) / 1000 + "s");
    }

    private void execute() {
        List<CurrencyPair> pairs = new ArrayList<>();
        candles.distinct(CurrencyPair.ATTR_NAME, String.class)
                .map(CurrencyPair::new)
                .iterator()
                .forEachRemaining(pairs::add);

        pairs.parallelStream()
                .map(SingleMALoader::new)
                .forEach(SingleMALoader::run);
    }

    private class SingleMALoader implements Runnable {
        private final CurrencyPair pair;


        private SingleMALoader(CurrencyPair pair) {
            this.pair = pair;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            String currencyPair = pair.getLabel();
            LOG.info("Starting: " + currencyPair);

            candles.find(new Document(CurrencyPair.ATTR_NAME, currencyPair).append(TimeFrame.ATTR_NAME, timeFrame.getLabel()))
                    .map(Candle::create)
                    .iterator()
                    .forEachRemaining(this::processCandle);

            LOG.info("Finished: " + currencyPair + " in " + (System.currentTimeMillis() - start) + "ms");
        }

        private void processCandle(Candle candle) {
            sMAs.forEach(ma -> ma.add(candle.getMid()));

            Document doc = new Document();
            doc.append(TimeFrame.ATTR_NAME, timeFrame.getLabel());
            doc.append(Candle.TIMESTAMP_ATTR_NAME, candle.getTimestamp());
            sMAs.forEach(ma -> doc.append(ma.getName(), ma.getValue()));

            movingAverages.insertOne(doc);
        }
    }
}
