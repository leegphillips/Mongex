package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MongoLoader {
    private final static Logger LOG = LoggerFactory.getLogger(MongoLoader.class);

    private final static ExecutorService SERVICE = Executors.newSingleThreadExecutor();

    public static void main(String[] args) throws InterruptedException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create();
        new MongoLoader(properties, db, TimeFrame.THIRTY_SECONDS);
    }

    public MongoLoader(Properties properties, MongoDatabase db, TimeFrame tf) throws InterruptedException {
        MongoCollection<Document> collection = db.getCollection("MAIN STREAM " + tf.getLabel());
        collection.createIndex(new Document(Candle.TIMESTAMP_ATTR_NAME, 1));
        BlockingQueue<AggregateState.FlatState> input = new AggregateState(new TimeFrameMarketStateIterable(tf));
        AggregateState.FlatState state = input.take();
        List<Document> inserts = new ArrayList<>();
        while (state != AggregateState.FlatState.POISON) {
            inserts.add(state.toDocument());

            if (inserts.size() % 5000 == 0) {
                SERVICE.execute(new Inserter(collection, inserts));
                inserts = new ArrayList<>();
            }

            state = input.take();
        }
        SERVICE.execute(new Inserter(collection, inserts));
    }

    private class Inserter implements Runnable {

        private final MongoCollection<Document> collection;
        private final List<Document> inserts;

        private Inserter(MongoCollection<Document> collection, List<Document> inserts) {
            this.collection = collection;
            this.inserts = inserts;
        }

        @Override
        public void run() {
            LOG.info(inserts.get(inserts.size() -1 ).get(Candle.TIMESTAMP_ATTR_NAME).toString());
            collection.insertMany(inserts);
        }
    }
}
