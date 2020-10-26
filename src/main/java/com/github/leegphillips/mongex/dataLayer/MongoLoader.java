package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Properties;

public class MongoLoader {
    public static void main(String[] args) {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        new MongoLoader(properties, db, TimeFrame.TEN_SECONDS);
    }

    public MongoLoader(Properties properties, MongoDatabase db, TimeFrame tf) {
        MongoCollection<Document> collection = db.getCollection("MAIN STREAM " + tf.getLabel());
        collection.createIndex(new Document(Candle.TIMESTAMP_ATTR_NAME, 1));
        new AggregateState(tf).iterator().forEachRemaining(state -> collection.insertOne(state.toDocument()));
    }
}
