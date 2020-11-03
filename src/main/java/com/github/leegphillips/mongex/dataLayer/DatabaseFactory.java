package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.MongoClient;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Properties;

public class DatabaseFactory {
    private final static String DB_NAME = "Mongex";

    private static MongoDatabase database;

    public static MongoDatabase create() {
        if (database == null) {
            Properties properties = PropertiesSingleton.getInstance();
            MongoClient mongoClient = new MongoClient(
                    properties.getProperty("dataloader.mongo.host"),
                    Integer.parseInt(properties.getProperty("dataloader.mongo.port")));
            database = mongoClient.getDatabase(DB_NAME);
        }
        return database;
    }

    public static MongoCollection<Document> getStream(CurrencyPair pair, TimeFrame tf) {
        if (database == null)
            create();

        MongoCollection<Document> stream = database.getCollection(pair.getLabel() + " " + tf.getLabel());
        stream.createIndex(new Document(Candle.TIMESTAMP_ATTR_NAME, 1));

        return stream;
    }
}
