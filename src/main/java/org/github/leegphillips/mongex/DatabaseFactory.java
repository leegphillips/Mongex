package org.github.leegphillips.mongex;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.Properties;

public class DatabaseFactory {
    private final static String DB_NAME = "Mongex";

    public static MongoDatabase create(Properties properties) {
        MongoClient mongoClient = new MongoClient(properties.getProperty("dataloader.mongo.host")
                , Integer.valueOf(properties.getProperty("dataloader.mongo.port")));
        MongoDatabase db = mongoClient.getDatabase(DB_NAME);
        return db;
    }
}
