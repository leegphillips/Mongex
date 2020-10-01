package com.github.leegphillips.mongex;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.Properties;

public class DatabaseFactory {
    private final static String DB_NAME = "Mongex";

    public static MongoDatabase create(Properties properties) {
        MongoClient mongoClient = new MongoClient(
                properties.getProperty("dataloader.mongo.host"),
                Integer.parseInt(properties.getProperty("dataloader.mongo.port")));
        MongoDatabase database = mongoClient.getDatabase(DB_NAME);
//        database.createCollection("log");
        return database;
    }
}
